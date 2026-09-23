using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Globalization;
using System.IO.Compression;
using System.IO.Pipelines;
using System.Runtime.InteropServices;
using System.Text;
using Microsoft.Extensions.Options;
using Mk8.Sava.Configuration;

namespace Mk8.Sava.Storage;

public sealed class ChunkStore
{
    private const ulong Magic = 0x324B4E5548433853UL; // S8CHUNK2
    private const byte EncryptionVersion = 1;
    private const int NonceLength = 12;
    private const int TagLength = 16;
    private const int AuthenticatedHeaderLength = sizeof(ulong) + sizeof(byte) + sizeof(byte) + sizeof(int) + 32 + NonceLength;
    private const int HeaderLength = AuthenticatedHeaderLength + TagLength;
    private const ulong PackRecordMagic = 0x314B4341503853UL; // S8PACK1
    private const ulong PackRecordFooterMagic = 0x31444E454B503853UL; // S8PKEND1
    private const int PackRecordHeaderLength = sizeof(ulong) + sizeof(ushort) + sizeof(int) + 32;
    private const int PackRecordFooterLength = sizeof(ulong);

    private readonly StoragePaths _paths;
    private readonly MetadataStore _metadata;
    private readonly IStorageFaultInjector _faultInjector;
    private readonly SavaOptions _options;
    private readonly ContentDefinedChunker _chunker;
    private readonly Func<byte[], byte[]> _chunkDigest;
    private readonly Lock _pinGate = new();
    private readonly Dictionary<string, int> _pins = new(StringComparer.Ordinal);
    private readonly Dictionary<string, ChunkMutationReservation> _mutationReservations = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _packGates = new(StringComparer.Ordinal);
    private string? _orphanPackCursor;
    private StoragePhysicalInventoryScanner? _physicalInventoryScanner;

    internal bool IsPhysicalUsageScanInProgress => _physicalInventoryScanner is not null;
    internal int PhysicalUsageScanStepsLastPass { get; private set; }

    public ChunkStore(
        StoragePaths paths,
        MetadataStore metadata,
        IStorageFaultInjector faultInjector,
        IOptions<SavaOptions> options)
    {
        ArgumentNullException.ThrowIfNull(options);
        _paths = paths;
        _metadata = metadata;
        _faultInjector = faultInjector;
        _options = options.Value;
        _chunkDigest = SHA256.HashData;
        _chunker = new ContentDefinedChunker(
            _options.MinimumChunkBytes,
            _options.TargetChunkBytes,
            _options.MaximumChunkBytes);
    }

    internal ChunkStore(
        StoragePaths paths,
        MetadataStore metadata,
        IStorageFaultInjector faultInjector,
        IOptions<SavaOptions> options,
        Func<byte[], byte[]> chunkDigest)
        : this(paths, metadata, faultInjector, options)
    {
        _chunkDigest = chunkDigest ?? throw new ArgumentNullException(nameof(chunkDigest));
    }

    public async Task<StoredContent> StorePinnedAsync(
        string account,
        BlobEncryption encryption,
        Stream source,
        CancellationToken cancellationToken) =>
        await StorePinnedCoreAsync(
            account,
            encryption,
            source,
            _options.MaximumRequestBodyBytes,
            cancellationToken).ConfigureAwait(false);

    public async Task<StoredContent> CopyToDomainPinnedAsync(
        string destinationAccount,
        BlobEncryption sourceEncryption,
        BlobEncryption destinationEncryption,
        ContentManifest source,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(source);
        ValidateManifest(source);
        if (IsInDomain(destinationAccount, destinationEncryption, source))
            return new StoredContent(source, Pin(source));

        using var transferCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var pipe = new Pipe(new PipeOptions(
            pauseWriterThreshold: checked(_options.MaximumChunkBytes * 2L),
            resumeWriterThreshold: _options.MaximumChunkBytes,
            useSynchronizationContext: false));
        var producer = ProduceDomainCopyAsync(pipe, source, sourceEncryption, transferCancellation.Token);
        StoredContent? copied = null;
        try
        {
            var input = pipe.Reader.AsStream(leaveOpen: true);
            await using var inputDisposal = input.ConfigureAwait(false);
            copied = await StorePinnedCoreAsync(
                destinationAccount,
                destinationEncryption,
                input,
                long.MaxValue,
                cancellationToken).ConfigureAwait(false);
            await producer.ConfigureAwait(false);
            return copied;
        }
        catch
        {
            copied?.Dispose();
            await transferCancellation.CancelAsync().ConfigureAwait(false);
            try
            {
                await producer.ConfigureAwait(false);
            }
#pragma warning disable CA1031 // Preserve the original storage failure after observing producer cancellation.
            catch
            {
                // Preserve the transfer/storage exception that caused cancellation.
            }
#pragma warning restore CA1031
            throw;
        }
        finally
        {
            await pipe.Reader.CompleteAsync().ConfigureAwait(false);
        }
    }

    private async Task ProduceDomainCopyAsync(
        Pipe pipe,
        ContentManifest source,
        BlobEncryption sourceEncryption,
        CancellationToken cancellationToken)
    {
        Exception? failure = null;
        try
        {
            var output = pipe.Writer.AsStream(leaveOpen: true);
            await using var outputDisposal = output.ConfigureAwait(false);
            await WriteRangeAsync(
                source,
                sourceEncryption,
                0,
                source.Length,
                output,
                cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            failure = exception;
            throw;
        }
        finally
        {
            await pipe.Writer.CompleteAsync(failure).ConfigureAwait(false);
        }
    }

    private async Task<StoredContent> StorePinnedCoreAsync(
        string account,
        BlobEncryption encryption,
        Stream source,
        long maximumBytes,
        CancellationToken cancellationToken)
    {
        var domain = ResolveDomain(account, encryption);
        using var completeHash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        using var completeMd5 = IncrementalHash.CreateHash(HashAlgorithmName.MD5);
        var references = new List<ChunkReference>();
        var pinnedIds = new HashSet<string>(StringComparer.Ordinal);
        long offset = 0;

        try
        {
            await foreach (var bytes in _chunker.ReadChunksAsync(source, maximumBytes, cancellationToken).ConfigureAwait(false))
            {
                completeHash.AppendData(bytes);
                completeMd5.AppendData(bytes);
                if (bytes.AsSpan().IndexOfAnyExcept((byte)0) < 0)
                {
                    var zeroId = ZeroId(domain);
                    if (references.Count > 0 && string.Equals(references[^1].Id, zeroId, StringComparison.Ordinal))
                        references[^1] = references[^1] with { Length = checked(references[^1].Length + bytes.Length) };
                    else
                        references.Add(new ChunkReference(zeroId, offset, bytes.Length));
                }
                else
                {
                    var id = await StoreVerifiedChunkAsync(domain, bytes, encryption.CustomerProvidedKey, cancellationToken).ConfigureAwait(false);
                    if (!pinnedIds.Add(id))
                        UnpinId(id);
                    references.Add(new ChunkReference(id, offset, bytes.Length));
                }
                offset += bytes.Length;
            }

            var manifest = new ContentManifest(
                domain,
                offset,
                Convert.ToHexStringLower(completeHash.GetHashAndReset()),
                references);
            ValidateManifest(manifest);
            return new StoredContent(
                manifest,
                new PinLease(this, pinnedIds),
                Convert.ToBase64String(completeMd5.GetHashAndReset()));
        }
        catch
        {
            foreach (var id in pinnedIds)
                UnpinId(id);
            throw;
        }
    }

    public IDisposable Pin(ContentManifest manifest)
    {
        ArgumentNullException.ThrowIfNull(manifest);
        ValidateManifest(manifest);
        var ids = manifest.Chunks
            .Where(chunk => !IsZero(chunk, manifest.Domain))
            .Select(chunk => chunk.Id)
            .Distinct(StringComparer.Ordinal)
            .ToArray();
        PinIds(ids);
        return new PinLease(this, ids);
    }

    internal IDisposable PinChunkIds(IReadOnlySet<string> chunkIds)
    {
        var ids = chunkIds
            .Where(id => !id.EndsWith("/$zero", StringComparison.Ordinal))
            .Distinct(StringComparer.Ordinal)
            .ToArray();
        PinIds(ids);
        return new PinLease(this, ids);
    }

    internal async Task CopyChunkFileForBackupAsync(
        string id,
        string destination,
        CancellationToken cancellationToken)
    {
        var bytes = await ReadStoredChunkFileBytesAsync(id, cancellationToken).ConfigureAwait(false);
        var output = new FileStream(
            destination,
            FileMode.CreateNew,
            FileAccess.Write,
            FileShare.None,
            128 * 1024,
            FileOptions.Asynchronous);
        await using var outputDisposal = output.ConfigureAwait(false);
        await output.WriteAsync(bytes, cancellationToken).ConfigureAwait(false);
        await output.FlushAsync(cancellationToken).ConfigureAwait(false);
        StorageDurability.FlushFileToDisk(output);
    }

    public ContentManifest Empty(string account, BlobEncryption encryption) =>
        ContentManifest.Empty(ResolveDomain(account, encryption));

    public ContentManifest Sparse(string account, BlobEncryption encryption, long length)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(length);
        var domain = ResolveDomain(account, encryption);
        return length == 0
            ? ContentManifest.Empty(domain)
            : new ContentManifest(
                domain,
                length,
                ContentManifest.SparseHash,
                [new ChunkReference(ZeroId(domain), 0, length)]);
    }

    public bool IsInDomain(string account, BlobEncryption encryption, ContentManifest manifest)
    {
        ArgumentNullException.ThrowIfNull(manifest);
        return string.Equals(ResolveDomain(account, encryption), manifest.Domain, StringComparison.Ordinal);
    }

    public async Task<ContentManifest> ComposeAsync(
        string account,
        BlobEncryption encryption,
        IReadOnlyList<ContentManifest> manifests,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(manifests);
        var domain = ResolveDomain(account, encryption);
        if (manifests.Any(manifest => !string.Equals(manifest.Domain, domain, StringComparison.Ordinal)))
            throw new InvalidOperationException("Content from different encryption domains must be copied through verified plaintext.");

        if (manifests.Any(manifest => string.Equals(manifest.Sha256, ContentManifest.SparseHash, StringComparison.Ordinal)))
        {
            var sparseReferences = new List<ChunkReference>();
            foreach (var manifest in manifests)
            {
                cancellationToken.ThrowIfCancellationRequested();
                ValidateManifest(manifest);
                foreach (var chunk in manifest.Chunks)
                    AddReference(sparseReferences, chunk.Id, chunk.Length, domain);
            }
            return CreateSparseManifest(domain, sparseReferences);
        }

        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        var references = new List<ChunkReference>();
        long offset = 0;
        foreach (var manifest in manifests)
        {
            ValidateManifest(manifest);
            foreach (var chunk in manifest.Chunks)
            {
                if (IsZero(chunk, domain))
                {
                    AppendZeroesToHash(hash, chunk.Length);
                    if (references.Count > 0 && string.Equals(references[^1].Id, chunk.Id, StringComparison.Ordinal))
                        references[^1] = references[^1] with { Length = checked(references[^1].Length + chunk.Length) };
                    else
                        references.Add(new ChunkReference(chunk.Id, offset, chunk.Length));
                    offset += chunk.Length;
                    continue;
                }
                var bytes = await ReadVerifiedChunkAsync(chunk.Id, domain, encryption.CustomerProvidedKey, cancellationToken).ConfigureAwait(false);
                if (bytes.LongLength != chunk.Length)
                    throw new InvalidDataException($"Chunk '{chunk.Id}' has an unexpected decoded length.");
                hash.AppendData(bytes);
                references.Add(new ChunkReference(chunk.Id, offset, chunk.Length));
                offset += chunk.Length;
            }
        }

        return new ContentManifest(domain, offset, Convert.ToHexStringLower(hash.GetHashAndReset()), references);
    }

    public async Task<StoredContent> ReplaceRangePinnedAsync(
        string account,
        BlobEncryption encryption,
        ContentManifest current,
        long start,
        long length,
        Stream? replacement,
        bool clear,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(current);
        ValidateManifest(current);
        if (!IsInDomain(account, encryption, current) || start < 0 || length < 0 || start > current.Length || length > current.Length - start)
            throw new ArgumentOutOfRangeException(nameof(start));
        if (!clear && replacement is null)
            throw new ArgumentNullException(nameof(replacement));

        var references = new List<ChunkReference>();
        var temporaryPins = new List<IDisposable>();
        try
        {
            await AppendSliceAsync(account, encryption, current, 0, start, references, temporaryPins, cancellationToken).ConfigureAwait(false);
            if (clear)
            {
                AddReference(references, ZeroId(current.Domain), length, current.Domain);
            }
            else
            {
                var stored = await StorePinnedAsync(account, encryption, replacement!, cancellationToken).ConfigureAwait(false);
                temporaryPins.Add(stored);
                if (stored.Manifest.Length != length)
                    throw new EndOfStreamException("The replacement stream length did not match the requested range.");
                foreach (var chunk in stored.Manifest.Chunks)
                    AddReference(references, chunk.Id, chunk.Length, current.Domain);
            }
            await AppendSliceAsync(
                account,
                encryption,
                current,
                start + length,
                current.Length - start - length,
                references,
                temporaryPins,
                cancellationToken).ConfigureAwait(false);
            return PinSparseManifest(current.Domain, references, temporaryPins);
        }
        catch
        {
            foreach (ref var pin in CollectionsMarshal.AsSpan(temporaryPins))
                pin.Dispose();
            throw;
        }
    }

    public async Task<StoredContent> ResizeSparsePinnedAsync(
        string account,
        BlobEncryption encryption,
        ContentManifest current,
        long length,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(current);
        ValidateManifest(current);
        if (!IsInDomain(account, encryption, current) || length < 0)
            throw new ArgumentOutOfRangeException(nameof(length));
        if (length == current.Length)
            return new StoredContent(current, Pin(current));

        var references = new List<ChunkReference>();
        var temporaryPins = new List<IDisposable>();
        try
        {
            var retained = Math.Min(length, current.Length);
            await AppendSliceAsync(account, encryption, current, 0, retained, references, temporaryPins, cancellationToken).ConfigureAwait(false);
            if (length > current.Length)
                AddReference(references, ZeroId(current.Domain), length - current.Length, current.Domain);
            return PinSparseManifest(current.Domain, references, temporaryPins);
        }
        catch
        {
            foreach (ref var pin in CollectionsMarshal.AsSpan(temporaryPins))
                pin.Dispose();
            throw;
        }
    }

    public async Task WriteRangeAsync(
        ContentManifest manifest,
        BlobEncryption encryption,
        long offset,
        long length,
        Stream destination,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(manifest);
        ArgumentNullException.ThrowIfNull(destination);
        ValidateManifest(manifest);
        if (encryption.CustomerProvidedKeySha256 is not null &&
            !string.Equals(manifest.Domain, ResolveDomain(manifest.Domain.Split('/', 2)[0], encryption), StringComparison.Ordinal))
        {
            throw new InvalidDataException("The supplied customer-provided key does not match the content encryption domain.");
        }
        if (offset < 0 || length < 0 || offset > manifest.Length || length > manifest.Length - offset)
            throw new ArgumentOutOfRangeException(nameof(offset));
        if (length == 0)
            return;

        using var pin = Pin(manifest);
        IncrementalHash? completeHash = offset == 0 &&
                                        length == manifest.Length &&
                                        !string.Equals(manifest.Sha256, ContentManifest.SparseHash, StringComparison.Ordinal)
            ? IncrementalHash.CreateHash(HashAlgorithmName.SHA256)
            : null;
        try
        {
            var rangeEnd = offset + length;
            foreach (var chunk in manifest.Chunks)
            {
                var chunkEnd = chunk.Offset + chunk.Length;
                if (chunkEnd <= offset)
                    continue;
                if (chunk.Offset >= rangeEnd)
                    break;

                var startInChunk = Math.Max(0, offset - chunk.Offset);
                var endInChunk = Math.Min(chunk.Length, rangeEnd - chunk.Offset);
                var sliceLength = endInChunk - startInChunk;
                if (IsZero(chunk, manifest.Domain))
                {
                    await WriteZeroesAsync(destination, sliceLength, completeHash, cancellationToken).ConfigureAwait(false);
                    continue;
                }

                var bytes = await ReadVerifiedChunkAsync(chunk.Id, manifest.Domain, encryption.CustomerProvidedKey, cancellationToken).ConfigureAwait(false);
                if (bytes.LongLength != chunk.Length)
                    throw new InvalidDataException($"Chunk '{chunk.Id}' has an unexpected decoded length.");
                var slice = bytes.AsMemory(checked((int)startInChunk), checked((int)sliceLength));
                completeHash?.AppendData(slice.Span);
                await destination.WriteAsync(slice, cancellationToken).ConfigureAwait(false);
            }

            if (completeHash is not null)
            {
                var actual = Convert.ToHexStringLower(completeHash.GetHashAndReset());
                if (!string.Equals(actual, manifest.Sha256, StringComparison.Ordinal))
                    throw new InvalidDataException("The content manifest failed its complete-object integrity check.");
            }
        }
        finally
        {
            completeHash?.Dispose();
        }
    }

    public async Task<byte[]> ReadAllAsync(
        ContentManifest manifest,
        BlobEncryption encryption,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(manifest);
        if (manifest.Length > int.MaxValue)
            throw new InvalidOperationException("The content is too large to materialize in memory.");
        using var buffer = new MemoryStream((int)manifest.Length);
        await WriteRangeAsync(manifest, encryption, 0, manifest.Length, buffer, cancellationToken).ConfigureAwait(false);
        return buffer.ToArray();
    }

    public async Task<string> MaterializeAsync(
        ContentManifest manifest,
        BlobEncryption encryption,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(manifest);
        var path = Path.Combine(_paths.Staging, $"materialized-{Guid.NewGuid():N}.tmp");
        var output = new FileStream(path, FileMode.CreateNew, FileAccess.Write, FileShare.None, 128 * 1024, FileOptions.Asynchronous);
        await using var outputDisposal = output.ConfigureAwait(false);
        await WriteRangeAsync(manifest, encryption, 0, manifest.Length, output, cancellationToken).ConfigureAwait(false);
        await output.FlushAsync(cancellationToken).ConfigureAwait(false);
        StorageDurability.FlushFileToDisk(output);
        return path;
    }

    internal async Task<PhysicalChunkPage> EnumerateChunkIdsPageAsync(
        string? after,
        int maximum,
        CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maximum);
        var standalone = EnumerateStorageIdsOrdered(_paths.Chunks, relativeDirectory: string.Empty, after, ".chunk")
            .Take(checked(maximum + 1))
            .ToList();
        var packed = await _metadata.ListPackedChunkIdsAsync(after, maximum, cancellationToken).ConfigureAwait(false);
        var items = standalone
            .Concat(packed.Items)
            .Distinct(StringComparer.Ordinal)
            .Order(StringComparer.Ordinal)
            .Take(checked(maximum + 1))
            .ToList();
        var hasMore = items.Count > maximum || standalone.Count > maximum || packed.HasMore;
        if (hasMore)
        {
            while (items.Count > maximum)
                items.RemoveAt(items.Count - 1);
        }
        return new PhysicalChunkPage(items, hasMore);
    }

    internal async IAsyncEnumerable<string> EnumeratePhysicalChunkIdsForStartupAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        foreach (var id in EnumerateStorageIdsOrdered(
                     _paths.Chunks,
                     relativeDirectory: string.Empty,
                     after: null,
                     ".chunk"))
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return id;
        }

        string? after = null;
        while (true)
        {
            var page = await _metadata.ListPackedChunkIdsAsync(after, 512, cancellationToken).ConfigureAwait(false);
            foreach (var id in page.Items)
                yield return id;
            if (!page.HasMore)
                break;
            if (page.Items.Count == 0)
                throw new InvalidDataException("The packed-chunk inventory failed to advance.");
            after = page.Items[^1];
        }
    }

    internal async Task<PackCompactionResult> TryCompactPackAsync(
        ChunkPackRecord pack,
        CancellationToken cancellationToken)
    {
        if (!pack.Sealed)
            return PackCompactionResult.Skipped;
        var gate = _packGates.GetOrAdd(pack.Domain, _ => new SemaphoreSlim(1, 1));
        await gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            return await CompactSealedPackAsync(pack, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            gate.Release();
        }
    }

    private async Task<PackCompactionResult> CompactSealedPackAsync(
        ChunkPackRecord pack,
        CancellationToken cancellationToken)
    {
        var oldPath = GetPackPath(pack.PackId);
        var locations = await _metadata.ListPackedChunkLocationsAsync(pack.PackId, cancellationToken).ConfigureAwait(false);
        long oldLength;
        try
        {
            oldLength = new FileInfo(oldPath).Length;
        }
        catch (FileNotFoundException)
        {
            if (locations.Count == 0)
            {
                await _metadata.ReplaceChunkPackAsync(pack, null, locations, [], cancellationToken).ConfigureAwait(false);
                return new PackCompactionResult(1, 1, 0, 0);
            }
            throw new InvalidDataException($"Chunk pack '{pack.PackId}' is missing.");
        }

        var liveBytes = locations.Sum(location => (long)location.RecordLength);
        if (liveBytes < 0 || liveBytes > oldLength)
            throw new InvalidDataException($"Chunk pack '{pack.PackId}' has inconsistent live locations.");
        var deadBytes = oldLength - liveBytes;
        if (locations.Count > 0 &&
            (deadBytes < _options.ChunkPackCompactionMinimumSavingsBytes ||
             (double)deadBytes / oldLength < _options.ChunkPackCompactionMinimumDeadRatio))
        {
            return new PackCompactionResult(1, 0, 0, 0);
        }

        return await CompactPackLocationsAsync(pack, oldPath, oldLength, locations, cancellationToken).ConfigureAwait(false);
    }

    private async Task<PackCompactionResult> CompactPackLocationsAsync(
        ChunkPackRecord pack,
        string oldPath,
        long oldLength,
        IReadOnlyList<PackedChunkLocation> locations,
        CancellationToken cancellationToken)
    {
        var reservations = new List<ChunkMutationReservation>(locations.Count);
        try
        {
            foreach (var location in locations)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var reservation = TryReserveForMutation(location.ChunkId);
                if (reservation is null)
                    return new PackCompactionResult(1, 0, 0, 0);
                reservations.Add(reservation);
            }

            if (locations.Count == 0)
            {
                await _metadata.ReplaceChunkPackAsync(pack, null, locations, [], cancellationToken).ConfigureAwait(false);
                File.Delete(oldPath);
                return new PackCompactionResult(1, 1, 0, oldLength);
            }

            return await RewritePackAsync(pack, oldPath, oldLength, locations, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            foreach (ref var reservation in CollectionsMarshal.AsSpan(reservations))
                reservation.Dispose();
        }
    }

    private async Task<PackCompactionResult> RewritePackAsync(
        ChunkPackRecord pack,
        string oldPath,
        long oldLength,
        IReadOnlyList<PackedChunkLocation> locations,
        CancellationToken cancellationToken)
    {
        var replacement = new ChunkPackRecord(
            CreatePackId(pack.Domain),
            pack.Domain,
            _metadata.GetUtcNow(),
            Sealed: true);
        var replacementPath = GetPackPath(replacement.PackId);
        var temporaryPath = Path.Combine(_paths.Staging, $"pack-compact-{Guid.NewGuid():N}.tmp");
        var replacements = new List<PackedChunkLocation>(locations.Count);
        try
        {
            await CopyLivePackRecordsAsync(replacement, oldPath, temporaryPath, locations, replacements, cancellationToken)
                .ConfigureAwait(false);
            _paths.EnsureDurableDirectory(Path.GetDirectoryName(replacementPath)!);
            StorageDurability.PublishFile(temporaryPath, replacementPath, overwrite: false);
            // A failure reported after the SQLite commit is ambiguous.
            // Keep the published pack; orphan recovery can remove it if
            // the transaction did not commit.
            await _metadata.ReplaceChunkPackAsync(
                pack,
                replacement,
                locations,
                replacements,
                cancellationToken).ConfigureAwait(false);
            File.Delete(oldPath);
            var newLength = new FileInfo(replacementPath).Length;
            return new PackCompactionResult(1, 1, 0, oldLength - newLength);
        }
        finally
        {
            if (File.Exists(temporaryPath))
                File.Delete(temporaryPath);
        }
    }

    private async Task CopyLivePackRecordsAsync(
        ChunkPackRecord replacement,
        string oldPath,
        string temporaryPath,
        IReadOnlyList<PackedChunkLocation> locations,
        List<PackedChunkLocation> replacements,
        CancellationToken cancellationToken)
    {
        var source = new FileStream(
            oldPath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            128 * 1024,
            FileOptions.Asynchronous | FileOptions.RandomAccess);
        await using (source.ConfigureAwait(false))
        {
            var destination = new FileStream(
                temporaryPath,
                FileMode.CreateNew,
                FileAccess.Write,
                FileShare.None,
                128 * 1024,
                FileOptions.Asynchronous);
            await using (destination.ConfigureAwait(false))
            {
                foreach (var location in locations)
                {
                    _ = await ReadPackedChunkPayloadAsync(location, cancellationToken).ConfigureAwait(false);
                    var newOffset = destination.Position;
                    source.Position = location.RecordOffset;
                    await CopyExactlyAsync(source, destination, location.RecordLength, cancellationToken).ConfigureAwait(false);
                    replacements.Add(location with
                    {
                        PackId = replacement.PackId,
                        RecordOffset = newOffset,
                        PayloadOffset = checked(newOffset + location.PayloadOffset - location.RecordOffset)
                    });
                }
                await destination.FlushAsync(cancellationToken).ConfigureAwait(false);
                StorageDurability.FlushFileToDisk(destination);
            }
        }
    }

    internal async Task<(int ReclaimedPacks, long BytesSaved)> ReclaimOrphanedPacksAsync(
        DateTimeOffset olderThan,
        int maximumPacks,
        CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maximumPacks);

        var page = EnumerateStorageIdsOrdered(_paths.Packs, string.Empty, _orphanPackCursor, ".pack")
            .Take(checked(maximumPacks + 1))
            .ToList();
        var hasMore = page.Count > maximumPacks;
        if (hasMore)
            page.RemoveAt(page.Count - 1);

        var reclaimed = 0;
        long bytesSaved = 0;
#pragma warning disable HLQ012 // A Span enumerator cannot live across awaited pack cleanup.
        foreach (var packId in page)
#pragma warning restore HLQ012
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (await TryReclaimOrphanPackAsync(packId, olderThan, cancellationToken).ConfigureAwait(false) is { } length)
            {
                reclaimed++;
                bytesSaved = checked(bytesSaved + length);
            }
        }

        _orphanPackCursor = hasMore && page.Count > 0 ? page[^1] : null;
        return (reclaimed, bytesSaved);
    }

    private async Task<long?> TryReclaimOrphanPackAsync(
        string packId,
        DateTimeOffset olderThan,
        CancellationToken cancellationToken)
    {
        var separator = packId.LastIndexOf('/');
        if (separator <= 0)
            return null;
        var domain = packId[..separator];
        var gate = _packGates.GetOrAdd(domain, _ => new SemaphoreSlim(1, 1));
        await gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var path = GetPackPath(packId);
            FileInfo file;
            try
            {
                file = new FileInfo(path);
                if (!file.Exists || file.LastWriteTimeUtc > olderThan.UtcDateTime)
                    return null;
            }
            catch (IOException)
            {
                return null;
            }

            if (_metadata.ChunkPackExists(packId))
                return null;

            var length = file.Length;
            try
            {
                File.Delete(path);
                return length;
            }
            catch (FileNotFoundException)
            {
                // Another recovery pass removed the unregistered pack first.
            }
            catch (IOException)
            {
                // A writer still owns the file; reconsider it on the next cycle.
            }
            catch (UnauthorizedAccessException)
            {
                // Surface the bytes through physical usage and retry later.
            }
            return null;
        }
        finally
        {
            gate.Release();
        }
    }

    private static IEnumerable<string> EnumerateStorageIdsOrdered(
        string root,
        string relativeDirectory,
        string? after,
        string extension)
    {
        var directory = relativeDirectory.Length == 0
            ? root
            : Path.Combine(root, relativeDirectory.Replace('/', Path.DirectorySeparatorChar));
        string[] entries;
        try
        {
            entries = Directory.EnumerateFileSystemEntries(directory)
                .Order(StringComparer.Ordinal)
                .ToArray();
        }
        catch (DirectoryNotFoundException)
        {
            yield break;
        }

        foreach (var path in entries)
        {
            FileAttributes attributes;
            try
            {
                attributes = File.GetAttributes(path);
            }
            catch (FileNotFoundException)
            {
                continue;
            }
            catch (DirectoryNotFoundException)
            {
                continue;
            }
            if ((attributes & FileAttributes.ReparsePoint) != FileAttributes.None)
                continue;

            var name = Path.GetFileName(path);
            var relative = relativeDirectory.Length == 0 ? name : relativeDirectory + "/" + name;
            if ((attributes & FileAttributes.Directory) != FileAttributes.None)
            {
                var prefix = relative + "/";
                if (after is null ||
                    after.StartsWith(prefix, StringComparison.Ordinal) ||
                    string.CompareOrdinal(prefix, after) > 0)
                {
                    foreach (var nestedId in EnumerateStorageIdsOrdered(root, relative, after, extension))
                        yield return nestedId;
                }
                continue;
            }
            if (!name.EndsWith(extension, StringComparison.Ordinal))
                continue;
            var id = relative[..^extension.Length];
            if (after is null || string.CompareOrdinal(id, after) > 0)
                yield return id;
        }
    }

    public StoragePhysicalUsage MeasurePhysicalUsage()
    {
        var storedFileEnumeration = new EnumerationOptions
        {
            RecurseSubdirectories = true,
            AttributesToSkip = FileAttributes.ReparsePoint
        };
        long chunkBytes = 0;
        var chunkCount = 0;
        foreach (var path in Directory.EnumerateFiles(_paths.Chunks, "*.chunk", storedFileEnumeration))
        {
            try
            {
                chunkBytes = checked(chunkBytes + new FileInfo(path).Length);
                chunkCount++;
            }
            catch (FileNotFoundException)
            {
                // A concurrent reclamation removed the file after enumeration.
            }
        }
        foreach (var path in Directory.EnumerateFiles(_paths.Packs, "*.pack", storedFileEnumeration))
        {
            try
            {
                chunkBytes = checked(chunkBytes + new FileInfo(path).Length);
            }
            catch (FileNotFoundException)
            {
                // A concurrent compaction removed the pack after enumeration.
            }
        }
        chunkCount = checked(chunkCount + _metadata.CountPackedChunks());

        long stagingBytes = 0;
        foreach (var path in Directory.EnumerateFiles(_paths.Staging, "*", SearchOption.TopDirectoryOnly))
        {
            try
            {
                stagingBytes = checked(stagingBytes + new FileInfo(path).Length);
            }
            catch (FileNotFoundException)
            {
                // A request completed and removed its staging file after enumeration.
            }
        }

        var metadataBytes = MeasureMetadataBytes();

        return new StoragePhysicalUsage(
            chunkBytes,
            stagingBytes,
            metadataBytes,
            chunkCount,
            StorageAllocationMeter.MeasureRoot(_paths.Root));
    }

    private long MeasureMetadataBytes()
    {
        long metadataBytes = 0;
        foreach (var path in new[] { _paths.Database, _paths.Database + "-wal", _paths.Database + "-shm" })
        {
            try
            {
                metadataBytes = checked(metadataBytes + new FileInfo(path).Length);
            }
            catch (FileNotFoundException)
            {
            }
        }

        return metadataBytes;
    }

    internal StoragePhysicalUsage? ScanPhysicalUsageBatch(int maximumEntries)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maximumEntries);

        var scanner = _physicalInventoryScanner ??= new StoragePhysicalInventoryScanner(_paths);
        try
        {
            var complete = scanner.Advance(maximumEntries);
            PhysicalUsageScanStepsLastPass = scanner.LastPassSteps;
            if (!complete)
                return null;

            var result = scanner.ToPhysicalUsage(_metadata.CountPackedChunks());
            scanner.Dispose();
            _physicalInventoryScanner = null;
            return result;
        }
        catch
        {
            scanner.Dispose();
            _physicalInventoryScanner = null;
            throw;
        }
    }

    public int DeleteAbandonedStagingFiles(DateTimeOffset olderThan, int maximumFiles)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maximumFiles);

        var deleted = 0;
        foreach (var path in Directory.EnumerateFiles(_paths.Staging, "*.tmp", SearchOption.TopDirectoryOnly))
        {
            if (deleted >= maximumFiles)
                break;
            try
            {
                if ((File.GetAttributes(path) & FileAttributes.ReparsePoint) != FileAttributes.None ||
                    File.GetLastWriteTimeUtc(path) > olderThan.UtcDateTime)
                {
                    continue;
                }

                using var abandoned = new FileStream(
                    path,
                    FileMode.Open,
                    FileAccess.ReadWrite,
                    FileShare.None,
                    bufferSize: 1,
                    FileOptions.DeleteOnClose);
                deleted++;
            }
            catch (FileNotFoundException)
            {
                // The owning request completed after enumeration.
            }
            catch (IOException)
            {
                // An active request still owns the file, or another pass won the race.
            }
            catch (UnauthorizedAccessException)
            {
                // Leave a file that cannot be opened safely and report it through staging bytes.
            }
        }
        return deleted;
    }

    public async Task<ChunkIntegrityStatus> VerifyChunkAsync(string id, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(id);
        if (id.EndsWith("/$zero", StringComparison.Ordinal))
            return ChunkIntegrityStatus.Verified;

        string domain;
        try
        {
            domain = GetDomainFromChunkId(id);
            if (!await TryPinIdAsync(id, cancellationToken).ConfigureAwait(false))
                return ChunkIntegrityStatus.Missing;
        }
        catch (FileNotFoundException)
        {
            return ChunkIntegrityStatus.Missing;
        }
        catch (Exception exception) when (exception is InvalidDataException or IOException or UnauthorizedAccessException)
        {
            return ChunkIntegrityStatus.Corrupt;
        }

        try
        {
            if (domain.Contains("/$cpk-", StringComparison.Ordinal))
            {
                await VerifyCustomerKeyChunkStructureAsync(id, cancellationToken).ConfigureAwait(false);
                return ChunkIntegrityStatus.RequiresCustomerKey;
            }

            _ = await ReadVerifiedChunkAsync(id, domain, customerProvidedKey: null, cancellationToken).ConfigureAwait(false);
            return ChunkIntegrityStatus.Verified;
        }
        catch (FileNotFoundException)
        {
            return ChunkIntegrityStatus.Missing;
        }
        catch (Exception exception) when (exception is InvalidDataException or IOException or UnauthorizedAccessException)
        {
            return ChunkIntegrityStatus.Corrupt;
        }
        finally
        {
            UnpinId(id);
        }
    }

    public async Task<ChunkRecompressionResult> TryRecompressChunkAsync(
        string id,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(id);
        if (id.EndsWith("/$zero", StringComparison.Ordinal))
            return ChunkRecompressionResult.Skipped;

        var domain = GetDomainFromChunkId(id);
        if (domain.Contains("/$cpk-", StringComparison.Ordinal))
            return ChunkRecompressionResult.Skipped;

        var path = GetChunkPath(id);
        var eligibleBefore = now.Subtract(_options.BackgroundCompressionMinimumAge).UtcDateTime;
        if (!IsRecompressionCandidate(path, eligibleBefore))
            return ChunkRecompressionResult.Skipped;

        using var reservation = TryReserveForMutation(id);
        if (reservation is null || !IsRecompressionCandidate(path, eligibleBefore))
            return ChunkRecompressionResult.Skipped;

        return await RecompressReservedChunkAsync(id, domain, path, now, cancellationToken).ConfigureAwait(false);
    }

    private static bool IsRecompressionCandidate(string path, DateTime eligibleBefore)
    {
        if (!File.Exists(path))
            return false;
        try
        {
            return File.GetLastWriteTimeUtc(path) <= eligibleBefore;
        }
        catch (FileNotFoundException)
        {
            return false;
        }
    }

    private async Task<ChunkRecompressionResult> RecompressReservedChunkAsync(
        string id,
        string domain,
        string path,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        byte[] plaintext;
        try
        {
            plaintext = await ReadVerifiedChunkAsync(id, domain, customerProvidedKey: null, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is InvalidDataException or FileNotFoundException or UnauthorizedAccessException)
        {
            return ChunkRecompressionResult.Examined;
        }

        var temporaryPath = Path.Combine(_paths.Staging, $"recompress-{Guid.NewGuid():N}.tmp");
        try
        {
            var originalLength = new FileInfo(path).Length;
            await WriteChunkFileAsync(
                temporaryPath,
                domain,
                plaintext,
                customerProvidedKey: null,
                _options.BackgroundCompressionQuality,
                _options.CompressionMinimumSavingsBytes,
                cancellationToken).ConfigureAwait(false);
            var verified = await ReadVerifiedChunkFileAsync(
                temporaryPath,
                id,
                domain,
                customerProvidedKey: null,
                cancellationToken).ConfigureAwait(false);
            try
            {
                if (!CryptographicOperations.FixedTimeEquals(verified, plaintext))
                    throw new InvalidDataException($"Recompressed chunk '{id}' changed its plaintext content.");
            }
            finally
            {
                CryptographicOperations.ZeroMemory(verified);
            }

            var optimizedLength = new FileInfo(temporaryPath).Length;
            var bytesSaved = originalLength - optimizedLength;
            if (bytesSaved <= 0 || bytesSaved < _options.BackgroundCompressionMinimumSavingsBytes)
            {
                File.SetLastWriteTimeUtc(path, now.UtcDateTime);
                return ChunkRecompressionResult.Examined;
            }

            StorageDurability.PublishFile(temporaryPath, path, overwrite: true);
            return new ChunkRecompressionResult(1, 1, bytesSaved);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(plaintext);
            if (File.Exists(temporaryPath))
                File.Delete(temporaryPath);
        }
    }

    internal ChunkMutationReservation? TryReserveForMutation(string id)
    {
        lock (_pinGate)
        {
            var path = GetChunkPath(id);
            if (_pins.ContainsKey(id) ||
                _mutationReservations.ContainsKey(id) ||
                !File.Exists(path) && !_metadata.PackedChunkExists(id))
            {
                return null;
            }

            var reservation = new ChunkMutationReservation(this, id);
            _mutationReservations.Add(id, reservation);
            return reservation;
        }
    }

    private async Task AppendSliceAsync(
        string account,
        BlobEncryption encryption,
        ContentManifest manifest,
        long start,
        long length,
        List<ChunkReference> destination,
        List<IDisposable> temporaryPins,
        CancellationToken cancellationToken)
    {
        if (length == 0)
            return;
        var end = checked(start + length);
        foreach (var chunk in manifest.Chunks)
        {
            var chunkEnd = checked(chunk.Offset + chunk.Length);
            if (chunkEnd <= start)
                continue;
            if (chunk.Offset >= end)
                break;
            var overlapStart = Math.Max(start, chunk.Offset);
            var overlapEnd = Math.Min(end, chunkEnd);
            var overlapLength = overlapEnd - overlapStart;
            if (IsZero(chunk, manifest.Domain))
            {
                AddReference(destination, ZeroId(manifest.Domain), overlapLength, manifest.Domain);
                continue;
            }
            if (overlapStart == chunk.Offset && overlapLength == chunk.Length)
            {
                AddReference(destination, chunk.Id, chunk.Length, manifest.Domain);
                continue;
            }

            var bytes = await ReadVerifiedChunkAsync(chunk.Id, manifest.Domain, encryption.CustomerProvidedKey, cancellationToken).ConfigureAwait(false);
            if (bytes.LongLength != chunk.Length)
                throw new InvalidDataException($"Chunk '{chunk.Id}' has an unexpected decoded length.");
            var offset = checked((int)(overlapStart - chunk.Offset));
            var count = checked((int)overlapLength);
            using var slice = new MemoryStream(bytes, offset, count, writable: false);
            var stored = await StorePinnedAsync(account, encryption, slice, cancellationToken).ConfigureAwait(false);
            temporaryPins.Add(stored);
            foreach (var storedChunk in stored.Manifest.Chunks)
                AddReference(destination, storedChunk.Id, storedChunk.Length, manifest.Domain);
        }
    }

    private StoredContent PinSparseManifest(
        string domain,
        List<ChunkReference> references,
        List<IDisposable> temporaryPins)
    {
        try
        {
            var manifest = CreateSparseManifest(domain, references);
            return new StoredContent(manifest, Pin(manifest));
        }
        finally
        {
            foreach (ref var pin in CollectionsMarshal.AsSpan(temporaryPins))
                pin.Dispose();
            temporaryPins.Clear();
        }
    }

    private static ContentManifest CreateSparseManifest(string domain, List<ChunkReference> references)
    {
        var normalized = new List<ChunkReference>(references.Count);
        long offset = 0;
        foreach (ref var reference in CollectionsMarshal.AsSpan(references))
        {
            normalized.Add(reference with { Offset = offset });
            offset = checked(offset + reference.Length);
        }
        return offset == 0
            ? ContentManifest.Empty(domain)
            : new ContentManifest(domain, offset, ContentManifest.SparseHash, normalized);
    }

    private static void AddReference(
        List<ChunkReference> references,
        string id,
        long length,
        string domain)
    {
        if (length == 0)
            return;
        if (references.Count > 0 &&
            string.Equals(id, ZeroId(domain), StringComparison.Ordinal) &&
            string.Equals(references[^1].Id, id, StringComparison.Ordinal))
        {
            references[^1] = references[^1] with { Length = checked(references[^1].Length + length) };
            return;
        }
        references.Add(new ChunkReference(id, 0, length));
    }

    private static async Task WriteZeroesAsync(
        Stream destination,
        long length,
        IncrementalHash? hash,
        CancellationToken cancellationToken)
    {
        var buffer = new byte[128 * 1024];
        while (length > 0)
        {
            var count = (int)Math.Min(buffer.Length, length);
            hash?.AppendData(buffer.AsSpan(0, count));
            await destination.WriteAsync(buffer.AsMemory(0, count), cancellationToken).ConfigureAwait(false);
            length -= count;
        }
    }

    private static async Task CopyExactlyAsync(
        Stream source,
        Stream destination,
        long length,
        CancellationToken cancellationToken)
    {
        var buffer = new byte[128 * 1024];
        while (length > 0)
        {
            var count = (int)Math.Min(buffer.Length, length);
            await source.ReadExactlyAsync(buffer.AsMemory(0, count), cancellationToken).ConfigureAwait(false);
            await destination.WriteAsync(buffer.AsMemory(0, count), cancellationToken).ConfigureAwait(false);
            length -= count;
        }
    }

    private static void AppendZeroesToHash(IncrementalHash hash, long length)
    {
        var buffer = new byte[128 * 1024];
        while (length > 0)
        {
            var count = (int)Math.Min(buffer.Length, length);
            hash.AppendData(buffer.AsSpan(0, count));
            length -= count;
        }
    }

    private async Task<string> StoreVerifiedChunkAsync(
        string domain,
        byte[] bytes,
        byte[]? customerProvidedKey,
        CancellationToken cancellationToken)
    {
        var hash = Convert.ToHexStringLower(ComputeChunkDigest(bytes));
        var baseName = $"{hash}-{bytes.Length}";

        for (var collision = 0; ; collision++)
        {
            var name = collision == 0 ? baseName : $"{baseName}-{collision}";
            var id = $"{domain}/{hash[..2]}/{hash[2..4]}/{name}";
            var finalPath = GetChunkPath(id);
            while (true)
            {
                if (await TryPinIdAsync(id, cancellationToken).ConfigureAwait(false))
                {
                    if (await IsExactPinnedChunkAsync(id, domain, bytes, customerProvidedKey, cancellationToken).ConfigureAwait(false))
                        return id;
                    break;
                }

                var temporaryPath = Path.Combine(_paths.Staging, $"chunk-{Guid.NewGuid():N}.tmp");
                try
                {
                    await WriteChunkFileAsync(
                        temporaryPath,
                        domain,
                        bytes,
                        customerProvidedKey,
                        _options.CompressionQuality,
                        _options.CompressionMinimumSavingsBytes,
                        cancellationToken).ConfigureAwait(false);
                    _faultInjector.Inject(StorageFaultPoint.BeforeChunkPublication);
                    if (_options.EnableSmallChunkPacking &&
                        bytes.Length <= _options.SmallChunkPackingThresholdBytes)
                    {
                        var published = await TryStorePackedChunkFileAsync(
                            id,
                            domain,
                            temporaryPath,
                            cancellationToken).ConfigureAwait(false);
                        if (published && await TryPinIdAsync(id, cancellationToken).ConfigureAwait(false))
                        {
                            if (!await IsExactPinnedChunkAsync(
                                    id, domain, bytes, customerProvidedKey, cancellationToken).ConfigureAwait(false))
                                throw new InvalidDataException($"Packed chunk '{id}' changed while it was published.");
                            return id;
                        }
                        continue;
                    }

                    if (await TryPublishStandaloneChunkAsync(
                            id, finalPath, temporaryPath, cancellationToken).ConfigureAwait(false))
                        return id;
                }
                finally
                {
                    if (File.Exists(temporaryPath))
                        File.Delete(temporaryPath);
                }
            }
        }
    }

    private async Task<bool> TryPublishStandaloneChunkAsync(
        string id,
        string finalPath,
        string temporaryPath,
        CancellationToken cancellationToken)
    {
        Task? reservationCompletion = null;
        var created = false;
        lock (_pinGate)
        {
            if (_mutationReservations.TryGetValue(id, out var reservation))
            {
                reservationCompletion = reservation.Completion;
            }
            else if (!File.Exists(finalPath))
            {
                _paths.PublishStandaloneChunk(temporaryPath, finalPath);
                _pins[id] = _pins.GetValueOrDefault(id) + 1;
                created = true;
            }
        }

        if (reservationCompletion is not null)
            await reservationCompletion.WaitAsync(cancellationToken).ConfigureAwait(false);
        return created;
    }

    private async Task<bool> IsExactPinnedChunkAsync(
        string id,
        string domain,
        byte[] bytes,
        byte[]? customerProvidedKey,
        CancellationToken cancellationToken)
    {
        var keepPin = false;
        try
        {
            var existing = await ReadVerifiedChunkAsync(id, domain, customerProvidedKey, cancellationToken).ConfigureAwait(false);
            keepPin = CryptographicOperations.FixedTimeEquals(existing, bytes);
            return keepPin;
        }
        finally
        {
            if (!keepPin)
                UnpinId(id);
        }
    }

    private async Task<bool> TryStorePackedChunkFileAsync(
        string id,
        string domain,
        string chunkFilePath,
        CancellationToken cancellationToken)
    {
        var gate = _packGates.GetOrAdd(domain, _ => new SemaphoreSlim(1, 1));
        await gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            // Another writer may have published the same verified bytes while this
            // writer was encoding its temporary file. Skip an orphan pack record.
            if (File.Exists(GetChunkPath(id)) || _metadata.PackedChunkExists(id))
                return false;

            var payloadLength = checked((int)new FileInfo(chunkFilePath).Length);
            var idLength = Encoding.UTF8.GetByteCount(id);
            var recordLength = checked(PackRecordHeaderLength + idLength + payloadLength + PackRecordFooterLength);
            var (pack, committedLength) = await GetWritablePackAsync(domain, recordLength, cancellationToken).ConfigureAwait(false);
            pack ??= new ChunkPackRecord(
                CreatePackId(domain),
                domain,
                _metadata.GetUtcNow(),
                Sealed: false);
            var location = await AppendPackRecordAsync(pack, id, chunkFilePath, committedLength, cancellationToken).ConfigureAwait(false);
            var inserted = await _metadata.TryRegisterPackedChunkAsync(pack, location, cancellationToken).ConfigureAwait(false);
            if (inserted &&
                (new FileInfo(GetPackPath(pack.PackId)).Length >= _options.ChunkPackTargetBytes ||
                 await _metadata.CountPackedChunksAsync(pack.PackId, cancellationToken).ConfigureAwait(false) >=
                 _options.ChunkPackMaximumRecords))
            {
                await _metadata.SealChunkPackAsync(pack.PackId, cancellationToken).ConfigureAwait(false);
            }
            return inserted;
        }
        finally
        {
            gate.Release();
        }
    }

    private async Task<(ChunkPackRecord? Pack, long CommittedLength)> GetWritablePackAsync(
        string domain,
        int recordLength,
        CancellationToken cancellationToken)
    {
        var pack = await _metadata.GetActiveChunkPackAsync(domain, cancellationToken).ConfigureAwait(false);
        if (pack is null)
            return (null, 0);

        var path = GetPackPath(pack.PackId);
        var committedLength = await _metadata.GetPackIndexedLengthAsync(pack.PackId, cancellationToken).ConfigureAwait(false);
        if (!File.Exists(path))
        {
            if (committedLength != 0)
                throw new InvalidDataException($"Active chunk pack '{pack.PackId}' is missing.");
            await _metadata.SealChunkPackAsync(pack.PackId, cancellationToken).ConfigureAwait(false);
            return (null, 0);
        }

        if (new FileInfo(path).Length < committedLength)
            throw new InvalidDataException($"Active chunk pack '{pack.PackId}' is shorter than its indexed records.");
        var records = await _metadata.CountPackedChunksAsync(pack.PackId, cancellationToken).ConfigureAwait(false);
        if (records >= _options.ChunkPackMaximumRecords ||
            committedLength + recordLength > _options.ChunkPackTargetBytes)
        {
            await _metadata.SealChunkPackAsync(pack.PackId, cancellationToken).ConfigureAwait(false);
            return (null, 0);
        }
        return (pack, committedLength);
    }

    private async Task<PackedChunkLocation> AppendPackRecordAsync(
        ChunkPackRecord pack,
        string id,
        string chunkFilePath,
        long committedLength,
        CancellationToken cancellationToken)
    {
        var idBytes = Encoding.UTF8.GetBytes(id);
        if (idBytes.Length == 0 || idBytes.Length > ushort.MaxValue)
            throw new InvalidDataException("A chunk identifier is too long to pack.");
        var payload = await File.ReadAllBytesAsync(chunkFilePath, cancellationToken).ConfigureAwait(false);
        var header = new byte[PackRecordHeaderLength];
        BinaryPrimitives.WriteUInt64LittleEndian(header, PackRecordMagic);
        BinaryPrimitives.WriteUInt16LittleEndian(header.AsSpan(sizeof(ulong)), checked((ushort)idBytes.Length));
        BinaryPrimitives.WriteInt32LittleEndian(
            header.AsSpan(sizeof(ulong) + sizeof(ushort)),
            payload.Length);
        SHA256.HashData(payload, header.AsSpan(sizeof(ulong) + sizeof(ushort) + sizeof(int), 32));
        var footer = new byte[PackRecordFooterLength];
        BinaryPrimitives.WriteUInt64LittleEndian(footer, PackRecordFooterMagic);

        var path = GetPackPath(pack.PackId);
        _paths.EnsureDurableDirectory(Path.GetDirectoryName(path)!);
        var output = new FileStream(
            path,
            FileMode.OpenOrCreate,
            FileAccess.ReadWrite,
            FileShare.Read,
            128 * 1024,
            FileOptions.Asynchronous);
        await using var outputDisposal = output.ConfigureAwait(false);
        if (output.Length < committedLength)
            throw new InvalidDataException($"Chunk pack '{pack.PackId}' is shorter than its indexed records.");
        if (output.Length > committedLength)
        {
            // A previous append may have failed or crashed before its location was
            // committed. Never append behind bytes that have no authoritative index.
            output.SetLength(committedLength);
            StorageDurability.FlushFileToDisk(output);
        }
        output.Position = committedLength;
        var recordOffset = committedLength;
        await output.WriteAsync(header, cancellationToken).ConfigureAwait(false);
        await output.WriteAsync(idBytes, cancellationToken).ConfigureAwait(false);
        var payloadOffset = output.Position;
        var firstHalf = Math.Max(1, payload.Length / 2);
        await output.WriteAsync(payload.AsMemory(0, firstHalf), cancellationToken).ConfigureAwait(false);
        _faultInjector.Inject(StorageFaultPoint.DuringPackRecordAppend);
        await output.WriteAsync(payload.AsMemory(firstHalf), cancellationToken).ConfigureAwait(false);
        await output.WriteAsync(footer, cancellationToken).ConfigureAwait(false);
        await output.FlushAsync(cancellationToken).ConfigureAwait(false);
        StorageDurability.FlushFileToDisk(output);
        if (recordOffset == 0)
            StorageDurability.FlushDirectory(Path.GetDirectoryName(path)!);
        return new PackedChunkLocation(
            id,
            pack.PackId,
            recordOffset,
            checked((int)(output.Position - recordOffset)),
            payloadOffset,
            payload.Length);
    }

    private async Task WriteChunkFileAsync(
        string path,
        string domain,
        byte[] bytes,
        byte[]? customerProvidedKey,
        int compressionQuality,
        int compressionMinimumSavingsBytes,
        CancellationToken cancellationToken)
    {
        byte codec = 0;
        byte[] encoded = bytes;
        using (var compressed = new MemoryStream())
        {
            var brotli = new BrotliStream(compressed, new BrotliCompressionOptions
            {
                Quality = compressionQuality
            }, leaveOpen: true);
            await using (brotli.ConfigureAwait(false))
            {
                await brotli.WriteAsync(bytes, cancellationToken).ConfigureAwait(false);
            }
            if (compressed.Length + compressionMinimumSavingsBytes < bytes.Length)
            {
                codec = 1;
                encoded = compressed.ToArray();
            }
        }

        var header = new byte[HeaderLength];
        BinaryPrimitives.WriteUInt64LittleEndian(header, Magic);
        header[sizeof(ulong)] = codec;
        header[sizeof(ulong) + sizeof(byte)] = EncryptionVersion;
        BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(sizeof(ulong) + 2 * sizeof(byte)), bytes.Length);
        ComputeChunkDigest(bytes).CopyTo(header, sizeof(ulong) + 2 * sizeof(byte) + sizeof(int));
        var nonceOffset = sizeof(ulong) + 2 * sizeof(byte) + sizeof(int) + 32;
        RandomNumberGenerator.Fill(header.AsSpan(nonceOffset, NonceLength));
        var ciphertext = new byte[encoded.Length];
        var encryptionKey = DeriveEncryptionKey(domain, customerProvidedKey);
        try
        {
            using var aes = new AesGcm(encryptionKey, TagLength);
            aes.Encrypt(
                header.AsSpan(nonceOffset, NonceLength),
                encoded,
                ciphertext,
                header.AsSpan(AuthenticatedHeaderLength, TagLength),
                header.AsSpan(0, AuthenticatedHeaderLength));
        }
        finally
        {
            CryptographicOperations.ZeroMemory(encryptionKey);
        }

        var output = new FileStream(path, FileMode.CreateNew, FileAccess.Write, FileShare.None, 128 * 1024, FileOptions.Asynchronous);
        await using var outputDisposal = output.ConfigureAwait(false);
        await output.WriteAsync(header, cancellationToken).ConfigureAwait(false);
        _faultInjector.Inject(StorageFaultPoint.DuringChunkStagingWrite);
        await output.WriteAsync(ciphertext, cancellationToken).ConfigureAwait(false);
        await output.FlushAsync(cancellationToken).ConfigureAwait(false);
        StorageDurability.FlushFileToDisk(output);
    }

    private async Task<byte[]> ReadVerifiedChunkAsync(
        string id,
        string domain,
        byte[]? customerProvidedKey,
        CancellationToken cancellationToken)
    {
        var stored = await ReadStoredChunkFileBytesAsync(id, cancellationToken).ConfigureAwait(false);
        using var input = new MemoryStream(stored, writable: false);
        return await ReadVerifiedChunkStreamAsync(input, id, domain, customerProvidedKey, cancellationToken).ConfigureAwait(false);
    }

    private async Task<byte[]> ReadVerifiedChunkFileAsync(
        string path,
        string id,
        string domain,
        byte[]? customerProvidedKey,
        CancellationToken cancellationToken)
    {
        var input = new FileStream(
            path,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            128 * 1024,
            FileOptions.Asynchronous | FileOptions.SequentialScan);
        await using var inputDisposal = input.ConfigureAwait(false);
        return await ReadVerifiedChunkStreamAsync(input, id, domain, customerProvidedKey, cancellationToken).ConfigureAwait(false);
    }

    private async Task<byte[]> ReadVerifiedChunkStreamAsync(
        Stream input,
        string id,
        string domain,
        byte[]? customerProvidedKey,
        CancellationToken cancellationToken)
    {
        if (!id.StartsWith(domain + "/", StringComparison.Ordinal))
            throw new InvalidDataException("A chunk reference escaped its encryption domain.");
        var header = new byte[HeaderLength];
        await input.ReadExactlyAsync(header, cancellationToken).ConfigureAwait(false);
        if (BinaryPrimitives.ReadUInt64LittleEndian(header) != Magic)
            throw new InvalidDataException($"Chunk '{id}' has an invalid format marker.");
        if (header[sizeof(ulong) + sizeof(byte)] != EncryptionVersion)
            throw new InvalidDataException($"Chunk '{id}' uses an unsupported encryption format.");

        var codec = header[sizeof(ulong)];
        var decodedLength = BinaryPrimitives.ReadInt32LittleEndian(header.AsSpan(sizeof(ulong) + 2 * sizeof(byte)));
        if (decodedLength < 0 || decodedLength > _options.MaximumChunkBytes)
            throw new InvalidDataException($"Chunk '{id}' has an invalid decoded length.");
        var ciphertextLength = checked((int)(input.Length - HeaderLength));
        if (ciphertextLength < 0 || ciphertextLength > _options.MaximumChunkBytes)
            throw new InvalidDataException($"Chunk '{id}' has an invalid encoded length.");
        var ciphertext = new byte[ciphertextLength];
        await input.ReadExactlyAsync(ciphertext, cancellationToken).ConfigureAwait(false);
        var encoded = DecryptChunk(header, ciphertext, id, domain, customerProvidedKey);

        byte[] decoded;
        switch (codec)
        {
            case 0:
                decoded = encoded;
                if (decoded.Length != decodedLength)
                    throw new InvalidDataException($"Raw chunk '{id}' has an unexpected length.");
                break;
            case 1:
                decoded = new byte[decodedLength];
                using (var encodedStream = new MemoryStream(encoded, writable: false))
                using (var brotli = new BrotliStream(encodedStream, CompressionMode.Decompress))
                {
                    await brotli.ReadExactlyAsync(decoded, cancellationToken).ConfigureAwait(false);
                    if (brotli.ReadByte() != -1)
                        throw new InvalidDataException($"Compressed chunk '{id}' expands beyond its recorded length.");
                }
                break;
            default:
                throw new InvalidDataException($"Chunk '{id}' uses an unsupported codec.");
        }

        var expectedHashOffset = sizeof(ulong) + 2 * sizeof(byte) + sizeof(int);
        var actualHash = ComputeChunkDigest(decoded);
        if (!CryptographicOperations.FixedTimeEquals(actualHash, header.AsSpan(expectedHashOffset, 32)))
            throw new InvalidDataException($"Chunk '{id}' failed its plaintext integrity check.");
        ValidateChunkIdentity(id, decoded.Length, actualHash);
        return decoded;
    }

    private byte[] DecryptChunk(
        byte[] header,
        byte[] ciphertext,
        string id,
        string domain,
        byte[]? customerProvidedKey)
    {
        var encoded = new byte[ciphertext.Length];
        var nonceOffset = sizeof(ulong) + 2 * sizeof(byte) + sizeof(int) + 32;
        var encryptionKey = DeriveEncryptionKey(domain, customerProvidedKey);
        try
        {
            try
            {
                using var aes = new AesGcm(encryptionKey, TagLength);
                aes.Decrypt(
                    header.AsSpan(nonceOffset, NonceLength),
                    ciphertext,
                    header.AsSpan(AuthenticatedHeaderLength, TagLength),
                    encoded,
                    header.AsSpan(0, AuthenticatedHeaderLength));
            }
            catch (CryptographicException exception)
            {
                throw new InvalidDataException($"Chunk '{id}' failed authenticated decryption.", exception);
            }
        }
        finally
        {
            CryptographicOperations.ZeroMemory(encryptionKey);
        }
        return encoded;
    }

    private byte[] ComputeChunkDigest(byte[] bytes)
    {
        var digest = _chunkDigest(bytes);
        if (digest.Length != SHA256.HashSizeInBytes)
            throw new InvalidOperationException("A chunk digest must contain exactly 256 bits.");
        return digest;
    }

    private async Task VerifyCustomerKeyChunkStructureAsync(string id, CancellationToken cancellationToken)
    {
        var stored = await ReadStoredChunkFileBytesAsync(id, cancellationToken).ConfigureAwait(false);
        using var input = new MemoryStream(stored, writable: false);
        var header = new byte[HeaderLength];
        await input.ReadExactlyAsync(header, cancellationToken).ConfigureAwait(false);
        if (BinaryPrimitives.ReadUInt64LittleEndian(header) != Magic)
            throw new InvalidDataException($"Chunk '{id}' has an invalid format marker.");
        if (header[sizeof(ulong) + sizeof(byte)] != EncryptionVersion)
            throw new InvalidDataException($"Chunk '{id}' uses an unsupported encryption format.");
        if (header[sizeof(ulong)] is not (0 or 1))
            throw new InvalidDataException($"Chunk '{id}' uses an unsupported codec.");

        var decodedLength = BinaryPrimitives.ReadInt32LittleEndian(header.AsSpan(sizeof(ulong) + 2 * sizeof(byte)));
        if (decodedLength < 0 || decodedLength > _options.MaximumChunkBytes)
            throw new InvalidDataException($"Chunk '{id}' has an invalid decoded length.");
        var ciphertextLength = input.Length - HeaderLength;
        if (ciphertextLength < 0 || ciphertextLength > _options.MaximumChunkBytes)
            throw new InvalidDataException($"Chunk '{id}' has an invalid encoded length.");

        var expectedHashOffset = sizeof(ulong) + 2 * sizeof(byte) + sizeof(int);
        ValidateChunkIdentity(id, decodedLength, header.AsSpan(expectedHashOffset, 32));
    }

    private static void ValidateChunkIdentity(string id, int decodedLength, ReadOnlySpan<byte> actualHash)
    {
        var nameOffset = id.LastIndexOf('/') + 1;
        var name = id[nameOffset..];
        if (name.Length < 66 || name[64] != '-')
            throw new InvalidDataException($"Chunk '{id}' has an invalid content identity.");
        var collisionOffset = name.IndexOf('-', 65);
        var lengthCharacterCount = collisionOffset >= 0 ? collisionOffset - 65 : name.Length - 65;
        if (!int.TryParse(
                name.AsSpan(65, lengthCharacterCount),
                NumberStyles.None,
                CultureInfo.InvariantCulture,
                out var recordedLength) ||
            recordedLength != decodedLength)
        {
            throw new InvalidDataException($"Chunk '{id}' has an invalid content identity.");
        }

        byte[] recordedHash;
        try
        {
            recordedHash = Convert.FromHexString(name[..64]);
        }
        catch (FormatException exception)
        {
            throw new InvalidDataException($"Chunk '{id}' has an invalid content identity.", exception);
        }
        if (!CryptographicOperations.FixedTimeEquals(recordedHash, actualHash))
        {
            throw new InvalidDataException($"Chunk '{id}' does not match its content identity.");
        }
    }

    internal static string GetDomainFromChunkId(string id)
    {
        var firstSeparator = id.IndexOf('/', StringComparison.Ordinal);
        if (firstSeparator <= 0)
            throw new InvalidDataException("A chunk identifier is invalid.");
        if (id[0] == '$')
            return id[..firstSeparator];

        var secondSeparator = id.IndexOf('/', firstSeparator + 1);
        if (secondSeparator > firstSeparator + 1 && id[firstSeparator + 1] == '$')
            return id[..secondSeparator];
        return id[..firstSeparator];
    }

    private string ResolveDomain(string account, BlobEncryption encryption)
    {
        if (encryption.CustomerProvidedKeySha256 is not null)
        {
            var hash = Convert.FromBase64String(encryption.CustomerProvidedKeySha256);
            return $"{account}/$cpk-{Convert.ToHexStringLower(hash)}";
        }
        if (encryption.Scope is not null)
        {
            var scopeHash = Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(encryption.Scope)));
            return $"{account}/$scope-{scopeHash}";
        }
        return _options.EnableCrossAccountDeduplication ? "$global" : account;
    }

    private byte[] DeriveEncryptionKey(string domain, byte[]? customerProvidedKey)
    {
        if (domain.Contains("/$cpk-", StringComparison.Ordinal))
        {
            if (customerProvidedKey is not { Length: 32 })
                throw new InvalidDataException("The customer-provided key is required to decrypt this content.");
            var expectedHash = domain[(domain.IndexOf("/$cpk-", StringComparison.Ordinal) + "/$cpk-".Length)..];
            var actualHash = Convert.ToHexStringLower(SHA256.HashData(customerProvidedKey));
            if (!string.Equals(expectedHash, actualHash, StringComparison.Ordinal))
                throw new InvalidDataException("The customer-provided key does not match this content.");
            using var customerHmac = new HMACSHA256(customerProvidedKey);
            return customerHmac.ComputeHash(Encoding.UTF8.GetBytes($"mk8.sava/customer-chunk-encryption/v1/{domain}"));
        }

        string encodedRoot;
        if (string.Equals(domain, "$global", StringComparison.Ordinal))
            encodedRoot = _options.CrossAccountEncryptionKey ?? throw new InvalidOperationException("The cross-account encryption key is not configured.");
        else
            encodedRoot = _options.ResolveAccountDataEncryptionKey(domain.Split('/', 2)[0]);
        using var hmac = new HMACSHA256(Convert.FromBase64String(encodedRoot));
        return hmac.ComputeHash(Encoding.UTF8.GetBytes($"mk8.sava/chunk-encryption/v1/{domain}"));
    }

    private async Task<byte[]> ReadStoredChunkFileBytesAsync(
        string id,
        CancellationToken cancellationToken)
    {
        var standalone = GetChunkPath(id);
        try
        {
            var length = new FileInfo(standalone).Length;
            if (length < HeaderLength || length > _options.MaximumChunkBytes + HeaderLength)
                throw new InvalidDataException($"Chunk '{id}' has an invalid stored length.");
            return await File.ReadAllBytesAsync(standalone, cancellationToken).ConfigureAwait(false);
        }
        catch (FileNotFoundException)
        {
            // Packed storage is consulted below.
        }
        catch (DirectoryNotFoundException)
        {
            // Packed storage is consulted below.
        }

        var location = await _metadata.GetPackedChunkLocationAsync(id, cancellationToken).ConfigureAwait(false)
                       ?? throw new FileNotFoundException($"Chunk '{id}' is missing.");
        return await ReadPackedChunkPayloadAsync(location, cancellationToken).ConfigureAwait(false);
    }

    private async Task<byte[]> ReadPackedChunkPayloadAsync(
        PackedChunkLocation location,
        CancellationToken cancellationToken)
    {
        if (location.RecordOffset < 0 ||
            location.RecordLength <= PackRecordHeaderLength + PackRecordFooterLength ||
            location.PayloadOffset < location.RecordOffset + PackRecordHeaderLength ||
            location.PayloadLength < HeaderLength ||
            location.PayloadLength > _options.MaximumChunkBytes + HeaderLength)
        {
            throw new InvalidDataException($"Packed chunk '{location.ChunkId}' has an invalid location.");
        }

        var path = GetPackPath(location.PackId);
        var input = new FileStream(
            path,
            FileMode.Open,
            FileAccess.Read,
            FileShare.ReadWrite,
            128 * 1024,
            FileOptions.Asynchronous | FileOptions.RandomAccess);
        await using var inputDisposal = input.ConfigureAwait(false);
        if (location.RecordOffset > input.Length || location.RecordLength > input.Length - location.RecordOffset)
            throw new InvalidDataException($"Packed chunk '{location.ChunkId}' extends beyond its pack file.");
        input.Position = location.RecordOffset;
        var header = new byte[PackRecordHeaderLength];
        await input.ReadExactlyAsync(header, cancellationToken).ConfigureAwait(false);
        if (BinaryPrimitives.ReadUInt64LittleEndian(header) != PackRecordMagic)
            throw new InvalidDataException($"Packed chunk '{location.ChunkId}' has an invalid record marker.");
        var idLength = BinaryPrimitives.ReadUInt16LittleEndian(header.AsSpan(sizeof(ulong)));
        var payloadLength = BinaryPrimitives.ReadInt32LittleEndian(
            header.AsSpan(sizeof(ulong) + sizeof(ushort)));
        var expectedRecordLength = checked(
            PackRecordHeaderLength + idLength + payloadLength + PackRecordFooterLength);
        var expectedPayloadOffset = checked(location.RecordOffset + PackRecordHeaderLength + idLength);
        if (idLength == 0 ||
            payloadLength != location.PayloadLength ||
            expectedRecordLength != location.RecordLength ||
            expectedPayloadOffset != location.PayloadOffset)
        {
            throw new InvalidDataException($"Packed chunk '{location.ChunkId}' has inconsistent framing.");
        }

        var idBytes = new byte[idLength];
        await input.ReadExactlyAsync(idBytes, cancellationToken).ConfigureAwait(false);
        if (!string.Equals(Encoding.UTF8.GetString(idBytes), location.ChunkId, StringComparison.Ordinal))
            throw new InvalidDataException($"Packed chunk '{location.ChunkId}' has a mismatched record identity.");
        var payload = new byte[payloadLength];
        await input.ReadExactlyAsync(payload, cancellationToken).ConfigureAwait(false);
        var actualHash = SHA256.HashData(payload);
        var expectedHashOffset = sizeof(ulong) + sizeof(ushort) + sizeof(int);
        if (!CryptographicOperations.FixedTimeEquals(
                actualHash,
                header.AsSpan(expectedHashOffset, actualHash.Length)))
        {
            throw new InvalidDataException($"Packed chunk '{location.ChunkId}' failed its record integrity check.");
        }
        var footer = new byte[PackRecordFooterLength];
        await input.ReadExactlyAsync(footer, cancellationToken).ConfigureAwait(false);
        if (BinaryPrimitives.ReadUInt64LittleEndian(footer) != PackRecordFooterMagic)
            throw new InvalidDataException($"Packed chunk '{location.ChunkId}' has an invalid record footer.");
        return payload;
    }

    private string GetChunkPath(string id)
    {
        if (string.IsNullOrWhiteSpace(id) || Path.IsPathRooted(id) || id.Contains("..", StringComparison.Ordinal))
            throw new InvalidDataException("A chunk identifier is invalid.");
        var relative = id.Replace('/', Path.DirectorySeparatorChar) + ".chunk";
        var path = Path.GetFullPath(Path.Combine(_paths.Chunks, relative));
        var root = Path.GetFullPath(_paths.Chunks) + Path.DirectorySeparatorChar;
        if (!path.StartsWith(root, StringComparison.Ordinal))
            throw new InvalidDataException("A chunk identifier escaped the content root.");
        return path;
    }

    private string GetPackPath(string packId)
    {
        if (string.IsNullOrWhiteSpace(packId) ||
            Path.IsPathRooted(packId) ||
            packId.Contains("..", StringComparison.Ordinal))
        {
            throw new InvalidDataException("A chunk pack identifier is invalid.");
        }
        var relative = packId.Replace('/', Path.DirectorySeparatorChar) + ".pack";
        var path = Path.GetFullPath(Path.Combine(_paths.Packs, relative));
        var root = Path.GetFullPath(_paths.Packs) + Path.DirectorySeparatorChar;
        if (!path.StartsWith(root, StringComparison.Ordinal))
            throw new InvalidDataException("A chunk pack identifier escaped the pack root.");
        return path;
    }

    private static string CreatePackId(string domain) => $"{domain}/{Guid.NewGuid():N}";

    private static void ValidateManifest(ContentManifest manifest)
    {
        if (string.IsNullOrEmpty(manifest.Domain) ||
            manifest.Length < 0 ||
            manifest.Sha256.Length != 64 && !string.Equals(manifest.Sha256, ContentManifest.SparseHash, StringComparison.Ordinal))
            throw new InvalidDataException("The content manifest is invalid.");
        long expectedOffset = 0;
        foreach (var chunk in manifest.Chunks)
        {
            if (chunk.Offset != expectedOffset ||
                chunk.Length <= 0 ||
                !chunk.Id.StartsWith(manifest.Domain + "/", StringComparison.Ordinal) ||
                !IsZero(chunk, manifest.Domain) && chunk.Length > int.MaxValue)
                throw new InvalidDataException("The content manifest contains an invalid chunk reference.");
            expectedOffset = checked(expectedOffset + chunk.Length);
        }
        if (expectedOffset != manifest.Length || (manifest.Length == 0 && manifest.Chunks.Count != 0))
            throw new InvalidDataException("The content manifest length is inconsistent.");
    }

    private static string ZeroId(string domain) => domain + "/$zero";

    private static bool IsZero(ChunkReference reference, string domain) =>
        string.Equals(reference.Id, ZeroId(domain), StringComparison.Ordinal);

    private void PinIds(IReadOnlyList<string> ids)
    {
        while (true)
        {
            Task? reservationCompletion = null;
            lock (_pinGate)
            {
                foreach (var id in ids)
                {
                    if (_mutationReservations.TryGetValue(id, out var reservation))
                    {
                        reservationCompletion = reservation.Completion;
                        break;
                    }
                }

                if (reservationCompletion is null)
                {
                    foreach (var id in ids)
                    {
                        if (!File.Exists(GetChunkPath(id)) && !_metadata.PackedChunkExists(id))
                            throw new InvalidDataException($"Chunk '{id}' is missing.");
                    }
                    foreach (var id in ids)
                        _pins[id] = _pins.GetValueOrDefault(id) + 1;
                    return;
                }
            }

#pragma warning disable VSTHRD002 // Pin is a synchronous lease API; this waits only for an in-flight chunk mutation to release its reservation.
            reservationCompletion.GetAwaiter().GetResult();
#pragma warning restore VSTHRD002
        }
    }

    private async Task<bool> TryPinIdAsync(string id, CancellationToken cancellationToken)
    {
        while (true)
        {
            Task? reservationCompletion;
            lock (_pinGate)
            {
                if (_mutationReservations.TryGetValue(id, out var reservation))
                {
                    reservationCompletion = reservation.Completion;
                }
                else
                {
                    if (!File.Exists(GetChunkPath(id)) && !_metadata.PackedChunkExists(id))
                        return false;
                    _pins[id] = _pins.GetValueOrDefault(id) + 1;
                    return true;
                }
            }

            await reservationCompletion.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private void UnpinId(string id)
    {
        lock (_pinGate)
        {
            if (!_pins.TryGetValue(id, out var count))
                return;
            if (count == 1)
                _pins.Remove(id);
            else
                _pins[id] = count - 1;
        }
    }

    private sealed class PinLease(ChunkStore owner, IEnumerable<string> ids) : IDisposable
    {
        private readonly string[] _ids = ids.ToArray();
        private int _disposed;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
                return;
            foreach (var id in _ids)
                owner.UnpinId(id);
        }
    }

    internal sealed class ChunkMutationReservation : IDisposable
    {
        private readonly ChunkStore _owner;
        private int _completed;

        internal ChunkMutationReservation(ChunkStore owner, string id)
        {
            _owner = owner;
            Id = id;
        }

        internal TaskCompletionSource<bool> CompletionSource { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal Task Completion => CompletionSource.Task;

        public string Id { get; }

        public async Task<bool> TryDeleteAsync(CancellationToken cancellationToken)
        {
            if (Interlocked.CompareExchange(ref _completed, 1, 0) != 0)
                return false;
            try
            {
                return await _owner.CompleteMutationAsync(this, cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                _owner.ReleaseMutation(this);
                throw;
            }
        }

        public void Dispose()
        {
            if (Interlocked.CompareExchange(ref _completed, 1, 0) == 0)
                _owner.ReleaseMutation(this);
        }
    }

    private async Task<bool> CompleteMutationAsync(
        ChunkMutationReservation reservation,
        CancellationToken cancellationToken)
    {
        var deleted = false;
        lock (_pinGate)
        {
            if (!_mutationReservations.TryGetValue(reservation.Id, out var current) ||
                !ReferenceEquals(current, reservation))
            {
                return false;
            }
            if (_pins.ContainsKey(reservation.Id))
                throw new InvalidOperationException("A reserved chunk became pinned during an exclusive mutation.");
        }

        try
        {
            var path = GetChunkPath(reservation.Id);
            if (File.Exists(path))
            {
                File.Delete(path);
                _paths.PruneEmptyChunkDirectories(path);
                deleted = true;
            }
            deleted = await _metadata.DeletePackedChunkLocationAsync(
                reservation.Id,
                cancellationToken).ConfigureAwait(false) || deleted;
            return deleted;
        }
        finally
        {
            ReleaseMutation(reservation);
        }
    }

    private void ReleaseMutation(ChunkMutationReservation reservation)
    {
        lock (_pinGate)
        {
            if (!_mutationReservations.TryGetValue(reservation.Id, out var current) ||
                !ReferenceEquals(current, reservation))
            {
                return;
            }
            _mutationReservations.Remove(reservation.Id);
        }
        reservation.CompletionSource.TrySetResult(true);
    }
}
