using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Globalization;
using System.IO.Compression;
using System.Text;
using Microsoft.Extensions.Options;
using Mk8.Sava.Configuration;

namespace Mk8.Sava.Storage;

public sealed class StoredContent(
    ContentManifest manifest,
    IDisposable pin,
    string? contentMd5 = null) : IDisposable
{
    public ContentManifest Manifest { get; } = manifest;
    public string? ContentMd5 { get; } = contentMd5;

    public void Dispose() => pin.Dispose();
}

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
    private readonly SavaOptions _options;
    private readonly ContentDefinedChunker _chunker;
    private readonly object _pinGate = new();
    private readonly Dictionary<string, int> _pins = new(StringComparer.Ordinal);
    private readonly Dictionary<string, ChunkMutationReservation> _mutationReservations = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _packGates = new(StringComparer.Ordinal);
    private string? _orphanPackCursor;

    public ChunkStore(StoragePaths paths, MetadataStore metadata, IOptions<SavaOptions> options)
    {
        _paths = paths;
        _metadata = metadata;
        _options = options.Value;
        _chunker = new ContentDefinedChunker(
            _options.MinimumChunkBytes,
            _options.TargetChunkBytes,
            _options.MaximumChunkBytes);
    }

    public async Task<StoredContent> StorePinnedAsync(
        string account,
        BlobEncryption encryption,
        Stream source,
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
            await foreach (var bytes in _chunker.ReadChunksAsync(source, _options.MaximumRequestBodyBytes, cancellationToken))
            {
                completeHash.AppendData(bytes);
                completeMd5.AppendData(bytes);
                if (bytes.AsSpan().IndexOfAnyExcept((byte)0) < 0)
                {
                    var zeroId = ZeroId(domain);
                    if (references.Count > 0 && references[^1].Id == zeroId)
                        references[^1] = references[^1] with { Length = checked(references[^1].Length + bytes.Length) };
                    else
                        references.Add(new ChunkReference(zeroId, offset, bytes.Length));
                }
                else
                {
                    var id = await StoreVerifiedChunkAsync(domain, bytes, encryption.CustomerProvidedKey, cancellationToken);
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
        var bytes = await ReadStoredChunkFileBytesAsync(id, cancellationToken);
        await using var output = new FileStream(
            destination,
            FileMode.CreateNew,
            FileAccess.Write,
            FileShare.None,
            128 * 1024,
            FileOptions.Asynchronous);
        await output.WriteAsync(bytes, cancellationToken);
        await output.FlushAsync(cancellationToken);
        output.Flush(flushToDisk: true);
    }

    public ContentManifest Empty(string account, BlobEncryption encryption) =>
        ContentManifest.Empty(ResolveDomain(account, encryption));

    public ContentManifest Sparse(string account, BlobEncryption encryption, long length)
    {
        if (length < 0)
            throw new ArgumentOutOfRangeException(nameof(length));
        var domain = ResolveDomain(account, encryption);
        return length == 0
            ? ContentManifest.Empty(domain)
            : new ContentManifest(
                domain,
                length,
                ContentManifest.SparseHash,
                [new ChunkReference(ZeroId(domain), 0, length)]);
    }

    public bool IsInDomain(string account, BlobEncryption encryption, ContentManifest manifest) =>
        string.Equals(ResolveDomain(account, encryption), manifest.Domain, StringComparison.Ordinal);

    public async Task<ContentManifest> ComposeAsync(
        string account,
        BlobEncryption encryption,
        IReadOnlyList<ContentManifest> manifests,
        CancellationToken cancellationToken)
    {
        var domain = ResolveDomain(account, encryption);
        if (manifests.Any(manifest => !string.Equals(manifest.Domain, domain, StringComparison.Ordinal)))
            throw new InvalidOperationException("Content from different encryption domains must be copied through verified plaintext.");

        if (manifests.Any(manifest => manifest.Sha256 == ContentManifest.SparseHash))
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
                    if (references.Count > 0 && references[^1].Id == chunk.Id)
                        references[^1] = references[^1] with { Length = checked(references[^1].Length + chunk.Length) };
                    else
                        references.Add(new ChunkReference(chunk.Id, offset, chunk.Length));
                    offset += chunk.Length;
                    continue;
                }
                var bytes = await ReadVerifiedChunkAsync(chunk.Id, domain, encryption.CustomerProvidedKey, cancellationToken);
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
        ValidateManifest(current);
        if (!IsInDomain(account, encryption, current) || start < 0 || length < 0 || start > current.Length || length > current.Length - start)
            throw new ArgumentOutOfRangeException(nameof(start));
        if (!clear && replacement is null)
            throw new ArgumentNullException(nameof(replacement));

        var references = new List<ChunkReference>();
        var temporaryPins = new List<IDisposable>();
        try
        {
            await AppendSliceAsync(account, encryption, current, 0, start, references, temporaryPins, cancellationToken);
            if (clear)
            {
                AddReference(references, ZeroId(current.Domain), length, current.Domain);
            }
            else
            {
                var stored = await StorePinnedAsync(account, encryption, replacement!, cancellationToken);
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
                cancellationToken);
            return PinSparseManifest(current.Domain, references, temporaryPins);
        }
        catch
        {
            foreach (var pin in temporaryPins)
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
            await AppendSliceAsync(account, encryption, current, 0, retained, references, temporaryPins, cancellationToken);
            if (length > current.Length)
                AddReference(references, ZeroId(current.Domain), length - current.Length, current.Domain);
            return PinSparseManifest(current.Domain, references, temporaryPins);
        }
        catch
        {
            foreach (var pin in temporaryPins)
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
                                        manifest.Sha256 != ContentManifest.SparseHash
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
                    await WriteZeroesAsync(destination, sliceLength, completeHash, cancellationToken);
                    continue;
                }

                var bytes = await ReadVerifiedChunkAsync(chunk.Id, manifest.Domain, encryption.CustomerProvidedKey, cancellationToken);
                if (bytes.LongLength != chunk.Length)
                    throw new InvalidDataException($"Chunk '{chunk.Id}' has an unexpected decoded length.");
                var slice = bytes.AsMemory(checked((int)startInChunk), checked((int)sliceLength));
                completeHash?.AppendData(slice.Span);
                await destination.WriteAsync(slice, cancellationToken);
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
        if (manifest.Length > int.MaxValue)
            throw new InvalidOperationException("The content is too large to materialize in memory.");
        using var buffer = new MemoryStream((int)manifest.Length);
        await WriteRangeAsync(manifest, encryption, 0, manifest.Length, buffer, cancellationToken);
        return buffer.ToArray();
    }

    public async Task<string> MaterializeAsync(
        ContentManifest manifest,
        BlobEncryption encryption,
        CancellationToken cancellationToken)
    {
        var path = Path.Combine(_paths.Staging, $"materialized-{Guid.NewGuid():N}.tmp");
        await using var output = new FileStream(path, FileMode.CreateNew, FileAccess.Write, FileShare.None, 128 * 1024, FileOptions.Asynchronous);
        await WriteRangeAsync(manifest, encryption, 0, manifest.Length, output, cancellationToken);
        await output.FlushAsync(cancellationToken);
        output.Flush(flushToDisk: true);
        return path;
    }

    internal async Task<PhysicalChunkPage> EnumerateChunkIdsPageAsync(
        string? after,
        int maximum,
        CancellationToken cancellationToken)
    {
        if (maximum <= 0)
            throw new ArgumentOutOfRangeException(nameof(maximum));
        var standalone = EnumerateStorageIdsOrdered(_paths.Chunks, relativeDirectory: string.Empty, after, ".chunk")
            .Take(checked(maximum + 1))
            .ToList();
        var packed = await _metadata.ListPackedChunkIdsAsync(after, maximum, cancellationToken);
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

    internal async Task<PackCompactionResult> TryCompactPackAsync(
        ChunkPackRecord pack,
        CancellationToken cancellationToken)
    {
        if (!pack.Sealed)
            return PackCompactionResult.Skipped;
        var gate = _packGates.GetOrAdd(pack.Domain, _ => new SemaphoreSlim(1, 1));
        await gate.WaitAsync(cancellationToken);
        try
        {
            var oldPath = GetPackPath(pack.PackId);
            var locations = await _metadata.ListPackedChunkLocationsAsync(pack.PackId, cancellationToken);
            long oldLength;
            try
            {
                oldLength = new FileInfo(oldPath).Length;
            }
            catch (FileNotFoundException)
            {
                if (locations.Count == 0)
                {
                    await _metadata.ReplaceChunkPackAsync(pack, null, locations, [], cancellationToken);
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
                    await _metadata.ReplaceChunkPackAsync(pack, null, locations, [], cancellationToken);
                    File.Delete(oldPath);
                    return new PackCompactionResult(1, 1, 0, oldLength);
                }

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
                    await using (var source = new FileStream(
                                     oldPath,
                                     FileMode.Open,
                                     FileAccess.Read,
                                     FileShare.Read,
                                     128 * 1024,
                                     FileOptions.Asynchronous | FileOptions.RandomAccess))
                    await using (var destination = new FileStream(
                                     temporaryPath,
                                     FileMode.CreateNew,
                                     FileAccess.Write,
                                     FileShare.None,
                                     128 * 1024,
                                     FileOptions.Asynchronous))
                    {
                        foreach (var location in locations)
                        {
                            _ = await ReadPackedChunkPayloadAsync(location, cancellationToken);
                            var newOffset = destination.Position;
                            source.Position = location.RecordOffset;
                            await CopyExactlyAsync(source, destination, location.RecordLength, cancellationToken);
                            replacements.Add(location with
                            {
                                PackId = replacement.PackId,
                                RecordOffset = newOffset,
                                PayloadOffset = checked(newOffset + location.PayloadOffset - location.RecordOffset)
                            });
                        }
                        await destination.FlushAsync(cancellationToken);
                        destination.Flush(flushToDisk: true);
                    }

                    Directory.CreateDirectory(Path.GetDirectoryName(replacementPath)!);
                    File.Move(temporaryPath, replacementPath, overwrite: false);
                    try
                    {
                        await _metadata.ReplaceChunkPackAsync(
                            pack,
                            replacement,
                            locations,
                            replacements,
                            cancellationToken);
                    }
                    catch
                    {
                        File.Delete(replacementPath);
                        throw;
                    }
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
            finally
            {
                foreach (var reservation in reservations)
                    reservation.Dispose();
            }
        }
        finally
        {
            gate.Release();
        }
    }

    internal async Task<(int ReclaimedPacks, long BytesSaved)> ReclaimOrphanedPacksAsync(
        DateTimeOffset olderThan,
        int maximumPacks,
        CancellationToken cancellationToken)
    {
        if (maximumPacks <= 0)
            throw new ArgumentOutOfRangeException(nameof(maximumPacks));

        var page = EnumerateStorageIdsOrdered(_paths.Packs, string.Empty, _orphanPackCursor, ".pack")
            .Take(checked(maximumPacks + 1))
            .ToList();
        var hasMore = page.Count > maximumPacks;
        if (hasMore)
            page.RemoveAt(page.Count - 1);

        var reclaimed = 0;
        long bytesSaved = 0;
        foreach (var packId in page)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var separator = packId.LastIndexOf('/');
            if (separator <= 0)
                continue;
            var domain = packId[..separator];
            var gate = _packGates.GetOrAdd(domain, _ => new SemaphoreSlim(1, 1));
            await gate.WaitAsync(cancellationToken);
            try
            {
                var path = GetPackPath(packId);
                FileInfo file;
                try
                {
                    file = new FileInfo(path);
                    if (!file.Exists || file.LastWriteTimeUtc > olderThan.UtcDateTime)
                        continue;
                }
                catch (IOException)
                {
                    continue;
                }

                if (_metadata.ChunkPackExists(packId))
                    continue;

                var length = file.Length;
                try
                {
                    File.Delete(path);
                    reclaimed++;
                    bytesSaved = checked(bytesSaved + length);
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
            }
            finally
            {
                gate.Release();
            }
        }

        _orphanPackCursor = hasMore && page.Count > 0 ? page[^1] : null;
        return (reclaimed, bytesSaved);
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
            if ((attributes & FileAttributes.ReparsePoint) != 0)
                continue;

            var name = Path.GetFileName(path);
            var relative = relativeDirectory.Length == 0 ? name : relativeDirectory + "/" + name;
            if ((attributes & FileAttributes.Directory) != 0)
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
        long chunkBytes = 0;
        var chunkCount = 0;
        foreach (var path in Directory.EnumerateFiles(_paths.Chunks, "*.chunk", SearchOption.AllDirectories))
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
        foreach (var path in Directory.EnumerateFiles(_paths.Packs, "*.pack", SearchOption.AllDirectories))
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

        return new StoragePhysicalUsage(chunkBytes, stagingBytes, metadataBytes, chunkCount);
    }

    public int DeleteAbandonedStagingFiles(DateTimeOffset olderThan, int maximumFiles)
    {
        if (maximumFiles <= 0)
            throw new ArgumentOutOfRangeException(nameof(maximumFiles));

        var deleted = 0;
        foreach (var path in Directory.EnumerateFiles(_paths.Staging, "*.tmp", SearchOption.TopDirectoryOnly))
        {
            if (deleted >= maximumFiles)
                break;
            try
            {
                if ((File.GetAttributes(path) & FileAttributes.ReparsePoint) != 0 ||
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
        if (id.EndsWith("/$zero", StringComparison.Ordinal))
            return ChunkIntegrityStatus.Verified;

        string domain;
        try
        {
            domain = GetDomainFromChunkId(id);
            if (!await TryPinIdAsync(id, cancellationToken))
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
                await VerifyCustomerKeyChunkStructureAsync(id, cancellationToken);
                return ChunkIntegrityStatus.RequiresCustomerKey;
            }

            _ = await ReadVerifiedChunkAsync(id, domain, customerProvidedKey: null, cancellationToken);
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
        if (id.EndsWith("/$zero", StringComparison.Ordinal))
            return ChunkRecompressionResult.Skipped;

        var domain = GetDomainFromChunkId(id);
        if (domain.Contains("/$cpk-", StringComparison.Ordinal))
            return ChunkRecompressionResult.Skipped;

        var path = GetChunkPath(id);
        var eligibleBefore = now.Subtract(_options.BackgroundCompressionMinimumAge).UtcDateTime;
        if (!File.Exists(path))
            return ChunkRecompressionResult.Skipped;
        try
        {
            if (File.GetLastWriteTimeUtc(path) > eligibleBefore)
                return ChunkRecompressionResult.Skipped;
        }
        catch (FileNotFoundException)
        {
            return ChunkRecompressionResult.Skipped;
        }

        using var reservation = TryReserveForMutation(id);
        if (reservation is null)
            return ChunkRecompressionResult.Skipped;

        if (!File.Exists(path))
            return ChunkRecompressionResult.Skipped;
        try
        {
            if (File.GetLastWriteTimeUtc(path) > eligibleBefore)
                return ChunkRecompressionResult.Skipped;
        }
        catch (FileNotFoundException)
        {
            return ChunkRecompressionResult.Skipped;
        }

        byte[] plaintext;
        try
        {
            plaintext = await ReadVerifiedChunkAsync(id, domain, customerProvidedKey: null, cancellationToken);
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
                cancellationToken);
            var verified = await ReadVerifiedChunkFileAsync(
                temporaryPath,
                id,
                domain,
                customerProvidedKey: null,
                cancellationToken);
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

            File.Move(temporaryPath, path, overwrite: true);
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

            var bytes = await ReadVerifiedChunkAsync(chunk.Id, manifest.Domain, encryption.CustomerProvidedKey, cancellationToken);
            if (bytes.LongLength != chunk.Length)
                throw new InvalidDataException($"Chunk '{chunk.Id}' has an unexpected decoded length.");
            var offset = checked((int)(overlapStart - chunk.Offset));
            var count = checked((int)overlapLength);
            using var slice = new MemoryStream(bytes, offset, count, writable: false);
            var stored = await StorePinnedAsync(account, encryption, slice, cancellationToken);
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
            foreach (var pin in temporaryPins)
                pin.Dispose();
            temporaryPins.Clear();
        }
    }

    private static ContentManifest CreateSparseManifest(string domain, IReadOnlyList<ChunkReference> references)
    {
        var normalized = new List<ChunkReference>(references.Count);
        long offset = 0;
        foreach (var reference in references)
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
            id == ZeroId(domain) &&
            references[^1].Id == id)
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
            await destination.WriteAsync(buffer.AsMemory(0, count), cancellationToken);
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
            await source.ReadExactlyAsync(buffer.AsMemory(0, count), cancellationToken);
            await destination.WriteAsync(buffer.AsMemory(0, count), cancellationToken);
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
        var hash = Convert.ToHexStringLower(SHA256.HashData(bytes));
        var baseName = $"{hash}-{bytes.Length}";
        var directory = Path.Combine(_paths.Chunks, domain, hash[..2], hash[2..4]);
        Directory.CreateDirectory(directory);

        for (var collision = 0; ; collision++)
        {
            var name = collision == 0 ? baseName : $"{baseName}-{collision}";
            var id = $"{domain}/{hash[..2]}/{hash[2..4]}/{name}";
            var finalPath = GetChunkPath(id);
            while (true)
            {
                if (await TryPinIdAsync(id, cancellationToken))
                {
                    var keepPin = false;
                    try
                    {
                        var existing = await ReadVerifiedChunkAsync(id, domain, customerProvidedKey, cancellationToken);
                        if (CryptographicOperations.FixedTimeEquals(existing, bytes))
                        {
                            keepPin = true;
                            return id;
                        }
                    }
                    finally
                    {
                        if (!keepPin)
                            UnpinId(id);
                    }
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
                        cancellationToken);
                    if (_options.EnableSmallChunkPacking &&
                        bytes.Length <= _options.SmallChunkPackingThresholdBytes)
                    {
                        var published = await TryStorePackedChunkFileAsync(
                            id,
                            domain,
                            temporaryPath,
                            cancellationToken);
                        if (published && await TryPinIdAsync(id, cancellationToken))
                        {
                            var keepPin = false;
                            try
                            {
                                var stored = await ReadVerifiedChunkAsync(
                                    id,
                                    domain,
                                    customerProvidedKey,
                                    cancellationToken);
                                if (!CryptographicOperations.FixedTimeEquals(stored, bytes))
                                    throw new InvalidDataException($"Packed chunk '{id}' changed while it was published.");
                                keepPin = true;
                                return id;
                            }
                            finally
                            {
                                if (!keepPin)
                                    UnpinId(id);
                            }
                        }
                        continue;
                    }

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
                            File.Move(temporaryPath, finalPath, overwrite: false);
                            _pins[id] = _pins.GetValueOrDefault(id) + 1;
                            created = true;
                        }
                    }

                    if (created)
                        return id;
                    if (reservationCompletion is not null)
                        await reservationCompletion.WaitAsync(cancellationToken);
                }
                finally
                {
                    if (File.Exists(temporaryPath))
                        File.Delete(temporaryPath);
                }
            }
        }
    }

    private async Task<bool> TryStorePackedChunkFileAsync(
        string id,
        string domain,
        string chunkFilePath,
        CancellationToken cancellationToken)
    {
        var gate = _packGates.GetOrAdd(domain, _ => new SemaphoreSlim(1, 1));
        await gate.WaitAsync(cancellationToken);
        try
        {
            var payloadLength = checked((int)new FileInfo(chunkFilePath).Length);
            var idLength = Encoding.UTF8.GetByteCount(id);
            var recordLength = checked(PackRecordHeaderLength + idLength + payloadLength + PackRecordFooterLength);
            var pack = await _metadata.GetActiveChunkPackAsync(domain, cancellationToken);
            if (pack is not null)
            {
                var path = GetPackPath(pack.PackId);
                if (!File.Exists(path))
                {
                    var records = await _metadata.CountPackedChunksAsync(pack.PackId, cancellationToken);
                    if (records != 0)
                        throw new InvalidDataException($"Active chunk pack '{pack.PackId}' is missing.");
                    await _metadata.SealChunkPackAsync(pack.PackId, cancellationToken);
                    pack = null;
                }
                else
                {
                    var records = await _metadata.CountPackedChunksAsync(pack.PackId, cancellationToken);
                    if (records >= _options.ChunkPackMaximumRecords ||
                        new FileInfo(path).Length + recordLength > _options.ChunkPackTargetBytes)
                    {
                        await _metadata.SealChunkPackAsync(pack.PackId, cancellationToken);
                        pack = null;
                    }
                }
            }

            pack ??= new ChunkPackRecord(
                CreatePackId(domain),
                domain,
                _metadata.GetUtcNow(),
                Sealed: false);
            var location = await AppendPackRecordAsync(pack, id, chunkFilePath, cancellationToken);
            var inserted = await _metadata.TryRegisterPackedChunkAsync(pack, location, cancellationToken);
            if (inserted &&
                (new FileInfo(GetPackPath(pack.PackId)).Length >= _options.ChunkPackTargetBytes ||
                 await _metadata.CountPackedChunksAsync(pack.PackId, cancellationToken) >=
                 _options.ChunkPackMaximumRecords))
            {
                await _metadata.SealChunkPackAsync(pack.PackId, cancellationToken);
            }
            return inserted;
        }
        finally
        {
            gate.Release();
        }
    }

    private async Task<PackedChunkLocation> AppendPackRecordAsync(
        ChunkPackRecord pack,
        string id,
        string chunkFilePath,
        CancellationToken cancellationToken)
    {
        var idBytes = Encoding.UTF8.GetBytes(id);
        if (idBytes.Length == 0 || idBytes.Length > ushort.MaxValue)
            throw new InvalidDataException("A chunk identifier is too long to pack.");
        var payload = await File.ReadAllBytesAsync(chunkFilePath, cancellationToken);
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
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        await using var output = new FileStream(
            path,
            FileMode.OpenOrCreate,
            FileAccess.ReadWrite,
            FileShare.Read,
            128 * 1024,
            FileOptions.Asynchronous);
        var recordOffset = output.Seek(0, SeekOrigin.End);
        await output.WriteAsync(header, cancellationToken);
        await output.WriteAsync(idBytes, cancellationToken);
        var payloadOffset = output.Position;
        await output.WriteAsync(payload, cancellationToken);
        await output.WriteAsync(footer, cancellationToken);
        await output.FlushAsync(cancellationToken);
        output.Flush(flushToDisk: true);
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
            await using (var brotli = new BrotliStream(compressed, new BrotliCompressionOptions
            {
                Quality = compressionQuality
            }, leaveOpen: true))
            {
                await brotli.WriteAsync(bytes, cancellationToken);
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
        SHA256.HashData(bytes, header.AsSpan(sizeof(ulong) + 2 * sizeof(byte) + sizeof(int), 32));
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

        await using var output = new FileStream(path, FileMode.CreateNew, FileAccess.Write, FileShare.None, 128 * 1024, FileOptions.Asynchronous);
        await output.WriteAsync(header, cancellationToken);
        await output.WriteAsync(ciphertext, cancellationToken);
        await output.FlushAsync(cancellationToken);
        output.Flush(flushToDisk: true);
    }

    private async Task<byte[]> ReadVerifiedChunkAsync(
        string id,
        string domain,
        byte[]? customerProvidedKey,
        CancellationToken cancellationToken)
    {
        var stored = await ReadStoredChunkFileBytesAsync(id, cancellationToken);
        using var input = new MemoryStream(stored, writable: false);
        return await ReadVerifiedChunkStreamAsync(input, id, domain, customerProvidedKey, cancellationToken);
    }

    private async Task<byte[]> ReadVerifiedChunkFileAsync(
        string path,
        string id,
        string domain,
        byte[]? customerProvidedKey,
        CancellationToken cancellationToken)
    {
        await using var input = new FileStream(
            path,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            128 * 1024,
            FileOptions.Asynchronous | FileOptions.SequentialScan);
        return await ReadVerifiedChunkStreamAsync(input, id, domain, customerProvidedKey, cancellationToken);
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
        await input.ReadExactlyAsync(header, cancellationToken);
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
        await input.ReadExactlyAsync(ciphertext, cancellationToken);
        var encoded = new byte[ciphertextLength];
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
                    await brotli.ReadExactlyAsync(decoded, cancellationToken);
                    if (brotli.ReadByte() != -1)
                        throw new InvalidDataException($"Compressed chunk '{id}' expands beyond its recorded length.");
                }
                break;
            default:
                throw new InvalidDataException($"Chunk '{id}' uses an unsupported codec.");
        }

        var expectedHashOffset = sizeof(ulong) + 2 * sizeof(byte) + sizeof(int);
        var actualHash = SHA256.HashData(decoded);
        if (!CryptographicOperations.FixedTimeEquals(actualHash, header.AsSpan(expectedHashOffset, 32)))
            throw new InvalidDataException($"Chunk '{id}' failed its plaintext integrity check.");
        ValidateChunkIdentity(id, decoded.Length, actualHash);
        return decoded;
    }

    private async Task VerifyCustomerKeyChunkStructureAsync(string id, CancellationToken cancellationToken)
    {
        var stored = await ReadStoredChunkFileBytesAsync(id, cancellationToken);
        using var input = new MemoryStream(stored, writable: false);
        var header = new byte[HeaderLength];
        await input.ReadExactlyAsync(header, cancellationToken);
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
        if (domain == "$global")
            encodedRoot = _options.CrossAccountEncryptionKey ?? throw new InvalidOperationException("The cross-account encryption key is not configured.");
        else if (!_options.Accounts.TryGetValue(domain.Split('/', 2)[0], out encodedRoot!))
            throw new InvalidDataException("The chunk encryption domain is not configured.");
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
            return await File.ReadAllBytesAsync(standalone, cancellationToken);
        }
        catch (FileNotFoundException)
        {
            // Packed storage is consulted below.
        }
        catch (DirectoryNotFoundException)
        {
            // Packed storage is consulted below.
        }

        var location = await _metadata.GetPackedChunkLocationAsync(id, cancellationToken)
                       ?? throw new FileNotFoundException($"Chunk '{id}' is missing.");
        return await ReadPackedChunkPayloadAsync(location, cancellationToken);
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
        await using var input = new FileStream(
            path,
            FileMode.Open,
            FileAccess.Read,
            FileShare.ReadWrite,
            128 * 1024,
            FileOptions.Asynchronous | FileOptions.RandomAccess);
        if (location.RecordOffset > input.Length || location.RecordLength > input.Length - location.RecordOffset)
            throw new InvalidDataException($"Packed chunk '{location.ChunkId}' extends beyond its pack file.");
        input.Position = location.RecordOffset;
        var header = new byte[PackRecordHeaderLength];
        await input.ReadExactlyAsync(header, cancellationToken);
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
        await input.ReadExactlyAsync(idBytes, cancellationToken);
        if (!string.Equals(Encoding.UTF8.GetString(idBytes), location.ChunkId, StringComparison.Ordinal))
            throw new InvalidDataException($"Packed chunk '{location.ChunkId}' has a mismatched record identity.");
        var payload = new byte[payloadLength];
        await input.ReadExactlyAsync(payload, cancellationToken);
        var actualHash = SHA256.HashData(payload);
        var expectedHashOffset = sizeof(ulong) + sizeof(ushort) + sizeof(int);
        if (!CryptographicOperations.FixedTimeEquals(
                actualHash,
                header.AsSpan(expectedHashOffset, actualHash.Length)))
        {
            throw new InvalidDataException($"Packed chunk '{location.ChunkId}' failed its record integrity check.");
        }
        var footer = new byte[PackRecordFooterLength];
        await input.ReadExactlyAsync(footer, cancellationToken);
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
            manifest.Sha256.Length != 64 && manifest.Sha256 != ContentManifest.SparseHash)
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

            reservationCompletion.GetAwaiter().GetResult();
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

            await reservationCompletion.WaitAsync(cancellationToken);
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
                return await _owner.CompleteMutationAsync(this, cancellationToken);
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
                deleted = true;
            }
            deleted = await _metadata.DeletePackedChunkLocationAsync(
                reservation.Id,
                cancellationToken) || deleted;
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
