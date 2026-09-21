using System.Buffers.Binary;
using System.IO.Compression;
using System.Text;
using Microsoft.Extensions.Options;
using Mk8.Sava.Configuration;

namespace Mk8.Sava.Storage;

public sealed class StoredContent(ContentManifest manifest, IDisposable pin) : IDisposable
{
    public ContentManifest Manifest { get; } = manifest;

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

    private readonly StoragePaths _paths;
    private readonly SavaOptions _options;
    private readonly ContentDefinedChunker _chunker;
    private readonly object _pinGate = new();
    private readonly Dictionary<string, int> _pins = new(StringComparer.Ordinal);

    public ChunkStore(StoragePaths paths, IOptions<SavaOptions> options)
    {
        _paths = paths;
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
        var references = new List<ChunkReference>();
        var pinnedIds = new HashSet<string>(StringComparer.Ordinal);
        long offset = 0;

        try
        {
            await foreach (var bytes in _chunker.ReadChunksAsync(source, _options.MaximumRequestBodyBytes, cancellationToken))
            {
                completeHash.AppendData(bytes);
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
                    if (pinnedIds.Add(id))
                        PinId(id);
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
            return new StoredContent(manifest, new PinLease(this, pinnedIds));
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
        foreach (var id in ids)
            PinId(id);
        return new PinLease(this, ids);
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

    public IEnumerable<string> EnumerateChunkIds() =>
        Directory.EnumerateFiles(_paths.Chunks, "*.chunk", SearchOption.AllDirectories)
            .Select(path => Path.GetRelativePath(_paths.Chunks, path)[..^".chunk".Length].Replace(Path.DirectorySeparatorChar, '/'));

    public bool DeleteChunk(string id)
    {
        lock (_pinGate)
        {
            if (_pins.ContainsKey(id))
                return false;
            var path = GetChunkPath(id);
            if (!File.Exists(path))
                return false;
            File.Delete(path);
            return true;
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
            if (File.Exists(finalPath))
            {
                var existing = await ReadVerifiedChunkAsync(id, domain, customerProvidedKey, cancellationToken);
                if (CryptographicOperations.FixedTimeEquals(existing, bytes))
                    return id;
                continue;
            }

            var temporaryPath = Path.Combine(_paths.Staging, $"chunk-{Guid.NewGuid():N}.tmp");
            try
            {
                await WriteChunkFileAsync(temporaryPath, domain, bytes, customerProvidedKey, cancellationToken);
                try
                {
                    File.Move(temporaryPath, finalPath, overwrite: false);
                    return id;
                }
                catch (IOException) when (File.Exists(finalPath))
                {
                    var existing = await ReadVerifiedChunkAsync(id, domain, customerProvidedKey, cancellationToken);
                    if (CryptographicOperations.FixedTimeEquals(existing, bytes))
                        return id;
                }
            }
            finally
            {
                if (File.Exists(temporaryPath))
                    File.Delete(temporaryPath);
            }
        }
    }

    private async Task WriteChunkFileAsync(
        string path,
        string domain,
        byte[] bytes,
        byte[]? customerProvidedKey,
        CancellationToken cancellationToken)
    {
        byte codec = 0;
        byte[] encoded = bytes;
        using (var compressed = new MemoryStream())
        {
            await using (var brotli = new BrotliStream(compressed, new BrotliCompressionOptions
            {
                Quality = _options.CompressionQuality
            }, leaveOpen: true))
            {
                await brotli.WriteAsync(bytes, cancellationToken);
            }
            if (compressed.Length + _options.CompressionMinimumSavingsBytes < bytes.Length)
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
        using (var aes = new AesGcm(DeriveEncryptionKey(domain, customerProvidedKey), TagLength))
        {
            aes.Encrypt(
                header.AsSpan(nonceOffset, NonceLength),
                encoded,
                ciphertext,
                header.AsSpan(AuthenticatedHeaderLength, TagLength),
                header.AsSpan(0, AuthenticatedHeaderLength));
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
        if (!id.StartsWith(domain + "/", StringComparison.Ordinal))
            throw new InvalidDataException("A chunk reference escaped its encryption domain.");
        var path = GetChunkPath(id);
        await using var input = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 128 * 1024, FileOptions.Asynchronous | FileOptions.SequentialScan);
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
        try
        {
            using var aes = new AesGcm(DeriveEncryptionKey(domain, customerProvidedKey), TagLength);
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
        return decoded;
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

    private void PinId(string id)
    {
        lock (_pinGate)
            _pins[id] = _pins.GetValueOrDefault(id) + 1;
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
}
