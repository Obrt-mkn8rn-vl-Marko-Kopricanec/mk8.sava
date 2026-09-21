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
        Stream source,
        CancellationToken cancellationToken)
    {
        var domain = ResolveDomain(account);
        using var completeHash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        var references = new List<ChunkReference>();
        var pinnedIds = new HashSet<string>(StringComparer.Ordinal);
        long offset = 0;

        try
        {
            await foreach (var bytes in _chunker.ReadChunksAsync(source, _options.MaximumRequestBodyBytes, cancellationToken))
            {
                completeHash.AppendData(bytes);
                var id = await StoreVerifiedChunkAsync(domain, bytes, cancellationToken);
                if (pinnedIds.Add(id))
                    PinId(id);
                references.Add(new ChunkReference(id, offset, bytes.Length));
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
        var ids = manifest.Chunks.Select(chunk => chunk.Id).Distinct(StringComparer.Ordinal).ToArray();
        foreach (var id in ids)
            PinId(id);
        return new PinLease(this, ids);
    }

    public ContentManifest Empty(string account) => ContentManifest.Empty(ResolveDomain(account));

    public bool IsInDomain(string account, ContentManifest manifest) =>
        string.Equals(ResolveDomain(account), manifest.Domain, StringComparison.Ordinal);

    public async Task<ContentManifest> ComposeAsync(
        string account,
        IReadOnlyList<ContentManifest> manifests,
        CancellationToken cancellationToken)
    {
        var domain = ResolveDomain(account);
        if (manifests.Any(manifest => !string.Equals(manifest.Domain, domain, StringComparison.Ordinal)))
            throw new InvalidOperationException("Content from different encryption domains must be copied through verified plaintext.");

        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        var references = new List<ChunkReference>();
        long offset = 0;
        foreach (var manifest in manifests)
        {
            ValidateManifest(manifest);
            foreach (var chunk in manifest.Chunks)
            {
                var bytes = await ReadVerifiedChunkAsync(chunk.Id, domain, cancellationToken);
                if (bytes.Length != chunk.Length)
                    throw new InvalidDataException($"Chunk '{chunk.Id}' has an unexpected decoded length.");
                hash.AppendData(bytes);
                references.Add(new ChunkReference(chunk.Id, offset, chunk.Length));
                offset += chunk.Length;
            }
        }

        return new ContentManifest(domain, offset, Convert.ToHexStringLower(hash.GetHashAndReset()), references);
    }

    public async Task WriteRangeAsync(
        ContentManifest manifest,
        long offset,
        long length,
        Stream destination,
        CancellationToken cancellationToken)
    {
        ValidateManifest(manifest);
        if (offset < 0 || length < 0 || offset > manifest.Length || length > manifest.Length - offset)
            throw new ArgumentOutOfRangeException(nameof(offset));
        if (length == 0)
            return;

        using var pin = Pin(manifest);
        IncrementalHash? completeHash = offset == 0 && length == manifest.Length
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

                var bytes = await ReadVerifiedChunkAsync(chunk.Id, manifest.Domain, cancellationToken);
                if (bytes.Length != chunk.Length)
                    throw new InvalidDataException($"Chunk '{chunk.Id}' has an unexpected decoded length.");

                var startInChunk = (int)Math.Max(0, offset - chunk.Offset);
                var endInChunk = (int)Math.Min(chunk.Length, rangeEnd - chunk.Offset);
                var slice = bytes.AsMemory(startInChunk, endInChunk - startInChunk);
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

    public async Task<byte[]> ReadAllAsync(ContentManifest manifest, CancellationToken cancellationToken)
    {
        if (manifest.Length > int.MaxValue)
            throw new InvalidOperationException("The content is too large to materialize in memory.");
        using var buffer = new MemoryStream((int)manifest.Length);
        await WriteRangeAsync(manifest, 0, manifest.Length, buffer, cancellationToken);
        return buffer.ToArray();
    }

    public async Task<string> MaterializeAsync(ContentManifest manifest, CancellationToken cancellationToken)
    {
        var path = Path.Combine(_paths.Staging, $"materialized-{Guid.NewGuid():N}.tmp");
        await using var output = new FileStream(path, FileMode.CreateNew, FileAccess.Write, FileShare.None, 128 * 1024, FileOptions.Asynchronous);
        await WriteRangeAsync(manifest, 0, manifest.Length, output, cancellationToken);
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

    private async Task<string> StoreVerifiedChunkAsync(string domain, byte[] bytes, CancellationToken cancellationToken)
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
                var existing = await ReadVerifiedChunkAsync(id, domain, cancellationToken);
                if (CryptographicOperations.FixedTimeEquals(existing, bytes))
                    return id;
                continue;
            }

            var temporaryPath = Path.Combine(_paths.Staging, $"chunk-{Guid.NewGuid():N}.tmp");
            try
            {
                await WriteChunkFileAsync(temporaryPath, domain, bytes, cancellationToken);
                try
                {
                    File.Move(temporaryPath, finalPath, overwrite: false);
                    return id;
                }
                catch (IOException) when (File.Exists(finalPath))
                {
                    var existing = await ReadVerifiedChunkAsync(id, domain, cancellationToken);
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

    private async Task WriteChunkFileAsync(string path, string domain, byte[] bytes, CancellationToken cancellationToken)
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
        using (var aes = new AesGcm(DeriveEncryptionKey(domain), TagLength))
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

    private async Task<byte[]> ReadVerifiedChunkAsync(string id, string domain, CancellationToken cancellationToken)
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
            using var aes = new AesGcm(DeriveEncryptionKey(domain), TagLength);
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

    private string ResolveDomain(string account) => _options.EnableCrossAccountDeduplication ? "$global" : account;

    private byte[] DeriveEncryptionKey(string domain)
    {
        string encodedRoot;
        if (domain == "$global")
            encodedRoot = _options.CrossAccountEncryptionKey ?? throw new InvalidOperationException("The cross-account encryption key is not configured.");
        else if (!_options.Accounts.TryGetValue(domain, out encodedRoot!))
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
        if (string.IsNullOrEmpty(manifest.Domain) || manifest.Length < 0 || manifest.Sha256.Length != 64)
            throw new InvalidDataException("The content manifest is invalid.");
        long expectedOffset = 0;
        foreach (var chunk in manifest.Chunks)
        {
            if (chunk.Offset != expectedOffset || chunk.Length <= 0 || !chunk.Id.StartsWith(manifest.Domain + "/", StringComparison.Ordinal))
                throw new InvalidDataException("The content manifest contains an invalid chunk reference.");
            expectedOffset = checked(expectedOffset + chunk.Length);
        }
        if (expectedOffset != manifest.Length || (manifest.Length == 0 && manifest.Chunks.Count != 0))
            throw new InvalidDataException("The content manifest length is inconsistent.");
    }

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
