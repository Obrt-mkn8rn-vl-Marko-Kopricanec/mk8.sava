using System.Text.Json;
using Microsoft.Extensions.Options;
using Mk8.Sava.Configuration;

namespace Mk8.Sava.Storage;

public sealed record StorageBackupValidation(
    string BackupPath,
    DateTimeOffset CreatedAt,
    int BlobRecordCount,
    int StagedBlockCount,
    int ChunkCount,
    long LogicalBytes,
    long PhysicalBytes);

public sealed class StorageBackupService(
    MetadataStore metadata,
    ChunkStore chunks,
    StoragePaths paths,
    IOptions<SavaOptions> configuredOptions)
{
    private const string BackupFormat = "mk8.sava.backup";
    private const int BackupFormatVersion = 1;
    private const string ManifestFileName = "backup-manifest.json";
    private const string MetadataFileName = "metadata.db";
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web)
    {
        WriteIndented = true
    };
    private readonly SavaOptions _options = configuredOptions.Value;

    public async Task<StorageBackupValidation> CreateAsync(
        string destinationPath,
        CancellationToken cancellationToken)
    {
        var destination = Path.GetFullPath(destinationPath);
        if (IsWithin(destination, paths.Root))
            throw new InvalidOperationException("A backup destination must be outside the live storage root.");
        if (Directory.Exists(destination) || File.Exists(destination))
            throw new IOException($"The backup destination '{destination}' already exists.");

        var parent = Directory.GetParent(destination)?.FullName
            ?? throw new InvalidOperationException("The backup destination has no parent directory.");
        Directory.CreateDirectory(parent);
        var temporary = Path.Combine(parent, $".{Path.GetFileName(destination)}.creating-{Guid.NewGuid():N}");
        Directory.CreateDirectory(Path.Combine(temporary, "chunks"));
        try
        {
            var metadataPath = Path.Combine(temporary, MetadataFileName);
            using var snapshot = await metadata.CreateBackupSnapshotAsync(
                metadataPath,
                chunks.PinChunkIds,
                cancellationToken);
            FlushFileToDisk(metadataPath);
            var chunkEntries = new List<BackupChunkEntry>();
            foreach (var id in snapshot.Inventory.ReachableChunkIds
                         .Where(id => !id.EndsWith("/$zero", StringComparison.Ordinal))
                         .Order(StringComparer.Ordinal))
            {
                cancellationToken.ThrowIfCancellationRequested();
                var status = await chunks.VerifyChunkAsync(id, cancellationToken);
                if (status is ChunkIntegrityStatus.Missing or ChunkIntegrityStatus.Corrupt)
                    throw new InvalidDataException($"Cannot back up chunk '{id}' because its integrity status is {status}.");

                var source = chunks.GetChunkPathForBackup(id);
                var destinationChunk = GetChunkPath(Path.Combine(temporary, "chunks"), id);
                Directory.CreateDirectory(Path.GetDirectoryName(destinationChunk)!);
                var copied = await CopyAndHashAsync(source, destinationChunk, cancellationToken);
                chunkEntries.Add(new BackupChunkEntry(id, copied.Length, copied.Sha256));
            }

            var metadataEntry = await HashFileAsync(metadataPath, cancellationToken);
            var manifest = new BackupManifest
            {
                Format = BackupFormat,
                FormatVersion = BackupFormatVersion,
                MetadataSchemaVersion = MetadataStore.CurrentSchemaVersion,
                CreatedAt = metadata.GetUtcNow(),
                Metadata = metadataEntry,
                BlobRecordCount = snapshot.Inventory.BlobRecordCount,
                StagedBlockCount = snapshot.Inventory.StagedBlockCount,
                LogicalBlobBytes = snapshot.Inventory.LogicalBlobBytes,
                LogicalStagedBlockBytes = snapshot.Inventory.LogicalStagedBlockBytes,
                KeyRequirements = BuildKeyRequirements(snapshot.Inventory.ReachableChunkIds, _options),
                Chunks = chunkEntries
            };
            await WriteManifestAsync(Path.Combine(temporary, ManifestFileName), manifest, cancellationToken);
            var validation = await ValidateCoreAsync(temporary, _options, cancellationToken);
            Directory.Move(temporary, destination);
            return validation with { BackupPath = destination };
        }
        catch
        {
            if (Directory.Exists(temporary))
                Directory.Delete(temporary, recursive: true);
            throw;
        }
    }

    public Task<StorageBackupValidation> ValidateAsync(
        string backupPath,
        CancellationToken cancellationToken) =>
        ValidateCoreAsync(Path.GetFullPath(backupPath), _options, cancellationToken);

    public static Task<StorageBackupValidation> ValidateBackupAsync(
        string backupPath,
        SavaOptions options,
        CancellationToken cancellationToken) =>
        ValidateCoreAsync(Path.GetFullPath(backupPath), options, cancellationToken);

    public static async Task<StorageBackupValidation> RestoreAsync(
        string backupPath,
        string targetDataPath,
        SavaOptions options,
        CancellationToken cancellationToken)
    {
        var backup = Path.GetFullPath(backupPath);
        var target = Path.GetFullPath(targetDataPath);
        if (Directory.Exists(target) || File.Exists(target))
            throw new IOException($"The restore target '{target}' already exists; restore requires a new offline target.");
        if (IsWithin(target, backup) || IsWithin(backup, target))
            throw new InvalidOperationException("The backup and restore target must not contain one another.");

        var validation = await ValidateCoreAsync(backup, options, cancellationToken);
        var manifest = await ReadManifestAsync(backup, cancellationToken);
        var parent = Directory.GetParent(target)?.FullName
            ?? throw new InvalidOperationException("The restore target has no parent directory.");
        Directory.CreateDirectory(parent);
        var temporary = Path.Combine(parent, $".{Path.GetFileName(target)}.restoring-{Guid.NewGuid():N}");
        Directory.CreateDirectory(Path.Combine(temporary, "chunks"));
        Directory.CreateDirectory(Path.Combine(temporary, "staging"));
        try
        {
            var metadataCopy = await CopyAndHashAsync(
                Path.Combine(backup, MetadataFileName),
                Path.Combine(temporary, MetadataFileName),
                cancellationToken);
            EnsureFileMatches("metadata database", manifest.Metadata, metadataCopy);
            foreach (var chunk in manifest.Chunks)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var source = GetChunkPath(Path.Combine(backup, "chunks"), chunk.Id);
                var destination = GetChunkPath(Path.Combine(temporary, "chunks"), chunk.Id);
                Directory.CreateDirectory(Path.GetDirectoryName(destination)!);
                var copied = await CopyAndHashAsync(source, destination, cancellationToken);
                EnsureFileMatches($"chunk '{chunk.Id}'", new BackupFileEntry(chunk.Length, chunk.Sha256), copied);
            }

            var inspection = await MetadataStore.InspectDatabaseAsync(
                Path.Combine(temporary, MetadataFileName),
                cancellationToken);
            if (inspection.SchemaVersion != manifest.MetadataSchemaVersion)
                throw new InvalidDataException("The restored metadata schema version changed while copying the backup.");
            Directory.Move(temporary, target);
            return validation with { BackupPath = backup };
        }
        catch
        {
            if (Directory.Exists(temporary))
                Directory.Delete(temporary, recursive: true);
            throw;
        }
    }

    private static async Task<StorageBackupValidation> ValidateCoreAsync(
        string backup,
        SavaOptions options,
        CancellationToken cancellationToken)
    {
        if (!Directory.Exists(backup))
            throw new DirectoryNotFoundException($"The backup directory '{backup}' does not exist.");
        EnsureNotReparsePoint(backup, "backup root");
        var manifest = await ReadManifestAsync(backup, cancellationToken);
        if (manifest.Metadata is null || manifest.KeyRequirements is null || manifest.Chunks is null)
            throw new InvalidDataException("The backup manifest is missing required fields.");
        if (!string.Equals(manifest.Format, BackupFormat, StringComparison.Ordinal) ||
            manifest.FormatVersion != BackupFormatVersion)
        {
            throw new InvalidDataException("The backup format or version is unsupported.");
        }
        if (manifest.MetadataSchemaVersion != MetadataStore.CurrentSchemaVersion)
        {
            throw new InvalidDataException(
                $"The backup metadata schema version {manifest.MetadataSchemaVersion} is not supported by this service.");
        }
        if (manifest.CreatedAt == default)
            throw new InvalidDataException("The backup manifest has an invalid creation time.");

        var expectedRootFiles = new HashSet<string>(StringComparer.Ordinal)
        {
            Path.GetFullPath(Path.Combine(backup, ManifestFileName)),
            Path.GetFullPath(Path.Combine(backup, MetadataFileName))
        };
        if (Directory.EnumerateFiles(backup, "*", SearchOption.TopDirectoryOnly)
                .Select(Path.GetFullPath)
                .Any(path => !expectedRootFiles.Contains(path)) ||
            Directory.EnumerateDirectories(backup, "*", SearchOption.TopDirectoryOnly)
                .Select(Path.GetFullPath)
                .Any(path => !string.Equals(
                    path,
                    Path.GetFullPath(Path.Combine(backup, "chunks")),
                    StringComparison.Ordinal)))
        {
            throw new InvalidDataException("The backup root contains files or directories outside the versioned format.");
        }

        var metadataPath = Path.Combine(backup, MetadataFileName);
        EnsureRegularFile(metadataPath, "metadata database");
        var actualMetadata = await HashFileAsync(metadataPath, cancellationToken);
        EnsureFileMatches("metadata database", manifest.Metadata, actualMetadata);
        var inspection = await MetadataStore.InspectDatabaseAsync(metadataPath, cancellationToken);
        if (inspection.SchemaVersion != manifest.MetadataSchemaVersion)
            throw new InvalidDataException("The backup manifest and metadata database schema versions do not match.");
        if (inspection.Inventory.BlobRecordCount != manifest.BlobRecordCount ||
            inspection.Inventory.StagedBlockCount != manifest.StagedBlockCount ||
            inspection.Inventory.LogicalBlobBytes != manifest.LogicalBlobBytes ||
            inspection.Inventory.LogicalStagedBlockBytes != manifest.LogicalStagedBlockBytes)
        {
            throw new InvalidDataException("The backup manifest does not match the metadata inventory.");
        }

        var expectedIds = inspection.Inventory.ReachableChunkIds
            .Where(id => !id.EndsWith("/$zero", StringComparison.Ordinal))
            .ToHashSet(StringComparer.Ordinal);
        if (manifest.Chunks.Count != expectedIds.Count ||
            manifest.Chunks.Select(chunk => chunk.Id).Distinct(StringComparer.Ordinal).Count() != manifest.Chunks.Count ||
            manifest.Chunks.Any(chunk => !expectedIds.Contains(chunk.Id)))
        {
            throw new InvalidDataException("The backup chunk manifest does not exactly match metadata reachability.");
        }
        if (!manifest.Chunks.Select(chunk => chunk.Id).SequenceEqual(
                manifest.Chunks.Select(chunk => chunk.Id).Order(StringComparer.Ordinal),
                StringComparer.Ordinal))
        {
            throw new InvalidDataException("The backup chunk manifest is not in canonical order.");
        }

        var actualRequirements = BuildKeyRequirements(inspection.Inventory.ReachableChunkIds, options);
        if (!DictionaryEqual(manifest.KeyRequirements, actualRequirements))
            throw new InvalidDataException("The configured encryption keys do not satisfy the backup requirements.");

        long physicalBytes = actualMetadata.Length;
        var declaredPaths = new HashSet<string>(StringComparer.Ordinal);
        foreach (var chunk in manifest.Chunks)
        {
            cancellationToken.ThrowIfCancellationRequested();
            ValidateDigest(chunk.Sha256, $"chunk '{chunk.Id}'");
            if (chunk.Length <= 0)
                throw new InvalidDataException($"Chunk '{chunk.Id}' has an invalid physical length.");
            var path = GetChunkPath(Path.Combine(backup, "chunks"), chunk.Id);
            EnsureRegularFile(path, $"chunk '{chunk.Id}'");
            var actual = await HashFileAsync(path, cancellationToken);
            EnsureFileMatches($"chunk '{chunk.Id}'", new BackupFileEntry(chunk.Length, chunk.Sha256), actual);
            physicalBytes = checked(physicalBytes + actual.Length);
            declaredPaths.Add(Path.GetFullPath(path));
        }

        foreach (var path in EnumerateBackupChunkFiles(Path.Combine(backup, "chunks")))
        {
            if (!declaredPaths.Contains(Path.GetFullPath(path)))
                throw new InvalidDataException($"The backup contains an undeclared chunk file '{path}'.");
        }

        return new StorageBackupValidation(
            backup,
            manifest.CreatedAt,
            manifest.BlobRecordCount,
            manifest.StagedBlockCount,
            manifest.Chunks.Count,
            checked(manifest.LogicalBlobBytes + manifest.LogicalStagedBlockBytes),
            physicalBytes);
    }

    private static SortedDictionary<string, string> BuildKeyRequirements(
        IReadOnlySet<string> reachableChunkIds,
        SavaOptions options)
    {
        var requirements = new SortedDictionary<string, string>(StringComparer.Ordinal);
        foreach (var id in reachableChunkIds)
        {
            if (id.EndsWith("/$zero", StringComparison.Ordinal))
                continue;
            var domain = ChunkStore.GetDomainFromChunkId(id);
            if (string.Equals(domain, "$global", StringComparison.Ordinal))
            {
                var encoded = options.CrossAccountEncryptionKey
                    ?? throw new InvalidDataException("The backup needs the configured cross-account encryption key.");
                requirements["cross-account"] = FingerprintKey(encoded, "cross-account encryption key");
                continue;
            }
            if (domain.Contains("/$cpk-", StringComparison.Ordinal))
                continue;

            var account = domain.Split('/', 2)[0];
            if (!options.Accounts.TryGetValue(account, out var accountKey))
                throw new InvalidDataException($"The backup needs the configured key for account '{account}'.");
            requirements[$"account:{account}"] = FingerprintKey(accountKey, $"account '{account}' key");
        }
        return requirements;
    }

    private static string FingerprintKey(string encoded, string description)
    {
        try
        {
            return Convert.ToHexStringLower(SHA256.HashData(Convert.FromBase64String(encoded)));
        }
        catch (FormatException exception)
        {
            throw new InvalidDataException($"The configured {description} is not valid base64.", exception);
        }
    }

    private static bool DictionaryEqual(
        IReadOnlyDictionary<string, string> left,
        IReadOnlyDictionary<string, string> right) =>
        left.Count == right.Count && left.All(pair =>
            right.TryGetValue(pair.Key, out var value) &&
            string.Equals(pair.Value, value, StringComparison.Ordinal));

    private static async Task<BackupManifest> ReadManifestAsync(
        string backup,
        CancellationToken cancellationToken)
    {
        var path = Path.Combine(backup, ManifestFileName);
        EnsureRegularFile(path, "backup manifest");
        var length = new FileInfo(path).Length;
        if (length <= 0)
            throw new InvalidDataException("The backup manifest has an invalid size.");
        await using var input = new FileStream(
            path,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            64 * 1024,
            FileOptions.Asynchronous | FileOptions.SequentialScan);
        try
        {
            return await JsonSerializer.DeserializeAsync<BackupManifest>(input, JsonOptions, cancellationToken)
                ?? throw new InvalidDataException("The backup manifest is empty.");
        }
        catch (JsonException exception)
        {
            throw new InvalidDataException("The backup manifest is invalid JSON.", exception);
        }
    }

    private static async Task WriteManifestAsync(
        string path,
        BackupManifest manifest,
        CancellationToken cancellationToken)
    {
        var bytes = JsonSerializer.SerializeToUtf8Bytes(manifest, JsonOptions);
        await using var output = new FileStream(
            path,
            FileMode.CreateNew,
            FileAccess.Write,
            FileShare.None,
            64 * 1024,
            FileOptions.Asynchronous | FileOptions.WriteThrough);
        await output.WriteAsync(bytes, cancellationToken);
        await output.FlushAsync(cancellationToken);
        output.Flush(flushToDisk: true);
    }

    private static async Task<BackupFileEntry> CopyAndHashAsync(
        string source,
        string destination,
        CancellationToken cancellationToken)
    {
        EnsureRegularFile(source, "backup source file");
        await using var input = new FileStream(
            source,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            1024 * 1024,
            FileOptions.Asynchronous | FileOptions.SequentialScan);
        await using var output = new FileStream(
            destination,
            FileMode.CreateNew,
            FileAccess.Write,
            FileShare.None,
            1024 * 1024,
            FileOptions.Asynchronous | FileOptions.SequentialScan | FileOptions.WriteThrough);
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        var buffer = new byte[1024 * 1024];
        long length = 0;
        while (true)
        {
            var read = await input.ReadAsync(buffer, cancellationToken);
            if (read == 0)
                break;
            hash.AppendData(buffer, 0, read);
            await output.WriteAsync(buffer.AsMemory(0, read), cancellationToken);
            length = checked(length + read);
        }
        await output.FlushAsync(cancellationToken);
        output.Flush(flushToDisk: true);
        return new BackupFileEntry(length, Convert.ToHexStringLower(hash.GetHashAndReset()));
    }

    private static async Task<BackupFileEntry> HashFileAsync(string path, CancellationToken cancellationToken)
    {
        EnsureRegularFile(path, "backup file");
        await using var input = new FileStream(
            path,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            1024 * 1024,
            FileOptions.Asynchronous | FileOptions.SequentialScan);
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        var buffer = new byte[1024 * 1024];
        long length = 0;
        while (true)
        {
            var read = await input.ReadAsync(buffer, cancellationToken);
            if (read == 0)
                break;
            hash.AppendData(buffer, 0, read);
            length = checked(length + read);
        }
        return new BackupFileEntry(length, Convert.ToHexStringLower(hash.GetHashAndReset()));
    }

    private static void FlushFileToDisk(string path)
    {
        using var file = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.Read, bufferSize: 1);
        file.Flush(flushToDisk: true);
    }

    private static void EnsureFileMatches(string description, BackupFileEntry expected, BackupFileEntry actual)
    {
        ValidateDigest(expected.Sha256, description);
        if (expected.Length != actual.Length ||
            !CryptographicOperations.FixedTimeEquals(
                Convert.FromHexString(expected.Sha256),
                Convert.FromHexString(actual.Sha256)))
        {
            throw new InvalidDataException($"The {description} does not match its backup manifest.");
        }
    }

    private static void ValidateDigest(string digest, string description)
    {
        if (digest.Length != 64)
            throw new InvalidDataException($"The {description} has an invalid SHA-256 digest.");
        try
        {
            if (Convert.FromHexString(digest).Length != 32)
                throw new InvalidDataException($"The {description} has an invalid SHA-256 digest.");
        }
        catch (FormatException exception)
        {
            throw new InvalidDataException($"The {description} has an invalid SHA-256 digest.", exception);
        }
    }

    private static string GetChunkPath(string chunksRoot, string id)
    {
        if (string.IsNullOrWhiteSpace(id) || Path.IsPathRooted(id) || id.Contains("..", StringComparison.Ordinal))
            throw new InvalidDataException("A backup chunk identifier is invalid.");
        var relative = id.Replace('/', Path.DirectorySeparatorChar) + ".chunk";
        var path = Path.GetFullPath(Path.Combine(chunksRoot, relative));
        var root = Path.GetFullPath(chunksRoot) + Path.DirectorySeparatorChar;
        if (!path.StartsWith(root, StringComparison.Ordinal))
            throw new InvalidDataException("A backup chunk identifier escaped the content root.");
        return path;
    }

    private static IEnumerable<string> EnumerateBackupChunkFiles(string chunksRoot)
    {
        if (!Directory.Exists(chunksRoot))
            throw new InvalidDataException("The backup chunks directory is missing.");
        var pending = new Stack<string>();
        pending.Push(chunksRoot);
        while (pending.TryPop(out var directory))
        {
            EnsureNotReparsePoint(directory, "backup chunk directory");
            foreach (var child in Directory.EnumerateDirectories(directory))
                pending.Push(child);
            foreach (var file in Directory.EnumerateFiles(directory))
            {
                EnsureRegularFile(file, "backup chunk file");
                if (!file.EndsWith(".chunk", StringComparison.Ordinal))
                    throw new InvalidDataException($"The backup chunks directory contains an unexpected file '{file}'.");
                yield return file;
            }
        }
    }

    private static void EnsureRegularFile(string path, string description)
    {
        if (!File.Exists(path))
            throw new FileNotFoundException($"The {description} is missing.", path);
        if ((File.GetAttributes(path) & FileAttributes.ReparsePoint) != 0)
            throw new InvalidDataException($"The {description} must not be a symbolic link or reparse point.");
    }

    private static void EnsureNotReparsePoint(string path, string description)
    {
        if ((File.GetAttributes(path) & FileAttributes.ReparsePoint) != 0)
            throw new InvalidDataException($"The {description} must not be a symbolic link or reparse point.");
    }

    private static bool IsWithin(string candidate, string root)
    {
        var relative = Path.GetRelativePath(Path.GetFullPath(root), Path.GetFullPath(candidate));
        return !Path.IsPathRooted(relative) &&
               !string.Equals(relative, "..", StringComparison.Ordinal) &&
               !relative.StartsWith(".." + Path.DirectorySeparatorChar, StringComparison.Ordinal);
    }

    private sealed record BackupFileEntry(long Length, string Sha256);

    private sealed record BackupChunkEntry(string Id, long Length, string Sha256);

    private sealed record BackupManifest
    {
        public required string Format { get; init; }
        public required int FormatVersion { get; init; }
        public required int MetadataSchemaVersion { get; init; }
        public required DateTimeOffset CreatedAt { get; init; }
        public required BackupFileEntry Metadata { get; init; }
        public required int BlobRecordCount { get; init; }
        public required int StagedBlockCount { get; init; }
        public required long LogicalBlobBytes { get; init; }
        public required long LogicalStagedBlockBytes { get; init; }
        public required SortedDictionary<string, string> KeyRequirements { get; init; }
        public required List<BackupChunkEntry> Chunks { get; init; }
    }
}
