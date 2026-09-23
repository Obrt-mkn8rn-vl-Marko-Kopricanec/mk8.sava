using System.Text.Json;
using Microsoft.Extensions.Options;
using Mk8.Sava.Configuration;

namespace Mk8.Sava.Storage;

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
        StorageDurability.EnsureDirectory(parent);
        var temporary = Path.Combine(parent, $".{Path.GetFileName(destination)}.creating-{Guid.NewGuid():N}");
        StorageDurability.EnsureDirectory(Path.Combine(temporary, "chunks"));
        try
        {
            var metadataPath = Path.Combine(temporary, MetadataFileName);
            using var snapshot = await metadata.CreateBackupSnapshotAsync(
                metadataPath,
                chunks.PinChunkIds,
                cancellationToken).ConfigureAwait(false);
            await MetadataStore.NormalizePackedLocationsForStandaloneBackupAsync(
                metadataPath,
                cancellationToken).ConfigureAwait(false);
            FlushFileToDisk(metadataPath);
            StorageDurability.FlushDirectory(temporary);
            var chunkEntries = await CopyChunksForBackupAsync(
                snapshot.Inventory.ReachableChunkIds,
                temporary,
                cancellationToken).ConfigureAwait(false);

            var metadataEntry = await HashFileAsync(metadataPath, cancellationToken).ConfigureAwait(false);
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
            await WriteManifestAsync(Path.Combine(temporary, ManifestFileName), manifest, cancellationToken).ConfigureAwait(false);
            StorageDurability.FlushDirectory(temporary);
            var validation = await ValidateCoreAsync(temporary, _options, cancellationToken).ConfigureAwait(false);
            StorageDurability.PublishDirectory(temporary, destination);
            return validation with { BackupPath = destination };
        }
        catch
        {
            if (Directory.Exists(temporary))
                Directory.Delete(temporary, recursive: true);
            throw;
        }
    }

    private async Task<List<BackupChunkEntry>> CopyChunksForBackupAsync(
        IReadOnlySet<string> reachableChunkIds,
        string temporary,
        CancellationToken cancellationToken)
    {
        var chunkEntries = new List<BackupChunkEntry>();
        foreach (var id in reachableChunkIds
                     .Where(id => !id.EndsWith("/$zero", StringComparison.Ordinal))
                     .Order(StringComparer.Ordinal))
        {
            cancellationToken.ThrowIfCancellationRequested();
            var status = await chunks.VerifyChunkAsync(id, cancellationToken).ConfigureAwait(false);
            if (status is ChunkIntegrityStatus.Missing or ChunkIntegrityStatus.Corrupt)
                throw new InvalidDataException($"Cannot back up chunk '{id}' because its integrity status is {status}.");

            var destinationChunk = GetChunkPath(Path.Combine(temporary, "chunks"), id);
            StorageDurability.EnsureDirectory(Path.GetDirectoryName(destinationChunk)!);
            await chunks.CopyChunkFileForBackupAsync(id, destinationChunk, cancellationToken).ConfigureAwait(false);
            StorageDurability.FlushDirectory(Path.GetDirectoryName(destinationChunk)!);
            var copied = await HashFileAsync(destinationChunk, cancellationToken).ConfigureAwait(false);
            chunkEntries.Add(new BackupChunkEntry(id, copied.Length, copied.Sha256));
        }
        return chunkEntries;
    }

    public Task<StorageBackupValidation> ValidateAsync(
        string backupPath,
        CancellationToken cancellationToken) =>
        ValidateCoreAsync(Path.GetFullPath(backupPath), _options, cancellationToken);

    public static Task<StorageBackupValidation> ValidateBackupAsync(
        string backupPath,
        SavaOptions options,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(options);
        return ValidateCoreAsync(Path.GetFullPath(backupPath), options, cancellationToken);
    }

    public static async Task<StorageBackupValidation> RestoreAsync(
        string backupPath,
        string targetDataPath,
        SavaOptions options,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(options);
        var backup = Path.GetFullPath(backupPath);
        var target = Path.GetFullPath(targetDataPath);
        if (Directory.Exists(target) || File.Exists(target))
            throw new IOException($"The restore target '{target}' already exists; restore requires a new offline target.");
        if (IsWithin(target, backup) || IsWithin(backup, target))
            throw new InvalidOperationException("The backup and restore target must not contain one another.");

        var validation = await ValidateCoreAsync(backup, options, cancellationToken).ConfigureAwait(false);
        var manifest = await ReadManifestAsync(backup, cancellationToken).ConfigureAwait(false);
        var parent = Directory.GetParent(target)?.FullName
            ?? throw new InvalidOperationException("The restore target has no parent directory.");
        StorageDurability.EnsureDirectory(parent);
        var temporary = Path.Combine(parent, $".{Path.GetFileName(target)}.restoring-{Guid.NewGuid():N}");
        StorageDurability.EnsureDirectory(Path.Combine(temporary, "chunks"));
        StorageDurability.EnsureDirectory(Path.Combine(temporary, "staging"));
        try
        {
            var metadataCopy = await CopyAndHashAsync(
                Path.Combine(backup, MetadataFileName),
                Path.Combine(temporary, MetadataFileName),
                cancellationToken).ConfigureAwait(false);
            EnsureFileMatches("metadata database", manifest.Metadata, metadataCopy);
#pragma warning disable HLQ012 // A Span enumerator cannot live across awaited file copies.
            foreach (var chunk in manifest.Chunks)
#pragma warning restore HLQ012
            {
                cancellationToken.ThrowIfCancellationRequested();
                var source = GetChunkPath(Path.Combine(backup, "chunks"), chunk.Id);
                var destination = GetChunkPath(Path.Combine(temporary, "chunks"), chunk.Id);
                StorageDurability.EnsureDirectory(Path.GetDirectoryName(destination)!);
                var copied = await CopyAndHashAsync(source, destination, cancellationToken).ConfigureAwait(false);
                EnsureFileMatches($"chunk '{chunk.Id}'", new BackupFileEntry(chunk.Length, chunk.Sha256), copied);
            }

            var inspection = await MetadataStore.InspectDatabaseAsync(
                Path.Combine(temporary, MetadataFileName),
                cancellationToken).ConfigureAwait(false);
            if (inspection.SchemaVersion != manifest.MetadataSchemaVersion)
                throw new InvalidDataException("The restored metadata schema version changed while copying the backup.");
            StorageDurability.PublishDirectory(temporary, target);
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
        var manifest = await ReadManifestAsync(backup, cancellationToken).ConfigureAwait(false);
        ValidateManifestHeaderAndRoot(backup, manifest);

        var metadataPath = Path.Combine(backup, MetadataFileName);
        EnsureRegularFile(metadataPath, "metadata database");
        var actualMetadata = await HashFileAsync(metadataPath, cancellationToken).ConfigureAwait(false);
        EnsureFileMatches("metadata database", manifest.Metadata, actualMetadata);
        var inspection = await MetadataStore.InspectDatabaseAsync(metadataPath, cancellationToken).ConfigureAwait(false);
        ValidateMetadataAgainstManifest(inspection, manifest, options);
        ValidateChunkManifest(inspection.Inventory.ReachableChunkIds, manifest, options);
        var physicalBytes = await VerifyChunkFilesAsync(
            backup, manifest.Chunks, actualMetadata.Length, cancellationToken).ConfigureAwait(false);

        return new StorageBackupValidation(
            backup,
            manifest.CreatedAt,
            manifest.BlobRecordCount,
            manifest.StagedBlockCount,
            manifest.Chunks.Count,
            checked(manifest.LogicalBlobBytes + manifest.LogicalStagedBlockBytes),
            physicalBytes);
    }

    private static void ValidateManifestHeaderAndRoot(string backup, BackupManifest manifest)
    {
        if (manifest.Metadata is null || manifest.KeyRequirements is null || manifest.Chunks is null)
            throw new InvalidDataException("The backup manifest is missing required fields.");
        if (!string.Equals(manifest.Format, BackupFormat, StringComparison.Ordinal) ||
            manifest.FormatVersion != BackupFormatVersion)
        {
            throw new InvalidDataException("The backup format or version is unsupported.");
        }
        if (manifest.MetadataSchemaVersion is < 1 or > MetadataStore.CurrentSchemaVersion)
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
    }

    private static void ValidateMetadataAgainstManifest(
        MetadataDatabaseInspection inspection,
        BackupManifest manifest,
        SavaOptions options)
    {
        if (inspection.SchemaVersion != manifest.MetadataSchemaVersion)
            throw new InvalidDataException("The backup manifest and metadata database schema versions do not match.");
        foreach (var (account, recordedMode) in inspection.AccountNamespaceModes)
        {
            if (options.Accounts.ContainsKey(account) &&
                (options.AccountCapabilities.TryGetValue(account, out var capabilities) &&
                 capabilities.HierarchicalNamespaceEnabled) != recordedMode)
            {
                throw new InvalidDataException(
                    $"The configured hierarchical namespace mode for account '{account}' differs from the backed-up mode.");
            }
        }
        if (inspection.Inventory.BlobRecordCount != manifest.BlobRecordCount ||
            inspection.Inventory.StagedBlockCount != manifest.StagedBlockCount ||
            inspection.Inventory.LogicalBlobBytes != manifest.LogicalBlobBytes ||
            inspection.Inventory.LogicalStagedBlockBytes != manifest.LogicalStagedBlockBytes)
        {
            throw new InvalidDataException("The backup manifest does not match the metadata inventory.");
        }
    }

    private static void ValidateChunkManifest(
        IReadOnlySet<string> reachableChunkIds,
        BackupManifest manifest,
        SavaOptions options)
    {
        var expectedIds = reachableChunkIds
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

        var actualRequirements = BuildKeyRequirements(reachableChunkIds, options);
        if (!DictionaryEqual(manifest.KeyRequirements, actualRequirements))
            throw new InvalidDataException("The configured encryption keys do not satisfy the backup requirements.");
    }

    private static async Task<long> VerifyChunkFilesAsync(
        string backup,
        List<BackupChunkEntry> chunks,
        long metadataLength,
        CancellationToken cancellationToken)
    {
        long physicalBytes = metadataLength;
        var declaredPaths = new HashSet<string>(StringComparer.Ordinal);
#pragma warning disable HLQ012 // A Span enumerator cannot live across awaited file verification.
        foreach (var chunk in chunks)
#pragma warning restore HLQ012
        {
            cancellationToken.ThrowIfCancellationRequested();
            ValidateDigest(chunk.Sha256, $"chunk '{chunk.Id}'");
            if (chunk.Length <= 0)
                throw new InvalidDataException($"Chunk '{chunk.Id}' has an invalid physical length.");
            var path = GetChunkPath(Path.Combine(backup, "chunks"), chunk.Id);
            EnsureRegularFile(path, $"chunk '{chunk.Id}'");
            var actual = await HashFileAsync(path, cancellationToken).ConfigureAwait(false);
            EnsureFileMatches($"chunk '{chunk.Id}'", new BackupFileEntry(chunk.Length, chunk.Sha256), actual);
            physicalBytes = checked(physicalBytes + actual.Length);
            declaredPaths.Add(Path.GetFullPath(path));
        }

        foreach (var path in EnumerateBackupChunkFiles(Path.Combine(backup, "chunks")))
        {
            if (!declaredPaths.Contains(Path.GetFullPath(path)))
                throw new InvalidDataException($"The backup contains an undeclared chunk file '{path}'.");
        }

        return physicalBytes;
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
            var dataKey = options.ResolveAccountDataEncryptionKey(account);
            requirements[$"account:{account}"] = FingerprintKey(dataKey, $"account '{account}' data encryption key");
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
        SortedDictionary<string, string> left,
        SortedDictionary<string, string> right) =>
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
        var input = new FileStream(
            path,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            64 * 1024,
            FileOptions.Asynchronous | FileOptions.SequentialScan);
        await using var inputDisposal = input.ConfigureAwait(false);
        try
        {
            return await JsonSerializer.DeserializeAsync<BackupManifest>(input, JsonOptions, cancellationToken).ConfigureAwait(false)
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
        var output = new FileStream(
            path,
            FileMode.CreateNew,
            FileAccess.Write,
            FileShare.None,
            64 * 1024,
            FileOptions.Asynchronous | FileOptions.WriteThrough);
        await using var outputDisposal = output.ConfigureAwait(false);
        await output.WriteAsync(bytes, cancellationToken).ConfigureAwait(false);
        await output.FlushAsync(cancellationToken).ConfigureAwait(false);
        StorageDurability.FlushFileToDisk(output);
    }

    private static async Task<BackupFileEntry> CopyAndHashAsync(
        string source,
        string destination,
        CancellationToken cancellationToken)
    {
        EnsureRegularFile(source, "backup source file");
        var input = new FileStream(
            source,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            1024 * 1024,
            FileOptions.Asynchronous | FileOptions.SequentialScan);
        await using var inputDisposal = input.ConfigureAwait(false);
        var output = new FileStream(
            destination,
            FileMode.CreateNew,
            FileAccess.Write,
            FileShare.None,
            1024 * 1024,
            FileOptions.Asynchronous | FileOptions.SequentialScan | FileOptions.WriteThrough);
        await using var outputDisposal = output.ConfigureAwait(false);
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        var buffer = new byte[1024 * 1024];
        long length = 0;
        while (true)
        {
            var read = await input.ReadAsync(buffer, cancellationToken).ConfigureAwait(false);
            if (read == 0)
                break;
            hash.AppendData(buffer, 0, read);
            await output.WriteAsync(buffer.AsMemory(0, read), cancellationToken).ConfigureAwait(false);
            length = checked(length + read);
        }
        await output.FlushAsync(cancellationToken).ConfigureAwait(false);
        StorageDurability.FlushFileToDisk(output);
        StorageDurability.FlushDirectory(Path.GetDirectoryName(destination)!);
        return new BackupFileEntry(length, Convert.ToHexStringLower(hash.GetHashAndReset()));
    }

    private static async Task<BackupFileEntry> HashFileAsync(string path, CancellationToken cancellationToken)
    {
        EnsureRegularFile(path, "backup file");
        var input = new FileStream(
            path,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            1024 * 1024,
            FileOptions.Asynchronous | FileOptions.SequentialScan);
        await using var inputDisposal = input.ConfigureAwait(false);
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        var buffer = new byte[1024 * 1024];
        long length = 0;
        while (true)
        {
            var read = await input.ReadAsync(buffer, cancellationToken).ConfigureAwait(false);
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
        StorageDurability.FlushFileToDisk(file);
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
        if ((File.GetAttributes(path) & FileAttributes.ReparsePoint) != FileAttributes.None)
            throw new InvalidDataException($"The {description} must not be a symbolic link or reparse point.");
    }

    private static void EnsureNotReparsePoint(string path, string description)
    {
        if ((File.GetAttributes(path) & FileAttributes.ReparsePoint) != FileAttributes.None)
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
