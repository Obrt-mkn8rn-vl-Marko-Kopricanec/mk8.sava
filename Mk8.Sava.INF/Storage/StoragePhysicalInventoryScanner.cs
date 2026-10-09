using Mk8.Sava.Protocol;

namespace Mk8.Sava.Storage;

// A scan is a sampled inventory, not an atomic filesystem snapshot. Each pass
// advances at most the configured number of directory or entry operations.
internal sealed class StoragePhysicalInventoryScanner(StoragePaths paths) : IDisposable
{
    private readonly string _chunkPrefix = paths.Chunks + Path.DirectorySeparatorChar;
    private readonly string _packPrefix = paths.Packs + Path.DirectorySeparatorChar;
    private readonly string _databaseWal = paths.Database + "-wal";
    private readonly string _databaseSharedMemory = paths.Database + "-shm";
    private readonly Stack<IEnumerator<FileSystemInfo>> _directories = new();
    private readonly HashSet<(uint Major, uint Minor, ulong Inode)> _hardLinks = [];
    private FileSystemInfo? _nextEntry = new DirectoryInfo(paths.Root);
    private bool _measureAllocation = OperatingSystem.IsLinux();
    private long? _allocatedBytes = OperatingSystem.IsLinux() ? 0 : null;
    private long _chunkBytes;
    private long _stagingBytes;
    private long _metadataBytes;
    private int _standaloneChunkCount;
    private bool _disposed;

    private static StringComparison PathComparison => OperatingSystem.IsWindows()
        ? StringComparison.OrdinalIgnoreCase
        : StringComparison.Ordinal;

    public int LastPassSteps { get; private set; }

    public bool Advance(int maximumSteps)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maximumSteps);
        ObjectDisposedException.ThrowIf(_disposed, this);

        LastPassSteps = 0;
        while (LastPassSteps < maximumSteps && (_nextEntry is not null || _directories.Count > 0))
        {
            if (_nextEntry is { } entry)
            {
                _nextEntry = null;
                MeasureEntry(entry);
            }
            else
            {
                AdvanceDirectory();
            }
            LastPassSteps++;
        }

        return _nextEntry is null && _directories.Count == 0;
    }

    public StoragePhysicalUsage ToPhysicalUsage(int packedChunkCount)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_nextEntry is not null || _directories.Count > 0)
            throw new InvalidOperationException("The physical inventory is incomplete.");

        return new StoragePhysicalUsage(
            _chunkBytes,
            _stagingBytes,
            _metadataBytes,
            checked(_standaloneChunkCount + packedChunkCount),
            _allocatedBytes);
    }

    private void AdvanceDirectory()
    {
        var entries = _directories.Peek();
        try
        {
            if (entries.MoveNext())
            {
                _nextEntry = entries.Current;
                return;
            }
        }
        catch (DirectoryNotFoundException)
        {
            // A concurrent reclamation removed this directory during the scan.
        }
        catch (FileNotFoundException)
        {
            // A concurrent reclamation removed this directory during the scan.
        }

        _directories.Pop().Dispose();
    }

    private void MeasureEntry(FileSystemInfo entry)
    {
        if (!TryGetAttributes(entry, out var attributes))
            return;

        var path = entry.FullName;
        MeasureAllocation(path);
        if ((attributes & FileAttributes.ReparsePoint) != FileAttributes.None)
            return;
        if ((attributes & FileAttributes.Directory) != FileAttributes.None)
        {
            QueueDirectory(path);
            return;
        }

        if (entry is FileInfo file && TryGetLength(file, out var length))
            ClassifyFile(path, length);
    }

    private static bool TryGetAttributes(FileSystemInfo entry, out FileAttributes attributes)
    {
        try
        {
            attributes = entry.Attributes;
            return true;
        }
        catch (FileNotFoundException)
        {
            attributes = default;
            return false;
        }
        catch (DirectoryNotFoundException)
        {
            attributes = default;
            return false;
        }
    }

    private void MeasureAllocation(string path)
    {
        if (_measureAllocation)
        {
            try
            {
                if (StorageAllocationMeter.TryStat(path, out var stat) &&
                    (stat.LinkCount <= 1 ||
                     _hardLinks.Add((stat.DeviceMajor, stat.DeviceMinor, stat.Inode))))
                {
                    _allocatedBytes = checked(_allocatedBytes!.Value + stat.AllocatedBytes);
                }
            }
            catch (Exception error) when (
                (error is EntryPointNotFoundException or DllNotFoundException or PlatformNotSupportedException) &&
                !CatastrophicExceptionPolicy.Contains(error))
            {
                _measureAllocation = false;
                _allocatedBytes = null;
                _hardLinks.Clear();
            }
        }
    }

    private void QueueDirectory(string path)
    {
        try
        {
            // Enumeration caches metadata. Reject directory-link replacements
            // observed between maintenance passes before descending into them.
            var attributes = File.GetAttributes(path);
            if ((attributes & (FileAttributes.Directory | FileAttributes.ReparsePoint)) != FileAttributes.Directory)
                return;
            _directories.Push(new DirectoryInfo(path).EnumerateFileSystemInfos().GetEnumerator());
        }
        catch (DirectoryNotFoundException)
        {
            // The directory disappeared between stat and enumeration.
        }
        catch (FileNotFoundException)
        {
            // The queued directory disappeared between maintenance passes.
        }
    }

    private static bool TryGetLength(FileInfo file, out long length)
    {
        try
        {
            length = file.Length;
            return true;
        }
        catch (FileNotFoundException)
        {
            length = 0;
            return false;
        }
        catch (DirectoryNotFoundException)
        {
            length = 0;
            return false;
        }
    }

    private void ClassifyFile(string path, long length)
    {
        if (path.StartsWith(_chunkPrefix, PathComparison) &&
            path.EndsWith(".chunk", StringComparison.Ordinal))
        {
            _chunkBytes = checked(_chunkBytes + length);
            _standaloneChunkCount++;
        }
        else if (path.StartsWith(_packPrefix, PathComparison) &&
                 path.EndsWith(".pack", StringComparison.Ordinal))
        {
            _chunkBytes = checked(_chunkBytes + length);
        }
        // FileSystemInfo.FullName and StoragePaths are already canonical paths.
        else if (Path.GetDirectoryName(path.AsSpan()).Equals(paths.Staging.AsSpan(), PathComparison))
        {
            _stagingBytes = checked(_stagingBytes + length);
        }
        else if (string.Equals(path, paths.Database, PathComparison) ||
                 string.Equals(path, _databaseWal, PathComparison) ||
                 string.Equals(path, _databaseSharedMemory, PathComparison))
        {
            _metadataBytes = checked(_metadataBytes + length);
        }
    }

    public void Dispose()
    {
        _disposed = true;
        _nextEntry = null;
        while (_directories.TryPop(out var entries))
            entries.Dispose();
    }
}
