namespace Mk8.Sava.Storage;

// A scan is a sampled inventory, not an atomic filesystem snapshot. Each pass
// advances at most the configured number of directory or entry operations.
internal sealed class StoragePhysicalInventoryScanner(StoragePaths paths) : IDisposable
{
    private readonly Stack<IEnumerator<string>> _directories = new();
    private readonly HashSet<(uint Major, uint Minor, ulong Inode)> _hardLinks = [];
    private string? _nextPath = paths.Root;
    private bool _measureAllocation = OperatingSystem.IsLinux();
    private long? _allocatedBytes = OperatingSystem.IsLinux() ? 0 : null;
    private long _chunkBytes;
    private long _stagingBytes;
    private long _metadataBytes;
    private int _standaloneChunkCount;

    private static StringComparison PathComparison => OperatingSystem.IsWindows()
        ? StringComparison.OrdinalIgnoreCase
        : StringComparison.Ordinal;

    public int LastPassSteps { get; private set; }

    public bool Advance(int maximumSteps)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maximumSteps);

        LastPassSteps = 0;
        while (LastPassSteps < maximumSteps && (_nextPath is not null || _directories.Count > 0))
        {
            if (_nextPath is { } path)
            {
                _nextPath = null;
                MeasureEntry(path);
            }
            else
            {
                AdvanceDirectory();
            }
            LastPassSteps++;
        }

        return _nextPath is null && _directories.Count == 0;
    }

    public StoragePhysicalUsage ToPhysicalUsage(int packedChunkCount)
    {
        if (_nextPath is not null || _directories.Count > 0)
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
                _nextPath = entries.Current;
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

    private void MeasureEntry(string path)
    {
        if (!TryGetAttributes(path, out var attributes))
            return;

        MeasureAllocation(path);
        if ((attributes & FileAttributes.ReparsePoint) != FileAttributes.None)
            return;
        if ((attributes & FileAttributes.Directory) != FileAttributes.None)
        {
            QueueDirectory(path);
            return;
        }

        if (TryGetLength(path, out var length))
            ClassifyFile(path, length);
    }

    private static bool TryGetAttributes(string path, out FileAttributes attributes)
    {
        try
        {
            attributes = File.GetAttributes(path);
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
            catch (Exception error) when (error is EntryPointNotFoundException or
                                          DllNotFoundException or
                                          PlatformNotSupportedException)
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
            _directories.Push(Directory.EnumerateFileSystemEntries(path).GetEnumerator());
        }
        catch (DirectoryNotFoundException)
        {
            // The directory disappeared between stat and enumeration.
        }
    }

    private static bool TryGetLength(string path, out long length)
    {
        try
        {
            length = new FileInfo(path).Length;
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
        if (path.StartsWith(paths.Chunks + Path.DirectorySeparatorChar, PathComparison) &&
            path.EndsWith(".chunk", StringComparison.Ordinal))
        {
            _chunkBytes = checked(_chunkBytes + length);
            _standaloneChunkCount++;
        }
        else if (path.StartsWith(paths.Packs + Path.DirectorySeparatorChar, PathComparison) &&
                 path.EndsWith(".pack", StringComparison.Ordinal))
        {
            _chunkBytes = checked(_chunkBytes + length);
        }
        else if (string.Equals(Path.GetDirectoryName(path), paths.Staging, PathComparison))
        {
            _stagingBytes = checked(_stagingBytes + length);
        }
        else if (string.Equals(path, paths.Database, PathComparison) ||
                 string.Equals(path, paths.Database + "-wal", PathComparison) ||
                 string.Equals(path, paths.Database + "-shm", PathComparison))
        {
            _metadataBytes = checked(_metadataBytes + length);
        }
    }

    public void Dispose()
    {
        while (_directories.TryPop(out var entries))
            entries.Dispose();
    }
}
