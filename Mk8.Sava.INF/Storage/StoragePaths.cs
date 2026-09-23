using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Mk8.Sava.Configuration;

namespace Mk8.Sava.Storage;

public sealed class StoragePaths : IStoragePaths, IDisposable
{
    private readonly Lock _directoryGate = new();
    private readonly HashSet<string> _durableDirectories = new(
        OperatingSystem.IsWindows() ? StringComparer.OrdinalIgnoreCase : StringComparer.Ordinal);
    private readonly FileStream _rootLease;

    public StoragePaths(IHostEnvironment environment, IOptions<SavaOptions> options)
    {
        ArgumentNullException.ThrowIfNull(environment);
        ArgumentNullException.ThrowIfNull(options);
        Root = ResolveRoot(environment.ContentRootPath, options.Value.DataPath);
        Chunks = Path.Combine(Root, "chunks");
        Packs = Path.Combine(Root, "packs");
        Staging = Path.Combine(Root, "staging");
        Database = Path.Combine(Root, "metadata.db");

        EnsureDurableDirectory(Root);
        EnsureDurableDirectory(Chunks);
        EnsureDurableDirectory(Packs);
        EnsureDurableDirectory(Staging);
        _rootLease = new FileStream(
            Path.Combine(Root, ".mk8-sava.lock"),
            FileMode.OpenOrCreate,
            FileAccess.ReadWrite,
            FileShare.None);
    }

    public string Root { get; }
    public string Chunks { get; }
    public string Packs { get; }
    public string Staging { get; }
    public string Database { get; }

    public void EnsureDurableDirectory(string directory)
    {
        var fullPath = Path.GetFullPath(directory);
        lock (_directoryGate)
            EnsureDurableDirectoryCore(fullPath);
    }

    internal void PublishStandaloneChunk(string source, string destination)
    {
        var fullPath = Path.GetFullPath(destination);
        EnsureChunkPath(fullPath);
        lock (_directoryGate)
        {
            EnsureDurableDirectoryCore(Path.GetDirectoryName(fullPath)!);
            StorageDurability.PublishFile(source, fullPath, overwrite: false);
        }
    }

    internal void PruneEmptyChunkDirectories(string chunkPath)
    {
        var fullPath = Path.GetFullPath(chunkPath);
        EnsureChunkPath(fullPath);
        var chunksRoot = Path.GetFullPath(Chunks);
        lock (_directoryGate)
        {
            var current = Path.GetDirectoryName(fullPath)!;
            while (!string.Equals(current, chunksRoot, PathComparison))
            {
                try
                {
                    Directory.Delete(current, recursive: false);
                }
                catch (DirectoryNotFoundException)
                {
                    _durableDirectories.Remove(current);
                    break;
                }
                catch (IOException)
                {
                    break;
                }

                _durableDirectories.Remove(current);
                var parent = Directory.GetParent(current)!.FullName;
                StorageDurability.FlushDirectory(parent);
                current = parent;
            }
        }
    }

    public int PruneLegacyEmptyChunkDirectories()
    {
        lock (_directoryGate)
        {
            if ((File.GetAttributes(Chunks) & FileAttributes.ReparsePoint) != FileAttributes.None)
                return 0;

            var removed = 0;
            var pending = new Stack<(string Path, bool ChildrenVisited)>();
            pending.Push((Chunks, false));
            while (pending.TryPop(out var entry))
            {
                if (!entry.ChildrenVisited)
                {
                    pending.Push((entry.Path, true));
                    foreach (var child in Directory.EnumerateDirectories(entry.Path))
                    {
                        if ((File.GetAttributes(child) & FileAttributes.ReparsePoint) == FileAttributes.None)
                            pending.Push((child, false));
                    }
                    continue;
                }

                if (string.Equals(entry.Path, Chunks, PathComparison))
                    continue;
                try
                {
                    Directory.Delete(entry.Path, recursive: false);
                }
                catch (IOException)
                {
                    continue;
                }
                catch (UnauthorizedAccessException)
                {
                    continue;
                }

                _durableDirectories.Remove(Path.GetFullPath(entry.Path));
                StorageDurability.FlushDirectory(Directory.GetParent(entry.Path)!.FullName);
                removed++;
            }

            return removed;
        }
    }

    private void EnsureDurableDirectoryCore(string fullPath)
    {
        var pending = new Stack<string>();
        var current = fullPath;
        while (!_durableDirectories.Contains(current))
        {
            pending.Push(current);
            var parent = Directory.GetParent(current)?.FullName;
            if (parent is null)
                break;
            current = parent;
        }

        while (pending.TryPop(out var path))
        {
            Directory.CreateDirectory(path);
            var parent = Directory.GetParent(path)?.FullName;
            if (parent is not null)
                StorageDurability.FlushDirectory(parent);
            _durableDirectories.Add(path);
        }
    }

    private void EnsureChunkPath(string fullPath)
    {
        var prefix = Path.GetFullPath(Chunks) + Path.DirectorySeparatorChar;
        if (!fullPath.StartsWith(prefix, PathComparison))
            throw new InvalidOperationException("The chunk path escaped the chunk storage root.");
    }

    private static StringComparison PathComparison => OperatingSystem.IsWindows()
        ? StringComparison.OrdinalIgnoreCase
        : StringComparison.Ordinal;

    public void Dispose() => _rootLease.Dispose();

    public static string ResolveRoot(string contentRootPath, string configuredPath) =>
        Path.GetFullPath(
            Path.IsPathRooted(configuredPath)
                ? configuredPath
                : Path.Combine(contentRootPath, configuredPath));
}
