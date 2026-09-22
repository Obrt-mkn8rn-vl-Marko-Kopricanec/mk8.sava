using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Mk8.Sava.Configuration;

namespace Mk8.Sava.Storage;

public sealed class StoragePaths : IStoragePaths, IDisposable
{
    private readonly object _directoryGate = new();
    private readonly HashSet<string> _durableDirectories = new(
        OperatingSystem.IsWindows() ? StringComparer.OrdinalIgnoreCase : StringComparer.Ordinal);
    private readonly FileStream _rootLease;

    public StoragePaths(IHostEnvironment environment, IOptions<SavaOptions> options)
    {
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
    }

    public void Dispose() => _rootLease.Dispose();

    public static string ResolveRoot(string contentRootPath, string configuredPath) =>
        Path.GetFullPath(
            Path.IsPathRooted(configuredPath)
                ? configuredPath
                : Path.Combine(contentRootPath, configuredPath));
}
