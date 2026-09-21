using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Mk8.Sava.Configuration;

namespace Mk8.Sava.Storage;

public sealed class StoragePaths : IStoragePaths
{
    public StoragePaths(IHostEnvironment environment, IOptions<SavaOptions> options)
    {
        Root = ResolveRoot(environment.ContentRootPath, options.Value.DataPath);
        Chunks = Path.Combine(Root, "chunks");
        Packs = Path.Combine(Root, "packs");
        Staging = Path.Combine(Root, "staging");
        Database = Path.Combine(Root, "metadata.db");

        Directory.CreateDirectory(Root);
        Directory.CreateDirectory(Chunks);
        Directory.CreateDirectory(Packs);
        Directory.CreateDirectory(Staging);
    }

    public string Root { get; }
    public string Chunks { get; }
    public string Packs { get; }
    public string Staging { get; }
    public string Database { get; }

    public static string ResolveRoot(string contentRootPath, string configuredPath) =>
        Path.GetFullPath(
            Path.IsPathRooted(configuredPath)
                ? configuredPath
                : Path.Combine(contentRootPath, configuredPath));
}
