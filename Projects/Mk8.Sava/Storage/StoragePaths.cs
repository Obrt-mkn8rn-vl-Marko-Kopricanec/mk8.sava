using Microsoft.Extensions.Options;
using Mk8.Sava.Configuration;

namespace Mk8.Sava.Storage;

public sealed class StoragePaths
{
    public StoragePaths(IHostEnvironment environment, IOptions<SavaOptions> options)
    {
        var configured = options.Value.DataPath;
        Root = Path.GetFullPath(
            Path.IsPathRooted(configured)
                ? configured
                : Path.Combine(environment.ContentRootPath, configured));
        Chunks = Path.Combine(Root, "chunks");
        Staging = Path.Combine(Root, "staging");
        Database = Path.Combine(Root, "metadata.db");

        Directory.CreateDirectory(Root);
        Directory.CreateDirectory(Chunks);
        Directory.CreateDirectory(Staging);
    }

    public string Root { get; }
    public string Chunks { get; }
    public string Staging { get; }
    public string Database { get; }
}

