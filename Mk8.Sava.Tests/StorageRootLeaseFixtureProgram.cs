using Microsoft.Extensions.FileProviders;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Mk8.Sava.Configuration;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

internal static class StorageRootLeaseFixtureProgram
{
    internal static async Task<int> RunAsync(string[] arguments)
    {
        if (arguments.Length != 2 || !Path.IsPathFullyQualified(arguments[1]) ||
            !Directory.Exists(arguments[1]) ||
            !Path.GetFileName(arguments[1]).StartsWith("mk8-sava-root-lock-control-", StringComparison.Ordinal) ||
            !string.Equals(Path.GetDirectoryName(arguments[1]),
                Path.TrimEndingDirectorySeparator(Path.GetFullPath(Path.GetTempPath())),
                OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal))
        {
            await Console.Error.WriteLineAsync("Invalid controlled root-lease invocation.").ConfigureAwait(false);
            return 64;
        }

        var root = arguments[1];
        var duplicateOpened = ProbeBclSharing(Path.Combine(root, "bcl-probe.lock"));
        await Console.Out.WriteLineAsync($"BclDuplicateOpened={duplicateOpened}").ConfigureAwait(false);
        try
        {
            using var paths = new StoragePaths(new TestEnvironment(root), Options.Create(new SavaOptions { DataPath = root }));
            await Console.Out.WriteLineAsync("lease-admitted").ConfigureAwait(false);
            return 0;
        }
        catch (StorageRootLeaseException exception)
        {
            await Console.Error.WriteLineAsync(exception.Message).ConfigureAwait(false);
            return 1;
        }
    }

    private static bool ProbeBclSharing(string path)
    {
        using var first = new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
        try
        {
            using var second = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
            return true;
        }
        catch (IOException)
        {
            return false;
        }
    }

    internal sealed class TestEnvironment(string root) : IHostEnvironment
    {
        public string EnvironmentName { get; set; } = Environments.Production;
        public string ApplicationName { get; set; } = nameof(StorageRootLeaseFixtureProgram);
        public string ContentRootPath { get; set; } = root;
        public IFileProvider ContentRootFileProvider { get; set; } = new NullFileProvider();
    }
}
