using Microsoft.Extensions.Options;
using Mk8.Sava.Configuration;
using Mk8.Sava.Hosting;
using Mk8.Sava.Protocol;

namespace Mk8.Sava.Tests;

internal static class GatewayStagingOwnerFixtureProgram
{
    internal static async Task<int> RunAsync(string[] arguments)
    {
        if (arguments.Length != 2 || !Path.IsPathFullyQualified(arguments[1]) || !Directory.Exists(arguments[1]) ||
            !Path.GetFileName(arguments[1]).StartsWith("mk8-sava-staging-lock-control-", StringComparison.Ordinal) ||
            !string.Equals(Path.GetDirectoryName(arguments[1]),
                Path.TrimEndingDirectorySeparator(Path.GetFullPath(Path.GetTempPath())),
                OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal))
        {
            await Console.Error.WriteLineAsync("Invalid controlled staging-owner invocation.").ConfigureAwait(false);
            return 64;
        }

        var root = arguments[1];
        await Console.Out.WriteLineAsync($"BclDuplicateOpened={ProbeBclSharing(Path.Combine(root, "bcl-probe.lock"))}")
            .ConfigureAwait(false);
        try
        {
            await PreserveLiveOwnerAsync(root).ConfigureAwait(false);
            var orphan = Path.Combine(root, "scratch", "gw-" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(orphan);
            await File.WriteAllBytesAsync(Path.Combine(orphan, ".owner"), GatewayStagingCleanup.OwnerMarker.ToArray())
                .ConfigureAwait(false);
            await File.WriteAllTextAsync(Path.Combine(orphan, "orphan-payload"), "owned fixture").ConfigureAwait(false);
            GatewayStagingCleanup.RemoveOrphans(Path.Combine(root, "scratch"));
            Assert.False(Directory.Exists(orphan));
            await Console.Out.WriteLineAsync("UnlockedOrphanReclaimed=True").ConfigureAwait(false);
            return 0;
        }
        catch (Exception failure) when (!CatastrophicExceptionPolicy.Contains(failure))
        {
            await Console.Error.WriteLineAsync(failure.ToString()).ConfigureAwait(false);
            return 1;
        }
    }

    private static async Task PreserveLiveOwnerAsync(string root)
    {
        GatewayStagingPaths? first = null;
        GatewayStagingPaths? second = null;
        Stream? payload = null;
        var primary = await Record.ExceptionAsync(async () =>
        {
            first = Create(root);
            var payloadPath = Path.Combine(first.Staging, "live-payload");
            payload = first.OpenTemporaryFile(payloadPath);
            await payload.WriteAsync(new byte[] { 2, 3, 5, 7 }).ConfigureAwait(false);
            await payload.FlushAsync().ConfigureAwait(false);
            second = Create(root);
            Assert.True(Directory.Exists(first.Staging));
            Assert.True(File.Exists(payloadPath));
            Assert.True(File.Exists(Path.Combine(first.Staging, ".owner")));
            payload.Position = 0;
            var actual = new byte[4];
            await payload.ReadExactlyAsync(actual).ConfigureAwait(false);
            Assert.Equal(new byte[] { 2, 3, 5, 7 }, actual);
            Assert.Equal(4, first.ReservedBytes);
            Assert.Equal(0, second.ReservedBytes);
            await Console.Out.WriteLineAsync("LiveOwnerPreserved=True").ConfigureAwait(false);
        }).ConfigureAwait(false);

        var failures = new List<Exception>();
        if (primary is not null)
            failures.Add(primary);
        var payloadFailure = await Record.ExceptionAsync(async () =>
        {
            if (payload is not null)
                await payload.DisposeAsync().ConfigureAwait(false);
        }).ConfigureAwait(false);
        if (payloadFailure is not null)
            failures.Add(payloadFailure);
        AddFailure(failures, Record.Exception(() => second?.Dispose()));
        AddFailure(failures, Record.Exception(() => first?.Dispose()));
        if (failures.Count != 0)
            throw new AggregateException(failures);
    }

    private static void AddFailure(List<Exception> failures, Exception? failure)
    {
        if (failure is not null)
            failures.Add(failure);
    }

    private static GatewayStagingPaths Create(string root) => new(
        new StorageRootLeaseFixtureProgram.TestEnvironment(root),
        Options.Create(new GatewayOptions { StagingPath = Path.Combine(root, "scratch"), MaximumStagingBytes = 8 }),
        Options.Create(new SavaOptions { DataPath = Path.Combine(root, "application-data") }));

    private static bool ProbeBclSharing(string path)
    {
        using var first = new FileStream(path, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None);
        try
        {
            using var contender = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
            return true;
        }
        catch (IOException)
        {
            return false;
        }
    }
}
