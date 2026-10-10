using System.Diagnostics;
using System.Globalization;
using Microsoft.Extensions.Options;
using Mk8.Sava.Configuration;
using Mk8.Sava.Storage;
using Xunit.Abstractions;

namespace Mk8.Sava.Tests;

public sealed class StorageRootLeaseConfirmationTests(ITestOutputHelper output)
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task DisablingRuntimeBestEffortLocksCannotAdmitAContender(bool disableRuntimeLocking)
    {
        var directory = Directory.CreateTempSubdirectory("mk8-sava-root-lock-control-");
        try
        {
            var options = Options.Create(new SavaOptions { DataPath = directory.FullName });
            using (var holder = new StoragePaths(new StorageRootLeaseFixtureProgram.TestEnvironment(directory.FullName), options))
            {
                var (heldExit, heldOutput, heldError) = await RunAsync(directory.FullName, disableRuntimeLocking).ConfigureAwait(true);
                output.WriteLine(string.Create(CultureInfo.InvariantCulture,
                    $"Held root: Exit={heldExit}; stdout={heldOutput}; stderr={heldError}"));
                Assert.Contains($"BclDuplicateOpened={disableRuntimeLocking && !OperatingSystem.IsWindows()}",
                    heldOutput, StringComparison.Ordinal);
                Assert.Equal(1, heldExit);
                Assert.DoesNotContain("lease-admitted", heldOutput, StringComparison.Ordinal);
                Assert.Contains("Unable to acquire the exclusive data-root lease", heldError, StringComparison.Ordinal);
                Assert.False(File.Exists(holder.Database));
            }

            var (releasedExit, releasedOutput, releasedError) = await RunAsync(directory.FullName, disableRuntimeLocking).ConfigureAwait(true);
            output.WriteLine(string.Create(CultureInfo.InvariantCulture,
                $"Released root: Exit={releasedExit}; stdout={releasedOutput}; stderr={releasedError}"));
            Assert.Equal(0, releasedExit);
            Assert.Contains("lease-admitted", releasedOutput, StringComparison.Ordinal);
            Assert.Empty(releasedError);
            using var fresh = new StoragePaths(new StorageRootLeaseFixtureProgram.TestEnvironment(directory.FullName), options);
            Assert.False(File.Exists(fresh.Database));
        }
        finally
        {
            directory.Delete(recursive: true);
        }
    }

    private static async Task<(int ExitCode, string StandardOutput, string StandardError)> RunAsync(string root, bool disableRuntimeLocking)
    {
        var start = new ProcessStartInfo(Environment.GetEnvironmentVariable("DOTNET_HOST_PATH") ?? "dotnet")
        {
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            WorkingDirectory = AppContext.BaseDirectory,
        };
        start.ArgumentList.Add(typeof(TestProcessFixtureProgram).Assembly.Location);
        start.ArgumentList.Add("--storage-root-lease-fixture");
        start.ArgumentList.Add(root);
        start.Environment["DOTNET_SYSTEM_IO_DISABLEFILELOCKING"] = disableRuntimeLocking ? "1" : "0";
        using var process = Process.Start(start)
            ?? throw new InvalidOperationException("The controlled root-lease child did not start.");
        return await TestProcessRunner.ObserveAsync(process, TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(5))
            .ConfigureAwait(false);
    }
}
