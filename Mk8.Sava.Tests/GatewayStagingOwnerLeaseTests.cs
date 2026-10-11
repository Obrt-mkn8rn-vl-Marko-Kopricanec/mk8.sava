using System.ComponentModel;
using System.Diagnostics;
using System.Globalization;
using Microsoft.Win32.SafeHandles;
using Mk8.Sava.Hosting;
using Xunit.Abstractions;

namespace Mk8.Sava.Tests;

public sealed class GatewayStagingOwnerLeaseTests(ITestOutputHelper output)
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RuntimeBestEffortLockingCannotLetPeerStartupDeleteALiveOwner(bool disableRuntimeLocking)
    {
        var directory = Directory.CreateTempSubdirectory("mk8-sava-staging-lock-control-");
        try
        {
            var start = new ProcessStartInfo(Environment.GetEnvironmentVariable("DOTNET_HOST_PATH") ?? "dotnet")
            {
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                WorkingDirectory = AppContext.BaseDirectory,
            };
            start.ArgumentList.Add(typeof(TestProcessFixtureProgram).Assembly.Location);
            start.ArgumentList.Add("--gateway-staging-owner-fixture");
            start.ArgumentList.Add(directory.FullName);
            start.Environment["DOTNET_SYSTEM_IO_DISABLEFILELOCKING"] = disableRuntimeLocking ? "1" : "0";
            using var process = Process.Start(start)
                ?? throw new InvalidOperationException("The controlled staging-owner child did not start.");
            var (exitCode, standardOutput, standardError) = await TestProcessRunner.ObserveAsync(
                process, TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(5))
                .ConfigureAwait(true);
            output.WriteLine(string.Create(CultureInfo.InvariantCulture,
                $"Owner control: Exit={exitCode}; stdout={standardOutput}; stderr={standardError}"));
            Assert.Contains($"BclDuplicateOpened={disableRuntimeLocking && !OperatingSystem.IsWindows()}",
                standardOutput, StringComparison.Ordinal);
            Assert.Equal(0, exitCode);
            Assert.Contains("LiveOwnerPreserved=True", standardOutput, StringComparison.Ordinal);
            Assert.Contains("UnlockedOrphanReclaimed=True", standardOutput, StringComparison.Ordinal);
            Assert.Empty(standardError);
        }
        finally
        {
            directory.Delete(recursive: true);
        }
    }

    [Theory]
    [InlineData(FileMode.CreateNew)]
    [InlineData(FileMode.Open)]
    public void FailedConfirmationClosesThePendingDescriptor(FileMode mode)
    {
        var directory = Directory.CreateTempSubdirectory("mk8-sava-staging-lock-model-");
        try
        {
            var path = Path.Combine(directory.FullName, "owner");
            if (mode == FileMode.Open)
                File.WriteAllBytes(path, []);
            SafeFileHandle? captured = null;
            var expected = new IOException("injected confirmation failure");
            var failure = Assert.Throws<IOException>(() =>
            {
                using var contender = GatewayStagingOwnerLease.Acquire(path, mode, handle =>
                {
                    captured = handle;
                    throw expected;
                });
            });
            Assert.Same(expected, failure);
            Assert.NotNull(captured);
            Assert.True(captured.IsClosed);
            using var reopened = GatewayStagingOwnerLease.Acquire(path, FileMode.Open);
            Assert.False(reopened.SafeFileHandle.IsClosed);
        }
        finally
        {
            directory.Delete(recursive: true);
        }
    }

    [Theory]
    [InlineData(-1, 4)]
    [InlineData(-1, 13)]
    [InlineData(1, 95)]
    public void EveryUnconfirmedResultRefusesWithOriginalNativeError(int result, int error)
    {
        var failure = Assert.Throws<IOException>(() => GatewayStagingOwnerLease.EnsureSuccess(result, error));
        Assert.Equal(error, Assert.IsType<Win32Exception>(failure.InnerException).NativeErrorCode);
        Assert.Contains("exclusive Gateway staging ownership", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ConfirmedDescriptorTransfersToItsConcreteStreamOwner()
    {
        var directory = Directory.CreateTempSubdirectory("mk8-sava-staging-lock-model-");
        try
        {
            SafeFileHandle? confirmed = null;
            var owner = GatewayStagingOwnerLease.Acquire(Path.Combine(directory.FullName, "owner"), FileMode.CreateNew,
                handle => confirmed = handle);
            using (owner)
            {
                Assert.Same(confirmed, owner.SafeFileHandle);
                Assert.False(owner.SafeFileHandle.IsClosed);
                GatewayStagingOwnerLease.EnsureSuccess(0, 95);
            }
            Assert.NotNull(confirmed);
            Assert.True(confirmed.IsClosed);
        }
        finally
        {
            directory.Delete(recursive: true);
        }
    }
}
