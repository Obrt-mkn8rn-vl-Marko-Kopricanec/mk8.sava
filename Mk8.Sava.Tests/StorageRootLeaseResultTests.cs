using System.ComponentModel;
using Microsoft.Win32.SafeHandles;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

public sealed class StorageRootLeaseResultTests
{
    [Theory]
    [InlineData(-1, 95)]
    [InlineData(-1, 13)]
    [InlineData(-1, 4)]
    [InlineData(1, 0)]
    public void UnconfirmedResultRefusesAdmissionAndClosesThePendingHandle(int result, int error)
    {
        WithPath(path =>
        {
            SafeFileHandle? pending = null;
            var failure = Assert.Throws<IOException>(() =>
            {
                using var unexpected = StorageRootLease.Acquire(path, handle =>
                {
                    pending = handle;
                    Assert.False(handle.IsClosed);
                    StorageRootLease.EnsureSuccess(result, error);
                });
            });

            Assert.Equal(error, Assert.IsType<Win32Exception>(failure.InnerException).NativeErrorCode);
            Assert.NotNull(pending);
            Assert.True(pending.IsClosed);
            using var fresh = StorageRootLease.Acquire(path);
            Assert.False(fresh.SafeFileHandle.IsClosed);
        });
    }

    [Fact]
    public void ConfirmedResultTransfersTheSameHandleUntilOwnedDisposal()
    {
        WithPath(path =>
        {
            SafeFileHandle? confirmed = null;
            using (var lease = StorageRootLease.Acquire(path, handle =>
            {
                confirmed = handle;
                StorageRootLease.EnsureSuccess(0, 95); // Success does not interpret stale errno.
            }))
            {
                Assert.Same(confirmed, lease.SafeFileHandle);
                Assert.False(lease.SafeFileHandle.IsClosed);
            }

            Assert.NotNull(confirmed);
            Assert.True(confirmed.IsClosed);
            using var fresh = StorageRootLease.Acquire(path);
            Assert.False(fresh.SafeFileHandle.IsClosed);
        });
    }

    [Fact]
    public void ConfirmationFailureRetainsIdentityAndReleasesThePendingHandle()
    {
        WithPath(path =>
        {
            var expected = new IOException("controlled confirmation failure");
            SafeFileHandle? pending = null;
            var actual = Assert.Throws<IOException>(() =>
            {
                using var unexpected = StorageRootLease.Acquire(path, handle =>
                {
                    pending = handle;
                    throw expected;
                });
            });

            Assert.Same(expected, actual);
            Assert.NotNull(pending);
            Assert.True(pending.IsClosed);
            using var fresh = StorageRootLease.Acquire(path);
            Assert.False(fresh.SafeFileHandle.IsClosed);
        });
    }

    private static void WithPath(Action<string> control)
    {
        var directory = Directory.CreateTempSubdirectory("mk8-sava-root-lock-result-");
        try
        {
            control(Path.Combine(directory.FullName, "owned.lock"));
        }
        finally
        {
            directory.Delete(recursive: true);
        }
    }
}
