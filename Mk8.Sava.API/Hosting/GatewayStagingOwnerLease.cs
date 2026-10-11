using System.ComponentModel;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;

namespace Mk8.Sava.Hosting;

internal static class GatewayStagingOwnerLease
{
    private const int ExclusiveNonblocking = 2 | 4;

    internal static FileStream Acquire(string path, FileMode mode) => Acquire(path, mode, ConfirmExclusiveLock);

    internal static FileStream Acquire(string path, FileMode mode, Action<SafeFileHandle> confirm)
    {
        var lease = new FileStream(path, mode, FileAccess.ReadWrite, FileShare.None);
        var transferred = false;
        try
        {
            confirm(lease.SafeFileHandle);
            transferred = true;
            return lease;
        }
        finally
        {
            if (!transferred)
                lease.Dispose();
        }
    }

    private static void ConfirmExclusiveLock(SafeFileHandle handle)
    {
        if (OperatingSystem.IsWindows())
            return; // Mandatory FileShare.None admission remains authoritative on Windows.

        // Both live ownership and orphan inspection require the same confirmed
        // descriptor-owned lock, even when Unix FileStream locking is disabled.
        var result = Flock(handle, ExclusiveNonblocking);
        var error = Marshal.GetLastPInvokeError();
        EnsureSuccess(result, error);
    }

    internal static void EnsureSuccess(int result, int error)
    {
        if (result != 0)
            throw new IOException("Unable to confirm exclusive Gateway staging ownership.", new Win32Exception(error));
    }

    [DefaultDllImportSearchPaths(DllImportSearchPath.SafeDirectories)]
    [DllImport("libc", EntryPoint = "flock", ExactSpelling = true, SetLastError = true)]
    private static extern int Flock(SafeFileHandle descriptor, int operation);
}
