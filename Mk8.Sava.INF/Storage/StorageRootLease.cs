using System.ComponentModel;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;

namespace Mk8.Sava.Storage;

internal static class StorageRootLease
{
    private const int ExclusiveNonblocking = 2 | 4;

    internal static FileStream Acquire(string path) => Acquire(path, ConfirmExclusiveLock);

    internal static FileStream Acquire(string path, Action<SafeFileHandle> confirm)
    {
        var lease = new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
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
            return; // FileShare.None is mandatory sharing admission on Windows.

        // Unix FileStream locking is best-effort and can be disabled. Admit only
        // an explicitly confirmed descriptor-owned lock, retained until close.
        var result = Flock(handle, ExclusiveNonblocking);
        var error = Marshal.GetLastPInvokeError();
        EnsureSuccess(result, error);
    }

    internal static void EnsureSuccess(int result, int error)
    {
        if (result != 0)
            throw new IOException("Unable to confirm an exclusive data-root lock.", new Win32Exception(error));
    }

    [DefaultDllImportSearchPaths(DllImportSearchPath.SafeDirectories)]
    [DllImport("libc", EntryPoint = "flock", ExactSpelling = true, SetLastError = true)]
    private static extern int Flock(SafeFileHandle descriptor, int operation);
}
