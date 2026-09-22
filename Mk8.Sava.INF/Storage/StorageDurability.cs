using System.ComponentModel;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;

namespace Mk8.Sava.Storage;

internal static class StorageDurability
{
    [DllImport("libc", EntryPoint = "open", CharSet = CharSet.Ansi, SetLastError = true)]
    private static extern int OpenDirectory(string path, int flags);

    public static void FlushDirectory(string directory)
    {
        // Windows has no equivalent directory-handle fsync in this path.
        if (OperatingSystem.IsWindows())
            return;

        var descriptor = OpenDirectory(directory, flags: 0);
        if (descriptor < 0)
        {
            var error = Marshal.GetLastPInvokeError();
            throw new IOException(
                $"Could not open storage directory '{directory}' for a durability flush.",
                new Win32Exception(error));
        }

        using var handle = new SafeFileHandle(descriptor, ownsHandle: true);
        RandomAccess.FlushToDisk(handle);
    }

    public static void PublishFile(string source, string destination, bool overwrite)
    {
        File.Move(source, destination, overwrite);
        var destinationDirectory = Path.GetDirectoryName(destination)!;
        FlushDirectory(destinationDirectory);
        var sourceDirectory = Path.GetDirectoryName(source)!;
        if (!string.Equals(sourceDirectory, destinationDirectory, StringComparison.Ordinal))
            FlushDirectory(sourceDirectory);
    }
}
