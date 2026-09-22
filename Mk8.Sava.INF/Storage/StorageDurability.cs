using System.ComponentModel;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;

namespace Mk8.Sava.Storage;

internal static class StorageDurability
{
    private const uint MoveFileReplaceExisting = 0x1;
    private const uint MoveFileWriteThrough = 0x8;

    [DllImport("libc", EntryPoint = "open", CharSet = CharSet.Ansi, SetLastError = true)]
    private static extern int OpenDirectory(string path, int flags);

    [DllImport("kernel32.dll", EntryPoint = "MoveFileExW", CharSet = CharSet.Unicode, SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool MoveFileEx(string source, string destination, uint flags);

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
        if (OperatingSystem.IsWindows())
        {
            MoveWriteThrough(source, destination, overwrite ? MoveFileReplaceExisting : 0);
            return;
        }

        File.Move(source, destination, overwrite);
        var destinationDirectory = Path.GetDirectoryName(destination)!;
        FlushDirectory(destinationDirectory);
        var sourceDirectory = Path.GetDirectoryName(source)!;
        if (!string.Equals(sourceDirectory, destinationDirectory, StringComparison.Ordinal))
            FlushDirectory(sourceDirectory);
    }

    public static void PublishDirectory(string source, string destination)
    {
        if (OperatingSystem.IsWindows())
        {
            MoveWriteThrough(source, destination, flags: 0);
            return;
        }

        Directory.Move(source, destination);
        var destinationDirectory = Path.GetDirectoryName(destination)!;
        FlushDirectory(destinationDirectory);
        var sourceDirectory = Path.GetDirectoryName(source)!;
        if (!string.Equals(sourceDirectory, destinationDirectory, StringComparison.Ordinal))
            FlushDirectory(sourceDirectory);
    }

    private static void MoveWriteThrough(string source, string destination, uint flags)
    {
        // Every publication source and destination is on one volume. Do not
        // allow MoveFileEx to fall back to a non-atomic copy-and-delete move.
        if (!MoveFileEx(source, destination, flags | MoveFileWriteThrough))
        {
            var error = Marshal.GetLastPInvokeError();
            throw new IOException(
                $"Could not publish storage entry '{source}' at '{destination}'.",
                new Win32Exception(error));
        }
    }

    public static void EnsureDirectory(string directory)
    {
        var pending = new Stack<string>();
        var current = Path.GetFullPath(directory);
        while (!Directory.Exists(current))
        {
            pending.Push(current);
            current = Directory.GetParent(current)?.FullName
                ?? throw new DirectoryNotFoundException($"No existing ancestor of '{directory}' was found.");
        }

        while (pending.TryPop(out var path))
        {
            Directory.CreateDirectory(path);
            FlushDirectory(Directory.GetParent(path)!.FullName);
        }
    }
}
