using System.Runtime.InteropServices;

namespace Mk8.Sava.Storage;

internal static class StorageAllocationMeter
{
    private const int AtFdcwd = -100;
    private const int AtSymlinkNoFollow = 0x100;
    private const uint StatxBasicStats = 0x7ff;
    private const uint StatxBlocks = 0x400;
    private const int NoEntry = 2;
    private const int NotDirectory = 20;
    private const int NotImplemented = 38;
    private const int NotSupported = 95;

    public static long? MeasureRoot(string root)
    {
        if (!OperatingSystem.IsLinux())
            return null;

        try
        {
            long allocated = 0;
            var hardLinks = new HashSet<(uint Major, uint Minor, ulong Inode)>();
            var pending = new Stack<string>();
            pending.Push(root);
            while (pending.TryPop(out var path))
            {
                if (!TryStat(path, out var stat))
                    continue;
                if (!TryGetAttributes(path, out var attributes))
                    continue;

                var isDirectory = (attributes & FileAttributes.Directory) != FileAttributes.None;
                if (isDirectory && (attributes & FileAttributes.ReparsePoint) == FileAttributes.None)
                {
                    try
                    {
                        foreach (var child in Directory.EnumerateFileSystemEntries(path))
                            pending.Push(child);
                    }
                    catch (DirectoryNotFoundException)
                    {
                        continue;
                    }
                }

                if (!isDirectory && stat.LinkCount > 1 &&
                    !hardLinks.Add((stat.DeviceMajor, stat.DeviceMinor, stat.Inode)))
                    continue;
                allocated = checked(allocated + stat.AllocatedBytes);
            }
            return allocated;
        }
        catch (EntryPointNotFoundException)
        {
            return null;
        }
        catch (DllNotFoundException)
        {
            return null;
        }
        catch (PlatformNotSupportedException)
        {
            return null;
        }
    }

    private static bool TryGetAttributes(string path, out FileAttributes attributes)
    {
        try
        {
            attributes = File.GetAttributes(path);
            return true;
        }
        catch (FileNotFoundException)
        {
            attributes = default;
            return false;
        }
        catch (DirectoryNotFoundException)
        {
            attributes = default;
            return false;
        }
    }

    internal static bool TryStat(string path, out AllocationStat stat)
    {
        if (Statx(AtFdcwd, path, AtSymlinkNoFollow, StatxBasicStats, out var buffer) != 0)
        {
            var error = Marshal.GetLastPInvokeError();
            if (error is NoEntry or NotDirectory)
            {
                stat = default;
                return false;
            }
            if (error is NotImplemented or NotSupported)
                throw new PlatformNotSupportedException("statx is not supported by this Linux host or filesystem.");
            throw new IOException($"Cannot measure filesystem allocation (statx error {error}).");
        }

        if ((buffer.Mask & StatxBlocks) == 0)
            throw new PlatformNotSupportedException("This filesystem does not report allocated blocks.");
        stat = new AllocationStat(
            checked((long)buffer.Blocks * 512),
            buffer.LinkCount,
            buffer.Inode,
            buffer.DeviceMajor,
            buffer.DeviceMinor);
        return true;
    }

    [StructLayout(LayoutKind.Sequential)]
    internal readonly record struct AllocationStat(
        long AllocatedBytes,
        uint LinkCount,
        ulong Inode,
        uint DeviceMajor,
        uint DeviceMinor);

    // Linux UAPI struct statx is 0x100 bytes, including reserved fields. Keep
    // its complete native output size while naming only the fields we consume.
    [StructLayout(LayoutKind.Explicit, Size = 256)]
    private readonly struct StatxBuffer
    {
        [field: FieldOffset(0)]
        public uint Mask { get; init; }

        [field: FieldOffset(16)]
        public uint LinkCount { get; init; }

        [field: FieldOffset(32)]
        public ulong Inode { get; init; }

        [field: FieldOffset(48)]
        public ulong Blocks { get; init; }

        [field: FieldOffset(136)]
        public uint DeviceMajor { get; init; }

        [field: FieldOffset(140)]
        public uint DeviceMinor { get; init; }
    }

    [DefaultDllImportSearchPaths(DllImportSearchPath.SafeDirectories)]
    [DllImport("libc", EntryPoint = "statx", CharSet = CharSet.Unicode, ExactSpelling = true,
        BestFitMapping = false, ThrowOnUnmappableChar = true, SetLastError = true)]
    private static extern int Statx(
        int directoryFileDescriptor,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string path,
        int flags,
        uint mask,
        out StatxBuffer buffer);
}
