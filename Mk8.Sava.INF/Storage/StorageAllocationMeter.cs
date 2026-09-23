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
                FileAttributes attributes;
                try
                {
                    attributes = File.GetAttributes(path);
                }
                catch (FileNotFoundException)
                {
                    continue;
                }
                catch (DirectoryNotFoundException)
                {
                    continue;
                }

                var isDirectory = (attributes & FileAttributes.Directory) != 0;
                if (isDirectory && (attributes & FileAttributes.ReparsePoint) == 0)
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

    private static bool TryStat(string path, out AllocationStat stat)
    {
        var buffer = new byte[256];
        if (Statx(AtFdcwd, path, AtSymlinkNoFollow, StatxBasicStats, buffer) != 0)
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

        if ((BitConverter.ToUInt32(buffer, 0) & StatxBlocks) == 0)
            throw new PlatformNotSupportedException("This filesystem does not report allocated blocks.");
        stat = new AllocationStat(
            checked((long)BitConverter.ToUInt64(buffer, 48) * 512),
            BitConverter.ToUInt32(buffer, 16),
            BitConverter.ToUInt64(buffer, 32),
            BitConverter.ToUInt32(buffer, 136),
            BitConverter.ToUInt32(buffer, 140));
        return true;
    }

    private readonly record struct AllocationStat(
        long AllocatedBytes,
        uint LinkCount,
        ulong Inode,
        uint DeviceMajor,
        uint DeviceMinor);

    [DllImport("libc", EntryPoint = "statx", SetLastError = true)]
    private static extern int Statx(
        int directoryFileDescriptor,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string path,
        int flags,
        uint mask,
        [Out] byte[] buffer);
}
