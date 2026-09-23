using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

public sealed class StorageNativeInteropTests
{
    [Fact]
    public void LinuxNativeFilesystemCallsPreserveUtf8PathNames()
    {
        if (!OperatingSystem.IsLinux())
            return;

        var root = Path.Combine(Path.GetTempPath(), $"mk8-native-ž-{Guid.NewGuid():N}");
        Directory.CreateDirectory(root);
        try
        {
            var file = Path.Combine(root, "čunk.bin");
            File.WriteAllBytes(file, [1, 2, 3]);

            Assert.True(StorageAllocationMeter.TryStat(file, out var stat));
            Assert.True(stat.AllocatedBytes > 0);
            StorageDurability.FlushDirectory(root);
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }
}
