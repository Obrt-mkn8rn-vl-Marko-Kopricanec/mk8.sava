using Mk8.Sava.Protocol;

namespace Mk8.Sava.Hosting;

internal static class GatewayStagingCleanup
{
    internal static ReadOnlySpan<byte> OwnerMarker => "mk8.sava-gateway-staging-v1"u8;

    public static void RemoveOrphans(string root)
    {
        foreach (var path in Directory.EnumerateDirectories(root, "gw-*", SearchOption.TopDirectoryOnly))
        {
            var name = Path.GetFileName(path);
            if (!name.StartsWith("gw-", StringComparison.Ordinal) || !Guid.TryParseExact(name.AsSpan(3), "N", out _) ||
                File.GetAttributes(path).HasFlag(FileAttributes.ReparsePoint))
                continue;
            TryRemoveOrphan(path);
        }
    }

    private static void TryRemoveOrphan(string path)
    {
        try
        {
            var ownerPath = Path.Combine(path, ".owner");
            if (!File.Exists(ownerPath) || File.GetAttributes(ownerPath).HasFlag(FileAttributes.ReparsePoint))
                return;
            using (var owner = new FileStream(ownerPath, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
            {
                var marker = new byte[OwnerMarker.Length];
                if (owner.Length != marker.Length || owner.Read(marker) != marker.Length || !marker.AsSpan().SequenceEqual(OwnerMarker))
                    return;
                var entries = Directory.EnumerateFileSystemEntries(path).ToArray();
                // Never follow links, delete nested directories, or sweep an
                // unrecognized folder. A locked owner identifies a live peer.
                if (entries.Any(entry => File.GetAttributes(entry).HasFlag(FileAttributes.Directory) ||
                    File.GetAttributes(entry).HasFlag(FileAttributes.ReparsePoint)))
                    return;
                foreach (var entry in entries)
                {
                    if (!string.Equals(entry, ownerPath, StringComparison.Ordinal))
                        File.Delete(entry);
                }
            }
            File.Delete(ownerPath);
            Directory.Delete(path, recursive: false);
        }
        catch (IOException exception) when (!CatastrophicExceptionPolicy.Contains(exception))
        {
            // Live owner, concurrent cleanup, or inaccessible scratch files:
            // leave the exact folder untouched for a later startup/operator.
        }
        catch (UnauthorizedAccessException exception) when (!CatastrophicExceptionPolicy.Contains(exception))
        {
            // Scratch cleanup must not broaden permissions to recover space.
        }
    }
}
