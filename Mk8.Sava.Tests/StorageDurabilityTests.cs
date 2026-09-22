using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

public sealed class StorageDurabilityTests
{
    [Fact]
    public async Task PublishFilePreservesNoOverwriteAndAtomicReplacementSemantics()
    {
        var root = Path.Combine(Path.GetTempPath(), $"mk8-sava-publish-file-{Guid.NewGuid():N}");
        StorageDurability.EnsureDirectory(root);
        try
        {
            var source = Path.Combine(root, "source.tmp");
            var destination = Path.Combine(root, "destination.chunk");
            await File.WriteAllTextAsync(source, "first");
            StorageDurability.PublishFile(source, destination, overwrite: false);
            Assert.False(File.Exists(source));
            Assert.Equal("first", await File.ReadAllTextAsync(destination));

            await File.WriteAllTextAsync(source, "second");
            Assert.ThrowsAny<IOException>(() =>
                StorageDurability.PublishFile(source, destination, overwrite: false));
            Assert.Equal("first", await File.ReadAllTextAsync(destination));
            Assert.Equal("second", await File.ReadAllTextAsync(source));

            StorageDurability.PublishFile(source, destination, overwrite: true);
            Assert.False(File.Exists(source));
            Assert.Equal("second", await File.ReadAllTextAsync(destination));
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task PublishDirectoryMovesCompleteTreeWithoutReplacingAnExistingTarget()
    {
        var root = Path.Combine(Path.GetTempPath(), $"mk8-sava-publish-directory-{Guid.NewGuid():N}");
        var source = Path.Combine(root, "staging");
        var destination = Path.Combine(root, "published");
        StorageDurability.EnsureDirectory(Path.Combine(source, "chunks"));
        try
        {
            await File.WriteAllTextAsync(Path.Combine(source, "chunks", "one.chunk"), "exact bytes");
            StorageDurability.PublishDirectory(source, destination);
            Assert.False(Directory.Exists(source));
            Assert.Equal("exact bytes", await File.ReadAllTextAsync(
                Path.Combine(destination, "chunks", "one.chunk")));

            StorageDurability.EnsureDirectory(source);
            Assert.ThrowsAny<IOException>(() => StorageDurability.PublishDirectory(source, destination));
            Assert.True(Directory.Exists(source));
            Assert.True(Directory.Exists(destination));
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }
}
