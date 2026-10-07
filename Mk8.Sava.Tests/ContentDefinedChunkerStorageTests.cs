using System.Security.Cryptography;
using Microsoft.Extensions.DependencyInjection;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

public sealed class ContentDefinedChunkerStorageTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task FragmentedUploadPreservesDeduplicationEncryptedReadsAndSparseContent(bool packed, bool customerKey)
    {
        var application = new SavaWebApplicationFactory(
            Path.Combine(Path.GetTempPath(), $"mk8-sava-chunker-{Guid.NewGuid():N}"),
            new NullStorageFaultInjector(),
            analyticsSink: null,
            configurationOverrides: new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                ["Sava:EnableSmallChunkPacking"] = packed.ToString(),
                ["Sava:SmallChunkPackingThresholdBytes"] = "16384"
            },
            deleteDataPath: true,
            disableMaintenance: true);
        await using var applicationDisposal = application.ConfigureAwait(false);
        await application.InitializeAsync();
        var chunks = application.Services.GetRequiredService<ChunkStore>();
        var bytes = new byte[196625];
        DeterministicTestBytes.Fill(0x6c10, bytes);
        bytes.AsSpan(65536, 32768).Clear();
        var key = new byte[32];
        DeterministicTestBytes.Fill(0x6c40, key);
        try
        {
            var encryption = customerKey
                ? new BlobEncryption(null, Convert.ToBase64String(SHA256.HashData(key)), key)
                : new BlobEncryption(null, null);
            using var originalSource = new MemoryStream(bytes, writable: false);
            using var original = await chunks.StorePinnedAsync(SavaWebApplicationFactory.AccountName,
                encryption, originalSource, CancellationToken.None);
            var before = chunks.MeasurePhysicalUsage();
            using var fragmented = new FragmentedInput(bytes);
            using var repeated = await chunks.StorePinnedAsync(SavaWebApplicationFactory.AccountName,
                encryption, fragmented, CancellationToken.None);
            Assert.Equal(original.Manifest.Chunks, repeated.Manifest.Chunks);
            Assert.Equal(Convert.ToHexStringLower(SHA256.HashData(bytes)), repeated.Manifest.Sha256);
            Assert.NotNull(original.ContentMd5);
            Assert.Equal(original.ContentMd5, repeated.ContentMd5);
            var after = chunks.MeasurePhysicalUsage();
            Assert.Equal(before.ChunkBytes, after.ChunkBytes);
            Assert.Equal(before.ChunkCount, after.ChunkCount);
            var packedCount = await application.Services.GetRequiredService<MetadataStore>()
                .CountPackedChunksAsync(CancellationToken.None);
            Assert.Equal(packed, packedCount > 0);

            using var output = new MemoryStream();
            await chunks.WriteRangeAsync(repeated.Manifest, encryption, 0, bytes.Length,
                output, CancellationToken.None);
            Assert.Equal(bytes, output.ToArray());
            output.SetLength(0);
            await chunks.WriteRangeAsync(repeated.Manifest, encryption, 4093, 131101,
                output, CancellationToken.None);
            Assert.Equal(bytes.AsSpan(4093, 131101).ToArray(), output.ToArray());
            Assert.True(originalSource.CanRead);
            Assert.True(fragmented.CanRead);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(key);
        }
    }

    private sealed class FragmentedInput(byte[] bytes) : MemoryStream(bytes, writable: false)
    {
        private int _read;

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            var maximum = (_read++ % 4) switch { 0 => 1, 1 => 4093, 2 => 65536, _ => 7 };
            return base.ReadAsync(buffer[..Math.Min(buffer.Length, maximum)], cancellationToken);
        }
    }
}
