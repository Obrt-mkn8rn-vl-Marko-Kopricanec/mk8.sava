using System.Diagnostics;
using System.Security.Cryptography;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Mk8.Sava.Storage;
using Xunit.Abstractions;

namespace Mk8.Sava.Tests;

public sealed class SparseRangeTransferTests(ITestOutputHelper output)
{
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(4096)]
    [InlineData(131072)]
    [InlineData(131089)]
    public async Task SparseReadsHaveAnAllocationBudgetProportionalToTheTransfer(int length)
    {
        var application = CreateApplication();
        await using var disposal = application.ConfigureAwait(true);
        await application.InitializeAsync();
        AssertNoAutomaticMaintenance(application);
        var chunks = application.Services.GetRequiredService<ChunkStore>();
        var encryption = new BlobEncryption(null, null);
        var manifest = chunks.Sparse(SavaWebApplicationFactory.AccountName, encryption, long.MaxValue);
        using var destination = new MemoryStream(length);
        for (var warmup = 0; warmup < 3; warmup++)
            await ReadAndAssertZeroesAsync(chunks, manifest, encryption, length, destination);

        const int operations = 16;
        var thread = Environment.CurrentManagedThreadId;
        var minimum = long.MaxValue;
        for (var batch = 0; batch < 3; batch++)
        {
            var watch = Stopwatch.StartNew();
            var before = GC.GetAllocatedBytesForCurrentThread();
            for (var iteration = 0; iteration < operations; iteration++)
                await ReadAndAssertZeroesAsync(chunks, manifest, encryption, length, destination);
            var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
            watch.Stop();
            Assert.Equal(thread, Environment.CurrentManagedThreadId);
            minimum = Math.Min(minimum, allocated);
            output.WriteLine("sparse_range_allocation,length={0},batch={1},operations={2},managed_bytes={3},elapsed_ms={4:F3}",
                length, batch, operations, allocated, watch.Elapsed.TotalMilliseconds);
        }
        Assert.True(destination.CanWrite);
        Assert.InRange(minimum, 0, operations * (Math.Min(length, 128L * 1024) + 8192));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(4096)]
    [InlineData(131071)]
    [InlineData(131072)]
    [InlineData(262175)]
    public async Task LongManifestRangesWriteOnlyRequestedZeroesAndPreserveBorrowedDestination(int length)
    {
        var application = CreateApplication();
        await using var disposal = application.ConfigureAwait(true);
        await application.InitializeAsync();
        var chunks = application.Services.GetRequiredService<ChunkStore>();
        var encryption = new BlobEncryption(null, null);
        var manifest = chunks.Sparse(SavaWebApplicationFactory.AccountName, encryption, long.MaxValue);
        using var destination = new RecordingDestination();
        destination.WriteByte(0x71);
        using var cancellation = new CancellationTokenSource();

        await chunks.WriteRangeAsync(manifest, encryption, (long)int.MaxValue + 31, length, destination, cancellation.Token);

        var bytes = destination.ToArray();
        Assert.Equal(length + 1, bytes.Length);
        Assert.Equal(0x71, bytes[0]);
        Assert.Equal(-1, bytes.AsSpan(1).IndexOfAnyExcept((byte)0));
        Assert.All(destination.Writes, write => Assert.InRange(write, 1, 128 * 1024));
        Assert.Equal(length, destination.Writes.Sum());
        Assert.All(destination.Tokens, token => Assert.Equal(cancellation.Token, token));
        Assert.True(destination.CanWrite);
        AssertReadsIdle(chunks);
    }

    [Fact]
    public async Task DestinationFailureKeepsItsIdentityAndReleasesReadAdmissionForRetry()
    {
        var application = CreateApplication();
        await using var disposal = application.ConfigureAwait(true);
        await application.InitializeAsync();
        var chunks = application.Services.GetRequiredService<ChunkStore>();
        var encryption = new BlobEncryption(null, null);
        var manifest = chunks.Sparse(SavaWebApplicationFactory.AccountName, encryption, 4096);
        var original = new IOException("controlled sparse destination failure");
        using var destination = new FailingDestination(original);

        Assert.Same(original, await Record.ExceptionAsync(() =>
            chunks.WriteRangeAsync(manifest, encryption, 0, manifest.Length, destination, CancellationToken.None)));

        Assert.Equal(1, destination.Attempts);
        Assert.Equal(0, destination.Length);
        Assert.True(destination.CanWrite);
        AssertReadsIdle(chunks);
        using var retry = new MemoryStream();
        await ReadAndAssertZeroesAsync(chunks, manifest, encryption, 4096, retry);
    }

    [Fact]
    public async Task CancellationBetweenBatchesKeepsTokenAndPartialBytesAndReleasesAdmission()
    {
        var application = CreateApplication();
        await using var disposal = application.ConfigureAwait(true);
        await application.InitializeAsync();
        var chunks = application.Services.GetRequiredService<ChunkStore>();
        var encryption = new BlobEncryption(null, null);
        var manifest = chunks.Sparse(SavaWebApplicationFactory.AccountName, encryption, 262175);
        using var cancellation = new CancellationTokenSource();
        using var destination = new CancelAfterFirstWrite(cancellation);

        var failure = await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            chunks.WriteRangeAsync(manifest, encryption, 0, manifest.Length, destination, cancellation.Token));

        Assert.Equal(cancellation.Token, failure.CancellationToken);
        Assert.Equal(128 * 1024, destination.Length);
        Assert.Equal(-1, destination.GetBuffer().AsSpan(0, (int)destination.Length).IndexOfAnyExcept((byte)0));
        Assert.True(destination.CanWrite);
        AssertReadsIdle(chunks);
        using var retry = new MemoryStream();
        await ReadAndAssertZeroesAsync(chunks, manifest, encryption, 4096, retry);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task MixedZeroAndStoredContentRetainsFullIntegrityRangeAndKeyChecks(bool customerKey)
    {
        var application = CreateApplication();
        await using var disposal = application.ConfigureAwait(true);
        await application.InitializeAsync();
        AssertNoAutomaticMaintenance(application);
        var chunks = application.Services.GetRequiredService<ChunkStore>();
        var key = new byte[32];
        DeterministicTestBytes.Fill(0x5710, key);
        try
        {
            var encryption = customerKey
                ? new BlobEncryption(null, Convert.ToBase64String(SHA256.HashData(key)), key)
                : new BlobEncryption(null, null);
            var payload = new byte[31];
            DeterministicTestBytes.Fill(0x5720, payload);
            using var source = new MemoryStream(payload, writable: false);
            using var stored = await chunks.StorePinnedAsync(SavaWebApplicationFactory.AccountName, encryption, source, CancellationToken.None);
            var prefix = HashedZeroManifest(chunks, encryption, 7);
            var suffix = HashedZeroManifest(chunks, encryption, 19);
            var manifest = await chunks.ComposeAsync(SavaWebApplicationFactory.AccountName, encryption,
                [prefix, stored.Manifest, suffix], CancellationToken.None);
            var expected = new byte[57];
            payload.CopyTo(expected, 7);
            Assert.Equal(Convert.ToHexStringLower(SHA256.HashData(expected)), manifest.Sha256);
            using var destination = new MemoryStream();
            await chunks.WriteRangeAsync(manifest, encryption, 0, expected.Length, destination, CancellationToken.None);
            Assert.Equal(expected, destination.ToArray());

            destination.SetLength(0);
            await chunks.WriteRangeAsync(manifest, encryption, 5, 42, destination, CancellationToken.None);
            Assert.Equal(expected.AsSpan(5, 42).ToArray(), destination.ToArray());

            destination.SetLength(0);
            var invalidHash = manifest with { Sha256 = new string('0', 64) };
            await Assert.ThrowsAsync<InvalidDataException>(() =>
                chunks.WriteRangeAsync(invalidHash, encryption, 0, expected.Length, destination, CancellationToken.None));
            Assert.Equal(expected, destination.ToArray()); // Failure does not withdraw already-written bytes.
            Assert.True(destination.CanWrite);
            Assert.True(source.CanRead);
            AssertReadsIdle(chunks);
            if (customerKey)
            {
                destination.SetLength(0);
                var wrong = new BlobEncryption(null, Convert.ToBase64String(SHA256.HashData(new byte[32])), new byte[32]);
                await Assert.ThrowsAsync<InvalidDataException>(() =>
                    chunks.WriteRangeAsync(manifest, wrong, 0, expected.Length, destination, CancellationToken.None));
                Assert.Equal(0, destination.Length);
            }
        }
        finally
        {
            CryptographicOperations.ZeroMemory(key);
        }
    }

    private static ContentManifest HashedZeroManifest(ChunkStore chunks, BlobEncryption encryption, int length) =>
        chunks.Sparse(SavaWebApplicationFactory.AccountName, encryption, length) with
        { Sha256 = Convert.ToHexStringLower(SHA256.HashData(new byte[length])) };

    private static async Task ReadAndAssertZeroesAsync(ChunkStore chunks, ContentManifest manifest,
        BlobEncryption encryption, int length, MemoryStream destination)
    {
        destination.SetLength(0);
        await chunks.WriteRangeAsync(manifest, encryption, 0, length, destination, CancellationToken.None).ConfigureAwait(false);
        Assert.Equal(length, destination.Length);
        Assert.Equal(-1, destination.GetBuffer().AsSpan(0, length).IndexOfAnyExcept((byte)0));
    }

    private static SavaWebApplicationFactory CreateApplication() =>
        new(TimeProvider.System, new Dictionary<string, string?>(StringComparer.Ordinal), disableMaintenance: true);

    private static void AssertNoAutomaticMaintenance(SavaWebApplicationFactory application) =>
        Assert.DoesNotContain(application.Services.GetServices<IHostedService>(), service => service is StorageMaintenanceService);

    private static void AssertReadsIdle(ChunkStore chunks) =>
        Assert.Contains("mk8_sava_storage_work_active{lane=\"reads\"} 0\n", chunks.Admission.RenderPrometheus(), StringComparison.Ordinal);

    private sealed class RecordingDestination : MemoryStream
    {
        internal List<int> Writes { get; } = [];
        internal List<CancellationToken> Tokens { get; } = [];

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            Writes.Add(buffer.Length);
            Tokens.Add(cancellationToken);
            return base.WriteAsync(buffer, cancellationToken);
        }
    }

    private sealed class FailingDestination(Exception failure) : MemoryStream
    {
        internal int Attempts { get; private set; }

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            Attempts++;
            return ValueTask.FromException(failure);
        }
    }

    private sealed class CancelAfterFirstWrite(CancellationTokenSource cancellation) : MemoryStream
    {
        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await base.WriteAsync(buffer, cancellationToken).ConfigureAwait(false);
            await cancellation.CancelAsync().ConfigureAwait(false);
        }
    }
}
