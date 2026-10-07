using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Mk8.Sava.Storage;
using Xunit.Abstractions;

namespace Mk8.Sava.Tests;

[Collection("Storage allocation benchmark")]
public sealed class PageBlobComparisonTests(ITestOutputHelper output)
{
    private const int BatchBytes = 4 * 1024 * 1024;
    private static readonly BlobEncryption Encryption = new(Scope: null, CustomerProvidedKeySha256: null);

    [Theory]
    [InlineData(512)]
    [InlineData(BatchBytes)]
    [InlineData(3 * BatchBytes + 512)]
    public async Task EqualAllocatedPagesHaveBoundedMeasuredComparisonAllocations(int length)
    {
        await using var application = CreateApplication();
        await application.InitializeAsync();
        var service = application.Services.GetRequiredService<BlobService>();
        var chunks = application.Services.GetRequiredService<ChunkStore>();
        var current = CreateRecord(chunks.Sparse(SavaWebApplicationFactory.AccountName, Encryption, length),
            [new PageRange(0, length - 1)]);
        var previous = current with { Snapshot = "previous" };
        for (var warmup = 0; warmup < 2; warmup++)
            AssertEmpty(await service.GetPageRangeDiffAsync(current, previous, Encryption, 0, length - 1, CancellationToken.None));

        for (var repetition = 0; repetition < 3; repetition++)
        {
            var thread = Environment.CurrentManagedThreadId;
            var before = GC.GetAllocatedBytesForCurrentThread();
            var started = Stopwatch.GetTimestamp();
            var result = await service.GetPageRangeDiffAsync(current, previous, Encryption, 0, length - 1, CancellationToken.None);
            var elapsed = Stopwatch.GetElapsedTime(started);
            var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
            // Sparse reads/admission and MemoryStream writes complete synchronously.
            // A switch invalidates this scoped thread-local measurement.
            Assert.Equal(thread, Environment.CurrentManagedThreadId);
            AssertEmpty(result);
            output.WriteLine(FormattableString.Invariant(
                $"page_comparison,length={length},repetition={repetition},managed_bytes={allocated},elapsed_ms={elapsed.TotalMilliseconds:F3}"));
            Assert.InRange(allocated, 0, 16L * 1024 * 1024);
        }
    }

    [Fact]
    public async Task ChangedPagesMergeAcrossBatchBoundariesAndIncludeShortTail()
    {
        await using var application = CreateApplication();
        await application.InitializeAsync();
        var service = application.Services.GetRequiredService<BlobService>();
        var chunks = application.Services.GetRequiredService<ChunkStore>();
        var bytes = new byte[2 * BatchBytes + 512];
        bytes[BatchBytes - 1] = 1;
        bytes[BatchBytes] = 2;
        bytes[2 * BatchBytes] = 3;
        using var input = new MemoryStream(bytes, writable: false);
        using var stored = await chunks.StorePinnedAsync(SavaWebApplicationFactory.AccountName, Encryption, input, CancellationToken.None);
        var current = CreateRecord(stored.Manifest, [new PageRange(0, bytes.Length - 1)]);
        var previous = current with { Content = chunks.Sparse(current.Account, Encryption, bytes.Length), Snapshot = "previous" };

        var results = await Task.WhenAll(
            service.GetPageRangeDiffAsync(current, previous, Encryption, 0, bytes.Length - 1, CancellationToken.None),
            service.GetPageRangeDiffAsync(previous, current, Encryption, 0, bytes.Length - 1, CancellationToken.None));

        foreach (var result in results)
        {
            Assert.Equal([new PageRange(BatchBytes - 512, BatchBytes + 511), new PageRange(2 * BatchBytes, bytes.Length - 1)], result.PageRanges);
            Assert.Empty(result.ClearRanges);
        }
    }

    [Fact]
    public async Task ClippedNonzeroOverlapRetainsAllocationAndClearTransitions()
    {
        await using var application = CreateApplication();
        await application.InitializeAsync();
        var service = application.Services.GetRequiredService<BlobService>();
        var chunks = application.Services.GetRequiredService<ChunkStore>();
        const int length = 2 * BatchBytes + 2048;
        var current = CreateRecord(chunks.Sparse(SavaWebApplicationFactory.AccountName, Encryption, length),
            [new PageRange(0, length - 1025)]);
        var previous = current with { Snapshot = "previous", PageRanges = [new PageRange(512, length - 1)] };

        var result = await service.GetPageRangeDiffAsync(current, previous, Encryption, 0, length - 513, CancellationToken.None);

        Assert.Equal([new PageRange(0, 511)], result.PageRanges);
        Assert.Equal([new PageRange(length - 1024, length - 513)], result.ClearRanges);
    }

    [Fact]
    public async Task EqualBytesDoNotHideExplicitRewriteHistory()
    {
        await using var application = CreateApplication();
        await application.InitializeAsync();
        var service = application.Services.GetRequiredService<BlobService>();
        var chunks = application.Services.GetRequiredService<ChunkStore>();
        const int length = BatchBytes + 512;
        var current = CreateRecord(chunks.Sparse(SavaWebApplicationFactory.AccountName, Encryption, length),
            [new PageRange(0, length - 1)]) with
        {
            PageMutationSequence = 3,
            PageMutationRanges = [new PageMutationRange(0, 511, 1), new PageMutationRange(BatchBytes - 512, length - 1, 3)]
        };
        var previous = current with { Snapshot = "previous", PageMutationSequence = 2, PageMutationRanges = null };

        var result = await service.GetPageRangeDiffAsync(current, previous, Encryption, 0, length - 1, CancellationToken.None);

        Assert.Equal([new PageRange(BatchBytes - 512, length - 1)], result.PageRanges);
        Assert.Empty(result.ClearRanges);
    }

    [Fact]
    public async Task CancellationOfAllocatedComparisonPropagatesAndNextReadSucceeds()
    {
        await using var application = CreateApplication();
        await application.InitializeAsync();
        var service = application.Services.GetRequiredService<BlobService>();
        var chunks = application.Services.GetRequiredService<ChunkStore>();
        var current = CreateRecord(chunks.Sparse(SavaWebApplicationFactory.AccountName, Encryption, 512), [new PageRange(0, 511)]);
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            service.GetPageRangeDiffAsync(current, current, Encryption, 0, 511, cancellation.Token));
        AssertEmpty(await service.GetPageRangeDiffAsync(current, current, Encryption, 0, 511, CancellationToken.None));
    }

    [Fact]
    public async Task IntegrityFailurePropagatesAndDoesNotContaminateNextComparison()
    {
        await using var application = CreateApplication();
        await application.InitializeAsync();
        var service = application.Services.GetRequiredService<BlobService>();
        var chunks = application.Services.GetRequiredService<ChunkStore>();
        using var input = new MemoryStream(new byte[512], writable: false);
        using var stored = await chunks.StorePinnedAsync(SavaWebApplicationFactory.AccountName, Encryption, input, CancellationToken.None);
        var current = CreateRecord(stored.Manifest, [new PageRange(0, 511)]);
        var corrupt = current with { Content = current.Content with { Sha256 = new string('0', 64) } };

        await Assert.ThrowsAsync<InvalidDataException>(() =>
            service.GetPageRangeDiffAsync(corrupt, current, Encryption, 0, 511, CancellationToken.None));
        AssertEmpty(await service.GetPageRangeDiffAsync(current, current, Encryption, 0, 511, CancellationToken.None));
    }

    private static void AssertEmpty(PageRangeDiff result)
    {
        Assert.Empty(result.PageRanges);
        Assert.Empty(result.ClearRanges);
    }

    private static BlobRecord CreateRecord(ContentManifest content, IReadOnlyList<PageRange> ranges) => new()
    {
        Account = SavaWebApplicationFactory.AccountName,
        Container = "comparison",
        Name = "pages",
        GenerationId = "generation",
        Revision = "revision",
        Kind = BlobKind.PageBlob,
        Content = content,
        ETag = "etag",
        CreatedAt = DateTimeOffset.UnixEpoch,
        LastModified = DateTimeOffset.UnixEpoch,
        PageBlobIncarnationId = "incarnation",
        PageRanges = ranges
    };

    private static SavaWebApplicationFactory CreateApplication() => new(
        Path.Combine(Path.GetTempPath(), $"mk8-sava-page-comparison-{Guid.NewGuid():N}"),
        new NullStorageFaultInjector(), analyticsSink: null, configurationOverrides: null,
        deleteDataPath: true, disableMaintenance: true);
}
