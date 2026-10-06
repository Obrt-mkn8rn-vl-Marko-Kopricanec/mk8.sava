using Mk8.Sava.Application;
using Mk8.Sava.Configuration;
using Mk8.Sava.Storage;
using Mk8.Sava.Transport;

namespace Mk8.Sava.Tests;

public sealed partial class ApplicationTransportSecurityTests
{
    [Fact]
    public async Task UploadBudgetAndDeclaredCopyBoundsStayDistinctForPreparedContent()
    {
        var limited = new SavaOptions { Accounts = storageOptions.Accounts, MaximumRequestBodyBytes = 32 };
        using var client = new ApplicationRpcClient(transportOptions, limited);
        using var upload = new BorrowedContentStream(new byte[33]);
        using var uploadRequest = await CreateBlockRequestAsync(client, upload).ConfigureAwait(true);
        using var rejectedWire = new MemoryStream();
        await Assert.ThrowsAnyAsync<Exception>(() => uploadRequest.Content!.CopyToAsync(rejectedWire)).ConfigureAwait(true);
        rejectedWire.Position = 0;
        _ = await RpcFrames.ReadControlAsync<RpcRequest>(rejectedWire, 65536, CancellationToken.None).ConfigureAwait(true);
        using var rejectedInput = new FramedReadStream(rejectedWire, 32);
        await Assert.ThrowsAsync<EndOfStreamException>(() => rejectedInput.EnsureCompletedAsync(CancellationToken.None))
            .ConfigureAwait(true);
        var bytes = ContentBytes("page");
        using var pages = ContentPages("page", bytes);
        using var unusedStream = new BorrowedContentStream([]);
        using var pageRequest = await CreateContentRequestAsync(client, "page", unusedStream, pages).ConfigureAwait(true);
        using var pageWire = new MemoryStream();
        await pageRequest.Content!.CopyToAsync(pageWire).ConfigureAwait(true);
        await AssertContentEnvelopeAsync(pageWire, "page", bytes).ConfigureAwait(true);
        using var copy = new BorrowedContentStream(bytes);
        var contract = RpcContracts.GetContract(typeof(IBlobApplication));
        var method = contract.GetMethod(typeof(IBlobApplication).GetMethod(nameof(IBlobApplication.CopyBlockBlobFromStreamAsync))!);
        using var copyRequest = await client.CreateRequestAsync(contract, method,
            ["devstoreaccount1", "container", "blob", copy, (long)bytes.Length, Array.Empty<CopySourceBlock>(),
                new BlobWriteOptions(new BlobHttpProperties(), new Dictionary<string, string>(StringComparer.Ordinal)),
                "https://example.invalid/container/blob", LeaseRecord.Available, null, null, CancellationToken.None],
            CancellationToken.None).ConfigureAwait(true);
        using var copyWire = new MemoryStream();
        await copyRequest.Content!.CopyToAsync(copyWire).ConfigureAwait(true);
        await AssertContentEnvelopeAsync(copyWire, "stream", bytes).ConfigureAwait(true);
        Assert.True(copy.CanRead);
    }

    [Theory]
    [InlineData("control")]
    [InlineData("empty-stream")]
    [InlineData("stream")]
    [InlineData("empty-page")]
    [InlineData("page")]
    [InlineData("clear-only-page")]
    public async Task PreparedContentPreservesDescriptorProofReplayAndBorrowedOwnership(string form)
    {
        var bytes = ContentBytes(form);
        using var stream = new BorrowedContentStream(bytes);
        using var pages = ContentPages(form, bytes);
        using var client = new ApplicationRpcClient(transportOptions, storageOptions);
        using var request = await CreateContentRequestAsync(client, form, stream, pages).ConfigureAwait(true);
        using var wire = new MemoryStream();
        using var replay = new MemoryStream();

        await request.Content!.CopyToAsync(wire).ConfigureAwait(true);
        await AssertContentEnvelopeAsync(wire, form, bytes).ConfigureAwait(true);
        var failure = await Assert.ThrowsAsync<InvalidOperationException>(() => request.Content.CopyToAsync(replay))
            .ConfigureAwait(true);

        Assert.Contains("more than once", failure.Message, StringComparison.Ordinal);
        Assert.Equal(0, replay.Length);
        Assert.Equal(IsPageContent(form) ? 1 : 0, pages.Preparations);
        Assert.Equal(string.Equals(form, "page", StringComparison.Ordinal) ? 1 : 0, pages.RangeReads);
        Assert.Equal(form is "stream" or "empty-stream" ? 1 : 0, stream.Copies);
        request.Dispose();
        Assert.True(stream.CanRead);
        Assert.Equal(0, pages.Disposals);
    }

    [Theory]
    [InlineData("control")]
    [InlineData("stream")]
    [InlineData("page")]
    public async Task ConcurrentSerializationCannotStartAnotherControlFrameOrProducer(string form)
    {
        var bytes = ContentBytes(form);
        using var stream = new BorrowedContentStream(bytes);
        using var pages = ContentPages(form, bytes);
        using var client = new ApplicationRpcClient(transportOptions, storageOptions);
        using var request = await CreateContentRequestAsync(client, form, stream, pages).ConfigureAwait(true);
        using var wire = new GatedContentWire();
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        await AssertConcurrentContentWritesAsync(request, wire, stream, pages, form, deadline.Token).ConfigureAwait(true);
    }

    private static async Task AssertConcurrentContentWritesAsync(
        HttpRequestMessage request, GatedContentWire wire, BorrowedContentStream stream,
        BorrowedPageContent pages, string form, CancellationToken cancellationToken)
    {
        using var competingWire = new MemoryStream();
        var first = request.Content!.CopyToAsync(wire, cancellationToken);
        try
        {
            await wire.Started.Task.WaitAsync(TimeSpan.FromSeconds(5), cancellationToken).ConfigureAwait(true);
            var failure = await Assert.ThrowsAsync<InvalidOperationException>(() => request.Content.CopyToAsync(competingWire, cancellationToken))
                .ConfigureAwait(true);
            Assert.Contains("more than once", failure.Message, StringComparison.Ordinal);
            Assert.Equal(0, competingWire.Length);
            Assert.Equal(0, stream.Copies);
            Assert.Equal(0, pages.RangeReads);
        }
        finally
        {
            wire.Release.TrySetResult();
            // Release the only fixture gate and join the operation itself before disposal.
            await first.ConfigureAwait(true);
        }
        await AssertContentEnvelopeAsync(wire, form, ContentBytes(form)).ConfigureAwait(true);
        Assert.Equal(IsPageContent(form) ? 1 : 0, pages.Preparations);
        Assert.Equal(string.Equals(form, "page", StringComparison.Ordinal) ? 1 : 0, pages.RangeReads);
        Assert.Equal(string.Equals(form, "stream", StringComparison.Ordinal) ? 1 : 0, stream.Copies);
    }

    [Theory]
    [InlineData("io")]
    [InlineData("canceled")]
    [InlineData("missing")]
    [InlineData("duplicate")]
    public async Task FaultedPageProductionNeverCreatesAValidProofOrAllowsReplay(string fault)
    {
        var bytes = ContentBytes("page");
        using var stream = new BorrowedContentStream([]);
        using var pages = new BorrowedPageContent(new PageRangeDiff([new(0, 511)], []), bytes, fault);
        using var client = new ApplicationRpcClient(transportOptions, storageOptions);
        using var request = await CreateContentRequestAsync(client, "page", stream, pages).ConfigureAwait(true);
        using var wire = new MemoryStream();
        using var replay = new MemoryStream();

        await Assert.ThrowsAnyAsync<Exception>(() => request.Content!.CopyToAsync(wire)).ConfigureAwait(true);
        var second = await Assert.ThrowsAsync<InvalidOperationException>(() => request.Content!.CopyToAsync(replay))
            .ConfigureAwait(true);
        Assert.Contains("more than once", second.Message, StringComparison.Ordinal);
        Assert.Equal(0, replay.Length);
        wire.Position = 0;
        var descriptor = await RpcFrames.ReadControlAsync<RpcRequest>(wire, 65536, CancellationToken.None).ConfigureAwait(true);
        Assert.True(descriptor.HasInput);
        Assert.Equal(new PageRange(0, 511), Assert.Single(descriptor.PageChanges!.PageRanges));
        using var framed = new FramedReadStream(wire, bytes.Length);
        using var received = new MemoryStream();
        await Assert.ThrowsAsync<EndOfStreamException>(() => framed.CopyToAsync(received)).ConfigureAwait(true);
        Assert.Equal(string.Equals(fault, "missing", StringComparison.Ordinal) ? [] : bytes, received.ToArray());
        Assert.Equal(1, pages.Preparations);
        request.Dispose();
        Assert.Equal(0, pages.Disposals);
    }

    [Theory]
    [InlineData("stream")]
    [InlineData("page")]
    public async Task CancellationBeforeSerializationDoesNotConsumeBorrowedInput(string form)
    {
        var bytes = ContentBytes(form);
        using var stream = new BorrowedContentStream(bytes);
        using var pages = ContentPages(form, bytes);
        using var cancellation = new CancellationTokenSource();
        using var client = new ApplicationRpcClient(transportOptions, storageOptions);
        using var request = await CreateContentRequestAsync(client, form, stream, pages, cancellation.Token).ConfigureAwait(true);
        using var wire = new MemoryStream();
        await cancellation.CancelAsync().WaitAsync(TimeSpan.FromSeconds(2)).ConfigureAwait(true);

        var failure = await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            request.Content!.CopyToAsync(wire, cancellation.Token)).ConfigureAwait(true);

        Assert.Equal(cancellation.Token, failure.CancellationToken);
        Assert.Equal(0, wire.Length);
        Assert.Equal(0, stream.Copies);
        Assert.Equal(0, pages.RangeReads);
        request.Dispose();
        Assert.True(stream.CanRead);
        Assert.Equal(0, pages.Disposals);
    }

    [Fact]
    public async Task CancellationDuringPagePreparationDoesNotTransferOrDisposeTheSource()
    {
        using var stream = new BorrowedContentStream([]);
        using var pages = new BorrowedPageContent(new PageRangeDiff([], []), [], waitForPreparation: true);
        using var cancellation = new CancellationTokenSource();
        using var client = new ApplicationRpcClient(transportOptions, storageOptions);

        await AssertCanceledPreparationAsync(client, stream, pages, cancellation).ConfigureAwait(true);
    }

    private static async Task AssertCanceledPreparationAsync(
        ApplicationRpcClient client, BorrowedContentStream stream, BorrowedPageContent pages,
        CancellationTokenSource cancellation)
    {
        var preparation = CreateContentRequestAsync(client, "empty-page", stream, pages, cancellation.Token);
        try
        {
            await pages.Preparing.Task.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(true);
            await cancellation.CancelAsync().WaitAsync(TimeSpan.FromSeconds(2)).ConfigureAwait(true);
            var failure = await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
                preparation.WaitAsync(TimeSpan.FromSeconds(5))).ConfigureAwait(true);
            Assert.Equal(cancellation.Token, failure.CancellationToken);
            Assert.Equal(1, pages.Preparations);
            Assert.Equal(0, pages.RangeReads);
            Assert.Equal(0, pages.Disposals);
            Assert.True(stream.CanRead);
        }
        finally
        {
            try
            {
                await cancellation.CancelAsync().WaitAsync(TimeSpan.FromSeconds(2)).ConfigureAwait(true);
            }
            finally
            {
                // Also release a producer whose caller incorrectly failed to forward cancellation.
                pages.CompletePreparation();
                try
                {
                    using var unexpectedRequest = await preparation.ConfigureAwait(true);
                }
                catch (OperationCanceledException)
                {
                    // The independently observed preparation is canceled, not a leaked returned request.
                }
            }
        }
    }

    private static bool IsPageContent(string form) => form is "page" or "empty-page" or "clear-only-page";

    private static byte[] ContentBytes(string form) => form switch
    {
        "stream" => Enumerable.Range(0, 65537).Select(static value => (byte)value).ToArray(),
        "page" => Enumerable.Range(0, 512).Select(static value => (byte)value).ToArray(),
        _ => [],
    };

    private static BorrowedPageContent ContentPages(string form, byte[] bytes) => new(
        new PageRangeDiff(string.Equals(form, "page", StringComparison.Ordinal) ? [new(0, 511)] : [],
            string.Equals(form, "clear-only-page", StringComparison.Ordinal) ? [new(2048, 2559)] : []), bytes);

    private static Task<HttpRequestMessage> CreateContentRequestAsync(
        ApplicationRpcClient client, string form, Stream stream, IPageCopySource pages,
        CancellationToken cancellationToken = default)
    {
        if (IsPageContent(form))
        {
            var contract = RpcContracts.GetContract(typeof(IBlobApplication));
            var method = contract.GetMethod(typeof(IBlobApplication)
                .GetMethod(nameof(IBlobApplication.BeginIncrementalCopyFromPageRangesAsync))!);
            return client.CreateRequestAsync(contract, method, CreatePageCopyArguments(pages), cancellationToken);
        }
        if (!string.Equals(form, "control", StringComparison.Ordinal))
            return CreateBlockRequestAsync(client, stream);
        var readiness = RpcContracts.GetContract(typeof(IApplicationReadiness));
        var readinessMethod = readiness.GetMethod(typeof(IApplicationReadiness)
            .GetMethod(nameof(IApplicationReadiness.GetAsync))!);
        return client.CreateRequestAsync(readiness, readinessMethod, [cancellationToken], cancellationToken);
    }

    private static async Task AssertContentEnvelopeAsync(MemoryStream wire, string form, byte[] bytes)
    {
        wire.Position = 0;
        var descriptor = await RpcFrames.ReadControlAsync<RpcRequest>(wire, 65536, CancellationToken.None).ConfigureAwait(false);
        Assert.Equal(!string.Equals(form, "control", StringComparison.Ordinal), descriptor.HasInput);
        if (IsPageContent(form))
        {
            Assert.NotNull(descriptor.PageChanges);
            Assert.Equal(string.Equals(form, "page", StringComparison.Ordinal) ? [new PageRange(0, 511)] : [],
                descriptor.PageChanges.PageRanges);
            Assert.Equal(string.Equals(form, "clear-only-page", StringComparison.Ordinal) ? [new PageRange(2048, 2559)] : [],
                descriptor.PageChanges.ClearRanges);
        }
        else
            Assert.Null(descriptor.PageChanges);
        using var framed = new FramedReadStream(wire, bytes.Length);
        using var received = new MemoryStream();
        await framed.CopyToAsync(received).ConfigureAwait(false);
        await framed.EnsureCompletedAsync(CancellationToken.None).ConfigureAwait(false);
        Assert.Equal(bytes, received.ToArray());
        Assert.Equal(wire.Length, wire.Position);
    }

    private sealed class BorrowedContentStream(byte[] bytes) : MemoryStream(bytes, writable: false)
    {
        internal int Copies { get; private set; }
        public override Task CopyToAsync(Stream destination, int bufferSize, CancellationToken cancellationToken)
        {
            Copies++;
            return base.CopyToAsync(destination, bufferSize, cancellationToken);
        }
    }

    private sealed class BorrowedPageContent(
        PageRangeDiff changes, byte[] bytes, string? fault = null, bool waitForPreparation = false) : IPageCopySource, IDisposable
    {
        private readonly TaskCompletionSource<PageRangeDiff>? prepared = waitForPreparation
            ? new(TaskCreationOptions.RunContinuationsAsynchronously) : null;
        internal TaskCompletionSource Preparing { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal int Preparations { get; private set; }
        internal int RangeReads { get; private set; }
        internal int Disposals { get; private set; }

        public Task<PageRangeDiff> ReadChangesAsync(string? previousSnapshot, long previousLength, CancellationToken cancellationToken)
        {
            Preparations++;
            cancellationToken.ThrowIfCancellationRequested();
            Preparing.TrySetResult();
            return prepared is null ? Task.FromResult(changes) : prepared.Task.WaitAsync(cancellationToken);
        }

        public async Task ReadRangeAsync(PageRange range, Func<Stream, CancellationToken, Task> consume, CancellationToken cancellationToken)
        {
            RangeReads++;
            if (string.Equals(fault, "missing", StringComparison.Ordinal))
                return;
            using var input = new MemoryStream(bytes, writable: false);
            await consume(input, cancellationToken).ConfigureAwait(false);
            if (string.Equals(fault, "duplicate", StringComparison.Ordinal))
            {
                input.Position = 0;
                await consume(input, cancellationToken).ConfigureAwait(false);
            }
            if (string.Equals(fault, "io", StringComparison.Ordinal))
                throw new IOException("Injected page producer failure after complete range bytes.");
            if (string.Equals(fault, "canceled", StringComparison.Ordinal))
                throw new OperationCanceledException("Injected page producer cancellation after complete range bytes.");
        }

        internal void CompletePreparation() => prepared?.TrySetResult(changes);

        public void Dispose()
        {
            Disposals++;
            CompletePreparation();
        }
    }

    private sealed class GatedContentWire : MemoryStream
    {
        private int started;
        internal TaskCompletionSource Started { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (Interlocked.Exchange(ref started, 1) == 0)
            {
                Started.TrySetResult();
                await Release.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
            await base.WriteAsync(buffer, cancellationToken).ConfigureAwait(false);
        }
    }
}
