using Mk8.Sava.Application;
using Mk8.Sava.Storage;
using Mk8.Sava.Transport;

namespace Mk8.Sava.Tests;

public sealed partial class ApplicationTransportSecurityTests
{
    [Fact]
    public async Task PageCopyPreparationSnapshotsDescriptorBeforeSerialization()
    {
        var pages = new List<PageRange> { new(0, 511), new(1024, 1535) };
        var clears = new List<PageRange> { new(2048, 2559) };
        var source = new MutablePageCopySource(new PageRangeDiff(pages, clears));
        using var client = new ApplicationRpcClient(transportOptions, storageOptions);
        using var request = await CreatePageCopyRequestAsync(client, source);
        pages[1] = new PageRange(2560, 3071);
        clears.Clear();
        using var wire = new MemoryStream();

        await request.Content!.CopyToAsync(wire);
        wire.Position = 0;
        var decoded = await RpcFrames.ReadControlAsync<RpcRequest>(wire, 65536, CancellationToken.None);

        Assert.Equal([new PageRange(0, 511), new PageRange(1024, 1535)], decoded.PageChanges!.PageRanges);
        Assert.Equal(new PageRange(2048, 2559), Assert.Single(decoded.PageChanges.ClearRanges));
        Assert.Equal(decoded.PageChanges.PageRanges, source.Requested);
        using var input = new FramedReadStream(wire, 1024);
        using var received = new MemoryStream();
        await input.CopyToAsync(received);
        Assert.Equal(new byte[1024], received.ToArray());
    }

    [Fact]
    public async Task PageCopyProducerCannotChangeRemainingPreparedRanges()
    {
        var pages = new List<PageRange> { new(0, 511), new(1024, 1535) };
        var source = new MutablePageCopySource(new PageRangeDiff(pages, []),
            () => pages[1] = new PageRange(2560, 3071));
        using var client = new ApplicationRpcClient(transportOptions, storageOptions);
        using var request = await CreatePageCopyRequestAsync(client, source);
        using var wire = new MemoryStream();

        await request.Content!.CopyToAsync(wire);

        Assert.Equal([new PageRange(0, 511), new PageRange(1024, 1535)], source.Requested);
    }

    [Theory]
    [InlineData("pages-missing")]
    [InlineData("clears-missing")]
    [InlineData("null-entry")]
    [InlineData("overlap")]
    [InlineData("unaligned")]
    public async Task InvalidWirePageDescriptorsCannotReachTheApplication(string invalid)
    {
        var changes = invalid switch
        {
            "pages-missing" => new PageRangeDiff(null!, []),
            "clears-missing" => new PageRangeDiff([], null!),
            "null-entry" => new PageRangeDiff([null!], []),
            "overlap" => new PageRangeDiff([new(0, 1023), new(512, 1535)], []),
            _ => new PageRangeDiff([new(1, 512)], []),
        };
        var source = new MutablePageCopySource(new PageRangeDiff([], []));
        var arguments = CreatePageCopyArguments(source)[..^2];
        using var wire = await CreateOperationRequestAsync(typeof(IBlobApplication),
            nameof(IBlobApplication.BeginIncrementalCopyFromPageRangesAsync), arguments, hasInput: true, pageChanges: changes);
        using var response = new MemoryStream();
        var context = CreateContext(wire, response);
        var dispatcher = new RecordingDispatcher();
        var admission = new RecordingAdmission();
        using var endpoint = new ApplicationRpcEndpoint(transportOptions, storageOptions, admission: admission);

        await endpoint.HandleAsync(context, dispatcher);
        response.Position = 0;
        var result = await RpcFrames.ReadControlAsync<RpcResponse>(response, 65536, CancellationToken.None);

        Assert.Equal(0, dispatcher.Calls);
        Assert.NotNull(result.Error);
        Assert.Equal(500, result.Error.StatusCode);
        Assert.Equal("InternalError", result.Error.Code);
        Assert.False(result.HasOutput);
        Assert.False(admission.Active);
        Assert.Equal(1, admission.Releases);
    }

    private static Task<HttpRequestMessage> CreatePageCopyRequestAsync(ApplicationRpcClient client, IPageCopySource source)
    {
        var contract = RpcContracts.GetContract(typeof(IBlobApplication));
        var method = contract.GetMethod(typeof(IBlobApplication)
            .GetMethod(nameof(IBlobApplication.BeginIncrementalCopyFromPageRangesAsync))!);
        return client.CreateRequestAsync(contract, method, CreatePageCopyArguments(source), CancellationToken.None);
    }

    private static object?[] CreatePageCopyArguments(IPageCopySource source) =>
            ["devstoreaccount1", "container", "blob", 3072L, "snapshot", "source", null, 0L,
                new PageRange[] { new(0, 511), new(1024, 1535) },
                new BlobWriteOptions(new BlobHttpProperties(), new Dictionary<string, string>(StringComparer.Ordinal)),
                "https://example.invalid/container/blob", null, source, CancellationToken.None];

    private sealed class MutablePageCopySource(PageRangeDiff changes, Action? mutate = null) : IPageCopySource
    {
        internal List<PageRange> Requested { get; } = [];

        public Task<PageRangeDiff> ReadChangesAsync(
            string? previousSnapshot, long previousLength, CancellationToken cancellationToken) => Task.FromResult(changes);

        public async Task ReadRangeAsync(
            PageRange range, Func<Stream, CancellationToken, Task> consume, CancellationToken cancellationToken)
        {
            Requested.Add(range);
            mutate?.Invoke();
            using var input = new MemoryStream(new byte[checked((int)(range.End - range.Start + 1))]);
            await consume(input, cancellationToken).ConfigureAwait(false);
        }
    }
}
