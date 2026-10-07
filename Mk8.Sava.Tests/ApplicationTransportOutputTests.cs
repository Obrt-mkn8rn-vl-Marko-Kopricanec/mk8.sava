using Mk8.Sava.Protocol;
using Mk8.Sava.Storage;
using Mk8.Sava.Transport;
using Xunit.Abstractions;

namespace Mk8.Sava.Tests;

[Collection("Storage allocation benchmark")]
public sealed class ApplicationTransportOutputTests(ITestOutputHelper output)
{
    [Theory]
    [InlineData(0)]
    [InlineData(31)]
    [InlineData(4096)]
    [InlineData(65536)]
    [InlineData(131089)]
    public async Task RealEndpointCopiesExactOutputAndBorrowsDestination(int length)
    {
        var content = Content(length);
        var host = await RpcOutputTestHost.StartAsync(content).ConfigureAwait(true);
        await using var hostLifetime = host.ConfigureAwait(false);
        using var destination = new TrackingDestination(length);

        await ReadAsync(host, length, destination).ConfigureAwait(true);

        Assert.Equal(content, destination.ToArray());
        Assert.False(destination.WasDisposed);
        Assert.Equal(length > 0, destination.Writes > 0);
    }

    [Theory]
    [InlineData(0, 48)]
    [InlineData(31, 112)]
    [InlineData(65536, 192)]
    public async Task SmallRealEndpointOutputsStayWithinManagedAllocationBudget(int length, int kibibytesPerOperation)
    {
        const int iterations = 32;
        var host = await RpcOutputTestHost.StartAsync(Content(length)).ConfigureAwait(true);
        await using var hostLifetime = host.ConfigureAwait(false);
        using var destination = new TrackingDestination(length);
        for (var index = 0; index < 16; index++)
        {
            destination.SetLength(0);
            await ReadAsync(host, length, destination).ConfigureAwait(true);
        }
        var before = GC.GetTotalAllocatedBytes(precise: true);
        for (var index = 0; index < iterations; index++)
        {
            destination.SetLength(0);
            await ReadAsync(host, length, destination).ConfigureAwait(true);
        }
        var allocated = GC.GetTotalAllocatedBytes(precise: true) - before;
        output.WriteLine($"rpc_output_allocation,length={length},operations={iterations},managed_bytes={allocated}," +
            $"budget_bytes={iterations * kibibytesPerOperation * 1024L},scope=warmed_loopback_client_and_server_process");
        Assert.Equal(length, destination.Length);
        Assert.Equal(Content(length), destination.ToArray());
        Assert.False(destination.WasDisposed);
        Assert.True(allocated <= iterations * kibibytesPerOperation * 1024L,
            $"Observed {allocated} process-wide managed bytes for {iterations} output operations of {length} bytes.");
    }

    [Theory]
    [InlineData("missing-terminal", 0)]
    [InlineData("missing-terminal", 31)]
    [InlineData("corrupt-proof", 0)]
    [InlineData("corrupt-proof", 31)]
    [InlineData("truncated-proof", 0)]
    [InlineData("trailing-data", 0)]
    [InlineData("trailing-data", 31)]
    [InlineData("missing-output", 0)]
    [InlineData("excess-output", 0)]
    [InlineData("short-output", 31)]
    public async Task AuthenticatedMalformedOutputNeverReportsSuccess(string fault, int length)
    {
        var actualLength = fault switch { "excess-output" => 1, "short-output" => length - 1, _ => length };
        var packet = await CreateResponseAsync(Content(actualLength), hasOutput: !string.Equals(fault, "missing-output", StringComparison.Ordinal))
            .ConfigureAwait(true);
        packet = fault switch
        {
            "missing-terminal" => packet[..^36],
            "truncated-proof" => packet[..^1],
            "trailing-data" => [.. packet, 42],
            _ => packet,
        };
        if (string.Equals(fault, "corrupt-proof", StringComparison.Ordinal))
            packet[^1] ^= 1;
        var host = await RpcOutputTestHost.StartPeerAsync(
            context => context.Response.Body.WriteAsync(packet, context.RequestAborted).AsTask()).ConfigureAwait(true);
        await using var hostLifetime = host.ConfigureAwait(false);
        using var destination = new TrackingDestination(length);

        var failure = await Assert.ThrowsAsync<AzureStorageException>(() => ReadAsync(host, length, destination))
            .ConfigureAwait(true);

        Assert.Equal(503, failure.StatusCode);
        Assert.Equal("ServerBusy", failure.ErrorCode);
        Assert.False(destination.WasDisposed);
        Assert.Equal(1, host.RequestCount);
        Assert.True(destination.Length <= length);
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(long.MaxValue)]
    public async Task InvalidDeclaredLengthsCannotTurnAnEmptyProofIntoSuccess(long length)
    {
        var packet = await CreateResponseAsync([]).ConfigureAwait(true);
        var host = await RpcOutputTestHost.StartPeerAsync(
            context => context.Response.Body.WriteAsync(packet, context.RequestAborted).AsTask()).ConfigureAwait(true);
        await using var hostLifetime = host.ConfigureAwait(false);
        using var destination = new TrackingDestination(0);

        var failure = await Assert.ThrowsAsync<AzureStorageException>(() => ReadAsync(host, length, destination))
            .ConfigureAwait(true);

        Assert.Equal("ServerBusy", failure.ErrorCode);
        Assert.Equal(0, destination.Length);
        Assert.False(destination.WasDisposed);
        Assert.Equal(1, host.RequestCount);
    }

    [Fact]
    public async Task EmptyOutputRequiresTheHeldTerminalProofAndHonorsCallerCancellation()
    {
        var packet = await CreateResponseAsync([]).ConfigureAwait(true);
        var proofHeld = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var release = new SemaphoreSlim(0, 1);
        var host = await RpcOutputTestHost.StartPeerAsync(async context =>
        {
            await context.Response.Body.WriteAsync(packet.AsMemory(0, packet.Length - 32), context.RequestAborted)
                .ConfigureAwait(false);
            await context.Response.Body.FlushAsync(context.RequestAborted).ConfigureAwait(false);
            proofHeld.SetResult();
            await release.WaitAsync(context.RequestAborted).ConfigureAwait(false);
        }).ConfigureAwait(true);
        await using var hostLifetime = host.ConfigureAwait(false);
        using var destination = new TrackingDestination(0);
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await AssertHeldProofCancellationAsync(host, destination, cancellation, proofHeld, release).ConfigureAwait(true);
    }

    private static async Task AssertHeldProofCancellationAsync(RpcOutputTestHost host, TrackingDestination destination,
        CancellationTokenSource cancellation, TaskCompletionSource proofHeld, SemaphoreSlim release)
    {
        var operation = ReadAsync(host, 0, destination, cancellation.Token);
        try
        {
            await proofHeld.Task.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
            Assert.False(operation.IsCompleted);
            await cancellation.CancelAsync().ConfigureAwait(false);
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => operation.WaitAsync(TimeSpan.FromSeconds(5)))
                .ConfigureAwait(false);
            Assert.False(destination.WasDisposed);
            Assert.Equal(0, destination.Writes);
        }
        finally
        {
            release.Release();
            await ObserveAsync(operation).ConfigureAwait(false);
        }
    }

    [Theory]
    [MemberData(nameof(ParquetFatalExceptionTests.FatalFailures), MemberType = typeof(ParquetFatalExceptionTests))]
    public async Task FatalDestinationFailureEscapesWithoutDisposal(Exception injected)
    {
        var host = await RpcOutputTestHost.StartAsync(Content(31)).ConfigureAwait(true);
        await using var hostLifetime = host.ConfigureAwait(false);
        using var destination = new TrackingDestination(31, injected);
        var failure = await Assert.ThrowsAnyAsync<Exception>(() => ReadAsync(host, 31, destination)).ConfigureAwait(true);
        Assert.Same(injected, failure);
        Assert.False(destination.WasDisposed);
        Assert.Equal(1, destination.Writes);
        Assert.Equal(0, destination.Length);
    }

    [Fact]
    public async Task OrdinaryDestinationFailureDoesNotBecomeSuccessOrDisposeTheBorrowedStream()
    {
        var host = await RpcOutputTestHost.StartAsync(Content(31)).ConfigureAwait(true);
        await using var hostLifetime = host.ConfigureAwait(false);
        using var destination = new TrackingDestination(31, new IOException("private destination failure"));
        var failure = await Assert.ThrowsAsync<AzureStorageException>(() => ReadAsync(host, 31, destination)).ConfigureAwait(true);
        Assert.Equal("ServerBusy", failure.ErrorCode);
        Assert.DoesNotContain("private", failure.Message, StringComparison.Ordinal);
        Assert.False(destination.WasDisposed);
        Assert.Equal(1, destination.Writes);
        Assert.Equal(0, destination.Length);
    }

    private static Task ReadAsync(RpcOutputTestHost host, long length, Stream destination,
        CancellationToken cancellationToken = default) =>
        host.Sessions.WriteRangeAsync("test-session", new BlobEncryption(null, null), 0, length, destination, cancellationToken);

    private static byte[] Content(int length) => Enumerable.Range(0, length).Select(index => (byte)(index % 251)).ToArray();

    private static async Task<byte[]> CreateResponseAsync(byte[] content, bool hasOutput = true)
    {
        using var wire = new MemoryStream();
        await RpcFrames.WriteControlAsync(wire, new RpcResponsePayload(null, null, hasOutput), 65536, CancellationToken.None)
            .ConfigureAwait(false);
        if (hasOutput)
        {
            using var framed = new FramedWriteStream(wire, content.LongLength);
            await framed.WriteAsync(content).ConfigureAwait(false);
            await framed.CompleteAsync(CancellationToken.None).ConfigureAwait(false);
        }
        return wire.ToArray();
    }

    private static async Task ObserveAsync(Task operation)
    {
        try
        {
            await operation.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
        }
        catch (OperationCanceledException exception) when (!CatastrophicExceptionPolicy.Contains(exception))
        {
            // The deliberately canceled operation has now been joined before destination disposal.
        }
    }

    private sealed class TrackingDestination(int capacity, Exception? failure = null) : MemoryStream(capacity)
    {
        public bool WasDisposed { get; private set; }
        public int Writes { get; private set; }

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            Writes++;
            return failure is null ? base.WriteAsync(buffer, cancellationToken) : ValueTask.FromException(failure);
        }

        protected override void Dispose(bool disposing)
        {
            WasDisposed = true;
            base.Dispose(disposing);
        }
    }
}
