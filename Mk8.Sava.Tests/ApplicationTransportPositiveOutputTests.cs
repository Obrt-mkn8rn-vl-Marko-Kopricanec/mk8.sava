using Mk8.Sava.Storage;
using Xunit.Abstractions;

namespace Mk8.Sava.Tests;

[Collection("Storage allocation benchmark")]
public sealed class ApplicationTransportPositiveOutputTests(ITestOutputHelper output)
{
    [Theory]
    [InlineData(31)]
    [InlineData(4096)]
    public async Task RealEndpointSmallPositiveOutputDoesNotReserveAMaximumReaderFrame(int length)
    {
        const int iterations = 32;
        const long perOperationBudget = 48 * 1024;
        var content = Enumerable.Range(0, length).Select(index => (byte)(index % 251)).ToArray();
        var host = await RpcOutputTestHost.StartAsync(content).ConfigureAwait(true);
        await using var hostLifetime = host.ConfigureAwait(false);
        using var destination = new MemoryStream(length);
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
        output.WriteLine("positive_http_allocation,length={0},operations={1},managed_bytes={2},budget_bytes={3}",
            length, iterations, allocated, iterations * perOperationBudget);
        Assert.True(allocated <= iterations * perOperationBudget,
            $"Observed {allocated} managed bytes for {iterations} small positive HTTP outputs of {length} bytes.");
        Assert.Equal(content, destination.ToArray());
        Assert.True(destination.CanWrite);
    }

    private static Task ReadAsync(RpcOutputTestHost host, long length, Stream destination) =>
        host.Sessions.WriteRangeAsync("test-session", new BlobEncryption(null, null), 0, length, destination, CancellationToken.None);
}
