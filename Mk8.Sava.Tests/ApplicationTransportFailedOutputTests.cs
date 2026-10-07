using System.Reflection;
using Mk8.Sava.Application;
using Mk8.Sava.Protocol;
using Mk8.Sava.Storage;
using Mk8.Sava.Transport;

namespace Mk8.Sava.Tests;

public sealed class ApplicationTransportFailedOutputTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(31)]
    public async Task RealEndpointCannotCertifyOutputAfterDispatcherSwallowsABoundViolation(int length)
    {
        var dispatcher = new SwallowingOutputDispatcher(new byte[length]);
        var host = await RpcOutputTestHost.StartDispatcherAsync(dispatcher).ConfigureAwait(true);
        await using var hostLifetime = host.ConfigureAwait(false);
        using var destination = new MemoryStream();

        var failure = await Assert.ThrowsAsync<AzureStorageException>(() => host.Sessions.WriteRangeAsync(
            "test-session", new BlobEncryption(null, null), 0, length, destination, CancellationToken.None))
            .ConfigureAwait(true);

        Assert.Equal(503, failure.StatusCode);
        Assert.Equal("ServerBusy", failure.ErrorCode);
        Assert.True(dispatcher.SawRejectedOutput);
        Assert.Equal(1, dispatcher.Calls);
        Assert.True(destination.CanWrite);
        Assert.True(destination.Length <= length);
    }

    private sealed class SwallowingOutputDispatcher(byte[] content) : IApplicationRpcDispatcher
    {
        private int calls;
        private int rejected;
        internal int Calls => Volatile.Read(ref calls);
        internal bool SawRejectedOutput => Volatile.Read(ref rejected) != 0;

        public async ValueTask<object?> InvokeAsync(Type contract, MethodInfo method, object?[] arguments, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref calls);
            Assert.Equal(typeof(IApplicationReadSessions), contract);
            Assert.Equal(nameof(IApplicationReadSessions.WriteRangeAsync), method.Name);
            Assert.Equal(content.LongLength, arguments[3]);
            var output = Assert.IsAssignableFrom<Stream>(arguments[4]);
            await output.WriteAsync(content, cancellationToken).ConfigureAwait(false);
            try
            {
                await output.WriteAsync(new byte[1], cancellationToken).ConfigureAwait(false);
            }
            catch (InvalidDataException)
            {
                Volatile.Write(ref rejected, 1);
            }
            return null;
        }
    }
}
