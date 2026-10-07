using System.Buffers.Binary;
using System.Reflection;
using System.Security.Cryptography;
using Mk8.Sava.Application;
using Mk8.Sava.Storage;
using Mk8.Sava.Transport;

namespace Mk8.Sava.Tests;

public sealed partial class ApplicationTransportSecurityTests
{
    [Theory]
    [InlineData("header")]
    [InlineData("proof")]
    public async Task DispatcherCannotTurnARejectedInputIntoASuccessfulRpcResponseBySwallowingItsFirstFailure(string failure)
    {
        var admission = new RecordingAdmission();
        using var endpoint = new ApplicationRpcEndpoint(transportOptions, storageOptions, admission: admission);
        using var input = new MemoryStream();
        using var response = new MemoryStream();
        var contract = RpcContracts.GetContract(typeof(IBlobApplication));
        var method = contract.GetMethod(typeof(IBlobApplication).GetMethod(nameof(IBlobApplication.StageBlockAsync))!);
        await RpcFrames.WriteControlAsync(input, new RpcRequestPayload(contract.Type.FullName!, method.Id,
            ["devstoreaccount1", "container", "blob", "YmxvY2s=", new BlobEncryption(null, null)], HasInput: true, null),
            4096, CancellationToken.None).ConfigureAwait(true);
        var terminal = new byte[4 + SHA256.HashSizeInBytes];
        SHA256.HashData(ReadOnlySpan<byte>.Empty).CopyTo(terminal, 4);
        if (failure is "header")
        {
            var header = new byte[4];
            BinaryPrimitives.WriteInt32BigEndian(header, -1);
            input.Write(header);
        }
        else
        {
            var corrupt = (byte[])terminal.Clone();
            corrupt[^1] ^= 1;
            input.Write(corrupt);
        }
        input.Write(terminal);
        input.Position = 0;
        var dispatcher = new SwallowingInputDispatcher();

        await endpoint.HandleAsync(CreateContext(input, response), dispatcher).ConfigureAwait(true);

        Assert.Equal(1, dispatcher.Calls);
        Assert.True(dispatcher.SawRejectedInput);
        Assert.Equal(1, admission.Releases);
        Assert.False(admission.Active);
        response.Position = 0;
        var result = await RpcFrames.ReadControlAsync<RpcResponse>(response, 4096, CancellationToken.None).ConfigureAwait(true);
        Assert.NotNull(result.Error);
        Assert.False(result.HasOutput);
        Assert.True(input.CanRead);
    }

    private sealed class SwallowingInputDispatcher : IApplicationRpcDispatcher
    {
        internal int Calls { get; private set; }
        internal bool SawRejectedInput { get; private set; }

        public async ValueTask<object?> InvokeAsync(Type contract, MethodInfo method, object?[] arguments, CancellationToken cancellationToken)
        {
            Calls++;
            var input = Assert.Single(arguments.OfType<Stream>());
            try
            {
                await input.CopyToAsync(Stream.Null, cancellationToken).ConfigureAwait(false);
            }
            catch (InvalidDataException)
            {
                SawRejectedInput = true;
            }
            return new BlobEncryption(null, null);
        }
    }
}
