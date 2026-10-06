using System.Net;
using System.Security.Cryptography;
using System.Text.Json;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Mk8.Sava.Configuration;
using Mk8.Sava.Protocol;

namespace Mk8.Sava.Transport;

public sealed class ApplicationRpcEndpoint : IDisposable
{
    private const string ContractsHeader = "X-Mk8-Sava-Contracts";
    private readonly ApplicationTransportOptions options;
    private readonly SavaOptions storageOptions;
    private readonly byte[] accessKey;
    private readonly string policy;
    private readonly string peerFingerprint;
    private readonly ILogger<ApplicationRpcEndpoint> logger;
    private readonly IApplicationRpcAdmission? admission;
    private volatile bool disposed;

    public ApplicationRpcEndpoint(
        ApplicationTransportOptions options, SavaOptions storageOptions, ILogger<ApplicationRpcEndpoint>? logger = null,
        IApplicationRpcAdmission? admission = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(storageOptions);
        this.options = options;
        this.storageOptions = storageOptions;
        this.logger = logger ?? NullLogger<ApplicationRpcEndpoint>.Instance;
        this.admission = admission;
        accessKey = ApplicationTransportSecurity.ReadAccessKey(options);
        policy = ApplicationTransportSecurity.CreatePolicyFingerprint(storageOptions);
        peerFingerprint = Convert.ToHexStringLower(SHA256.HashData(accessKey));
    }

    public Task HandleAsync(HttpContext context, IApplicationRpcDispatcher dispatcher)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(dispatcher);
        return HandleCoreAsync(context, dispatcher, ApplicationRpcLane.Bulk);
    }

    public Task HandleControlAsync(HttpContext context, IApplicationRpcDispatcher dispatcher)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(dispatcher);
        return HandleCoreAsync(context, dispatcher, ApplicationRpcLane.Control);
    }

    private async Task HandleCoreAsync(
        HttpContext context, IApplicationRpcDispatcher dispatcher, ApplicationRpcLane lane)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(dispatcher);
        if (!ValidatePreflight(context))
            return;
        if (context.Features.Get<IHttpMaxRequestBodySizeFeature>() is { IsReadOnly: false } limit)
            limit.MaxRequestBodySize = lane == ApplicationRpcLane.Control ? RpcControlLane.MaximumRequestBytes :
                checked(RpcStreamLimits.MaximumBlobBytes * 5 + options.MaximumControlFrameBytes + 40);

        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(context.RequestAborted);
        timeout.CancelAfter(RpcControlLane.RequestTimeout(options.RequestTimeout, lane));
        var cancellationToken = timeout.Token;
        var frameLimit = RpcControlLane.FrameLimit(options.MaximumControlFrameBytes, lane);
        using var peer = ApplicationRpcIdentity.Enter(peerFingerprint);
        IAsyncDisposable? lease = null;
        try
        {
            if (admission is not null)
                lease = await admission.AcquireAsync(lane, cancellationToken).ConfigureAwait(false);
            var request = await RpcFrames.ReadControlAsync<RpcRequest>(context.Request.Body, frameLimit, cancellationToken).ConfigureAwait(false);
            await InvokeRequestAsync(context, dispatcher, request, lane, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException exception) when (cancellationToken.IsCancellationRequested && !CatastrophicExceptionPolicy.Contains(exception))
        {
            context.Abort();
        }
        catch (Exception exception) when (!CatastrophicExceptionPolicy.Contains(exception))
        {
            if (!StorageExceptionMapper.IsKnown(exception))
                RpcLogMessages.OperationFailed(logger, exception.GetType().Name, context.TraceIdentifier);
            if (context.Response.HasStarted)
                context.Abort();
            else
                await WriteResponseAsync(context, new RpcResponsePayload(null, RpcError.FromException(exception), HasOutput: false),
                    cancellationToken, frameLimit).ConfigureAwait(false);
        }
        finally
        {
            if (lease is not null)
                await lease.DisposeAsync().ConfigureAwait(false);
        }
    }

    private bool ValidatePreflight(HttpContext context)
    {
        if (!IsAuthenticated(context))
        {
            context.Response.StatusCode = StatusCodes.Status403Forbidden;
            return false;
        }
        if (!IsCompatibleRequest(context))
        {
            context.Response.StatusCode = StatusCodes.Status409Conflict;
            return false;
        }
        return true;
    }

    private async Task InvokeRequestAsync(
        HttpContext context, IApplicationRpcDispatcher dispatcher, RpcRequest request,
        ApplicationRpcLane lane, CancellationToken cancellationToken)
    {
        var contract = RpcContracts.GetContract(request.Contract);
        if (!contract.Methods.TryGetValue(request.Method, out var method))
            throw new InvalidDataException("The application operation is not supported.");
        if (lane == ApplicationRpcLane.Control &&
            (!RpcControlLane.IsAllowed(contract, method) || request.HasInput || request.PageChanges is not null))
            throw new InvalidDataException("The application operation is not permitted on the control lane.");
        var arguments = DeserializeArguments(method, request.Arguments, cancellationToken);
        try
        {
            await InvokePreparedAsync(context, dispatcher, request, contract, method, arguments, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            RpcSensitiveArguments.Clear(arguments);
        }
    }

    private async Task InvokePreparedAsync(
        HttpContext context, IApplicationRpcDispatcher dispatcher, RpcRequest request, RpcContract contract,
        RpcMethod method, object?[] arguments, CancellationToken cancellationToken)
    {
        var pageCopy = method.PageSourceIndex >= 0 ? PreparePageCopy(request, method, arguments) : null;
        using var input = new FramedReadStream(context.Request.Body,
            pageCopy?.DataLength ?? GetMaximumInputBytes(method, request, arguments));
        using var pageSource = pageCopy is null ? null : new PageCopyFrames.ServerSource(pageCopy, input);
        if (pageSource is not null)
            arguments[method.PageSourceIndex] = pageSource;
        else if (method.InputIndex >= 0)
            arguments[method.InputIndex] = request.HasInput ? input : null;
        if (!request.HasInput)
            await input.EnsureCompletedAsync(cancellationToken).ConfigureAwait(false);
        var output = CreateOutput(context, method, arguments);
        try
        {
            var result = await dispatcher.InvokeAsync(contract.Type, method.Method, arguments, cancellationToken).ConfigureAwait(false);
            await input.EnsureCompletedAsync(cancellationToken).ConfigureAwait(false);
            if (output is not null)
            {
                if (output.Length != ReadLongArgument(method, arguments, "length"))
                    throw new InvalidDataException("The application output length did not match its declared range.");
                await output.CompleteAsync(cancellationToken).ConfigureAwait(false);
            }
            else
                await WriteResponseAsync(context, new RpcResponsePayload(RpcListingProjection.Apply(method, result), null, HasOutput: false),
                    cancellationToken, RpcControlLane.IsAllowed(contract, method) ? RpcControlLane.MaximumFrameBytes : null).ConfigureAwait(false);
        }
        finally
        {
            if (output is not null)
                await output.DisposeAsync().ConfigureAwait(false);
        }
    }

    private long GetMaximumInputBytes(RpcMethod method, RpcRequest request, object?[] arguments)
    {
        if (request.PageChanges is not null || (request.HasInput && method.InputIndex < 0))
            throw new InvalidDataException("The application input does not match its operation.");
        if (method.InputIndex < 0)
            return 0;
        if (request.HasInput && string.Equals(method.Method.Name, "PutPageAsync", StringComparison.Ordinal))
        {
            var clearIndex = Array.FindIndex(method.Parameters, parameter => string.Equals(parameter.Name, "clear", StringComparison.Ordinal));
            if (clearIndex >= 0 && arguments[clearIndex] is true)
                throw new InvalidDataException("A page-clear operation cannot carry an input stream.");
        }
        return RpcStreamLimits.InputLimit(method, arguments, storageOptions.MaximumRequestBodyBytes);
    }

    private static PageCopyPlan PreparePageCopy(RpcRequest request, RpcMethod method, object?[] arguments)
    {
        if (request.PageChanges is null || !request.HasInput)
            throw new InvalidDataException("The application page-copy descriptor is missing.");
        return PageCopyPlan.Create(request.PageChanges, ReadLongArgument(method, arguments, "sourceLength"));
    }

    private FramedWriteStream? CreateOutput(HttpContext context, RpcMethod method, object?[] arguments)
    {
        if (method.OutputIndex < 0)
            return null;
        var maximumOutputBytes = ReadLongArgument(method, arguments, "length");
        if (maximumOutputBytes < 0 || maximumOutputBytes > RpcStreamLimits.MaximumBlobBytes)
            throw new InvalidDataException("The application output length is invalid.");
        var output = new FramedWriteStream(context.Response.Body, maximumOutputBytes, token =>
            WriteResponseAsync(context, new RpcResponsePayload(null, null, HasOutput: true), token));
        arguments[method.OutputIndex] = output;
        return output;
    }

    private bool IsCompatibleRequest(HttpContext context) =>
        HttpMethods.IsPost(context.Request.Method) &&
        string.Equals(context.Request.ContentType, ApplicationTransportSecurity.ContentType, StringComparison.Ordinal) &&
        context.Request.Headers[ApplicationTransportSecurity.ProtocolHeader] == ApplicationTransportSecurity.ProtocolVersion &&
        context.Request.Headers[ApplicationTransportSecurity.PolicyHeader] == policy &&
        context.Request.Headers[ContractsHeader] == RpcContracts.Fingerprint;

    public void Dispose()
    {
        disposed = true;
        CryptographicOperations.ZeroMemory(accessKey);
    }

    public bool IsAuthenticated(HttpContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        ObjectDisposedException.ThrowIf(disposed, this);
        if (string.Equals(options.Endpoint!.Scheme, Uri.UriSchemeHttp, StringComparison.Ordinal) &&
            (!IPAddress.IsLoopback(context.Connection.LocalIpAddress ?? IPAddress.Any) ||
             !IPAddress.IsLoopback(context.Connection.RemoteIpAddress ?? IPAddress.Any)))
            return false;
        if (string.Equals(options.Endpoint.Scheme, Uri.UriSchemeHttps, StringComparison.Ordinal) && !context.Request.IsHttps)
            return false;
        var value = context.Request.Headers[ApplicationTransportSecurity.AccessKeyHeader];
        if (value.Count != 1 || value[0] is not { Length: <= 1024 } encoded)
            return false;
        Span<byte> received = stackalloc byte[512];
        var authenticated = Convert.TryFromBase64String(encoded, received, out var count) &&
                            CryptographicOperations.FixedTimeEquals(accessKey, received[..count]);
        CryptographicOperations.ZeroMemory(received);
        return authenticated && !disposed;
    }

    private Task WriteResponseAsync(
        HttpContext context, RpcResponsePayload response, CancellationToken cancellationToken, int? frameLimit = null)
    {
        context.Response.StatusCode = StatusCodes.Status200OK;
        context.Response.ContentType = ApplicationTransportSecurity.ContentType;
        context.Response.Headers[ApplicationTransportSecurity.ProtocolHeader] = ApplicationTransportSecurity.ProtocolVersion;
        context.Response.Headers[ApplicationTransportSecurity.PolicyHeader] = policy;
        context.Response.Headers[ContractsHeader] = RpcContracts.Fingerprint;
        return RpcFrames.WriteControlAsync(context.Response.Body, response, frameLimit ?? options.MaximumControlFrameBytes, cancellationToken);
    }

    private static object?[] DeserializeArguments(RpcMethod method, JsonElement[] values, CancellationToken cancellationToken)
    {
        if (values.Length != method.Parameters.Count(parameter =>
                parameter.ParameterType != typeof(CancellationToken) && parameter.ParameterType != typeof(Stream) &&
                parameter.ParameterType != typeof(Mk8.Sava.Storage.IPageCopySource)))
            throw new InvalidDataException("The application parameter count is invalid.");
        var arguments = new object?[method.Parameters.Length];
        var valueIndex = 0;
        try
        {
            for (var index = 0; index < arguments.Length; index++)
            {
                if (method.Parameters[index].ParameterType == typeof(CancellationToken))
                    arguments[index] = cancellationToken;
                else if (method.IsControlParameter(index))
                    arguments[index] = values[valueIndex++].Deserialize(method.Parameters[index].ParameterType, RpcJson.Options);
            }
            return arguments;
        }
        catch
        {
            RpcSensitiveArguments.Clear(arguments);
            throw;
        }
    }

    internal static long ReadLongArgument(RpcMethod method, object?[] arguments, string name)
    {
        for (var index = 0; index < arguments.Length; index++)
        {
            if (string.Equals(method.Parameters[index].Name, name, StringComparison.Ordinal) && arguments[index] is long value)
                return value;
        }
        throw new InvalidDataException("The application streaming operation has no declared length.");
    }
}
