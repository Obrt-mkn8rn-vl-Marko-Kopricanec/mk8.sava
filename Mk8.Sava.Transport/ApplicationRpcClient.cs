using System.Net;
using System.Net.Http.Headers;
using System.Reflection;
using System.Runtime.ExceptionServices;
using System.Security.Cryptography;
using System.Text.Json;
using Mk8.Sava.Configuration;
using Mk8.Sava.Protocol;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Transport;

public sealed class ApplicationRpcClient : IDisposable
{
    private const string ContractsHeader = "X-Mk8-Sava-Contracts";
    private readonly ApplicationTransportOptions options;
    private readonly SavaOptions storageOptions;
    private readonly HttpClient client;
    private readonly SocketsHttpHandler handler;
    private readonly byte[] accessKey;
    private readonly string policy;

    public ApplicationRpcClient(ApplicationTransportOptions options, SavaOptions storageOptions)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(storageOptions);
        this.options = options;
        this.storageOptions = storageOptions;
        accessKey = ApplicationTransportSecurity.ReadAccessKey(options);
        policy = ApplicationTransportSecurity.CreatePolicyFingerprint(storageOptions);
        handler = new SocketsHttpHandler
        {
            UseProxy = false,
            AllowAutoRedirect = false,
            AutomaticDecompression = DecompressionMethods.None,
            ConnectTimeout = options.ConnectTimeout,
        };
        client = new HttpClient(handler, disposeHandler: false)
        {
            Timeout = Timeout.InfiniteTimeSpan,
        };
    }

    public T CreateProxy<T>(object? localPolicy = null) where T : class
    {
        var contract = RpcContracts.GetContract(typeof(T));
        var proxy = DispatchProxy.Create<T, ApplicationRpcProxy>();
        ((ApplicationRpcProxy)(object)proxy).Initialize(this, contract, localPolicy);
        return proxy;
    }

    public void Dispose()
    {
        client.Dispose();
        handler.Dispose();
        CryptographicOperations.ZeroMemory(accessKey);
    }

    internal async Task<RpcResponse> InvokeAsync(
        RpcContract contract, RpcMethod method, object?[] arguments, CancellationToken cancellationToken)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var lane = RpcControlLane.IsAllowed(contract, method) ? ApplicationRpcLane.Control : ApplicationRpcLane.Bulk;
        timeout.CancelAfter(RpcControlLane.RequestTimeout(options.RequestTimeout, lane));
        RpcContent? content = null;
        try
        {
            using var request = await CreateRequestAsync(contract, method, arguments, timeout.Token).ConfigureAwait(false);
            content = (RpcContent)request.Content!;
            using var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, timeout.Token).ConfigureAwait(false);
            ValidateResponseHeaders(response);
            return await ReadResponseAsync(response, method, arguments, timeout.Token).ConfigureAwait(false);
        }
        catch (Exception exception) when (!CatastrophicExceptionPolicy.Contains(exception))
        {
            content?.RethrowProducerFailure();
            if (exception is AzureStorageException || cancellationToken.IsCancellationRequested)
                throw;
            throw Unavailable();
        }
    }

    internal async Task<HttpRequestMessage> CreateRequestAsync(
        RpcContract contract, RpcMethod method, object?[] arguments, CancellationToken cancellationToken)
    {
        var maximumInputBytes = RpcStreamLimits.InputLimit(method, arguments, storageOptions.MaximumRequestBodyBytes);
        var lane = RpcControlLane.IsAllowed(contract, method) ? ApplicationRpcLane.Control : ApplicationRpcLane.Bulk;
        RpcContent content;
        if (method.PageSourceIndex >= 0)
        {
            var pageSource = (IPageCopySource)arguments[method.PageSourceIndex]!;
            var currentIndex = Array.FindIndex(method.Parameters, parameter => string.Equals(parameter.Name, "current", StringComparison.Ordinal));
            var current = currentIndex < 0 ? null : (BlobRecord?)arguments[currentIndex];
            var changes = await pageSource.ReadChangesAsync(current?.IncrementalCopySourceSnapshot,
                current?.Content.Length ?? 0, cancellationToken).ConfigureAwait(false);
            var pageCopy = PageCopyPlan.Create(changes,
                ApplicationRpcEndpoint.ReadLongArgument(method, arguments, "sourceLength"));
            content = new PageRpcContent(contract, method, arguments, pageSource, pageCopy,
                RpcControlLane.FrameLimit(options.MaximumControlFrameBytes, lane), cancellationToken);
        }
        else
        {
            var input = method.InputIndex >= 0 ? (Stream?)arguments[method.InputIndex] : null;
            var maximumControlBytes = RpcControlLane.FrameLimit(options.MaximumControlFrameBytes, lane);
            content = input is null
                ? new EmptyRpcContent(contract, method, arguments, maximumControlBytes, maximumInputBytes, cancellationToken)
                : new StreamRpcContent(contract, method, arguments, input, maximumControlBytes, maximumInputBytes, cancellationToken);
        }

        HttpRequestMessage request;
        try
        {
            request = new HttpRequestMessage(HttpMethod.Post, RpcControlLane.GetEndpoint(options.Endpoint!, lane));
        }
        catch
        {
            content.Dispose();
            throw;
        }
        try
        {
            // Transfer content ownership before header setup, including its failure paths.
            request.Content = content;
            request.Headers.Add(ApplicationTransportSecurity.AccessKeyHeader, Convert.ToBase64String(accessKey));
            request.Headers.Add(ApplicationTransportSecurity.ProtocolHeader, ApplicationTransportSecurity.ProtocolVersion);
            request.Headers.Add(ApplicationTransportSecurity.PolicyHeader, policy);
            request.Headers.Add(ContractsHeader, RpcContracts.Fingerprint);
            return request;
        }
        catch
        {
            request.Dispose();
            throw;
        }
    }

    private void ValidateResponseHeaders(HttpResponseMessage response)
    {
        if (response.StatusCode != HttpStatusCode.OK ||
            !string.Equals(response.Content.Headers.ContentType?.MediaType, ApplicationTransportSecurity.ContentType, StringComparison.Ordinal) ||
            !HeaderMatches(response, ApplicationTransportSecurity.ProtocolHeader, ApplicationTransportSecurity.ProtocolVersion) ||
            !HeaderMatches(response, ApplicationTransportSecurity.PolicyHeader, policy) ||
            !HeaderMatches(response, ContractsHeader, RpcContracts.Fingerprint))
            throw Unavailable();
    }

    private async Task<RpcResponse> ReadResponseAsync(
        HttpResponseMessage response, RpcMethod method, object?[] arguments, CancellationToken cancellationToken)
    {
        var body = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
        await using var bodyLifetime = body.ConfigureAwait(false);
        var contract = RpcContracts.GetContract(method.Method.DeclaringType!);
        var lane = RpcControlLane.IsAllowed(contract, method) ? ApplicationRpcLane.Control : ApplicationRpcLane.Bulk;
        var result = await RpcFrames.ReadControlAsync<RpcResponse>(body,
            RpcControlLane.FrameLimit(options.MaximumControlFrameBytes, lane), cancellationToken).ConfigureAwait(false);
        if (result.Error is not null)
        {
            if (result.HasOutput || result.Result.ValueKind is not (JsonValueKind.Null or JsonValueKind.Undefined))
                throw new InvalidDataException("The application returned an invalid error response.");
            throw result.Error.ToException();
        }
        if (result.HasOutput != (method.OutputIndex >= 0))
            throw new InvalidDataException("The application response does not match its operation.");
        if (result.HasOutput)
            await CopyOutputAsync(body, method, arguments, cancellationToken).ConfigureAwait(false);
        else
        {
            var trailing = new byte[1];
            if (await body.ReadAsync(trailing, cancellationToken).ConfigureAwait(false) != 0)
                throw new InvalidDataException("The application response has trailing data.");
        }
        return result;
    }

    private static async Task CopyOutputAsync(
        Stream body, RpcMethod method, object?[] arguments, CancellationToken cancellationToken)
    {
        var length = ApplicationRpcEndpoint.ReadLongArgument(method, arguments, "length");
        using var output = new FramedReadStream(body, length);
        var destination = (Stream)arguments[method.OutputIndex]!;
        // Even empty output needs a positive read to verify terminal proof and EOF.
        var buffer = new byte[(int)Math.Clamp(length, 1, RpcFrames.MaximumDataFrameBytes)];
        long written = 0;
        while (true)
        {
            var count = await output.ReadAsync(buffer, cancellationToken).ConfigureAwait(false);
            if (count == 0)
                break;
            await destination.WriteAsync(buffer.AsMemory(0, count), cancellationToken).ConfigureAwait(false);
            written += count;
        }
        if (written != length)
            throw new InvalidDataException("The application output was truncated.");
    }

    private static bool HeaderMatches(HttpResponseMessage response, string name, string expected) =>
        response.Headers.TryGetValues(name, out var values) && values.SequenceEqual([expected], StringComparer.Ordinal);

    private static AzureStorageException Unavailable() => new(
        503, "ServerBusy", "The storage application is unavailable or could not complete the operation.");

    private static RpcRequestPayload CreatePayload(
        RpcContract contract, RpcMethod method, object?[] arguments, bool hasInput, PageRangeDiff? pageChanges = null) => new(
            contract.Type.FullName!, method.Id, arguments.Where((_, index) => method.IsControlParameter(index)).ToArray(),
            hasInput, pageChanges);

    // Only the three private sealed cases select a producer/descriptor/bound.
    // The content itself owns that choice; no extra per-request wrapper is needed.
    private abstract class RpcContent : HttpContent
    {
        private readonly RpcRequestPayload request;
        private readonly int maximumControlBytes;
        private readonly long maximumInputBytes;
        private readonly CancellationToken requestCancellation;
        private ExceptionDispatchInfo? producerFailure;
        private int serializationStarted;

        protected RpcContent(RpcRequestPayload request,
            int maximumControlBytes, long maximumInputBytes, CancellationToken requestCancellation)
        {
            this.request = request;
            this.maximumControlBytes = maximumControlBytes;
            this.maximumInputBytes = maximumInputBytes;
            this.requestCancellation = requestCancellation;
            Headers.ContentType = new MediaTypeHeaderValue(ApplicationTransportSecurity.ContentType);
        }

        protected sealed override Task SerializeToStreamAsync(Stream stream, TransportContext? context) =>
            WriteAsync(stream, requestCancellation);

        protected sealed override Task SerializeToStreamAsync(Stream stream, TransportContext? context, CancellationToken cancellationToken) =>
            WriteAsync(stream, cancellationToken);

        private async Task WriteAsync(Stream stream, CancellationToken cancellationToken)
        {
            // A handler-level retry must never replay a mutation or turn an
            // already-consumed producer into a valid empty input stream.
            if (Interlocked.Exchange(ref serializationStarted, 1) != 0)
                throw new InvalidOperationException("An application request body cannot be serialized more than once.");
            Exception? wireFailure = null;
            using var trackedWire = new RpcTransportWriteStream(stream, exception => wireFailure = exception);
            try
            {
                await RpcFrames.WriteControlAsync(trackedWire, request, maximumControlBytes, cancellationToken).ConfigureAwait(false);
                using var framed = new FramedWriteStream(trackedWire, maximumInputBytes);
                await WriteInputAsync(framed, cancellationToken).ConfigureAwait(false);
                // This terminal proof is never written if the producer throws or
                // is canceled, even after every expected payload byte arrived.
                await framed.CompleteAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                if (wireFailure is null || CatastrophicExceptionPolicy.Contains(exception))
                    Interlocked.CompareExchange(ref producerFailure, ExceptionDispatchInfo.Capture(exception), null);
                throw;
            }
        }

        protected abstract Task WriteInputAsync(FramedWriteStream destination, CancellationToken cancellationToken);

        internal void RethrowProducerFailure()
        {
            var failure = Volatile.Read(ref producerFailure);
            if (failure is not null && (CatastrophicExceptionPolicy.Contains(failure.SourceException) ||
                StorageExceptionMapper.IsKnown(failure.SourceException) || failure.SourceException is IOException or HttpRequestException))
                failure.Throw();
        }

        protected sealed override bool TryComputeLength(out long length)
        {
            length = 0;
            return false;
        }
    }

    private sealed class EmptyRpcContent(
        RpcContract contract, RpcMethod method, object?[] arguments,
        int maximumControlBytes, long maximumInputBytes, CancellationToken cancellationToken)
        : RpcContent(CreatePayload(contract, method, arguments, hasInput: false),
            maximumControlBytes, maximumInputBytes, cancellationToken)
    {
        protected override Task WriteInputAsync(FramedWriteStream destination, CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private sealed class StreamRpcContent : RpcContent
    {
#pragma warning disable CA2213 // Borrowed request stream: its Gateway owner retains and disposes it after RPC completion.
        private readonly Stream input;
#pragma warning restore CA2213

        internal StreamRpcContent(RpcContract contract, RpcMethod method, object?[] arguments, Stream input,
            int maximumControlBytes, long maximumInputBytes, CancellationToken cancellationToken)
            : base(CreatePayload(contract, method, arguments, hasInput: true), maximumControlBytes, maximumInputBytes, cancellationToken)
        {
            this.input = input;
        }

        protected override Task WriteInputAsync(FramedWriteStream destination, CancellationToken cancellationToken) =>
            input.CopyToAsync(destination, RpcFrames.MaximumDataFrameBytes, cancellationToken);
    }

    private sealed class PageRpcContent : RpcContent
    {
        private readonly IPageCopySource source;
        private readonly PageCopyPlan plan;

        internal PageRpcContent(RpcContract contract, RpcMethod method, object?[] arguments, IPageCopySource source,
            PageCopyPlan plan, int maximumControlBytes, CancellationToken cancellationToken)
            : base(CreatePayload(contract, method, arguments, hasInput: true, plan.Descriptor),
                maximumControlBytes, plan.DataLength, cancellationToken)
        {
            this.source = source;
            this.plan = plan;
        }

        protected override Task WriteInputAsync(FramedWriteStream destination, CancellationToken cancellationToken) =>
            PageCopyFrames.WriteAsync(source, plan, destination, cancellationToken);
    }
}
