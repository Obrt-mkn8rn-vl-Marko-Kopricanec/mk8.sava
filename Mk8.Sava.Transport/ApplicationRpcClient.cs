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
        PageRangeDiff? changes = null;
        IPageCopySource? pageSource = null;
        var maximumInputBytes = RpcStreamLimits.InputLimit(method, arguments, storageOptions.MaximumRequestBodyBytes);
        if (method.PageSourceIndex >= 0)
        {
            pageSource = (IPageCopySource)arguments[method.PageSourceIndex]!;
            var currentIndex = Array.FindIndex(method.Parameters, parameter => string.Equals(parameter.Name, "current", StringComparison.Ordinal));
            var current = currentIndex < 0 ? null : (BlobRecord?)arguments[currentIndex];
            changes = await pageSource.ReadChangesAsync(current?.IncrementalCopySourceSnapshot,
                current?.Content.Length ?? 0, cancellationToken).ConfigureAwait(false);
            maximumInputBytes = PageCopyFrames.Validate(changes,
                ApplicationRpcEndpoint.ReadLongArgument(method, arguments, "sourceLength"));
        }
        var input = method.InputIndex >= 0 ? (Stream?)arguments[method.InputIndex] : null;
        var payload = new RpcRequestPayload(contract.Type.FullName!, method.Id,
            arguments.Where((_, index) => method.IsControlParameter(index)).ToArray(),
            input is not null || pageSource is not null, changes);
        var lane = RpcControlLane.IsAllowed(contract, method) ? ApplicationRpcLane.Control : ApplicationRpcLane.Bulk;
        var request = new HttpRequestMessage(HttpMethod.Post, RpcControlLane.GetEndpoint(options.Endpoint!, lane));
        try
        {
            request.Headers.Add(ApplicationTransportSecurity.AccessKeyHeader, Convert.ToBase64String(accessKey));
            request.Headers.Add(ApplicationTransportSecurity.ProtocolHeader, ApplicationTransportSecurity.ProtocolVersion);
            request.Headers.Add(ApplicationTransportSecurity.PolicyHeader, policy);
            request.Headers.Add(ContractsHeader, RpcContracts.Fingerprint);
            request.Content = new RpcContent(payload, input, pageSource, changes,
                RpcControlLane.FrameLimit(options.MaximumControlFrameBytes, lane), maximumInputBytes, cancellationToken);
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
        var buffer = new byte[RpcFrames.MaximumDataFrameBytes];
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

    private sealed class RpcContent : HttpContent
    {
        private readonly RpcRequestPayload request;
#pragma warning disable CA2213 // Borrowed request stream: its Gateway owner retains and disposes it after RPC completion.
        private readonly Stream? input;
#pragma warning restore CA2213
        private readonly IPageCopySource? pageSource;
        private readonly PageRangeDiff? changes;
        private readonly int maximumControlBytes;
        private readonly long maximumInputBytes;
        private readonly CancellationToken requestCancellation;
        private ExceptionDispatchInfo? producerFailure;
        private int serializationStarted;

        public RpcContent(RpcRequestPayload request, Stream? input, IPageCopySource? pageSource, PageRangeDiff? changes,
            int maximumControlBytes, long maximumInputBytes, CancellationToken requestCancellation)
        {
            this.request = request;
            this.input = input;
            this.pageSource = pageSource;
            this.changes = changes;
            this.maximumControlBytes = maximumControlBytes;
            this.maximumInputBytes = maximumInputBytes;
            this.requestCancellation = requestCancellation;
            Headers.ContentType = new MediaTypeHeaderValue(ApplicationTransportSecurity.ContentType);
        }

        protected override Task SerializeToStreamAsync(Stream stream, TransportContext? context) =>
            WriteAsync(stream, requestCancellation);

        protected override Task SerializeToStreamAsync(Stream stream, TransportContext? context, CancellationToken cancellationToken) =>
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
                if (pageSource is not null)
                    await PageCopyFrames.WriteAsync(pageSource, changes!, framed, cancellationToken).ConfigureAwait(false);
                else if (input is not null)
                    await input.CopyToAsync(framed, RpcFrames.MaximumDataFrameBytes, cancellationToken).ConfigureAwait(false);
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

        internal void RethrowProducerFailure()
        {
            var failure = Volatile.Read(ref producerFailure);
            if (failure is not null && (CatastrophicExceptionPolicy.Contains(failure.SourceException) ||
                StorageExceptionMapper.IsKnown(failure.SourceException) || failure.SourceException is IOException or HttpRequestException))
                failure.Throw();
        }

        protected override bool TryComputeLength(out long length)
        {
            length = 0;
            return false;
        }
    }
}
