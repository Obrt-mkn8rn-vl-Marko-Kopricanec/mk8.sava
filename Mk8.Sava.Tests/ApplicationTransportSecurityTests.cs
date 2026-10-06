using System.ComponentModel.DataAnnotations;
using System.Net;
using System.Reflection;
using System.Security.Cryptography;
using System.Text.Json;
using Microsoft.AspNetCore.Http;
using Mk8.Sava.Application;
using Mk8.Sava.Configuration;
using Mk8.Sava.Protocol;
using Mk8.Sava.Storage;
using Mk8.Sava.Transport;

namespace Mk8.Sava.Tests;

public sealed partial class ApplicationTransportSecurityTests : IDisposable
{
    private readonly DirectoryInfo directory = Directory.CreateTempSubdirectory("mk8-sava-rpc-security-");
    private readonly byte[] accessKey = RandomNumberGenerator.GetBytes(32);
    private readonly ApplicationTransportOptions transportOptions;
    private readonly SavaOptions storageOptions = new()
    {
        Accounts = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["devstoreaccount1"] = Convert.ToBase64String(new byte[32]),
        },
    };

    public ApplicationTransportSecurityTests()
    {
        var keyPath = Path.Combine(directory.FullName, "access.key");
        File.WriteAllText(keyPath, Convert.ToBase64String(accessKey));
        if (!OperatingSystem.IsWindows())
            File.SetUnixFileMode(keyPath, UnixFileMode.UserRead | UnixFileMode.UserWrite);
        transportOptions = new ApplicationTransportOptions { AccessKeyFile = keyPath };
    }

    public void Dispose()
    {
        CryptographicOperations.ZeroMemory(accessKey);
        directory.Delete(recursive: true);
    }

    [Theory]
    [InlineData("http://127.0.0.1:18581/internal/application", true)]
    [InlineData("http://[::1]:18581/internal/application", true)]
    [InlineData("https://application.example/internal/application", true)]
    [InlineData("http://localhost:18581/internal/application", false)]
    [InlineData("http://192.0.2.1:18581/internal/application", false)]
    [InlineData("https://user:secret@application.example/internal/application", false)]
    [InlineData("https://application.example/internal/application?secret=1", false)]
    [InlineData("https://application.example/internal/application#fragment", false)]
    [InlineData("file:///internal/application", false)]
    public void RemoteTransportRequiresOrdinaryHttpsAndHttpRequiresLiteralLoopback(string endpoint, bool allowed)
    {
        var options = new ApplicationTransportOptions
        {
            Endpoint = new Uri(endpoint, UriKind.Absolute),
            AccessKeyFile = transportOptions.AccessKeyFile,
        };
        var failures = new List<ValidationResult>();

        var valid = Validator.TryValidateObject(options, new ValidationContext(options), failures, validateAllProperties: true);

        Assert.Equal(allowed, valid);
    }

    [Fact]
    public void PolicyFingerprintIsCanonicalAndIndependentOfStorageRootAndCompression()
    {
        var first = CreatePolicyOptions(reverse: false, dataPath: "application-data", compression: 5);
        var second = CreatePolicyOptions(reverse: true, dataPath: "gateway-must-not-create-this", compression: 11);

        Assert.Equal(ApplicationTransportSecurity.CreatePolicyFingerprint(first),
            ApplicationTransportSecurity.CreatePolicyFingerprint(second));
    }

    [Fact]
    public void ContractShapeIncludesNestedDtosEnumsAndNegotiatedReadSessionTimeout()
    {
        var shape = RpcContractShapes.Describe([typeof(ApplicationReadSession), typeof(ContainerRecord)]);
        var reordered = RpcContractShapes.Describe([typeof(ContainerRecord), typeof(ApplicationReadSession)]);

        Assert.Equal(shape, reordered);
        Assert.Contains("member:Mk8.Sava.Application.ApplicationReadSession:IdleTimeout:System.TimeSpan:",
            shape, StringComparison.Ordinal);
        Assert.Contains("member:Mk8.Sava.Storage.BlobRecord:Revision:System.String:", shape, StringComparison.Ordinal);
        Assert.Contains("enum:Mk8.Sava.Storage.BlobKind:BlockBlob:", shape, StringComparison.Ordinal);
        Assert.DoesNotContain("member:Mk8.Sava.Storage.BlobRecord:Acl:", shape, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("key")]
    [InlineData("anonymous")]
    [InlineData("maximumBody")]
    [InlineData("role")]
    public void PolicyFingerprintDetectsAuthenticationAndProtocolBudgetDifferences(string changed)
    {
        var original = new SavaOptions
        {
            Accounts = new Dictionary<string, string>(StringComparer.Ordinal) { ["devstoreaccount1"] = "first-key" },
        };
        var altered = new SavaOptions
        {
            Accounts = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["devstoreaccount1"] = string.Equals(changed, "key", StringComparison.Ordinal) ? "second-key" : "first-key",
            },
            AllowAnonymousPublicAccess = string.Equals(changed, "anonymous", StringComparison.Ordinal),
            MaximumRequestBodyBytes = string.Equals(changed, "maximumBody", StringComparison.Ordinal)
                ? 1024 : original.MaximumRequestBodyBytes,
            BearerAuthentication = new BearerAuthenticationOptions
            {
                RolePermissions = string.Equals(changed, "role", StringComparison.Ordinal)
                    ? new Dictionary<string, string>(StringComparer.Ordinal) { ["reader"] = "r" }
                    : new Dictionary<string, string>(StringComparer.Ordinal),
            },
        };

        Assert.NotEqual(ApplicationTransportSecurity.CreatePolicyFingerprint(original),
            ApplicationTransportSecurity.CreatePolicyFingerprint(altered), StringComparer.Ordinal);
    }

    [Theory]
    [InlineData("key", 403, false)]
    [InlineData("protocol", 409, false)]
    [InlineData("policy", 409, false)]
    [InlineData("contracts", 409, false)]
    [InlineData("key", 403, true)]
    [InlineData("protocol", 409, true)]
    [InlineData("policy", 409, true)]
    [InlineData("contracts", 409, true)]
    public async Task AuthenticationAndCompatibilityAreCheckedBeforeBodyDecode(string changed, int expectedStatus, bool control)
    {
        var admission = new RecordingAdmission();
        using var endpoint = new ApplicationRpcEndpoint(transportOptions, storageOptions, admission: admission);
        using var input = new NeverReadStream();
        using var response = new MemoryStream();
        var context = CreateContext(input, response);
        var header = changed switch
        {
            "key" => ApplicationTransportSecurity.AccessKeyHeader,
            "protocol" => ApplicationTransportSecurity.ProtocolHeader,
            "policy" => ApplicationTransportSecurity.PolicyHeader,
            _ => "X-Mk8-Sava-Contracts",
        };
        context.Request.Headers[header] = "incorrect";
        var dispatcher = new RecordingDispatcher();

        await HandleLaneAsync(endpoint, context, dispatcher, control);

        Assert.Equal(expectedStatus, context.Response.StatusCode);
        Assert.Equal(0, dispatcher.Calls);
        Assert.Equal(0, admission.Calls);
        Assert.Equal(0, input.Reads);
        Assert.Equal(0, response.Length);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PreviousPrivateProtocolIsRejectedBeforeAdmissionOrDecode(bool control)
    {
        var admission = new RecordingAdmission();
        using var endpoint = new ApplicationRpcEndpoint(transportOptions, storageOptions, admission: admission);
        using var input = new NeverReadStream();
        using var response = new MemoryStream();
        var context = CreateContext(input, response);
        context.Request.Headers[ApplicationTransportSecurity.ProtocolHeader] = "mk8-sava-rpc-1";
        var dispatcher = new RecordingDispatcher();

        await HandleLaneAsync(endpoint, context, dispatcher, control);

        Assert.Equal("mk8-sava-rpc-2", ApplicationTransportSecurity.ProtocolVersion);
        Assert.Equal(409, context.Response.StatusCode);
        Assert.Equal(0, admission.Calls);
        Assert.Equal(0, input.Reads);
        Assert.Equal(0, dispatcher.Calls);
    }

    [Fact]
    public async Task AdmissionLeaseCoversDecodeDispatchAndCompleteResponse()
    {
        using var input = await CreateMetricsRequestAsync();
        using var response = new MemoryStream();
        var admission = new RecordingAdmission
        {
            OnAcquire = () => Assert.Equal(0, input.Position),
            OnRelease = () =>
            {
                Assert.Equal(input.Length, input.Position);
                Assert.True(response.Length > 4);
            },
        };
        using var endpoint = new ApplicationRpcEndpoint(transportOptions, storageOptions, admission: admission);
        var dispatcher = new RecordingDispatcher { BeforeInvoke = () => Assert.True(admission.Active) };

        await endpoint.HandleAsync(CreateContext(input, response), dispatcher);

        Assert.Equal(1, admission.Calls);
        Assert.Equal(ApplicationRpcLane.Bulk, admission.Lane);
        Assert.Equal(1, dispatcher.Calls);
        Assert.Equal(1, admission.Releases);
        Assert.False(admission.Active);
    }

    [Fact]
    public async Task DeniedAdmissionReturnsSafeServerBusyWithoutReadingControlBody()
    {
        var admission = new RecordingAdmission
        {
            Failure = new AzureStorageException(503, "ServerBusy", "The application ingress limit was reached."),
        };
        using var endpoint = new ApplicationRpcEndpoint(transportOptions, storageOptions, admission: admission);
        using var input = new NeverReadStream();
        using var response = new MemoryStream();
        var dispatcher = new RecordingDispatcher();

        await endpoint.HandleAsync(CreateContext(input, response), dispatcher);

        Assert.Equal(1, admission.Calls);
        Assert.Equal(0, input.Reads);
        Assert.Equal(0, dispatcher.Calls);
        Assert.Equal(0, admission.Releases);
        response.Position = 0;
        var result = await RpcFrames.ReadControlAsync<RpcResponse>(response, 4096, CancellationToken.None);
        Assert.Equal(503, result.Error?.StatusCode);
        Assert.Equal("ServerBusy", result.Error?.Code);
    }

    [Fact]
    public async Task CancellationDuringAdmittedDecodeReleasesLease()
    {
        using var cancellation = new CancellationTokenSource();
        var admission = new RecordingAdmission();
        using var endpoint = new ApplicationRpcEndpoint(transportOptions, storageOptions, admission: admission);
        using var input = new NeverReadStream(cancellation.Cancel);
        using var response = new MemoryStream();
        var context = CreateContext(input, response);
        context.RequestAborted = cancellation.Token;
        var dispatcher = new RecordingDispatcher();

        await endpoint.HandleAsync(context, dispatcher);

        Assert.Equal(1, admission.Calls);
        Assert.Equal(1, admission.Releases);
        Assert.False(admission.Active);
        Assert.Equal(0, dispatcher.Calls);
        Assert.Equal(0, response.Length);
    }

    [Theory]
    [InlineData(typeof(IApplicationReadSessions), nameof(IApplicationReadSessions.TouchAsync), true)]
    [InlineData(typeof(IApplicationReadSessions), nameof(IApplicationReadSessions.CloseAsync), true)]
    [InlineData(typeof(IApplicationReadiness), nameof(IApplicationReadiness.GetAsync), true)]
    [InlineData(typeof(IApplicationReadiness), nameof(IApplicationReadiness.RenderStorageMetricsAsync), false)]
    [InlineData(typeof(IApplicationReadSessions), nameof(IApplicationReadSessions.OpenAsync), false)]
    [InlineData(typeof(IApplicationReadSessions), nameof(IApplicationReadSessions.WriteRangeAsync), false)]
    public async Task ControlLaneDispatchesOnlyExactShortOperations(Type contractType, string operation, bool permitted)
    {
        ArgumentNullException.ThrowIfNull(contractType);
        var admission = new RecordingAdmission();
        using var endpoint = new ApplicationRpcEndpoint(transportOptions, storageOptions, admission: admission);
        var arguments = contractType == typeof(IApplicationReadSessions) && permitted
            ? new object?[] { "session-token" } : [];
        using var input = await CreateOperationRequestAsync(contractType, operation, arguments);
        using var response = new MemoryStream();
        var dispatcher = new RecordingDispatcher();

        await endpoint.HandleControlAsync(CreateContext(input, response), dispatcher);

        Assert.Equal(ApplicationRpcLane.Control, admission.Lane);
        Assert.Equal(1, admission.Releases);
        Assert.Equal(permitted ? 1 : 0, dispatcher.Calls);
        response.Position = 0;
        var result = await RpcFrames.ReadControlAsync<RpcResponse>(response, 4096, CancellationToken.None);
        Assert.Equal(permitted, result.Error is null);
    }

    [Theory]
    [InlineData("stream")]
    [InlineData("pages")]
    public async Task ControlLaneCannotCarryStreamOrPageDescriptors(string changed)
    {
        var admission = new RecordingAdmission();
        using var endpoint = new ApplicationRpcEndpoint(transportOptions, storageOptions, admission: admission);
        using var input = await CreateOperationRequestAsync(typeof(IApplicationReadSessions), nameof(IApplicationReadSessions.TouchAsync),
            ["session-token"], hasInput: string.Equals(changed, "stream", StringComparison.Ordinal),
            pageChanges: string.Equals(changed, "pages", StringComparison.Ordinal) ? new PageRangeDiff([], []) : null);
        using var response = new MemoryStream();
        var dispatcher = new RecordingDispatcher();

        await endpoint.HandleControlAsync(CreateContext(input, response), dispatcher);

        Assert.Equal(0, dispatcher.Calls);
        Assert.Equal(1, admission.Releases);
        response.Position = 0;
        var result = await RpcFrames.ReadControlAsync<RpcResponse>(response, 4096, CancellationToken.None);
        Assert.Equal("InternalError", result.Error?.Code);
    }

    [Fact]
    public async Task ControlEnvelopeIsBoundedToFourKiBBeforeDispatch()
    {
        var admission = new RecordingAdmission();
        using var endpoint = new ApplicationRpcEndpoint(transportOptions, storageOptions, admission: admission);
        using var input = await CreateOperationRequestAsync(typeof(IApplicationReadSessions), nameof(IApplicationReadSessions.TouchAsync),
            [new string('s', 4096)]);
        using var response = new MemoryStream();
        var dispatcher = new RecordingDispatcher();

        await endpoint.HandleControlAsync(CreateContext(input, response), dispatcher);

        Assert.Equal(0, dispatcher.Calls);
        Assert.Equal(4, input.Position);
        Assert.Equal(1, admission.Releases);
        response.Position = 0;
        var result = await RpcFrames.ReadControlAsync<RpcResponse>(response, 4096, CancellationToken.None);
        Assert.Equal("InternalError", result.Error?.Code);
    }

    [Fact]
    public async Task ControlOperationCannotDispatchBeforeSuccessfulEmptyTerminalProof()
    {
        var admission = new RecordingAdmission();
        using var endpoint = new ApplicationRpcEndpoint(transportOptions, storageOptions, admission: admission);
        using var input = await CreateOperationRequestAsync(typeof(IApplicationReadSessions), nameof(IApplicationReadSessions.TouchAsync),
            ["session-token"]);
        input.SetLength(input.Length - 36);
        using var response = new MemoryStream();
        var dispatcher = new RecordingDispatcher();

        await endpoint.HandleControlAsync(CreateContext(input, response), dispatcher);

        Assert.Equal(0, dispatcher.Calls);
        Assert.Equal(1, admission.Releases);
        response.Position = 0;
        var result = await RpcFrames.ReadControlAsync<RpcResponse>(response, 4096, CancellationToken.None);
        Assert.Equal("InternalError", result.Error?.Code);
    }

    [Fact]
    public void ControlClientUsesDedicatedOriginPreservingRouteAndFiniteSmallLimits()
    {
        var endpoint = new Uri("https://application.example:9443/internal/application", UriKind.Absolute);
        Assert.Equal("https://application.example:9443/internal/application/control",
            RpcControlLane.GetEndpoint(endpoint, ApplicationRpcLane.Control).AbsoluteUri);
        Assert.Same(endpoint, RpcControlLane.GetEndpoint(endpoint, ApplicationRpcLane.Bulk));
        Assert.Equal(4096, RpcControlLane.FrameLimit(64 * 1024 * 1024, ApplicationRpcLane.Control));
        Assert.Equal(4136, RpcControlLane.MaximumRequestBytes);
        Assert.Equal(TimeSpan.FromSeconds(5), RpcControlLane.RequestTimeout(TimeSpan.FromMinutes(10), ApplicationRpcLane.Control));
        Assert.Equal(TimeSpan.FromMilliseconds(100), RpcControlLane.RequestTimeout(TimeSpan.FromMilliseconds(100), ApplicationRpcLane.Control));
        Assert.Equal(TimeSpan.FromMinutes(10), RpcControlLane.RequestTimeout(TimeSpan.FromMinutes(10), ApplicationRpcLane.Bulk));
    }

    [Fact]
    public async Task CompletedRequestBodyCannotBeSerializedAgainAsAnEmptyWrite()
    {
        using var client = new ApplicationRpcClient(transportOptions, storageOptions);
        using var source = new MemoryStream([1, 2, 3], writable: false);
        using var request = await CreateBlockRequestAsync(client, source);
        using var firstWire = new MemoryStream();
        using var secondWire = new MemoryStream();

        await request.Content!.CopyToAsync(firstWire);
        var failure = await Assert.ThrowsAsync<InvalidOperationException>(() => request.Content.CopyToAsync(secondWire));

        Assert.Contains("more than once", failure.Message, StringComparison.Ordinal);
        Assert.Equal(source.Length, source.Position);
        Assert.True(firstWire.Length > 36);
        Assert.Equal(0, secondWire.Length);
    }

    [Theory]
    [InlineData("producer")]
    [InlineData("transport")]
    [InlineData("fatal")]
    [InlineData("cancellation")]
    public async Task FaultedRequestBodyCannotWriteAnySecondControlOrPayloadBytes(string fault)
    {
        using var client = new ApplicationRpcClient(transportOptions, storageOptions);
#pragma warning disable CA2201 // Deliberately construct a wrapped runtime-reserved failure to test producer fatal identity and replay prevention.
        var sourceFailure = fault switch
        {
            "fatal" => (Exception)new InvalidOperationException("producer failed", new OutOfMemoryException("fatal producer")),
            "cancellation" => new OperationCanceledException("producer canceled"),
            _ => new IOException("producer failed"),
        };
#pragma warning restore CA2201
        using Stream source = string.Equals(fault, "transport", StringComparison.Ordinal)
            ? new MemoryStream([1, 2, 3], writable: false) : new FaultedProducerStream(sourceFailure);
        using var request = await CreateBlockRequestAsync(client, source);
        using Stream firstWire = string.Equals(fault, "transport", StringComparison.Ordinal)
            ? new FaultedDestinationStream() : new MemoryStream();
        using var secondWire = new MemoryStream();

        var initial = await Assert.ThrowsAnyAsync<Exception>(() => request.Content!.CopyToAsync(firstWire));
        var replay = await Assert.ThrowsAsync<InvalidOperationException>(() => request.Content!.CopyToAsync(secondWire));

        if (string.Equals(fault, "fatal", StringComparison.Ordinal))
            Assert.Same(sourceFailure, initial);
        if (string.Equals(fault, "cancellation", StringComparison.Ordinal))
            Assert.Same(sourceFailure, initial);
        Assert.Contains("more than once", replay.Message, StringComparison.Ordinal);
        Assert.Equal(0, secondWire.Length);
    }

    [Fact]
    public async Task HttpRpcRejectsANonloopbackPeerEvenWithCorrectKey()
    {
        using var endpoint = new ApplicationRpcEndpoint(transportOptions, storageOptions);
        using var input = new NeverReadStream();
        using var response = new MemoryStream();
        var context = CreateContext(input, response);
        context.Connection.RemoteIpAddress = IPAddress.Parse("192.0.2.1");
        var dispatcher = new RecordingDispatcher();

        await endpoint.HandleAsync(context, dispatcher);

        Assert.Equal(403, context.Response.StatusCode);
        Assert.Equal(0, dispatcher.Calls);
        Assert.Equal(0, input.Reads);
    }

    [Fact]
    public async Task AuthorizedRpcUsesOnlyWhitelistedMethodAndBindsPeerIdentity()
    {
        using var endpoint = new ApplicationRpcEndpoint(transportOptions, storageOptions);
        using var input = await CreateMetricsRequestAsync();
        using var response = new MemoryStream();
        var context = CreateContext(input, response);
        var dispatcher = new RecordingDispatcher();

        await endpoint.HandleAsync(context, dispatcher);

        Assert.Equal(1, dispatcher.Calls);
        Assert.Equal(typeof(IApplicationReadiness), dispatcher.Contract);
        Assert.Equal(Convert.ToHexStringLower(SHA256.HashData(accessKey)), dispatcher.Peer);
        Assert.Null(ApplicationRpcIdentity.CurrentPeerFingerprint);
        response.Position = 0;
        var result = await RpcFrames.ReadControlAsync<RpcResponse>(response, transportOptions.MaximumControlFrameBytes, CancellationToken.None);
        Assert.Null(result.Error);
        Assert.False(result.HasOutput);
        Assert.Equal("metrics-test", result.Result.GetString());
    }

    [Fact]
    public async Task TypedResultPreservesExplicitNullWithoutAcceptingAnOmittedResult()
    {
        using var response = new MemoryStream();
        await RpcFrames.WriteControlAsync(response, new RpcResponsePayload(null, null, HasOutput: false),
            4096, CancellationToken.None);
        response.Position = 0;
        var result = await RpcFrames.ReadControlAsync<RpcResponse>(response, 4096, CancellationToken.None);

        Assert.Equal(JsonValueKind.Null, result.Result.ValueKind);
        Assert.Null(ApplicationRpcProxy.ReadResult<BlobRecord>(result));

        using var omitted = new MemoryStream();
        await RpcFrames.WriteControlAsync(omitted, new { Error = (RpcError?)null, HasOutput = false },
            4096, CancellationToken.None);
        omitted.Position = 0;
        var invalid = await RpcFrames.ReadControlAsync<RpcResponse>(omitted, 4096, CancellationToken.None);
        Assert.Equal(JsonValueKind.Undefined, invalid.Result.ValueKind);
        Assert.Throws<InvalidDataException>(() => ApplicationRpcProxy.ReadResult<BlobRecord>(invalid));
    }

    [Fact]
    public async Task PageClearCannotCommitWhileIgnoringAnUnprovenInputStream()
    {
        using var endpoint = new ApplicationRpcEndpoint(transportOptions, storageOptions);
        using var input = new MemoryStream();
        using var response = new MemoryStream();
        var contract = RpcContracts.GetContract(typeof(IBlobApplication));
        var method = contract.GetMethod(typeof(IBlobApplication).GetMethod(nameof(IBlobApplication.PutPageAsync))!);
        var record = new BlobRecord
        {
            Account = "devstoreaccount1",
            Container = "container",
            Name = "blob",
            GenerationId = "generation",
            Revision = "revision",
            Kind = BlobKind.PageBlob,
            Content = new ContentManifest("domain", 512, "sparse", []),
            ETag = "etag",
            CreatedAt = DateTimeOffset.UnixEpoch,
            LastModified = DateTimeOffset.UnixEpoch,
        };
        await RpcFrames.WriteControlAsync(input, new RpcRequestPayload(contract.Type.FullName!, method.Id,
            [record, 0L, 511L, true, new BlobEncryption(null, null)], HasInput: true, null), 4096, CancellationToken.None);
        input.Position = 0;
        var dispatcher = new RecordingDispatcher();

        await endpoint.HandleAsync(CreateContext(input, response), dispatcher);

        Assert.Equal(0, dispatcher.Calls);
        response.Position = 0;
        var result = await RpcFrames.ReadControlAsync<RpcResponse>(response, 4096, CancellationToken.None);
        Assert.Equal("InternalError", result.Error?.Code);
    }

    [Fact]
    public async Task DecodedCustomerKeyIsClearedAndNeverEchoedInEncryptionResult()
    {
        using var endpoint = new ApplicationRpcEndpoint(transportOptions, storageOptions);
        using var input = new MemoryStream();
        using var response = new MemoryStream();
        var contract = RpcContracts.GetContract(typeof(IBlobApplication));
        var method = contract.GetMethod(typeof(IBlobApplication).GetMethod(nameof(IBlobApplication.StageBlockAsync))!);
        var key = RandomNumberGenerator.GetBytes(32);
        var encryption = new BlobEncryption("scope", "key-hash", key);
        await RpcFrames.WriteControlAsync(input, new RpcRequestPayload(contract.Type.FullName!, method.Id,
            ["devstoreaccount1", "container", "blob", "YmxvY2s=", encryption], HasInput: true, null), 4096, CancellationToken.None);
        using (var framed = new FramedWriteStream(input, 0))
            await framed.CompleteAsync(CancellationToken.None);
        input.Position = 0;
        var dispatcher = new RecordingDispatcher { ReturnEncryptionResult = true };

        await endpoint.HandleAsync(CreateContext(input, response), dispatcher);

        var captured = Assert.IsType<byte[]>(dispatcher.CapturedKey);
        Assert.NotSame(key, captured);
        Assert.All(captured, value => Assert.Equal(0, value));
        Assert.Contains(key, value => value != 0);
        response.Position = 0;
        var result = await RpcFrames.ReadControlAsync<RpcResponse>(response, 4096, CancellationToken.None);
        Assert.Null(result.Error);
        var returned = result.Result.Deserialize<BlobEncryption>(RpcJson.Options);
        Assert.Equal(encryption.Scope, returned.Scope);
        Assert.Equal(encryption.CustomerProvidedKeySha256, returned.CustomerProvidedKeySha256);
        Assert.Null(returned.CustomerProvidedKey);
        CryptographicOperations.ZeroMemory(key);
    }

    [Theory]
    [InlineData("contract")]
    [InlineData("method")]
    public async Task WireCannotSelectArbitraryRuntimeTypeOrSynchronousMethod(string changed)
    {
        using var endpoint = new ApplicationRpcEndpoint(transportOptions, storageOptions);
        using var input = await CreateMetricsRequestAsync(changed);
        using var response = new MemoryStream();
        var dispatcher = new RecordingDispatcher();

        await endpoint.HandleAsync(CreateContext(input, response), dispatcher);

        Assert.Equal(0, dispatcher.Calls);
        response.Position = 0;
        var result = await RpcFrames.ReadControlAsync<RpcResponse>(response, transportOptions.MaximumControlFrameBytes, CancellationToken.None);
        Assert.Equal("InternalError", result.Error?.Code);
        Assert.DoesNotContain("System.IO", result.Error?.Message ?? string.Empty, StringComparison.Ordinal);
    }

    [Theory]
    [MemberData(nameof(AzureExceptionMiddlewareTests.CatastrophicFailures), MemberType = typeof(AzureExceptionMiddlewareTests))]
    public async Task CatastrophicDispatcherFailuresEscapeRpcWithoutNormalization(Exception failure)
    {
        var admission = new RecordingAdmission();
        using var endpoint = new ApplicationRpcEndpoint(transportOptions, storageOptions, admission: admission);
        using var input = await CreateMetricsRequestAsync();
        using var response = new MemoryStream();
        var dispatcher = new RecordingDispatcher(failure);

        var escaped = await Assert.ThrowsAnyAsync<Exception>(() => endpoint.HandleAsync(CreateContext(input, response), dispatcher));

        Assert.Same(failure, escaped);
        Assert.Equal(0, response.Length);
        Assert.Same(failure, Assert.ThrowsAny<Exception>(() => StorageExceptionMapper.Map(failure)));
        Assert.Equal(1, admission.Releases);
        Assert.False(admission.Active);
    }

    [Theory]
    [MemberData(nameof(AzureExceptionMiddlewareTests.CatastrophicFailures), MemberType = typeof(AzureExceptionMiddlewareTests))]
    public async Task CatastrophicAdmissionFailureEscapesWithoutDecodeOrNormalization(Exception failure)
    {
        var admission = new RecordingAdmission { Failure = failure };
        using var endpoint = new ApplicationRpcEndpoint(transportOptions, storageOptions, admission: admission);
        using var input = new NeverReadStream();
        using var response = new MemoryStream();
        var dispatcher = new RecordingDispatcher();

        var escaped = await Assert.ThrowsAnyAsync<Exception>(() => endpoint.HandleAsync(CreateContext(input, response), dispatcher));

        Assert.Same(failure, escaped);
        Assert.Equal(0, input.Reads);
        Assert.Equal(0, dispatcher.Calls);
        Assert.Equal(0, admission.Releases);
        Assert.Equal(0, response.Length);
    }

    [Theory]
    [InlineData("concurrency", 412, "ConditionNotMet")]
    [InlineData("legalHold", 409, "BlobImmutableDueToLegalHold")]
    [InlineData("retention", 409, "BlobImmutableDueToPolicy")]
    [InlineData("pendingCopy", 409, "PendingCopyOperation")]
    [InlineData("type", 409, "InvalidBlobType")]
    [InlineData("path", 409, "PathAlreadyExists")]
    [InlineData("body", 413, "RequestBodyTooLarge")]
    public async Task KnownApplicationErrorsRetainTheirAzureStatusAndCode(string kind, int status, string code)
    {
        var failure = kind switch
        {
            "concurrency" => (Exception)new StorageConcurrencyException(),
            "legalHold" => new StorageImmutabilityException(legalHold: true),
            "retention" => new StorageImmutabilityException(legalHold: false),
            "pendingCopy" => new StoragePendingCopyException(),
            "type" => new StorageBlobTypeMismatchException(),
            "path" => new StoragePathConflictException(),
            _ => new RequestBodyTooLargeException(1024),
        };
        using var endpoint = new ApplicationRpcEndpoint(transportOptions, storageOptions);
        using var input = await CreateMetricsRequestAsync();
        using var response = new MemoryStream();

        await endpoint.HandleAsync(CreateContext(input, response), new RecordingDispatcher(failure));

        response.Position = 0;
        var result = await RpcFrames.ReadControlAsync<RpcResponse>(response, transportOptions.MaximumControlFrameBytes, CancellationToken.None);
        Assert.Equal(status, result.Error?.StatusCode);
        Assert.Equal(code, result.Error?.Code);
    }

    [Fact]
    public async Task OrdinaryDispatcherFailureIsSafeAndTypedAzureFailureIsPreserved()
    {
        using var endpoint = new ApplicationRpcEndpoint(transportOptions, storageOptions);
        using var input = await CreateMetricsRequestAsync();
        using var response = new MemoryStream();
        var failure = new InvalidOperationException("private-storage-secret");

        await endpoint.HandleAsync(CreateContext(input, response), new RecordingDispatcher(failure));

        response.Position = 0;
        var result = await RpcFrames.ReadControlAsync<RpcResponse>(response, transportOptions.MaximumControlFrameBytes, CancellationToken.None);
        Assert.Equal("InternalError", result.Error?.Code);
        Assert.DoesNotContain("private-storage-secret", result.Error?.Message ?? string.Empty, StringComparison.Ordinal);
        using var secondInput = await CreateMetricsRequestAsync();
        using var secondResponse = new MemoryStream();
        var azureFailure = new AzureStorageException(412, "ConditionNotMet", "The condition was not met.", "If-Match", "expected");
        await endpoint.HandleAsync(CreateContext(secondInput, secondResponse), new RecordingDispatcher(azureFailure));
        secondResponse.Position = 0;
        var typed = await RpcFrames.ReadControlAsync<RpcResponse>(secondResponse, transportOptions.MaximumControlFrameBytes, CancellationToken.None);
        Assert.Equal(412, typed.Error?.StatusCode);
        Assert.Equal("ConditionNotMet", typed.Error?.Code);
        Assert.Equal("If-Match", typed.Error?.HeaderName);
    }

    [Fact]
    public void SynchronousCapabilitiesUseOnlyExplicitLocalPolicyWithoutNetwork()
    {
        using var client = new ApplicationRpcClient(transportOptions, storageOptions);
        var proxy = client.CreateProxy<IBlobApplication>(new LocalPolicy());

        Assert.True(proxy.AllowsAnonymousPublicAccess("allowed"));
        Assert.False(proxy.AllowsAnonymousPublicAccess("denied"));
        Assert.True(proxy.IsHierarchicalNamespaceEnabled("allowed"));
        Assert.False(proxy.IsObjectReplicationDestinationContainer("allowed", "other"));
        Assert.Throws<InvalidOperationException>(() => client.CreateProxy<IBlobApplication>().AllowsAnonymousPublicAccess("allowed"));
        Assert.Throws<InvalidDataException>(() => client.CreateProxy<IBlobCapabilities>());
    }

    [Fact]
    public void ShortOrInvalidSecretFilesAreRejectedWithoutPrintingTheirContents()
    {
        File.WriteAllText(transportOptions.AccessKeyFile, Convert.ToBase64String(new byte[31]));
        var shortFailure = Assert.Throws<InvalidOperationException>(() =>
        {
            using var client = new ApplicationRpcClient(transportOptions, storageOptions);
        });
        Assert.Contains("256", shortFailure.Message, StringComparison.Ordinal);
        File.WriteAllText(transportOptions.AccessKeyFile, "not-base64-secret-material");
        var invalidFailure = Assert.Throws<InvalidOperationException>(() =>
        {
            using var endpoint = new ApplicationRpcEndpoint(transportOptions, storageOptions);
        });
        Assert.DoesNotContain("not-base64-secret-material", invalidFailure.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(UnixFileMode.UserRead | UnixFileMode.UserWrite, true)]
    [InlineData(UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.GroupRead, true)]
    [InlineData(UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.OtherRead, false)]
    [InlineData(UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.OtherWrite, false)]
    [InlineData(UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.GroupWrite, false)]
    [InlineData(UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.GroupExecute, false)]
    [InlineData(UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute, false)]
    public void UnixKeyFileRequiresOwnerOrTrustedReadOnlyGroupAccess(UnixFileMode mode, bool permitted)
    {
        if (OperatingSystem.IsWindows())
            return;
        File.SetUnixFileMode(transportOptions.AccessKeyFile, mode);
        if (permitted)
        {
            using var endpoint = new ApplicationRpcEndpoint(transportOptions, storageOptions);
            return;
        }
        var failure = Assert.Throws<InvalidOperationException>(() =>
        {
            using var client = new ApplicationRpcClient(transportOptions, storageOptions);
        });
        Assert.DoesNotContain(transportOptions.AccessKeyFile, failure.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(Convert.ToBase64String(accessKey), failure.Message, StringComparison.Ordinal);
    }

    private DefaultHttpContext CreateContext(Stream input, Stream response)
    {
        var context = new DefaultHttpContext();
        context.Connection.LocalIpAddress = IPAddress.Loopback;
        context.Connection.RemoteIpAddress = IPAddress.Loopback;
        context.Request.Method = HttpMethods.Post;
        context.Request.ContentType = ApplicationTransportSecurity.ContentType;
        context.Request.Body = input;
        context.Response.Body = response;
        context.Request.Headers[ApplicationTransportSecurity.AccessKeyHeader] = Convert.ToBase64String(accessKey);
        context.Request.Headers[ApplicationTransportSecurity.ProtocolHeader] = ApplicationTransportSecurity.ProtocolVersion;
        context.Request.Headers[ApplicationTransportSecurity.PolicyHeader] = ApplicationTransportSecurity.CreatePolicyFingerprint(storageOptions);
        context.Request.Headers["X-Mk8-Sava-Contracts"] = RpcContracts.Fingerprint;
        return context;
    }

    private static Task HandleLaneAsync(
        ApplicationRpcEndpoint endpoint, HttpContext context, IApplicationRpcDispatcher dispatcher, bool control) =>
        control ? endpoint.HandleControlAsync(context, dispatcher) : endpoint.HandleAsync(context, dispatcher);

    private static Task<HttpRequestMessage> CreateBlockRequestAsync(ApplicationRpcClient client, Stream source)
    {
        var contract = RpcContracts.GetContract(typeof(IBlobApplication));
        var method = contract.GetMethod(typeof(IBlobApplication).GetMethod(nameof(IBlobApplication.StageBlockAsync))!);
        return client.CreateRequestAsync(contract, method,
            ["devstoreaccount1", "container", "blob", "YmxvY2s=", source, new BlobEncryption(null, null), CancellationToken.None],
            CancellationToken.None);
    }

    private static async Task<MemoryStream> CreateOperationRequestAsync(
        Type contractType, string operation, object?[] arguments, bool hasInput = false, PageRangeDiff? pageChanges = null)
    {
        var wire = new MemoryStream();
        try
        {
            var contract = RpcContracts.GetContract(contractType);
            var method = contract.GetMethod(contractType.GetMethod(operation)!);
            await RpcFrames.WriteControlAsync(wire, new RpcRequestPayload(
                contract.Type.FullName!, method.Id, arguments, hasInput, pageChanges), 65536, CancellationToken.None).ConfigureAwait(false);
            using var framed = new FramedWriteStream(wire, 0);
            await framed.CompleteAsync(CancellationToken.None).ConfigureAwait(false);
            wire.Position = 0;
            return wire;
        }
        catch
        {
            await wire.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    private static SavaOptions CreatePolicyOptions(bool reverse, string dataPath, int compression)
    {
        var pairs = new[] { KeyValuePair.Create("accountone", "key-one"), KeyValuePair.Create("accounttwo", "key-two") };
        return new SavaOptions
        {
            DataPath = dataPath,
            CompressionQuality = compression,
            Accounts = (reverse ? pairs.Reverse() : pairs).ToDictionary(pair => pair.Key, pair => pair.Value, StringComparer.Ordinal),
            DefaultAccount = "accountone",
            AccountCapabilities = (reverse ? pairs.Reverse() : pairs).ToDictionary(pair => pair.Key,
                _ => new StorageAccountCapabilities { VersioningEnabled = true }, StringComparer.Ordinal),
        };
    }

    private static async Task<MemoryStream> CreateMetricsRequestAsync(string? changed = null)
    {
        var wire = new MemoryStream();
        try
        {
            var contract = RpcContracts.GetContract(typeof(IApplicationReadiness));
            var method = contract.GetMethod(typeof(IApplicationReadiness).GetMethod(nameof(IApplicationReadiness.RenderStorageMetricsAsync))!);
            await RpcFrames.WriteControlAsync(wire, new RpcRequestPayload(
                string.Equals(changed, "contract", StringComparison.Ordinal) ? "System.IO.File" : contract.Type.FullName!,
                string.Equals(changed, "method", StringComparison.Ordinal) ? "AllowsAnonymousPublicAccess" : method.Id,
                [], HasInput: false, null), 4096, CancellationToken.None).ConfigureAwait(false);
            using var framed = new FramedWriteStream(wire, 0);
            await framed.CompleteAsync(CancellationToken.None).ConfigureAwait(false);
            wire.Position = 0;
            return wire;
        }
        catch
        {
            await wire.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    private sealed class RecordingDispatcher(Exception? failure = null) : IApplicationRpcDispatcher
    {
        internal int Calls { get; private set; }
        internal Type? Contract { get; private set; }
        internal string? Peer { get; private set; }
        internal byte[]? CapturedKey { get; private set; }
        internal bool ReturnEncryptionResult { get; init; }
        internal Action? BeforeInvoke { get; init; }

        public ValueTask<object?> InvokeAsync(Type contract, MethodInfo method, object?[] arguments, CancellationToken cancellationToken)
        {
            Calls++;
            Contract = contract;
            Peer = ApplicationRpcIdentity.CurrentPeerFingerprint;
            BeforeInvoke?.Invoke();
            if (failure is not null)
                throw failure;
            if (ReturnEncryptionResult)
            {
                var encryption = arguments.OfType<BlobEncryption>().First();
                CapturedKey = encryption.CustomerProvidedKey;
                return ValueTask.FromResult<object?>(encryption);
            }
            if (contract == typeof(IApplicationReadSessions))
                return ValueTask.FromResult<object?>(null);
            if (string.Equals(method.Name, nameof(IApplicationReadiness.GetAsync), StringComparison.Ordinal))
                return ValueTask.FromResult<object?>(new ApplicationReadiness(true, true, StorageIntegritySnapshot.Pending));
            return ValueTask.FromResult<object?>("metrics-test");
        }
    }

    private sealed class RecordingAdmission : IApplicationRpcAdmission
    {
        internal int Calls { get; private set; }
        internal int Releases { get; private set; }
        internal bool Active { get; private set; }
        internal ApplicationRpcLane? Lane { get; private set; }
        internal Exception? Failure { get; init; }
        internal Action? OnAcquire { get; init; }
        internal Action? OnRelease { get; init; }

        public ValueTask<IAsyncDisposable> AcquireAsync(ApplicationRpcLane lane, CancellationToken cancellationToken)
        {
            Calls++;
            Lane = lane;
            OnAcquire?.Invoke();
            if (Failure is not null)
                throw Failure;
            cancellationToken.ThrowIfCancellationRequested();
            Active = true;
#pragma warning disable CA2000 // All throwing checks precede construction; ownership transfers through the immediate ValueTask return to the endpoint's awaited-finally disposal.
            return new ValueTask<IAsyncDisposable>(new AdmissionLease(this));
#pragma warning restore CA2000
        }

        private sealed class AdmissionLease(RecordingAdmission owner) : IAsyncDisposable, IDisposable
        {
            public ValueTask DisposeAsync()
            {
                Release();
                return ValueTask.CompletedTask;
            }

            public void Dispose() => Release();

            private void Release()
            {
                try
                {
                    owner.OnRelease?.Invoke();
                }
                finally
                {
                    owner.Active = false;
                    owner.Releases++;
                }
            }
        }
    }

    private sealed class FaultedProducerStream(Exception failure) : MemoryStream
    {
        public override async Task CopyToAsync(Stream destination, int bufferSize, CancellationToken cancellationToken)
        {
            await destination.WriteAsync(new byte[] { 1, 2, 3 }, cancellationToken).ConfigureAwait(false);
            throw failure;
        }
    }

    private sealed class FaultedDestinationStream : MemoryStream
    {
        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default) =>
            throw new IOException("destination failed");
    }

    private sealed class LocalPolicy : IBlobCapabilities
    {
        public bool AllowsAnonymousPublicAccess(string account) => string.Equals(account, "allowed", StringComparison.Ordinal);
        public bool IsHierarchicalNamespaceEnabled(string account) => AllowsAnonymousPublicAccess(account);
        public bool SupportsBlobIndexTags(string account) => AllowsAnonymousPublicAccess(account);
        public bool SupportsBlobSnapshots(string account) => AllowsAnonymousPublicAccess(account);
        public bool IsLastAccessTimeTrackingEnabled(string account) => AllowsAnonymousPublicAccess(account);
        public bool IsImmutableStorageWithVersioningEnabled(string account, string container) => AllowsAnonymousPublicAccess(account);
        public bool IsObjectReplicationDestinationContainer(string account, string container) =>
            string.Equals(container, "destination", StringComparison.Ordinal);
    }

    private sealed class NeverReadStream(Action? beforeRead = null) : Stream
    {
        internal int Reads { get; private set; }
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            Reads++;
            beforeRead?.Invoke();
            cancellationToken.ThrowIfCancellationRequested();
            throw new InvalidOperationException("Authentication must precede body decoding.");
        }

        public override void Flush() => throw new NotSupportedException();
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }
}
