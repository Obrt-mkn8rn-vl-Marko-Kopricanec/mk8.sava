using System.Net;
using System.Text;
using Xunit.Sdk;

namespace Mk8.Sava.Tests;

public sealed class SplitProcessReadinessProbeTests
{
    private static readonly Uri Address = new("http://127.0.0.1:12345/");
    private const string Secret = "response-secret-must-not-appear-in-diagnostic";

    [Fact]
    public async Task SuccessUsesOneRequestWithoutReadingDiagnosticFieldsOrHostState()
    {
        using var handler = new RecordingHandler((_, _) => Task.FromResult(Response(HttpStatusCode.OK, Secret)));
        using var client = new HttpClient(handler);

        await SplitProcessReadinessProbe.AssertReadyAsync(client, Address,
            () => throw new InvalidOperationException("Successful probes must not snapshot the host.")).ConfigureAwait(true);

        AssertRequest(handler);
    }

    [Theory]
    [InlineData("{\"status\":\"unavailable\",\"component\":\"gateway\",\"application\":\"unavailable\",\"metadata\":\"unavailable\",\"integrity\":{\"status\":\"healthy\"}}",
        "metadata=unavailable, integrity=healthy")]
    [InlineData("{\"status\":\"unavailable\",\"component\":\"gateway\",\"application\":\"unavailable\"}", "metadata=absent")]
    [InlineData("{\"status\":\"response-secret-must-not-appear-in-diagnostic\",\"application\":\"ready\",\"detail\":\"response-secret-must-not-appear-in-diagnostic\"}",
        "status=unrecognized")]
    [InlineData("response-secret-must-not-appear-in-diagnostic", "invalid JSON (fields omitted)")]
    [InlineData("[\"response-secret-must-not-appear-in-diagnostic\"]", "invalid JSON shape (fields omitted)")]
    public async Task FailureKeepsSingleStatusAssertionAndOnlyAllowlistedBodyFields(string body, string expected)
    {
        using var handler = new RecordingHandler((_, _) => Task.FromResult(Response(HttpStatusCode.ServiceUnavailable, body)));
        using var client = new HttpClient(handler);
        var snapshots = 0;

        var failure = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            SplitProcessReadinessProbe.AssertReadyAsync(client, Address, () =>
            {
                snapshots++;
                return "owned-host-before-cleanup";
            })).ConfigureAwait(true);

        Assert.IsType<EqualException>(failure.InnerException);
        Assert.Contains("Status=503", failure.Message, StringComparison.Ordinal);
        Assert.Contains(expected, failure.Message, StringComparison.Ordinal);
        Assert.Contains("owned-host-before-cleanup", failure.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(Secret, failure.Message, StringComparison.Ordinal);
        Assert.Equal(1, snapshots);
        AssertRequest(handler);
    }

    [Fact]
    public async Task OversizedBodyIsOmittedWithoutLosingTheStatusAssertion()
    {
        var body = "{\"status\":\"unavailable\",\"detail\":\"" + new string('x', 4096) + Secret + "\"}";
        using var handler = new RecordingHandler((_, _) => Task.FromResult(Response(HttpStatusCode.ServiceUnavailable, body)));
        using var client = new HttpClient(handler);

        var failure = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            SplitProcessReadinessProbe.AssertReadyAsync(client, Address, () => "host")).ConfigureAwait(true);

        Assert.IsType<EqualException>(failure.InnerException);
        Assert.Contains("Body=oversized (fields omitted)", failure.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(Secret, failure.Message, StringComparison.Ordinal);
        AssertRequest(handler);
    }

    [Fact]
    public async Task RequestFailureRetainsTheOriginalExceptionWithoutASecondRequest()
    {
        var original = new HttpRequestException("controlled request failure");
        using var handler = new RecordingHandler((_, _) => Task.FromException<HttpResponseMessage>(original));
        using var client = new HttpClient(handler);

        var failure = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            SplitProcessReadinessProbe.AssertReadyAsync(client, Address, () => "host before cleanup")).ConfigureAwait(true);

        Assert.Same(original, failure.InnerException);
        Assert.Contains("Status=not observed, Body=not observed", failure.Message, StringComparison.Ordinal);
        AssertRequest(handler);
    }

    [Fact]
    public async Task ClientTimeoutRemainsCancellationAndKeepsItsOriginalClientBudget()
    {
        using var handler = new RecordingHandler(async (_, cancellationToken) =>
        {
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
            throw new InvalidOperationException("A canceled request cannot return success.");
        });
        using var client = new HttpClient(handler) { Timeout = TimeSpan.FromMilliseconds(50) };
        var timeout = client.Timeout;

        var failure = await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            SplitProcessReadinessProbe.AssertReadyAsync(client, Address, () => "host before cleanup"))
            .WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(true);

        var original = Assert.IsAssignableFrom<OperationCanceledException>(failure.InnerException);
        Assert.Equal(original.CancellationToken, failure.CancellationToken);
        Assert.Equal(timeout, client.Timeout);
        AssertRequest(handler);
    }

    [Fact]
    public async Task KnownFatalRequestGraphEscapesWithoutHostObservationOrNormalization()
    {
#pragma warning disable CA2201 // Synthetic fatal payload proves graph/identity escape; no real exhaustion or native corruption is induced.
        var original = new IOException("wrapped fatal", new OutOfMemoryException("synthetic fatal"));
#pragma warning restore CA2201
        using var handler = new RecordingHandler((_, _) => Task.FromException<HttpResponseMessage>(original));
        using var client = new HttpClient(handler);

        var actual = await Record.ExceptionAsync(() => SplitProcessReadinessProbe.AssertReadyAsync(client, Address,
            () => throw new InvalidOperationException("Fatal requests must not snapshot the host."))).ConfigureAwait(true);

        Assert.Same(original, actual);
        AssertRequest(handler);
    }

    [Fact]
    public async Task SnapshotFailureRetainsBothTheStatusAssertionAndIndependentFailure()
    {
        using var handler = new RecordingHandler((_, _) => Task.FromResult(Response(HttpStatusCode.ServiceUnavailable, "{}")));
        using var client = new HttpClient(handler);
        var original = new IOException("controlled snapshot failure");

        var failure = await Assert.ThrowsAsync<AggregateException>(() =>
            SplitProcessReadinessProbe.AssertReadyAsync(client, Address, () => throw original)).ConfigureAwait(true);

        Assert.Equal(2, failure.InnerExceptions.Count);
        Assert.IsType<EqualException>(failure.InnerExceptions[0]);
        Assert.Same(original, failure.InnerExceptions[1]);
        AssertRequest(handler);
    }

    [Fact]
    [Trait("Category", "SplitProcess")]
    public async Task RealGatewayUnavailableProbeFailsOnceAndLaterApplicationRecoversWithoutGatewayRestart()
    {
        var host = await SplitProcessHost.StartAsync(application: false).ConfigureAwait(true);
        await using var lifetime = host.ConfigureAwait(false);
        var gateway = host.GatewayProcessId;

        var failure = await Assert.ThrowsAsync<InvalidOperationException>(host.AssertGatewayReadyAsync).ConfigureAwait(true);

        Assert.IsType<EqualException>(failure.InnerException);
        Assert.Contains("Status=503", failure.Message, StringComparison.Ordinal);
        Assert.Contains("application=unavailable, metadata=absent", failure.Message, StringComparison.Ordinal);
        Assert.Contains("Application=wrapper absent", failure.Message, StringComparison.Ordinal);
        Assert.Contains("Gateway=owned PID=", failure.Message, StringComparison.Ordinal);
        Assert.Contains("Now listening on:", failure.Message, StringComparison.Ordinal);
        await host.StartApplicationAsync().ConfigureAwait(true);
        await host.AssertGatewayReadyAsync().ConfigureAwait(true);
        Assert.Equal(gateway, host.GatewayProcessId);
        Assert.False(Directory.Exists(host.GatewayUnusedDataPath));
    }

    private static HttpResponseMessage Response(HttpStatusCode status, string body) => new(status)
    {
        Content = new StringContent(body, Encoding.UTF8, "application/json")
    };

    private static void AssertRequest(RecordingHandler handler)
    {
        Assert.Equal(1, handler.Requests);
        Assert.Equal(HttpMethod.Get, handler.Method);
        Assert.Equal("/health/ready", handler.Path, StringComparer.Ordinal);
    }

    private sealed class RecordingHandler(Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> send)
        : HttpMessageHandler
    {
        internal int Requests { get; private set; }
        internal HttpMethod? Method { get; private set; }
        internal string? Path { get; private set; }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Requests++;
            Method = request.Method;
            Path = request.RequestUri!.AbsolutePath;
            return send(request, cancellationToken);
        }
    }
}
