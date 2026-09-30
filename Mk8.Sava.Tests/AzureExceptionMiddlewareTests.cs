using System.Text;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.Extensions.Logging.Abstractions;
using Mk8.Sava.Protocol;

namespace Mk8.Sava.Tests;

public sealed class AzureExceptionMiddlewareTests
{
    [Fact]
    public async Task UnexpectedFailureIsAzureShapedWithoutExposingDetails()
    {
        var context = CreateContext();
        var middleware = CreateMiddleware(_ => throw new InvalidOperationException("secret failure detail"));

        await middleware.InvokeAsync(context);

        Assert.Equal(StatusCodes.Status500InternalServerError, context.Response.StatusCode);
        Assert.Equal("InternalError", context.Response.Headers["x-ms-error-code"]);
        Assert.False(string.IsNullOrEmpty(context.Response.Headers["x-ms-request-id"]));
        var body = ReadBody(context);
        Assert.Contains("<Code>InternalError</Code>", body, StringComparison.Ordinal);
        Assert.DoesNotContain("secret failure detail", body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task RequestAbortedCancellationDoesNotWriteAnErrorResponse()
    {
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();
        var context = CreateContext();
        context.RequestAborted = cancellation.Token;
        var middleware = CreateMiddleware(_ => throw new OperationCanceledException(cancellation.Token));

        await middleware.InvokeAsync(context);

        Assert.Equal(0, context.Response.Body.Length);
        Assert.False(context.Response.Headers.ContainsKey("x-ms-error-code"));
    }

    [Fact]
    public async Task NonAbortedCancellationIsAnAzureInternalError()
    {
        var context = CreateContext();
        var middleware = CreateMiddleware(_ => throw new OperationCanceledException("timed out"));

        await middleware.InvokeAsync(context);

        Assert.Equal(StatusCodes.Status500InternalServerError, context.Response.StatusCode);
        Assert.Equal("InternalError", context.Response.Headers["x-ms-error-code"]);
    }

    [Fact]
    public async Task AlreadyStartedResponseIsNotRewrittenAsAzureXml()
    {
        var context = new DefaultHttpContext();
        using var body = new MemoryStream();
        context.Features.Set<IHttpResponseFeature>(new StartedResponseFeature(body));
        context.Response.Body = body;
        var middleware = CreateMiddleware(_ => throw new InvalidOperationException("failure after headers"));

        await middleware.InvokeAsync(context);

        Assert.Equal(StatusCodes.Status200OK, context.Response.StatusCode);
        Assert.Empty(body.ToArray());
        Assert.False(context.Response.Headers.ContainsKey("x-ms-error-code"));
    }

    [Theory]
    [MemberData(nameof(CatastrophicFailures))]
    public async Task CatastrophicFailureEscapesWithoutFormatting(Exception failure)
    {
        var context = CreateContext();
        var middleware = CreateMiddleware(_ => throw failure);

        var escaped = await Assert.ThrowsAnyAsync<Exception>(() => middleware.InvokeAsync(context));

        Assert.Same(failure, escaped);
        Assert.Equal(0, context.Response.Body.Length);
        Assert.False(context.Response.Headers.ContainsKey("x-ms-error-code"));
    }

    public static TheoryData<Exception> CatastrophicFailures => new()
    {
#pragma warning disable CA2201 // Deliberately construct runtime-reserved exceptions to verify the outer HTTP boundary does not swallow them.
        new OutOfMemoryException(),
        new AccessViolationException(),
        new InvalidOperationException("wrapped", new OutOfMemoryException()),
        new AggregateException(new InvalidOperationException("ordinary"),
            new InvalidOperationException("wrapped", new AccessViolationException()))
#pragma warning restore CA2201
    };

    [Fact]
    public async Task RequestAbortDoesNotSwallowNestedCatastrophicFailure()
    {
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();
        var context = CreateContext();
        context.RequestAborted = cancellation.Token;
#pragma warning disable CA2201 // Deliberately verify that a nested runtime-reserved failure escapes the canceled-request path.
        var failure = new OperationCanceledException("aborted", new OutOfMemoryException(), cancellation.Token);
#pragma warning restore CA2201
        var middleware = CreateMiddleware(_ => throw failure);

        var escaped = await Assert.ThrowsAsync<OperationCanceledException>(() => middleware.InvokeAsync(context));

        Assert.Same(failure, escaped);
        Assert.Equal(0, context.Response.Body.Length);
    }

    private static AzureExceptionMiddleware CreateMiddleware(RequestDelegate next) =>
        new(next, NullLogger<AzureExceptionMiddleware>.Instance);

    [Theory]
    [InlineData("PUT")]
    [InlineData("HEAD")]
    public async Task TransportBodyLimitIsAnAzure413WithoutLeakingServerDetails(string method)
    {
        var context = CreateContext();
        context.Request.Method = method;
        var middleware = CreateMiddleware(_ => throw new BadHttpRequestException(
            "private transport details", StatusCodes.Status413PayloadTooLarge));

        await middleware.InvokeAsync(context);

        Assert.Equal(StatusCodes.Status413PayloadTooLarge, context.Response.StatusCode);
        Assert.Equal("RequestBodyTooLarge", context.Response.Headers["x-ms-error-code"]);
        var body = ReadBody(context);
        Assert.DoesNotContain("private transport details", body, StringComparison.Ordinal);
        if (HttpMethods.IsHead(method))
            Assert.Empty(body);
        else
            Assert.Contains("<Code>RequestBodyTooLarge</Code>", body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task OtherTransportFailuresAreNotMisclassifiedAsOversizedBodies()
    {
        var context = CreateContext();
        var middleware = CreateMiddleware(_ => throw new BadHttpRequestException(
            "private malformed request details", StatusCodes.Status400BadRequest));

        await middleware.InvokeAsync(context);

        Assert.Equal(StatusCodes.Status500InternalServerError, context.Response.StatusCode);
        Assert.Equal("InternalError", context.Response.Headers["x-ms-error-code"]);
        Assert.DoesNotContain("private malformed request details", ReadBody(context), StringComparison.Ordinal);
    }

    private static DefaultHttpContext CreateContext()
    {
        var context = new DefaultHttpContext();
        context.Response.Body = new MemoryStream();
        return context;
    }

    private static string ReadBody(HttpContext context)
    {
        context.Response.Body.Position = 0;
        using var reader = new StreamReader(context.Response.Body, Encoding.UTF8, leaveOpen: true);
        return reader.ReadToEnd();
    }

    private sealed class StartedResponseFeature(Stream body) : IHttpResponseFeature
    {
        public int StatusCode { get; set; } = StatusCodes.Status200OK;
        public string? ReasonPhrase { get; set; }
        public IHeaderDictionary Headers { get; set; } = new HeaderDictionary();
        public Stream Body { get; set; } = body;
        public bool HasStarted => true;

        public void OnStarting(Func<object, Task> callback, object state) { }

        public void OnCompleted(Func<object, Task> callback, object state) { }
    }
}
