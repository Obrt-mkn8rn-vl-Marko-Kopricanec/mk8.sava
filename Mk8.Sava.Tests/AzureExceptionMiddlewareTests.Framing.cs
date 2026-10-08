using System.Net;
using System.Runtime.ExceptionServices;
using System.Text;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Mk8.Sava.Protocol;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

public sealed partial class AzureExceptionMiddlewareTests
{
    [Theory]
    [InlineData("ordinary error")]
    [InlineData("ž <>& 🧪 error")]
    public async Task BufferedXmlErrorDeclaresItsExactUtf8Length(string message)
    {
        var context = CreateContext();
        var middleware = CreateMiddleware(_ => throw new AzureStorageException(400, "InvalidInput", message));

        await middleware.InvokeAsync(context).ConfigureAwait(true);

        var body = ReadBody(context);
        Assert.Equal((long)Encoding.UTF8.GetByteCount(body), context.Response.ContentLength);
        Assert.Equal(StatusCodes.Status400BadRequest, context.Response.StatusCode);
        Assert.Equal("InvalidInput", context.Response.Headers["x-ms-error-code"]);
        Assert.Equal("application/xml", context.Response.ContentType);
        Assert.Contains("<Code>InvalidInput</Code>", body, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("HEAD", 413)]
    [InlineData("GET", 304)]
    public async Task BodylessErrorsDoNotInventAnXmlPayloadOrLength(string method, int status)
    {
        var context = CreateContext();
        context.Request.Method = method;
        var middleware = CreateMiddleware(_ => throw new AzureStorageException(status, "Control", "unused"));

        await middleware.InvokeAsync(context).ConfigureAwait(true);

        Assert.Equal(status, context.Response.StatusCode);
        Assert.Empty(ReadBody(context));
        Assert.Null(context.Response.ContentLength);
    }

    [Fact]
    public async Task CleanupRetainsFatalBodyFaultPublishedDuringHostRetirement()
    {
        var pipeline = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var body = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
#pragma warning disable CA2201 // A synthetic wrapped fatal graph verifies identity preservation, not actual exhaustion.
        var failure = new InvalidOperationException("controlled body failure", new OutOfMemoryException());
#pragma warning restore CA2201
        var builder = WebApplication.CreateSlimBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(options => options.Listen(IPAddress.Loopback, 0));
        var app = builder.Build();
        await using var lifetime = app.ConfigureAwait(false);
        await app.StartAsync().ConfigureAwait(true);
        using var callback = app.Lifetime.ApplicationStopping.Register(() => body.TrySetException(failure));
        Exception? escaped;
        try
        {
#pragma warning disable CA2025 // The immediately following awaited observer joins this owned task before callback/host disposal.
            var retirement = RetireErrorControlAsync(app, null, pipeline.Task, body.Task);
#pragma warning restore CA2025
#pragma warning disable VSTHRD003 // Observe this immediately-started owned retirement task; its cleanup awaits are context-independent.
            escaped = await Record.ExceptionAsync(() => retirement)
                .ConfigureAwait(true);
#pragma warning restore VSTHRD003
            Assert.False(pipeline.Task.IsCompleted);
        }
        finally
        {
            pipeline.TrySetResult();
        }
        var observed = await Assert.ThrowsAsync<InvalidOperationException>(
            () => Task.WhenAll(pipeline.Task, body.Task).WaitAsync(TimeSpan.FromSeconds(5))).ConfigureAwait(true);
        Assert.Same(failure, observed);
        var aggregate = Assert.IsType<AggregateException>(escaped);
        Assert.Contains(aggregate.Flatten().InnerExceptions, candidate => ReferenceEquals(candidate, failure));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RealKestrelErrorBodyCompletesBeforeLaterPipelineRetirement(bool transportLimit)
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var finished = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var app = CreateHeldErrorApplication(transportLimit, entered, release, finished);
        await using var lifetime = app.ConfigureAwait(false);
        await app.StartAsync().ConfigureAwait(true);
        var address = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.Single();
        using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };
        using var request = new HttpRequestMessage(HttpMethod.Post, new Uri(new Uri(address), "/error"))
        {
            Content = new ByteArrayContent(transportLimit ? [1, 2] : [])
        };
        Task<byte[]>? observedBody = null;
        HttpResponseMessage? response = null;
        Exception? primary = null;
        var cleanupCompleted = false;
        try
        {
            try
            {
                response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead).ConfigureAwait(true);
                await entered.Task.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(true);
                observedBody = response.Content.ReadAsByteArrayAsync();
                var body = await observedBody.WaitAsync(TimeSpan.FromSeconds(2)).ConfigureAwait(true);
                Assert.False(finished.Task.IsCompleted);
                Assert.False(release.Task.IsCompleted);
                AssertBufferedError(response, body, transportLimit);
            }
            catch (Exception failure)
            {
                primary = failure;
                throw;
            }
            finally
            {
                release.TrySetResult();
                await RetireErrorControlAsync(app, response,
                    entered.Task.IsCompletedSuccessfully ? finished.Task : Task.CompletedTask,
                    observedBody ?? Task.CompletedTask).ConfigureAwait(true);
                cleanupCompleted = true;
            }
        }
        catch (Exception cleanupFailure)
        {
            if (primary is not null && !cleanupCompleted)
                throw new AggregateException("Error assertion and owned cleanup both failed.", primary, cleanupFailure);
            throw;
        }
    }

    private static WebApplication CreateHeldErrorApplication(bool transportLimit, TaskCompletionSource entered,
        TaskCompletionSource release, TaskCompletionSource finished)
    {
        var builder = WebApplication.CreateSlimBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(options =>
        {
            options.Listen(IPAddress.Loopback, 0);
            options.Limits.MaxRequestBodySize = 1;
        });
        var app = builder.Build();
        app.Use(async (context, next) =>
        {
            await next(context).ConfigureAwait(false);
            entered.TrySetResult();
            try
            {
#pragma warning disable VSTHRD003 // This context-independent test-owned gate is always released by the fixture's finally.
                await release.Task.ConfigureAwait(false);
#pragma warning restore VSTHRD003
            }
            finally
            {
                finished.TrySetResult();
            }
        });
        app.UseMiddleware<AzureExceptionMiddleware>();
        app.MapPost("/error", async context =>
        {
            if (transportLimit)
                _ = await context.Request.Body.ReadAsync(new byte[1], context.RequestAborted).ConfigureAwait(false);
            throw new AzureStorageException(400, "InvalidInput", "ž <>& 🧪 controlled error");
        });
        return app;
    }

    private static void AssertBufferedError(HttpResponseMessage response, byte[] body, bool transportLimit)
    {
        Assert.Equal(transportLimit ? HttpStatusCode.RequestEntityTooLarge : HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal((long)body.Length, response.Content.Headers.ContentLength);
        Assert.Empty(response.Headers.TransferEncoding);
        var expected = transportLimit ? "RequestBodyTooLarge" : "InvalidInput";
        Assert.Equal(expected, response.Headers.GetValues("x-ms-error-code").Single(), StringComparer.Ordinal);
        Assert.Contains($"<Code>{expected}</Code>", Encoding.UTF8.GetString(body), StringComparison.Ordinal);
    }

    private static async Task RetireErrorControlAsync(WebApplication app, HttpResponseMessage? response,
        Task pipeline, Task body)
    {
        var failures = new List<Exception>();
        var observation = Task.WhenAll(pipeline, body);
        try
        {
            await observation.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
        }
#pragma warning disable CA1031 // Retain observation failures while attempting the remaining independent owned cleanup.
        catch (Exception failure)
        {
            failures.Add(failure);
            _ = observation.ContinueWith(static completed => _ = completed.Exception, CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously | TaskContinuationOptions.OnlyOnFaulted, TaskScheduler.Default);
        }
#pragma warning restore CA1031
        try { response?.Dispose(); }
#pragma warning disable CA1031 // Retain disposal failure without skipping host retirement; all collected failures are rethrown below.
        catch (Exception failure) { failures.Add(failure); }
#pragma warning restore CA1031
        try
        {
            using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await app.StopAsync(stop.Token).ConfigureAwait(false);
        }
#pragma warning disable CA1031 // Preserve host retirement failure together with any independent observation/disposal failures.
        catch (Exception failure) { failures.Add(failure); }
#pragma warning restore CA1031
        // Host retirement may publish a fault while another operation remains pending.
        foreach (var operation in new[] { pipeline, body })
            if (operation.Exception is { } known && CatastrophicExceptionPolicy.Contains(known))
                failures.Add(known);
        if (failures.Count == 1)
            ExceptionDispatchInfo.Throw(failures[0]);
        if (failures.Count > 1)
            throw new AggregateException("Independent error-control cleanup failures.", failures);
    }
}
