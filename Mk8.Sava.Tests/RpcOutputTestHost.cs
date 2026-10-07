using System.Net;
using System.Reflection;
using System.Security.Cryptography;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Mk8.Sava.Application;
using Mk8.Sava.Configuration;
using Mk8.Sava.Storage;
using Mk8.Sava.Transport;

namespace Mk8.Sava.Tests;

internal sealed class RpcOutputTestHost : IAsyncDisposable
{
    private readonly WebApplication application;
    private readonly ApplicationRpcEndpoint endpoint;
    private readonly DirectoryInfo directory;
    private ApplicationRpcClient? client;
    private int requests;

    private RpcOutputTestHost(WebApplication application, DirectoryInfo directory, string keyPath, SavaOptions options)
    {
        this.application = application;
        this.directory = directory;
        endpoint = new ApplicationRpcEndpoint(new ApplicationTransportOptions { AccessKeyFile = keyPath }, options);
    }

    public IApplicationReadSessions Sessions { get; private set; } = null!;
    public int RequestCount => Volatile.Read(ref requests);

    public static Task<RpcOutputTestHost> StartAsync(byte[] content) =>
        StartCoreAsync((context, endpoint) => endpoint.HandleAsync(context, new OutputDispatcher(content)));

    public static Task<RpcOutputTestHost> StartPeerAsync(Func<HttpContext, Task> writeResponse) =>
        StartCoreAsync(async (context, endpoint) =>
        {
            Assert.True(endpoint.IsAuthenticated(context));
            Assert.Equal(ApplicationTransportSecurity.ProtocolVersion,
                context.Request.Headers[ApplicationTransportSecurity.ProtocolHeader].ToString());
            Assert.Equal(ApplicationTransportSecurity.CreatePolicyFingerprint(new SavaOptions()),
                context.Request.Headers[ApplicationTransportSecurity.PolicyHeader].ToString());
            Assert.Equal(ApplicationTransportSecurity.ContentType, context.Request.ContentType);
            Assert.Equal(RpcContracts.Fingerprint, context.Request.Headers["X-Mk8-Sava-Contracts"].ToString());
            var request = await RpcFrames.ReadControlAsync<RpcRequest>(context.Request.Body, 65536, context.RequestAborted)
                .ConfigureAwait(false);
            Assert.Equal(typeof(IApplicationReadSessions).FullName, request.Contract);
            var contract = RpcContracts.GetContract(typeof(IApplicationReadSessions));
            Assert.Equal(contract.GetMethod(typeof(IApplicationReadSessions).GetMethod(nameof(IApplicationReadSessions.WriteRangeAsync))!).Id,
                request.Method);
            Assert.False(request.HasInput);
            using var input = new FramedReadStream(context.Request.Body, 0);
            await input.EnsureCompletedAsync(context.RequestAborted).ConfigureAwait(false);
            context.Response.ContentType = ApplicationTransportSecurity.ContentType;
            context.Response.Headers[ApplicationTransportSecurity.ProtocolHeader] = ApplicationTransportSecurity.ProtocolVersion;
            context.Response.Headers[ApplicationTransportSecurity.PolicyHeader] =
                context.Request.Headers[ApplicationTransportSecurity.PolicyHeader];
            context.Response.Headers["X-Mk8-Sava-Contracts"] = RpcContracts.Fingerprint;
            await writeResponse(context).ConfigureAwait(false);
        });

    private static async Task<RpcOutputTestHost> StartCoreAsync(Func<HttpContext, ApplicationRpcEndpoint, Task> handle)
    {
        var hostDirectory = Directory.CreateTempSubdirectory("mk8-sava-rpc-output-");
        WebApplication? webApplication = null;
        RpcOutputTestHost? host = null;
        try
        {
            var keyPath = Path.Combine(hostDirectory.FullName, "transport.key");
            var key = RandomNumberGenerator.GetBytes(32);
            try
            {
                var creation = new FileStreamOptions { Mode = FileMode.CreateNew, Access = FileAccess.Write };
                if (!OperatingSystem.IsWindows())
                    creation.UnixCreateMode = UnixFileMode.UserRead | UnixFileMode.UserWrite;
                var file = new FileStream(keyPath, creation);
                await using var fileLifetime = file.ConfigureAwait(false);
                await file.WriteAsync(System.Text.Encoding.ASCII.GetBytes(Convert.ToBase64String(key))).ConfigureAwait(false);
            }
            finally
            {
                CryptographicOperations.ZeroMemory(key);
            }
            var options = new SavaOptions();
            var builder = WebApplication.CreateSlimBuilder();
            builder.Logging.ClearProviders();
            builder.WebHost.ConfigureKestrel(server => server.Listen(IPAddress.Loopback, 0));
            webApplication = builder.Build();
            host = new RpcOutputTestHost(webApplication, hostDirectory, keyPath, options);
            var ownedHost = host;
            webApplication.MapPost("/internal/application", async context =>
            {
                await handle(context, ownedHost.endpoint).ConfigureAwait(false);
                Interlocked.Increment(ref ownedHost.requests);
            });
            using var startup = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            await webApplication.StartAsync(startup.Token).ConfigureAwait(false);
            var address = webApplication.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!
                .Addresses.Single();
            host.Connect(new Uri(new Uri(address), "/internal/application"), keyPath, options);
            return host;
        }
        catch
        {
            if (host is not null)
                await host.DisposeAsync().ConfigureAwait(false);
            else
            {
                if (webApplication is not null)
                    await webApplication.DisposeAsync().ConfigureAwait(false);
                hostDirectory.Delete(recursive: true);
            }
            throw;
        }
    }

    private void Connect(Uri address, string keyPath, SavaOptions options)
    {
        client = new ApplicationRpcClient(new ApplicationTransportOptions
        {
            AccessKeyFile = keyPath,
            Endpoint = address,
            RequestTimeout = TimeSpan.FromSeconds(15),
        }, options);
        Sessions = client.CreateProxy<IApplicationReadSessions>();
    }

    public async ValueTask DisposeAsync()
    {
        client?.Dispose();
        try
        {
            using var shutdown = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await application.StopAsync(shutdown.Token).ConfigureAwait(false);
        }
        finally
        {
            try
            {
                await application.DisposeAsync().ConfigureAwait(false);
            }
            finally
            {
                endpoint.Dispose();
                directory.Delete(recursive: true);
            }
        }
    }

    private sealed class OutputDispatcher(byte[] content) : IApplicationRpcDispatcher
    {
        public async ValueTask<object?> InvokeAsync(
            Type contract, MethodInfo method, object?[] arguments, CancellationToken cancellationToken)
        {
            Assert.Equal(typeof(IApplicationReadSessions), contract);
            Assert.Equal(nameof(IApplicationReadSessions.WriteRangeAsync), method.Name);
            Assert.Equal(content.LongLength, arguments[3]);
            var destination = Assert.IsAssignableFrom<Stream>(arguments[4]);
            await destination.WriteAsync(content, cancellationToken).ConfigureAwait(false);
            return null;
        }
    }
}
