using System.Net;
using System.Net.Http.Headers;
using System.Text;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Http;
using Microsoft.Extensions.Options;
using Mk8.Sava.Hosting;
using Mk8.Sava.Protocol;

namespace Mk8.Sava.Tests;

public sealed class UrlSourceCookieIsolationTests
{
    [Fact]
    public async Task IndependentSourceClientsNeitherStoreNorSendSharedCookies()
    {
        using var configuration = new ConfigurationManager();
        configuration.AddInMemoryCollection(new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["Sava:DefaultAccount"] = SavaWebApplicationFactory.AccountName,
            [$"Sava:Accounts:{SavaWebApplicationFactory.AccountName}"] = SavaWebApplicationFactory.AccountKey,
        });
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSavaGateway(configuration);
        var provider = services.BuildServiceProvider();
        await using var providerLifetime = provider.ConfigureAwait(false);
        using var handler = CreateSourceHandler(provider);
        var sockets = Assert.IsType<SocketsHttpHandler>(handler);
        // The actual SocketsHttpHandler parses HTTP/1.1 over memory-only streams.
        // Its connection callback replaces DNS/socket access, not cookie policy.
        var source = new Uri("http://cookies.example.test/source");
        sockets.CookieContainer.Add(source, new Cookie("ambient", "not-authority", "/"));
        using var firstWire = new ExchangeStream(setCookie: true);
        using var secondWire = new ExchangeStream(setCookie: false);
        var exchanges = new Queue<ExchangeStream>([firstWire, secondWire]);
        sockets.ConnectCallback = (context, cancellationToken) =>
        {
            cancellationToken.ThrowIfCancellationRequested();
            Assert.Equal(source.Host, context.DnsEndPoint.Host);
            Assert.Equal(80, context.DnsEndPoint.Port);
            return ValueTask.FromResult<Stream>(exchanges.Dequeue());
        };
        using var first = new HttpClient(handler, disposeHandler: false);
        using var second = new HttpClient(handler, disposeHandler: false);
        using var guard = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await SendAsync(first, source, "caller-one", guard.Token).ConfigureAwait(true);
        await SendAsync(second, source, null, guard.Token).ConfigureAwait(true);

        Assert.Empty(exchanges);
        Assert.Null(Header(firstWire.Request, "Cookie"));
        Assert.Null(Header(secondWire.Request, "Cookie"));
        Assert.Equal("Bearer caller-one", Header(firstWire.Request, "Authorization"));
        Assert.Null(Header(secondWire.Request, "Authorization"));
        Assert.DoesNotContain(sockets.CookieContainer.GetCookies(source).Cast<Cookie>(), cookie =>
            string.Equals(cookie.Name, "response-session", StringComparison.Ordinal));
        Assert.False(sockets.UseCookies);
        Assert.True(firstWire.Disposed);
        Assert.True(secondWire.Disposed);
    }

    private static string? Header(string request, string name) => request.Split("\r\n", StringSplitOptions.None)
        .Where(line => line.StartsWith(name + ":", StringComparison.OrdinalIgnoreCase))
        .Select(line => line[(name.Length + 1)..].Trim()).SingleOrDefault();

    private static HttpMessageHandler CreateSourceHandler(IServiceProvider services)
    {
        var builder = services.GetRequiredService<HttpMessageHandlerBuilder>();
        builder.Name = nameof(UrlTransferClient);
        try
        {
            var options = services.GetRequiredService<IOptionsMonitor<HttpClientFactoryOptions>>().Get(builder.Name);
            foreach (var configure in options.HttpMessageHandlerBuilderActions)
                configure(builder);
            return builder.Build();
        }
        catch
        {
            builder.PrimaryHandler.Dispose();
            foreach (var additional in builder.AdditionalHandlers)
                additional.Dispose();
            throw;
        }
    }

    private static async Task SendAsync(HttpClient client, Uri source, string? token, CancellationToken cancellationToken)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, source)
        {
            Version = HttpVersion.Version11,
            VersionPolicy = HttpVersionPolicy.RequestVersionExact,
        };
        if (token is not null)
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
        using var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken)
            .ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("ok", await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false));
    }

    private sealed class ExchangeStream(bool setCookie) : Stream
    {
        private readonly MemoryStream _response = new(Encoding.ASCII.GetBytes(
            "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n" +
            (setCookie ? "Set-Cookie: response-session=not-shared; Path=/\r\n" : string.Empty) + "\r\nok"), writable: false);
        private readonly MemoryStream _request = new();

        internal string Request => Encoding.ASCII.GetString(_request.ToArray());
        internal bool Disposed { get; private set; }
        public override bool CanRead => !Disposed;
        public override bool CanSeek => false;
        public override bool CanWrite => !Disposed;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override int Read(byte[] buffer, int offset, int count) => _response.Read(buffer, offset, count);
        public override int Read(Span<byte> buffer) => _response.Read(buffer);
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) =>
            _response.ReadAsync(buffer, cancellationToken);
        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
            _response.ReadAsync(buffer, offset, count, cancellationToken);
        public override void Write(byte[] buffer, int offset, int count) => _request.Write(buffer, offset, count);
        public override void Write(ReadOnlySpan<byte> buffer) => _request.Write(buffer);
        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default) =>
            _request.WriteAsync(buffer, cancellationToken);
        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
            _request.WriteAsync(buffer, offset, count, cancellationToken);
        public override void Flush() => _request.Flush();
        public override Task FlushAsync(CancellationToken cancellationToken) => _request.FlushAsync(cancellationToken);
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                Disposed = true;
                _response.Dispose();
                _request.Dispose();
            }
            base.Dispose(disposing);
        }
    }
}
