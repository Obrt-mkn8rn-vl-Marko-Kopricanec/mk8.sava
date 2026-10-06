using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Core.Pipeline;
using Microsoft.Extensions.Configuration;
using Mk8.Sava.Configuration;
using Mk8.Sava.Transport;

namespace Mk8.Sava.Tests;

internal sealed class SplitProcessHost : IAsyncDisposable
{
    private readonly IReadOnlyDictionary<string, string?> _settings;
    private ChildProcess? _application;
    private ChildProcess? _gateway;
    private readonly StringBuilder _completedLogs = new();
    private string? _gatewayCertificateHash;
    private HttpClient? _gatewayTransportClient;
    private IReadOnlyDictionary<string, string?> _applicationOverrides = new Dictionary<string, string?>(StringComparer.Ordinal);

    private SplitProcessHost(string root, IReadOnlyDictionary<string, string?> settings)
    {
        Root = root;
        _settings = settings;
        StoragePath = Path.Combine(root, "storage");
        GatewayUnusedDataPath = Path.Combine(root, "gateway-not-storage");
        AccessKeyFile = Path.Combine(root, "transport.key");
        ApplicationAddress = new Uri($"http://127.0.0.1:{ReservePort()}/");
    }

    public string Root { get; }
    public string StoragePath { get; }
    public string GatewayUnusedDataPath { get; }
    public string AccessKeyFile { get; }
    public Uri ApplicationAddress { get; private set; }
    public Uri GatewayAddress { get; private set; } = null!;
    public int GatewayProcessId => _gateway?.Id ?? throw new InvalidOperationException("Gateway is not running.");
    public BlobServiceClient Client { get; private set; } = null!;
    public string CapturedLogs => _completedLogs + (_application?.Logs ?? string.Empty) + (_gateway?.Logs ?? string.Empty);

    public static async Task<SplitProcessHost> StartAsync(bool application = true,
        IReadOnlyDictionary<string, string?>? settings = null)
    {
        var host = new SplitProcessHost(Directory.CreateTempSubdirectory("sava-split-").FullName,
            settings ?? new Dictionary<string, string?>(StringComparer.Ordinal));
        try
        {
            var creation = new FileStreamOptions { Mode = FileMode.CreateNew, Access = FileAccess.Write };
            if (!OperatingSystem.IsWindows())
                creation.UnixCreateMode = UnixFileMode.UserRead | UnixFileMode.UserWrite;
            var key = new FileStream(host.AccessKeyFile, creation);
            await using (key.ConfigureAwait(false))
                await key.WriteAsync(Encoding.ASCII.GetBytes(Convert.ToBase64String(RandomNumberGenerator.GetBytes(32)))).ConfigureAwait(false);
            if (application)
                await host.StartApplicationAsync().ConfigureAwait(false);
            await host.StartGatewayAsync().ConfigureAwait(false);
            return host;
        }
        catch
        {
            await host.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    public ApplicationRpcClient CreateApplicationClient()
    {
        var settings = CommonSettings();
        foreach (var pair in _applicationOverrides)
            settings[pair.Key] = pair.Value;
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(settings).Build();
        var options = configuration.GetSection(SavaOptions.SectionName).Get<SavaOptions>()!;
        return new ApplicationRpcClient(new ApplicationTransportOptions
        {
            Endpoint = new Uri(ApplicationAddress, "/internal/application"),
            AccessKeyFile = AccessKeyFile
        }, options);
    }

    public HttpClient CreateHttpClient()
    {
        HttpClientHandler? handler = null;
        try
        {
            handler = new HttpClientHandler { CheckCertificateRevocationList = true };
            if (_gatewayCertificateHash is { } expected)
            {
#pragma warning disable MA0039 // Only the isolated HTTPS fixture's exact synthetic leaf is pinned; name validation and all production TLS paths remain untouched.
                handler.ServerCertificateCustomValidationCallback = (_, certificate, _, errors) =>
                    errors == SslPolicyErrors.None || (errors == SslPolicyErrors.RemoteCertificateChainErrors &&
                        certificate is not null && certificate.NotBefore <= DateTime.Now && certificate.NotAfter >= DateTime.Now &&
                        string.Equals(certificate.GetCertHashString(HashAlgorithmName.SHA256), expected, StringComparison.Ordinal));
#pragma warning restore MA0039
            }
            var client = new HttpClient(handler, disposeHandler: true);
            handler = null;
            return client;
        }
        finally
        {
            handler?.Dispose();
        }
    }

    public BlobClientOptions CreateBlobClientOptions()
    {
        var options = new BlobClientOptions(BlobClientOptions.ServiceVersion.V2023_11_03);
        options.Retry.MaxRetries = 0;
        if (_gatewayCertificateHash is not null)
        {
            _gatewayTransportClient ??= CreateHttpClient();
            options.Transport = new HttpClientTransport(_gatewayTransportClient);
        }
        return options;
    }

    public async Task StartApplicationAsync(IReadOnlyDictionary<string, string?>? overrides = null)
    {
        if (_application is not null)
            throw new InvalidOperationException("Application is already running.");
        var start = CreateStart(typeof(Mk8.Sava.Application.ApplicationProgram).Assembly.Location);
        start.Environment["Sava__DataPath"] = StoragePath;
        _applicationOverrides = overrides ?? new Dictionary<string, string?>(StringComparer.Ordinal);
        foreach (var pair in _applicationOverrides)
            start.Environment[pair.Key.Replace(":", "__", StringComparison.Ordinal)] = pair.Value;
        _application = new ChildProcess(start);
        ApplicationAddress = await _application.Address.WaitAsync(TimeSpan.FromSeconds(60)).ConfigureAwait(false);
        await AssertStatusAsync(ApplicationAddress, "/health/ready", HttpStatusCode.OK).ConfigureAwait(false);
    }

    public async Task StopApplicationAsync()
    {
        if (_application is null)
            return;
        var child = _application;
        try
        {
            await child.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            _completedLogs.AppendLine("Application process").Append(child.Logs);
            _application = null;
        }
    }

    public async Task StopGatewayAsync()
    {
        if (_gateway is null)
            return;
        var child = _gateway;
        try
        {
            await child.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            _completedLogs.AppendLine("Gateway process").Append(child.Logs);
            _gateway = null;
            _gatewayTransportClient?.Dispose();
            _gatewayTransportClient = null;
        }
    }

    public async Task StartGatewayAsync(IReadOnlyDictionary<string, string?>? overrides = null,
        X509Certificate2? trustedGatewayCertificate = null)
    {
        if (_gateway is not null)
            throw new InvalidOperationException("Gateway is already running.");
        _gatewayCertificateHash = trustedGatewayCertificate?.GetCertHashString(HashAlgorithmName.SHA256);
        var start = CreateStart(typeof(Program).Assembly.Location);
        start.Environment["Sava__DataPath"] = GatewayUnusedDataPath;
        start.Environment["Gateway__StagingPath"] = Path.Combine(Root, "gateway-staging");
        if (overrides is not null)
        {
            foreach (var pair in overrides)
                start.Environment[pair.Key.Replace(":", "__", StringComparison.Ordinal)] = pair.Value;
        }
        _gateway = new ChildProcess(start);
        GatewayAddress = await _gateway.Address.WaitAsync(TimeSpan.FromSeconds(60)).ConfigureAwait(false);
        using var probe = CreateHttpClient();
        probe.Timeout = TimeSpan.FromSeconds(15);
        using var response = await probe.GetAsync(new Uri(GatewayAddress, "/health/live")).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Client = new BlobServiceClient(new Uri(GatewayAddress, "/" + SavaWebApplicationFactory.AccountName),
            new StorageSharedKeyCredential(SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey),
            CreateBlobClientOptions());
    }

    public static async Task AssertStatusAsync(Uri address, string path, HttpStatusCode status)
    {
        using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(15) };
        using var response = await client.GetAsync(new Uri(address, path)).ConfigureAwait(false);
        Assert.Equal(status, response.StatusCode);
    }

    public ProcessStartInfo CreateStart(string assembly)
    {
        var start = new ProcessStartInfo(Environment.GetEnvironmentVariable("DOTNET_HOST_PATH") ?? "dotnet")
        {
            WorkingDirectory = AppContext.BaseDirectory,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false
        };
        start.ArgumentList.Add(assembly);
        foreach (var key in start.Environment.Keys.Where(key => key.StartsWith("Sava__", StringComparison.OrdinalIgnoreCase) ||
                     key.StartsWith("Gateway__", StringComparison.OrdinalIgnoreCase) ||
                     key.StartsWith("ApplicationTransport__", StringComparison.OrdinalIgnoreCase) ||
                     key.StartsWith("ApplicationHosting__", StringComparison.OrdinalIgnoreCase) ||
                     key.StartsWith("Kestrel__", StringComparison.OrdinalIgnoreCase) ||
                     key.StartsWith("ASPNETCORE_", StringComparison.OrdinalIgnoreCase)).ToArray())
            start.Environment.Remove(key);
        foreach (var pair in CommonSettings())
            start.Environment[pair.Key.Replace(":", "__", StringComparison.Ordinal)] = pair.Value;
        start.Environment["ASPNETCORE_ENVIRONMENT"] = "Production";
        start.Environment["DOTNET_ENVIRONMENT"] = "Production";
        start.Environment["ASPNETCORE_URLS"] = "http://127.0.0.1:0";
        start.Environment["ApplicationTransport__Endpoint"] = new Uri(ApplicationAddress, "/internal/application").AbsoluteUri;
        start.Environment["ApplicationTransport__AccessKeyFile"] = AccessKeyFile;
        start.Environment["Logging__LogLevel__Microsoft.Hosting.Lifetime"] = "Information";
        return start;
    }

    private Dictionary<string, string?> CommonSettings()
    {
        var result = new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["Sava:DefaultAccount"] = SavaWebApplicationFactory.AccountName,
            [$"Sava:Accounts:{SavaWebApplicationFactory.AccountName}"] = SavaWebApplicationFactory.AccountKey,
            [$"Sava:Accounts:{SavaWebApplicationFactory.SecondAccountName}"] = SavaWebApplicationFactory.SecondAccountKey,
            ["Sava:MaintenanceScanInterval"] = "00:00:00.100"
        };
        foreach (var pair in _settings)
            result[pair.Key] = pair.Value;
        return result;
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            await StopGatewayAsync().ConfigureAwait(false);
        }
        finally
        {
            await StopApplicationAsync().ConfigureAwait(false);
            var logDirectory = Path.Combine(Directory.GetCurrentDirectory(), "TestResults", "split-process-logs");
            Directory.CreateDirectory(logDirectory);
            await File.WriteAllTextAsync(Path.Combine(logDirectory, Path.GetFileName(Root) + ".log"), CapturedLogs)
                .ConfigureAwait(false);
            await SavaWebApplicationFactory.DeleteDataPathAsync(Root).ConfigureAwait(false);
        }
    }

    private static int ReservePort()
    {
        using var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        socket.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        return ((IPEndPoint)socket.LocalEndPoint!).Port;
    }

    private sealed class ChildProcess : IAsyncDisposable
    {
        private readonly Process _process;
        private readonly Task _output;
        private readonly Task _error;
        private readonly StringBuilder _logs = new();
        private readonly TaskCompletionSource<Uri> _address = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public ChildProcess(ProcessStartInfo start)
        {
            _process = Process.Start(start) ?? throw new InvalidOperationException("A split service could not start.");
            _output = CaptureAsync(_process.StandardOutput, publishAddress: true);
            _error = CaptureAsync(_process.StandardError, publishAddress: false);
        }

        public int Id => _process.Id;
        public Task<Uri> Address => _address.Task;
        public string Logs { get { lock (_logs) return _logs.ToString(); } }

        private async Task CaptureAsync(StreamReader reader, bool publishAddress)
        {
            while (await reader.ReadLineAsync().ConfigureAwait(false) is { } line)
            {
                lock (_logs) _logs.AppendLine(line);
                const string prefix = "Now listening on: ";
                var offset = line.IndexOf(prefix, StringComparison.Ordinal);
                if (publishAddress && offset >= 0 && Uri.TryCreate(line[(offset + prefix.Length)..].Trim(), UriKind.Absolute, out var address))
                    _address.TrySetResult(address);
            }
            if (publishAddress)
                _address.TrySetException(new InvalidOperationException("Service exited before publishing its listener: " + Logs));
        }

        public async ValueTask DisposeAsync()
        {
            if (!_process.HasExited)
                _process.Kill(entireProcessTree: true);
            await _process.WaitForExitAsync().ConfigureAwait(false);
            await Task.WhenAll(_output, _error).WaitAsync(TimeSpan.FromSeconds(30)).ConfigureAwait(false);
            _process.Dispose();
        }
    }
}
