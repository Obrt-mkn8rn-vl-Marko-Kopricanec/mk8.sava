using System.Diagnostics;
using Microsoft.Extensions.Configuration;
using Mk8.Sava.Hosting;

namespace Mk8.Sava.Tests;

public sealed class PortableTestConfigurationTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AmbientServiceInputsCannotEnterTheProtectedFixtureCollection(bool application)
    {
        var root = Directory.CreateTempSubdirectory("sava-portable-fixture-").FullName;
        try
        {
            var start = new ProcessStartInfo { UseShellExecute = false };
            start.ArgumentList.Add(application
                ? typeof(Mk8.Sava.Application.ApplicationProgram).Assembly.Location
                : typeof(Mk8.Sava.Gateway.Program).Assembly.Location);
            var sentinels = CreateAmbientSentinels(root);
            foreach (var pair in sentinels)
                start.Environment[pair.Key] = pair.Value;
            start.Environment["MK8_FIXTURE_PROCESS_SENTINEL"] = "retained-nonconfiguration-input";
            var settings = new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                ["Sava:DefaultAccount"] = "fixtureaccount",
                ["urls"] = "http://127.0.0.1:0",
                ["Logging:LogLevel:Default"] = "Warning",
            };

            await PortableTestConfiguration.MaterializeAsync(start, root, settings).ConfigureAwait(true);
            using var configuration = new ConfigurationManager();
            await SavaServiceConfiguration.LoadAsync(configuration, Path.Combine(start.WorkingDirectory, "Environment"))
                .ConfigureAwait(true);

            Assert.Equal(application ? "Application" : "Gateway", configuration["Service:Role"]);
            Assert.Equal("fixtureaccount", configuration["Sava:DefaultAccount"]);
            Assert.Equal("http://127.0.0.1:0", configuration["urls"]);
            Assert.Equal("Warning", configuration["Logging:LogLevel:Default"]);
            Assert.Null(configuration["Service:CertificateBase64"]);
            Assert.Null(configuration["Kestrel:Certificates:Default:Path"]);
            Assert.Null(configuration["Kestrel:Endpoints:Foreign:Url"]);
            Assert.Null(configuration["Gateway:StagingPath"]);
            Assert.Null(configuration["ApplicationTransport:AccessKey"]);
            Assert.Null(configuration["ApplicationHosting:CertificatePath"]);
            Assert.Null(configuration["http_ports"]);
            Assert.Null(configuration["https_ports"]);
            foreach (var key in sentinels.Keys)
                Assert.False(start.Environment.ContainsKey(key), key);
            Assert.Equal("retained-nonconfiguration-input", start.Environment["MK8_FIXTURE_PROCESS_SENTINEL"]);
            Assert.False(File.Exists(sentinels["Kestrel__Certificates__Default__Path"]));
            Assert.Equal(3, settings.Count);
            Assert.False(settings.ContainsKey("Service:Role"));
            using var certificate = SavaServiceCertificate.Load(configuration, required: false);
            Assert.Null(certificate);
        }
        finally
        {
            await SavaWebApplicationFactory.DeleteDataPathAsync(root).ConfigureAwait(true);
        }
    }

    private static Dictionary<string, string?> CreateAmbientSentinels(string root) =>
        new(StringComparer.Ordinal)
        {
            ["Service__CertificateBase64"] = "not-base64",
            ["Service__Role"] = "foreign-role",
            ["Sava__DefaultAccount"] = "foreign-account",
            ["Gateway__StagingPath"] = "foreign-staging",
            ["ApplicationTransport__AccessKey"] = "foreign-key",
            ["ApplicationHosting__CertificatePath"] = "foreign-certificate",
            ["Kestrel__Certificates__Default__Path"] = Path.Combine(root, "must-not-be-read.pfx"),
            ["Kestrel__Endpoints__Foreign__Url"] = "invalid-listener",
            ["Logging__LogLevel__Default"] = "foreign-log-level",
            ["ASPNETCORE_URLS"] = "foreign-url",
            ["DOTNET_ENVIRONMENT"] = "Development",
            ["URLS"] = "foreign-url",
            ["HTTP_PORTS"] = "foreign-port",
            ["HTTPS_PORTS"] = "foreign-port",
        };
}
