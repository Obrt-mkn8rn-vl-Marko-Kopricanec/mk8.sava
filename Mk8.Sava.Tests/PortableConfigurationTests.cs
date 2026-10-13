using Microsoft.Extensions.Configuration;
using Mk8.Sava.Hosting;
using Supprocom.Secrets;

namespace Mk8.Sava.Tests;

public sealed class PortableConfigurationTests : IDisposable
{
    private readonly DirectoryInfo directory = Directory.CreateTempSubdirectory("sava-portable-configuration-");

    public void Dispose() => directory.Delete(recursive: true);

    [Fact]
    public async Task OneProtectedSnapshotReplacesOtherSourcesAndDoesNotReload()
    {
        var store = CreateStore();
        await store.SetAsync("Service__Role", "Gateway").ConfigureAwait(true);
        await store.SetAsync("Sava__DefaultAccount", "fixtureaccount").ConfigureAwait(true);
        using var configuration = new ConfigurationManager();
        configuration.AddInMemoryCollection(new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["Service:Role"] = "Application",
            ["foreign:setting"] = "must-not-survive",
        });

        await SavaServiceConfiguration.LoadAsync(configuration, directory.FullName).ConfigureAwait(true);

        Assert.Single(configuration.Sources);
        Assert.Equal("Gateway", configuration["Service:Role"]);
        Assert.Equal("fixtureaccount", configuration["Sava:DefaultAccount"]);
        Assert.Null(configuration["foreign:setting"]);
        Assert.Equal(SecretFileProtectionState.Protected, await store.GetStateAsync().ConfigureAwait(true));
        Assert.True(File.Exists(Path.Combine(directory.FullName, ".installation.key")));
        await store.SetAsync("Service__Role", "Application").ConfigureAwait(true);
        Assert.Equal("Gateway", configuration["Service:Role"]);
        using var restarted = new ConfigurationManager();
        await SavaServiceConfiguration.LoadAsync(restarted, directory.FullName).ConfigureAwait(true);
        Assert.Equal("Application", restarted["Service:Role"]);
    }

    [Theory]
    [InlineData("SUPPROCOM_SECRET_SOURCE=https://provider.example.test/collection")]
    [InlineData("{\"Service\":{\"Role\":\"Gateway\"}}")]
    [InlineData("Service__Role=Gateway\nService__Role=Application")]
    public async Task PointersLegacyJsonAndDuplicateAssignmentsRefuseWithoutValueDiagnostics(string document)
    {
        await WriteDocumentAsync(document).ConfigureAwait(true);
        using var configuration = new ConfigurationManager();

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            SavaServiceConfiguration.LoadAsync(configuration, directory.FullName)).ConfigureAwait(true);

        Assert.StartsWith("Supprocom.Secrets startup rejected:", error.Message, StringComparison.Ordinal);
        Assert.Null(error.InnerException);
        Assert.DoesNotContain(document, error.Message, StringComparison.Ordinal);
        Assert.Empty(configuration.AsEnumerable());
    }

    [Fact]
    public async Task ProductionIgnoresDevelopmentDocumentsAndProviderManifest()
    {
        var store = CreateStore();
        await store.SetAsync("Service__Role", "Gateway").ConfigureAwait(true);
        await File.WriteAllTextAsync(Path.Combine(directory.FullName, ".dev.env"),
            "Service__Role=Application").ConfigureAwait(true);
        await File.WriteAllTextAsync(Path.Combine(directory.FullName, "supprocom-secrets.providers.json"),
            "deliberately invalid manifest").ConfigureAwait(true);
        using var configuration = new ConfigurationManager();

        await SavaServiceConfiguration.LoadAsync(configuration, directory.FullName).ConfigureAwait(true);

        Assert.Equal("Gateway", configuration["Service:Role"]);
    }

    [Theory]
    [InlineData("--secrets-directory")]
    [InlineData("--secrets-file")]
    [InlineData("--urls")]
    [InlineData("--Sava:DefaultAccount")]
    public async Task CommandLineServiceConfigurationIsRefused(string argument)
    {
        var error = await Assert.ThrowsAsync<ArgumentException>(() =>
            SavaServiceConfiguration.CreateBuilderAsync([argument, "not-selected"])).ConfigureAwait(true);

        Assert.Contains("Supprocom.Secrets", error.Message, StringComparison.Ordinal);
        Assert.False(Directory.Exists(Path.Combine(directory.FullName, "data")));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("Application")]
    public void GatewayRoleMustBeExplicitAndMatched(string? role)
    {
        using var configuration = new ConfigurationManager();
        configuration.AddInMemoryCollection(new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["Service:Role"] = role,
        });

        var error = Assert.Throws<InvalidOperationException>(() =>
            SavaServiceConfiguration.RequireRole(configuration, "Gateway"));

        Assert.Contains("Service:Role", error.Message, StringComparison.Ordinal);
    }

    private SupprocomSecretFileStore CreateStore()
    {
        var options = new SupprocomSecretsOptions
        {
            EnvironmentName = "Production",
            FileOverridesProcessEnvironment = true,
        };
        options.File.Directory = directory.FullName;
        options.File.Protection = SecretFileProtection.InstallationBoundAesGcm;
        options.File.Import = SecretFileImport.None;
        options.File.Recovery = SecretFileRecovery.None;
        return new SupprocomSecretFileStore(options);
    }

    private async Task WriteDocumentAsync(string document)
    {
        var creation = new FileStreamOptions { Mode = FileMode.CreateNew, Access = FileAccess.Write };
        if (!OperatingSystem.IsWindows())
            creation.UnixCreateMode = UnixFileMode.UserRead | UnixFileMode.UserWrite;
        var file = new FileStream(Path.Combine(directory.FullName, ".env"), creation);
        await using var owned = file.ConfigureAwait(false);
        await file.WriteAsync(System.Text.Encoding.UTF8.GetBytes(document)).ConfigureAwait(true);
    }
}
