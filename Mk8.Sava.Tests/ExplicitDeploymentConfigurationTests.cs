using System.ComponentModel.DataAnnotations;
using Microsoft.Extensions.Configuration;
using Mk8.Sava.Configuration;
using Mk8.Sava.Transport;

namespace Mk8.Sava.Tests;

public sealed class ExplicitDeploymentConfigurationTests
{
    [Fact]
    public void ConfiguredAccountsDoNotChooseAnImplicitDefaultAccount()
    {
        var options = new SavaOptions
        {
            Accounts = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                [SavaWebApplicationFactory.AccountName] = SavaWebApplicationFactory.AccountKey,
            },
        };

        Assert.Empty(options.DefaultAccount);
        var errors = new List<ValidationResult>();
        Assert.False(Validator.TryValidateObject(options, new ValidationContext(options), errors, validateAllProperties: true));
        Assert.Contains(errors, error => error.MemberNames.Contains(nameof(SavaOptions.DefaultAccount), StringComparer.Ordinal));
    }

    [Fact]
    public void ATransportCredentialDoesNotChooseAnImplicitEndpoint()
    {
        var options = new ApplicationTransportOptions { AccessKeyFile = "not-opened-by-validation" };

        Assert.Null(options.Endpoint);
        var errors = new List<ValidationResult>();
        Assert.False(Validator.TryValidateObject(options, new ValidationContext(options), errors, validateAllProperties: true));
        Assert.Contains(errors, error => error.MemberNames.Contains(nameof(ApplicationTransportOptions.Endpoint), StringComparer.Ordinal));
    }

    [Theory]
    [InlineData("tenantalpha")]
    [InlineData("tenantbeta42")]
    public void AccountAndEndpointAreBoundFromCallerConfiguration(string account)
    {
        // Reserved documentation authority and synthetic key; no network or site allocation.
        const string endpoint = "https://application.example.test:9443/internal/application";
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["Sava:DefaultAccount"] = account,
            [$"Sava:Accounts:{account}"] = SavaWebApplicationFactory.AccountKey,
            ["ApplicationTransport:Endpoint"] = endpoint,
            ["ApplicationTransport:AccessKeyFile"] = "not-opened-by-validation",
        }).Build();
        var storage = configuration.GetSection(SavaOptions.SectionName).Get<SavaOptions>();
        var transport = configuration.GetSection(ApplicationTransportOptions.SectionName).Get<ApplicationTransportOptions>();

        Assert.NotNull(storage);
        Assert.NotNull(transport);
        Assert.Equal(account, storage.DefaultAccount);
        Assert.Equal(new Uri(endpoint), transport.Endpoint);
        Validator.ValidateObject(storage, new ValidationContext(storage), validateAllProperties: true);
        Validator.ValidateObject(transport, new ValidationContext(transport), validateAllProperties: true);
    }
}
