using System.ComponentModel.DataAnnotations;
using Mk8.Sava.Configuration;

namespace Mk8.Sava.Tests;

public sealed class StorageConfigurationTests
{
    [Theory]
    [InlineData("00:00:00")]
    [InlineData("-00:00:01")]
    public void PhysicalUsageScanIntervalMustBePositive(string interval)
    {
        var options = new SavaOptions
        {
            Accounts = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["devstoreaccount1"] = SavaWebApplicationFactory.AccountKey
            },
            PhysicalUsageScanInterval = TimeSpan.Parse(interval, System.Globalization.CultureInfo.InvariantCulture)
        };

        Assert.Contains(
            options.Validate(new ValidationContext(options)),
            error => error.MemberNames.Contains(nameof(SavaOptions.PhysicalUsageScanInterval)));
    }

    [Fact]
    public void PhysicalUsageMaintenanceBudgetMustBePositive()
    {
        var options = new SavaOptions
        {
            Accounts = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["devstoreaccount1"] = SavaWebApplicationFactory.AccountKey
            },
            PhysicalUsageEntriesPerMaintenancePass = 0
        };

        Assert.Contains(
            options.Validate(new ValidationContext(options)),
            error => error.MemberNames.Contains(nameof(SavaOptions.PhysicalUsageEntriesPerMaintenancePass)));
    }

    [Theory]
    [InlineData("")]
    [InlineData(" localhost ")]
    [InlineData("https://example.com")]
    [InlineData("example.com:443")]
    [InlineData("*.example.com")]
    public void PrivateUrlSourceExceptionsRequireExactHostNames(string host)
    {
        var options = new SavaOptions
        {
            Accounts = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["devstoreaccount1"] = SavaWebApplicationFactory.AccountKey
            },
            UrlTransferAllowedPrivateHosts = [host]
        };

        Assert.Contains(
            options.Validate(new ValidationContext(options)),
            error => error.MemberNames.Contains(nameof(SavaOptions.UrlTransferAllowedPrivateHosts)));
    }

    [Theory]
    [InlineData("ab")]
    [InlineData("abcdefghijklmnopqrstuvwxy")]
    [InlineData("Uppercase")]
    [InlineData("with-dash")]
    [InlineData("../outside")]
    [InlineData("unicodeé")]
    public void AccountNamesOutsideAzureRulesAreRejected(string name)
    {
        var options = new SavaOptions
        {
            DefaultAccount = name,
            Accounts = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                [name] = SavaWebApplicationFactory.AccountKey
            }
        };

        var errors = options.Validate(new ValidationContext(options)).ToArray();

        Assert.Contains(errors, error => error.MemberNames.Contains(nameof(SavaOptions.Accounts)) &&
                                         error.ErrorMessage!.Contains("Storage account name", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData("abc")]
    [InlineData("abc123")]
    [InlineData("abcdefghijklmnopqrstuvwx")]
    public void AzureAccountNamesAreAccepted(string name)
    {
        var options = new SavaOptions
        {
            DefaultAccount = name,
            Accounts = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                [name] = SavaWebApplicationFactory.AccountKey
            }
        };

        Assert.Empty(options.Validate(new ValidationContext(options)));
    }
}
