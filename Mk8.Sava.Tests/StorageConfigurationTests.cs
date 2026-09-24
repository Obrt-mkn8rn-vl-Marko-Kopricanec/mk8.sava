using System.ComponentModel.DataAnnotations;
using Mk8.Sava.Configuration;

namespace Mk8.Sava.Tests;

public sealed class StorageConfigurationTests
{
    [Fact]
    public void UnsupportedGraphCloudIsRejectedBeforeServingRequests()
    {
        var options = new SavaOptions
        {
            Accounts = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                [SavaWebApplicationFactory.AccountName] = SavaWebApplicationFactory.AccountKey
            },
            BearerAuthentication = new BearerAuthenticationOptions
            {
                GraphGroupResolution = new GraphGroupResolutionOptions
                {
                    Cloud = (MicrosoftGraphCloud)int.MaxValue
                }
            }
        };

        Assert.Contains(options.Validate(new ValidationContext(options)), error =>
            error.ErrorMessage?.Contains("supported Microsoft Graph cloud", StringComparison.Ordinal) == true);
    }

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
            error => error.MemberNames.Contains(nameof(SavaOptions.PhysicalUsageScanInterval), StringComparer.Ordinal));
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
            error => error.MemberNames.Contains(nameof(SavaOptions.PhysicalUsageEntriesPerMaintenancePass), StringComparer.Ordinal));
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
            error => error.MemberNames.Contains(nameof(SavaOptions.UrlTransferAllowedPrivateHosts), StringComparer.Ordinal));
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

        Assert.Contains(errors, error => error.MemberNames.Contains(nameof(SavaOptions.Accounts), StringComparer.Ordinal) &&
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

    [Theory]
    [InlineData(true, false)]
    [InlineData(false, true)]
    public void HnsRejectsUnsupportedVersioningAndChangeFeedCapabilities(
        bool versioningEnabled, bool changeFeedEnabled)
    {
        var options = new SavaOptions
        {
            Accounts = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                [SavaWebApplicationFactory.AccountName] = SavaWebApplicationFactory.AccountKey
            },
            AccountCapabilities = new Dictionary<string, StorageAccountCapabilities>(StringComparer.Ordinal)
            {
                [SavaWebApplicationFactory.AccountName] = new()
                {
                    HierarchicalNamespaceEnabled = true,
                    VersioningEnabled = versioningEnabled,
                    ChangeFeedEnabled = changeFeedEnabled
                }
            }
        };

        Assert.Contains(options.Validate(new ValidationContext(options)), error =>
            error.MemberNames.Contains(nameof(SavaOptions.AccountCapabilities), StringComparer.Ordinal) &&
            error.ErrorMessage!.Contains("hierarchical namespace", StringComparison.Ordinal));
    }

    [Fact]
    public void ImmutabilityAndExpirationCapabilityErrorsRetainTheirOrder()
    {
        var options = new SavaOptions
        {
            Accounts = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                [SavaWebApplicationFactory.AccountName] = SavaWebApplicationFactory.AccountKey
            },
            AccountCapabilities = new Dictionary<string, StorageAccountCapabilities>(StringComparer.Ordinal)
            {
                [SavaWebApplicationFactory.AccountName] = new()
                {
                    HierarchicalNamespaceEnabled = true,
                    LastAccessTimeTrackingEnabled = true,
                    ImmutableStorageWithVersioningEnabled = true,
                    ImmutableStorageWithVersioningContainers = new HashSet<string>(StringComparer.Ordinal) { "" },
                    SasExpirationPeriod = TimeSpan.Zero
                }
            }
        };

        var errors = options.Validate(new ValidationContext(options))
            .Where(error => error.MemberNames.Contains(nameof(SavaOptions.AccountCapabilities), StringComparer.Ordinal))
            .ToArray();
        Assert.Collection(errors,
            error => Assert.Contains("without blob versioning", error.ErrorMessage, StringComparison.Ordinal),
            error => Assert.Contains("hierarchical namespace", error.ErrorMessage, StringComparison.Ordinal),
            error => Assert.Contains("last-access-time tracking", error.ErrorMessage, StringComparison.Ordinal),
            error => Assert.Contains("blank immutable-storage", error.ErrorMessage, StringComparison.Ordinal),
            error => Assert.Contains("non-positive SAS expiration", error.ErrorMessage, StringComparison.Ordinal));
    }
}
