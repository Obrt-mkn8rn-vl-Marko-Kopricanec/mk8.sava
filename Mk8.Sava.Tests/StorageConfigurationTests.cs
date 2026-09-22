using System.ComponentModel.DataAnnotations;
using Mk8.Sava.Configuration;

namespace Mk8.Sava.Tests;

public sealed class StorageConfigurationTests
{
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
