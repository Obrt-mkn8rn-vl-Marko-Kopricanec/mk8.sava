using System.ComponentModel.DataAnnotations;

namespace Mk8.Sava.Configuration;

public sealed class SavaOptions : IValidatableObject
{
    public const string SectionName = "Sava";

    [Required]
    public string DataPath { get; init; } = "data";

    [Required]
    public string DefaultAccount { get; init; } = "devstoreaccount1";

    [Required]
    public Dictionary<string, string> Accounts { get; init; } = new(StringComparer.Ordinal);

    public bool AllowAnonymousPublicAccess { get; init; }
    public int MinimumChunkBytes { get; init; } = 64 * 1024;
    public int TargetChunkBytes { get; init; } = 256 * 1024;
    public int MaximumChunkBytes { get; init; } = 1024 * 1024;
    public int CompressionQuality { get; init; } = 5;
    public int CompressionMinimumSavingsBytes { get; init; } = 64;
    public bool EnableCrossAccountDeduplication { get; init; }
    public string? CrossAccountEncryptionKey { get; init; }
    public long MaximumRequestBodyBytes { get; init; } = 4L * 1024 * 1024 * 1024;
    public int SoftDeleteRetentionDays { get; init; } = 7;
    public BearerAuthenticationOptions BearerAuthentication { get; init; } = new();

    public IEnumerable<ValidationResult> Validate(ValidationContext validationContext)
    {
        if (Accounts.Count == 0)
            yield return new ValidationResult("At least one storage account is required.", [nameof(Accounts)]);

        if (!Accounts.ContainsKey(DefaultAccount))
            yield return new ValidationResult("DefaultAccount must identify a configured account.", [nameof(DefaultAccount)]);

        foreach (var (accountName, accountKey) in Accounts)
        {
            if (string.IsNullOrWhiteSpace(accountName))
                yield return new ValidationResult("Storage account names cannot be blank.", [nameof(Accounts)]);

            if (!TryDecodeKey(accountKey, out var keyBytes))
                yield return new ValidationResult($"The key for account '{accountName}' must be valid base64.", [nameof(Accounts)]);
            else if (keyBytes.Length < 32)
                yield return new ValidationResult($"The key for account '{accountName}' must contain at least 256 bits.", [nameof(Accounts)]);
        }

        if (EnableCrossAccountDeduplication &&
            (!TryDecodeKey(CrossAccountEncryptionKey ?? string.Empty, out var crossAccountKey) || crossAccountKey.Length < 32))
        {
            yield return new ValidationResult(
                "CrossAccountEncryptionKey must contain at least 256 bits when cross-account deduplication is enabled.",
                [nameof(CrossAccountEncryptionKey)]);
        }

        if (MinimumChunkBytes < 4 * 1024 ||
            TargetChunkBytes < MinimumChunkBytes ||
            MaximumChunkBytes < TargetChunkBytes)
        {
            yield return new ValidationResult(
                "Chunk sizes must satisfy 4 KiB <= minimum <= target <= maximum.",
                [nameof(MinimumChunkBytes), nameof(TargetChunkBytes), nameof(MaximumChunkBytes)]);
        }

        if (CompressionQuality is < 0 or > 11)
            yield return new ValidationResult("CompressionQuality must be between 0 and 11.", [nameof(CompressionQuality)]);

        if (CompressionMinimumSavingsBytes < 0)
            yield return new ValidationResult("CompressionMinimumSavingsBytes cannot be negative.", [nameof(CompressionMinimumSavingsBytes)]);

        if (MaximumRequestBodyBytes <= 0)
            yield return new ValidationResult("MaximumRequestBodyBytes must be positive.", [nameof(MaximumRequestBodyBytes)]);

        if (BearerAuthentication.Enabled)
        {
            if (BearerAuthentication.ValidAudiences.Count == 0)
                yield return new ValidationResult("At least one bearer-token audience is required.", [nameof(BearerAuthentication)]);
            if (BearerAuthentication.ValidIssuers.Count == 0)
                yield return new ValidationResult("At least one bearer-token issuer is required.", [nameof(BearerAuthentication)]);
            if (string.IsNullOrWhiteSpace(BearerAuthentication.Authority) &&
                string.IsNullOrWhiteSpace(BearerAuthentication.MetadataAddress) &&
                BearerAuthentication.SymmetricSigningKeys.Count == 0)
            {
                yield return new ValidationResult(
                    "Bearer authentication requires an authority, metadata address, or an explicitly configured signing key.",
                    [nameof(BearerAuthentication)]);
            }

            foreach (var (keyId, signingKey) in BearerAuthentication.SymmetricSigningKeys)
            {
                if (string.IsNullOrWhiteSpace(keyId) || !TryDecodeKey(signingKey, out var keyBytes) || keyBytes.Length < 32)
                    yield return new ValidationResult("Bearer symmetric signing keys must be named base64 values of at least 256 bits.", [nameof(BearerAuthentication)]);
            }
        }
    }

    private static bool TryDecodeKey(string value, out byte[] bytes)
    {
        try
        {
            bytes = Convert.FromBase64String(value);
            return true;
        }
        catch (FormatException)
        {
            bytes = [];
            return false;
        }
    }
}

public sealed class BearerAuthenticationOptions
{
    public bool Enabled { get; init; }
    public string? Authority { get; init; }
    public string? MetadataAddress { get; init; }
    public bool RequireHttpsMetadata { get; init; } = true;
    public List<string> ValidAudiences { get; init; } = ["https://storage.azure.com/"];
    public List<string> ValidIssuers { get; init; } = [];
    public Dictionary<string, string> SymmetricSigningKeys { get; init; } = new(StringComparer.Ordinal);
    public Dictionary<string, BearerPrincipalAccess> Principals { get; init; } = new(StringComparer.Ordinal);
    public Dictionary<string, string> RolePermissions { get; init; } = new(StringComparer.Ordinal);
}

public sealed class BearerPrincipalAccess
{
    public string Permissions { get; init; } = string.Empty;
    public List<string> Accounts { get; init; } = [];
    public List<string> Containers { get; init; } = [];
    public bool CanGenerateUserDelegationKey { get; init; }
}
