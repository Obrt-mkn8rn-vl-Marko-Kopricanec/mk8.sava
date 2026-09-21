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
