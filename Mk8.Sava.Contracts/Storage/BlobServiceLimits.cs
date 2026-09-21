namespace Mk8.Sava.Storage;

internal static class BlobServiceLimits
{
    public const int MaximumCommittedBlockCount = 50_000;
    public const int MaximumUncommittedBlockCount = 100_000;
    public const int MaximumBlockIdBytes = 64;
}
