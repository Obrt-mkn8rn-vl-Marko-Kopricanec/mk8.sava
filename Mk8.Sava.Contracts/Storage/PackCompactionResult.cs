namespace Mk8.Sava.Storage;

internal sealed record PackCompactionResult(
    int ExaminedPacks,
    int CompactedPacks,
    int ReclaimedRecords,
    long BytesSaved)
{
    public static PackCompactionResult Skipped { get; } = new(0, 0, 0, 0);
}
