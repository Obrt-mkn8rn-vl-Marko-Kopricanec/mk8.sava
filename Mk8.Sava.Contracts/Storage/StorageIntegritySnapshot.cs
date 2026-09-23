namespace Mk8.Sava.Storage;

public sealed record StorageIntegritySnapshot(
    int ReachableChunks,
    int CheckedChunks,
    int VerifiedChunks,
    int CustomerKeyChunks,
    int MissingChunks,
    int CorruptChunks,
    bool Complete,
    DateTimeOffset CheckedAt)
{
    public static StorageIntegritySnapshot Pending { get; } =
        new(0, 0, 0, 0, 0, 0, false, DateTimeOffset.MinValue);

    public bool Healthy => MissingChunks == 0 && CorruptChunks == 0;
}
