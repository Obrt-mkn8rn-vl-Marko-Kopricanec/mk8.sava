namespace Mk8.Sava.Storage;

internal readonly record struct StagingReclamationBatch(int ExaminedEntries, int ReclaimedFiles, bool CycleCompleted);
