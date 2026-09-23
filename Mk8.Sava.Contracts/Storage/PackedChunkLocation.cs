namespace Mk8.Sava.Storage;

internal sealed record PackedChunkLocation(
    string ChunkId,
    string PackId,
    long RecordOffset,
    int RecordLength,
    long PayloadOffset,
    int PayloadLength);
