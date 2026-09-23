namespace Mk8.Sava.Storage;

public sealed record ChunkRecompressionResult(
    int ExaminedChunks,
    int RecompressedChunks,
    long BytesSaved)
{
    public static ChunkRecompressionResult Skipped { get; } = new(0, 0, 0);
    public static ChunkRecompressionResult Examined { get; } = new(1, 0, 0);
}
