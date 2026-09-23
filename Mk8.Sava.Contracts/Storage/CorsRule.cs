namespace Mk8.Sava.Storage;

public sealed record CorsRule
{
    public required string AllowedOrigins { get; init; }
    public required string AllowedMethods { get; init; }
    public required string AllowedHeaders { get; init; }
    public required string ExposedHeaders { get; init; }
    public required int MaxAgeInSeconds { get; init; }
}
