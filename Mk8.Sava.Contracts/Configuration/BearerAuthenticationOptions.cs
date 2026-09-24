namespace Mk8.Sava.Configuration;

public sealed class BearerAuthenticationOptions
{
    public bool Enabled { get; init; }
    public string? Authority { get; init; }
    public string? MetadataAddress { get; init; }
    public bool RequireHttpsMetadata { get; init; } = true;
    public IList<string> ValidAudiences { get; init; } = ["https://storage.azure.com/"];
    public IList<string> ValidIssuers { get; init; } = [];
    public IDictionary<string, string> SymmetricSigningKeys { get; init; } =
        new Dictionary<string, string>(StringComparer.Ordinal);
    public IDictionary<string, BearerPrincipalAccess> Principals { get; init; } =
        new Dictionary<string, BearerPrincipalAccess>(StringComparer.Ordinal);
    public IDictionary<string, string> RolePermissions { get; init; } =
        new Dictionary<string, string>(StringComparer.Ordinal);
    public GraphGroupResolutionOptions GraphGroupResolution { get; init; } = new();
}
