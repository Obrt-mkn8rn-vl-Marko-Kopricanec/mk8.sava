namespace Mk8.Sava.Configuration;

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
