namespace Mk8.Sava.Configuration;

public sealed class GraphGroupResolutionOptions
{
    public bool Enabled { get; init; }
    public string? TenantId { get; init; }
    public MicrosoftGraphCloud Cloud { get; init; } = MicrosoftGraphCloud.Global;

    public Uri GraphEndpoint => Cloud switch
    {
        MicrosoftGraphCloud.Global => new("https://graph.microsoft.com", UriKind.Absolute),
        MicrosoftGraphCloud.UsGovernment => new("https://graph.microsoft.us", UriKind.Absolute),
        MicrosoftGraphCloud.UsGovernmentDod => new("https://dod-graph.microsoft.us", UriKind.Absolute),
        MicrosoftGraphCloud.China => new("https://microsoftgraph.chinacloudapi.cn", UriKind.Absolute),
        _ => throw new InvalidOperationException("Unsupported Microsoft Graph cloud.")
    };
}
