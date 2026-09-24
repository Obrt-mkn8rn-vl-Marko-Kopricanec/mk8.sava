using System.Net.Http.Headers;
using System.Security.Claims;
using System.Text;
using System.Text.Json;
using Azure.Core;
using Azure.Identity;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Mk8.Sava.Configuration;
using Mk8.Sava.Protocol;

namespace Mk8.Sava.Identity;

internal sealed partial class MicrosoftGraphGroupMembershipResolver(
    IOptions<SavaOptions> options,
    TokenCredential credential,
    HttpClient client,
    ILogger<MicrosoftGraphGroupMembershipResolver> logger) : IGroupMembershipResolver
{
    private static readonly TokenRequestContext GraphTokenRequest =
        new(["https://graph.microsoft.com/.default"]);
    private const int MaximumResponseBytes = 1024 * 1024;
    private const int MaximumGroups = 11_000;

    public async Task<HashSet<string>> ResolveAsync(
        ClaimsPrincipal principal,
        string objectId,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(principal);
        if (!HasGroupOverage(principal))
        {
            return principal.FindAll("groups")
                .Select(claim => claim.Value)
                .Where(value => Guid.TryParse(value, out _))
                .ToHashSet(StringComparer.OrdinalIgnoreCase);
        }

        var configuration = options.Value.BearerAuthentication.GraphGroupResolution;
        if (!configuration.Enabled ||
            !Guid.TryParse(objectId, out var objectGuid) ||
            !Guid.TryParse(configuration.TenantId, out var configuredTenant) ||
            !Guid.TryParse(principal.FindFirst("tid")?.Value, out var tokenTenant) ||
            configuredTenant != tokenTenant)
        {
            throw AzureStorageException.AuthorizationFailure();
        }

        try
        {
            return await FetchGroupsAsync(objectGuid, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception error) when (error is AuthenticationFailedException or CredentialUnavailableException or
                                      HttpRequestException or JsonException or InvalidOperationException or TaskCanceledException &&
                                      !cancellationToken.IsCancellationRequested)
        {
            GraphLookupFailed(logger, error);
            throw AzureStorageException.AuthorizationFailure();
        }
    }

    private async Task<HashSet<string>> FetchGroupsAsync(Guid objectId, CancellationToken cancellationToken)
    {
        var token = await credential.GetTokenAsync(GraphTokenRequest, cancellationToken).ConfigureAwait(false);
        using var request = new HttpRequestMessage(
            HttpMethod.Post,
            $"https://graph.microsoft.com/v1.0/directoryObjects/{objectId:D}/getMemberGroups");
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token.Token);
        request.Content = new StringContent("{\"securityEnabledOnly\":true}", Encoding.UTF8, "application/json");
        using var response = await client.SendAsync(
            request, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);
        if (!response.IsSuccessStatusCode)
        {
            GraphLookupRejected(logger, (int)response.StatusCode);
            throw AzureStorageException.AuthorizationFailure();
        }

        await response.Content.LoadIntoBufferAsync(MaximumResponseBytes, cancellationToken).ConfigureAwait(false);
        using var stream = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
        using var document = await JsonDocument.ParseAsync(stream, cancellationToken: cancellationToken)
            .ConfigureAwait(false);
        return ParseGroups(document);
    }

    private static HashSet<string> ParseGroups(JsonDocument document)
    {
        if (!document.RootElement.TryGetProperty("value", out var values) ||
            values.ValueKind != JsonValueKind.Array ||
            values.GetArrayLength() > MaximumGroups)
        {
            throw AzureStorageException.AuthorizationFailure();
        }

        var groups = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var value in values.EnumerateArray())
        {
            if (value.ValueKind != JsonValueKind.String ||
                !Guid.TryParse(value.GetString(), out var groupId))
            {
                throw AzureStorageException.AuthorizationFailure();
            }
            groups.Add(groupId.ToString("D"));
        }
        return groups;
    }

    [LoggerMessage(EventId = 1301, Level = LogLevel.Warning,
        Message = "Microsoft Graph group lookup returned HTTP {StatusCode}.")]
    private static partial void GraphLookupRejected(ILogger logger, int statusCode);

    [LoggerMessage(EventId = 1302, Level = LogLevel.Warning,
        Message = "Microsoft Graph group lookup failed.")]
    private static partial void GraphLookupFailed(ILogger logger, Exception exception);

    private static bool HasGroupOverage(ClaimsPrincipal principal)
    {
        if (string.Equals(principal.FindFirst("hasgroups")?.Value, "true", StringComparison.OrdinalIgnoreCase))
            return true;
        var names = principal.FindFirst("_claim_names")?.Value;
        if (names is null)
            return false;
        try
        {
            using var document = JsonDocument.Parse(names);
            return document.RootElement.ValueKind == JsonValueKind.Object &&
                   document.RootElement.TryGetProperty("groups", out _);
        }
        catch (JsonException)
        {
            return false;
        }
    }
}
