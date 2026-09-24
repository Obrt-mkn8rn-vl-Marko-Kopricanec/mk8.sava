using System.Net.Http.Headers;
using System.Net;
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
    private const int MaximumPagedGroups = 100_000;
    private const int MaximumPages = 128;

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
        if (response.StatusCode == HttpStatusCode.BadRequest)
        {
            using var error = await ReadDocumentAsync(response, cancellationToken).ConfigureAwait(false);
            if (IsGroupLimitError(error))
                return await FetchPagedGroupsAsync(objectId, token.Token, cancellationToken).ConfigureAwait(false);
        }
        if (!response.IsSuccessStatusCode)
        {
            GraphLookupRejected(logger, (int)response.StatusCode);
            throw AzureStorageException.AuthorizationFailure();
        }

        using var document = await ReadDocumentAsync(response, cancellationToken).ConfigureAwait(false);
        return ParseGroups(document);
    }

    private async Task<HashSet<string>> FetchPagedGroupsAsync(
        Guid objectId, string token, CancellationToken cancellationToken)
    {
        var kind = await GetDirectoryObjectKindAsync(objectId, token, cancellationToken).ConfigureAwait(false);
        var path = $"/v1.0/{kind}/{objectId:D}/transitiveMemberOf/microsoft.graph.group";
        var next = new Uri($"https://graph.microsoft.com{path}?%24select=id%2CsecurityEnabled&%24top=999&%24count=true");
        var seen = new HashSet<string>(StringComparer.Ordinal);
        var groups = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        for (var page = 0; page < MaximumPages; page++)
        {
            if (!IsSafePageUrl(next, path) || !seen.Add(next.AbsoluteUri))
                throw AzureStorageException.AuthorizationFailure();
            using var document = await GetGraphPageAsync(next, token, cancellationToken).ConfigureAwait(false);
            AddPageGroups(document, groups);
            if (groups.Count > MaximumPagedGroups)
                throw AzureStorageException.AuthorizationFailure();
            if (!document.RootElement.TryGetProperty("@odata.nextLink", out var link))
                return groups;
            if (link.ValueKind != JsonValueKind.String ||
                !Uri.TryCreate(link.GetString(), UriKind.Absolute, out next))
            {
                throw AzureStorageException.AuthorizationFailure();
            }
        }
        throw AzureStorageException.AuthorizationFailure();
    }

    private async Task<string> GetDirectoryObjectKindAsync(
        Guid objectId, string token, CancellationToken cancellationToken)
    {
        var uri = new Uri($"https://graph.microsoft.com/v1.0/directoryObjects/{objectId:D}");
        using var document = await GetGraphPageAsync(uri, token, cancellationToken, consistency: false)
            .ConfigureAwait(false);
        if (!document.RootElement.TryGetProperty("@odata.type", out var type) ||
            type.ValueKind != JsonValueKind.String)
        {
            throw AzureStorageException.AuthorizationFailure();
        }
        return type.GetString() switch
        {
            "#microsoft.graph.user" => "users",
            "#microsoft.graph.servicePrincipal" => "servicePrincipals",
            _ => throw AzureStorageException.AuthorizationFailure()
        };
    }

    private async Task<JsonDocument> GetGraphPageAsync(
        Uri uri, string token, CancellationToken cancellationToken, bool consistency = true)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, uri);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
        if (consistency)
            request.Headers.Add("ConsistencyLevel", "eventual");
        using var response = await client.SendAsync(
            request, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);
        if (!response.IsSuccessStatusCode)
        {
            GraphLookupRejected(logger, (int)response.StatusCode);
            throw AzureStorageException.AuthorizationFailure();
        }
        return await ReadDocumentAsync(response, cancellationToken).ConfigureAwait(false);
    }

    private static void AddPageGroups(JsonDocument document, HashSet<string> groups)
    {
        if (!document.RootElement.TryGetProperty("value", out var values) ||
            values.ValueKind != JsonValueKind.Array)
        {
            throw AzureStorageException.AuthorizationFailure();
        }
        foreach (var value in values.EnumerateArray())
        {
            if (value.ValueKind != JsonValueKind.Object ||
                !value.TryGetProperty("id", out var id) ||
                id.ValueKind != JsonValueKind.String ||
                !Guid.TryParse(id.GetString(), out var groupId) ||
                !value.TryGetProperty("securityEnabled", out var securityEnabled) ||
                securityEnabled.ValueKind is not (JsonValueKind.True or JsonValueKind.False))
            {
                throw AzureStorageException.AuthorizationFailure();
            }
            if (securityEnabled.ValueKind == JsonValueKind.True)
                groups.Add(groupId.ToString("D"));
        }
    }

    private static bool IsSafePageUrl(Uri uri, string path) =>
        uri.Scheme == Uri.UriSchemeHttps &&
        uri.Host.Equals("graph.microsoft.com", StringComparison.OrdinalIgnoreCase) &&
        uri.IsDefaultPort &&
        uri.UserInfo.Length == 0 &&
        uri.Fragment.Length == 0 &&
        uri.AbsolutePath.Equals(path, StringComparison.Ordinal) &&
        uri.AbsoluteUri.Length <= 4096;

    private static bool IsGroupLimitError(JsonDocument document) =>
        document.RootElement.TryGetProperty("error", out var error) &&
        error.ValueKind == JsonValueKind.Object &&
        error.TryGetProperty("code", out var code) &&
        code.ValueKind == JsonValueKind.String &&
        code.GetString() == "Directory_ResultSizeLimitExceeded";

    private static async Task<JsonDocument> ReadDocumentAsync(
        HttpResponseMessage response, CancellationToken cancellationToken)
    {
        await response.Content.LoadIntoBufferAsync(MaximumResponseBytes, cancellationToken).ConfigureAwait(false);
        using var stream = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
        return await JsonDocument.ParseAsync(stream, cancellationToken: cancellationToken).ConfigureAwait(false);
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
