using System.IdentityModel.Tokens.Jwt;
using System.Net;
using System.Security.Claims;
using System.Text;
using Azure;
using Azure.Core;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.IdentityModel.Tokens;
using Mk8.Sava.Configuration;
using Mk8.Sava.Identity;
using Mk8.Sava.Protocol;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

public sealed class MicrosoftGraphGroupMembershipResolverTests
{
    private const string ReaderObjectId = "dd2af586-602b-4b90-9e7d-f32ffac9c88e";
    private const string ReaderGroupId = "69c6e8bb-a8f4-4fc9-a022-8149b983621a";

    [Fact]
    public async Task DirectSignedGroupsDoNotCallGraph()
    {
        using var handler = new GraphHandler(_ => throw new InvalidOperationException("Graph must not be called."));
        var credential = new GraphCredential();
        using var client = new HttpClient(handler);
        var resolver = CreateResolver(client, credential);
        var principal = Principal(new Claim("groups", ReaderGroupId), new Claim("groups", "not-a-guid"));

        var groups = await resolver.ResolveAsync(principal, ReaderObjectId, CancellationToken.None);

        Assert.Equal([ReaderGroupId], groups);
        Assert.Equal(0, handler.Calls);
        Assert.Equal(0, credential.Calls);
    }

    [Theory]
    [InlineData("hasgroups")]
    [InlineData("_claim_names")]
    public async Task OverageUsesTenantPinnedGraphAndIgnoresClaimSourceUrl(string indicator)
    {
        using var handler = new GraphHandler(request =>
        {
            Assert.Equal(HttpMethod.Post, request.Method);
            Assert.Equal(
                $"https://graph.microsoft.com/v1.0/directoryObjects/{ReaderObjectId}/getMemberGroups",
                request.RequestUri?.ToString());
            Assert.Equal("Bearer", request.Headers.Authorization?.Scheme);
            Assert.Equal("graph-test-token", request.Headers.Authorization?.Parameter);
            return JsonResponse(HttpStatusCode.OK, $"{{\"value\":[\"{ReaderGroupId}\"]}}");
        });
        var credential = new GraphCredential();
        using var client = new HttpClient(handler);
        var resolver = CreateResolver(client, credential);
        var claims = string.Equals(indicator, "hasgroups", StringComparison.Ordinal)
            ? new[] { new Claim("hasgroups", "true") }
            : new[]
            {
                new Claim("_claim_names", "{\"groups\":\"src1\"}"),
                new Claim("_claim_sources", "{\"src1\":{\"endpoint\":\"http://127.0.0.1/private\"}}")
            };

        var groups = await resolver.ResolveAsync(Principal(claims), ReaderObjectId, CancellationToken.None);

        Assert.Equal([ReaderGroupId], groups);
        Assert.Equal(1, handler.Calls);
        Assert.Equal(1, credential.Calls);
        Assert.NotNull(credential.Scopes);
        Assert.Equal(["https://graph.microsoft.com/.default"], credential.Scopes);
    }

    [Theory]
    [InlineData(MicrosoftGraphCloud.UsGovernment, "graph.microsoft.us")]
    [InlineData(MicrosoftGraphCloud.UsGovernmentDod, "dod-graph.microsoft.us")]
    [InlineData(MicrosoftGraphCloud.China, "microsoftgraph.chinacloudapi.cn")]
    public async Task NationalCloudOverageUsesOnlySelectedGraphHostAndAudience(
        MicrosoftGraphCloud cloud, string host)
    {
        using var handler = new GraphHandler(request =>
        {
            Assert.Equal(host, request.RequestUri?.Host);
            Assert.Equal($"/v1.0/directoryObjects/{ReaderObjectId}/getMemberGroups",
                request.RequestUri?.AbsolutePath);
            return JsonResponse(HttpStatusCode.OK, $"{{\"value\":[\"{ReaderGroupId}\"]}}");
        });
        var credential = new GraphCredential();
        using var client = new HttpClient(handler);
        var resolver = CreateResolver(client, credential, cloud: cloud);

        var groups = await resolver.ResolveAsync(
            Principal(new Claim("hasgroups", "true")), ReaderObjectId, CancellationToken.None);

        Assert.Equal([ReaderGroupId], groups);
        Assert.NotNull(credential.Scopes);
        Assert.Equal([$"https://{host}/.default"], credential.Scopes);
        Assert.Equal(1, handler.Calls);
    }

    [Theory]
    [InlineData(HttpStatusCode.Forbidden, "{\"error\":\"forbidden\"}")]
    [InlineData(HttpStatusCode.BadRequest, "{\"error\":{\"code\":\"InvalidRequest\"}}")]
    [InlineData(HttpStatusCode.OK, "{\"value\":[\"not-a-guid\"]}")]
    [InlineData(HttpStatusCode.OK, "{\"value\":null}")]
    [InlineData(HttpStatusCode.OK, "not-json")]
    public async Task FailedOrMalformedGraphLookupNeverGrantsGroups(HttpStatusCode status, string body)
    {
        using var handler = new GraphHandler(_ => JsonResponse(status, body));
        using var client = new HttpClient(handler);
        var resolver = CreateResolver(client, new GraphCredential());

        var error = await Assert.ThrowsAsync<AzureStorageException>(() => resolver.ResolveAsync(
            Principal(new Claim("hasgroups", "true")), ReaderObjectId, CancellationToken.None));

        Assert.Equal("AuthorizationFailure", error.ErrorCode);
        Assert.Equal(1, handler.Calls);
    }

    [Theory]
    [InlineData("#microsoft.graph.user", "users")]
    [InlineData("#microsoft.graph.servicePrincipal", "servicePrincipals")]
    public async Task GroupLimitFallsBackToValidatedTransitivePages(string objectType, string collection)
    {
        var membershipPath = $"/v1.0/{collection}/{ReaderObjectId}/transitiveMemberOf/microsoft.graph.group";
        var continuation = $"https://graph.microsoft.com{membershipPath}?%24skiptoken=page-two";
        using var handler = new GraphHandler(request =>
        {
            if (request.Method == HttpMethod.Post)
                return JsonResponse(HttpStatusCode.BadRequest,
                    "{\"error\":{\"code\":\"Directory_ResultSizeLimitExceeded\"}}");
            if (string.Equals(request.RequestUri?.AbsolutePath,
                    $"/v1.0/directoryObjects/{ReaderObjectId}", StringComparison.Ordinal))
                return JsonResponse(HttpStatusCode.OK, $"{{\"@odata.type\":\"{objectType}\"}}");

            Assert.Equal(membershipPath, request.RequestUri?.AbsolutePath);
            Assert.Equal("eventual", request.Headers.GetValues("ConsistencyLevel").Single());
            Assert.Equal("graph-test-token", request.Headers.Authorization?.Parameter);
            return request.RequestUri?.Query.Contains("skiptoken", StringComparison.Ordinal) == true
                ? JsonResponse(HttpStatusCode.OK,
                    $"{{\"value\":[{{\"id\":\"{ReaderGroupId}\",\"securityEnabled\":true}}]}}")
                : JsonResponse(HttpStatusCode.OK,
                    $"{{\"value\":[{{\"id\":\"{Guid.NewGuid():D}\",\"securityEnabled\":false}}]," +
                    $"\"@odata.nextLink\":\"{continuation}\"}}");
        });
        using var client = new HttpClient(handler);
        var resolver = CreateResolver(client, new GraphCredential());

        var groups = await resolver.ResolveAsync(
            Principal(new Claim("hasgroups", "true")), ReaderObjectId, CancellationToken.None);

        Assert.Equal([ReaderGroupId], groups);
        Assert.Equal(4, handler.Calls);
    }

    [Theory]
    [InlineData(MicrosoftGraphCloud.UsGovernment, "graph.microsoft.us", false)]
    [InlineData(MicrosoftGraphCloud.UsGovernmentDod, "dod-graph.microsoft.us", false)]
    [InlineData(MicrosoftGraphCloud.China, "microsoftgraph.chinacloudapi.cn", false)]
    [InlineData(MicrosoftGraphCloud.UsGovernment, "graph.microsoft.us", true)]
    [InlineData(MicrosoftGraphCloud.UsGovernmentDod, "dod-graph.microsoft.us", true)]
    [InlineData(MicrosoftGraphCloud.China, "microsoftgraph.chinacloudapi.cn", true)]
    public async Task NationalCloudPagingStaysWithinSelectedGraph(
        MicrosoftGraphCloud cloud, string host, bool crossCloud)
    {
        var membershipPath = $"/v1.0/users/{ReaderObjectId}/transitiveMemberOf/microsoft.graph.group";
        using var handler = new GraphHandler(request =>
        {
            Assert.Equal(host, request.RequestUri?.Host);
            if (request.Method == HttpMethod.Post)
                return JsonResponse(HttpStatusCode.BadRequest,
                    "{\"error\":{\"code\":\"Directory_ResultSizeLimitExceeded\"}}");
            if (string.Equals(request.RequestUri?.AbsolutePath,
                    $"/v1.0/directoryObjects/{ReaderObjectId}", StringComparison.Ordinal))
                return JsonResponse(HttpStatusCode.OK, "{\"@odata.type\":\"#microsoft.graph.user\"}");
            Assert.Equal(membershipPath, request.RequestUri?.AbsolutePath);
            if (request.RequestUri?.Query.Contains("skiptoken", StringComparison.Ordinal) == true)
                return JsonResponse(HttpStatusCode.OK,
                    $"{{\"value\":[{{\"id\":\"{ReaderGroupId}\",\"securityEnabled\":true}}]}}");
            var continuationHost = crossCloud ? "graph.microsoft.com" : host;
            return JsonResponse(HttpStatusCode.OK,
                $"{{\"value\":[],\"@odata.nextLink\":\"https://{continuationHost}" +
                $"{membershipPath}?%24skiptoken=page-two\"}}");
        });
        using var client = new HttpClient(handler);
        var resolver = CreateResolver(client, new GraphCredential(), cloud: cloud);

        if (crossCloud)
        {
            var error = await Assert.ThrowsAsync<AzureStorageException>(() => resolver.ResolveAsync(
                Principal(new Claim("hasgroups", "true")), ReaderObjectId, CancellationToken.None));
            Assert.Equal("AuthorizationFailure", error.ErrorCode);
            Assert.Equal(3, handler.Calls);
        }
        else
        {
            var groups = await resolver.ResolveAsync(
                Principal(new Claim("hasgroups", "true")), ReaderObjectId, CancellationToken.None);
            Assert.Equal([ReaderGroupId], groups);
            Assert.Equal(4, handler.Calls);
        }
    }

    [Theory]
    [InlineData("wrong-host")]
    [InlineData("wrong-scheme")]
    [InlineData("wrong-path")]
    [InlineData("repeated-page")]
    public async Task GroupLimitRejectsUntrustedOrCyclicNextLinks(string linkKind)
    {
        var path = $"/v1.0/users/{ReaderObjectId}/transitiveMemberOf/microsoft.graph.group";
        var nextLink = linkKind switch
        {
            "wrong-host" => $"https://example.com{path}?%24skiptoken=stolen",
            "wrong-scheme" => $"http://graph.microsoft.com{path}?%24skiptoken=stolen",
            "wrong-path" => $"https://graph.microsoft.com/v1.0/users/{ReaderObjectId}/messages",
            "repeated-page" => $"https://graph.microsoft.com{path}?%24skiptoken=loop",
            _ => throw new ArgumentOutOfRangeException(nameof(linkKind))
        };
        using var handler = new GraphHandler(request =>
        {
            if (request.Method == HttpMethod.Post)
                return JsonResponse(HttpStatusCode.BadRequest,
                    "{\"error\":{\"code\":\"Directory_ResultSizeLimitExceeded\"}}");
            if (string.Equals(request.RequestUri?.AbsolutePath,
                    $"/v1.0/directoryObjects/{ReaderObjectId}", StringComparison.Ordinal))
                return JsonResponse(HttpStatusCode.OK, "{\"@odata.type\":\"#microsoft.graph.user\"}");
            Assert.Equal(path, request.RequestUri?.AbsolutePath);
            return JsonResponse(HttpStatusCode.OK,
                $"{{\"value\":[{{\"id\":\"{ReaderGroupId}\",\"securityEnabled\":true}}]," +
                $"\"@odata.nextLink\":\"{nextLink}\"}}");
        });
        using var client = new HttpClient(handler);
        var resolver = CreateResolver(client, new GraphCredential());

        var error = await Assert.ThrowsAsync<AzureStorageException>(() => resolver.ResolveAsync(
            Principal(new Claim("hasgroups", "true")), ReaderObjectId, CancellationToken.None));

        Assert.Equal("AuthorizationFailure", error.ErrorCode);
        Assert.InRange(handler.Calls, 3, 4);
    }

    [Fact]
    public async Task OversizedGraphResponseFailsClosed()
    {
        using var handler = new GraphHandler(_ => JsonResponse(
            HttpStatusCode.OK, new string('x', 1024 * 1024 + 1)));
        using var client = new HttpClient(handler);
        var resolver = CreateResolver(client, new GraphCredential());

        var error = await Assert.ThrowsAsync<AzureStorageException>(() => resolver.ResolveAsync(
            Principal(new Claim("hasgroups", "true")), ReaderObjectId, CancellationToken.None));

        Assert.Equal("AuthorizationFailure", error.ErrorCode);
    }

    [Fact]
    public async Task WrongTenantAndDisabledResolutionNeverCallGraph()
    {
        using var handler = new GraphHandler(_ => throw new InvalidOperationException("Graph must not be called."));
        var credential = new GraphCredential();
        using var client = new HttpClient(handler);
        var wrongTenant = CreateResolver(client, credential);
        var disabled = CreateResolver(client, credential, enabled: false);
        var principal = new ClaimsPrincipal(new ClaimsIdentity(
            [
                new Claim("oid", ReaderObjectId),
                new Claim("tid", Guid.NewGuid().ToString("D")),
                new Claim("hasgroups", "true")
            ], "test"));

        var wrongTenantError = await Assert.ThrowsAsync<AzureStorageException>(() => wrongTenant.ResolveAsync(
            principal, ReaderObjectId, CancellationToken.None));
        var disabledError = await Assert.ThrowsAsync<AzureStorageException>(() => disabled.ResolveAsync(
            principal, ReaderObjectId, CancellationToken.None));

        Assert.Equal("AuthorizationFailure", wrongTenantError.ErrorCode);
        Assert.Equal("AuthorizationFailure", disabledError.ErrorCode);
        Assert.Equal(0, handler.Calls);
        Assert.Equal(0, credential.Calls);
    }

    [Theory]
    [InlineData(false, MicrosoftGraphCloud.Global, "graph.microsoft.com")]
    [InlineData(true, MicrosoftGraphCloud.Global, "graph.microsoft.com")]
    [InlineData(false, MicrosoftGraphCloud.UsGovernment, "graph.microsoft.us")]
    [InlineData(false, MicrosoftGraphCloud.UsGovernmentDod, "dod-graph.microsoft.us")]
    [InlineData(false, MicrosoftGraphCloud.China, "microsoftgraph.chinacloudapi.cn")]
    public async Task HnsBlobReadUsesResolvedOverageGroupThroughBearerAclFallback(
        bool distributedClaim, MicrosoftGraphCloud cloud, string host)
    {
        using var handler = new GraphHandler(request =>
        {
            Assert.Equal(host, request.RequestUri?.Host);
            return JsonResponse(HttpStatusCode.OK, $"{{\"value\":[\"{ReaderGroupId}\"]}}");
        });
        var credential = new GraphCredential();
        var configuration = new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:HierarchicalNamespaceEnabled"] = "true",
            ["Sava:BearerAuthentication:GraphGroupResolution:Enabled"] = "true",
            ["Sava:BearerAuthentication:GraphGroupResolution:TenantId"] = SavaWebApplicationFactory.TenantId,
            ["Sava:BearerAuthentication:GraphGroupResolution:Cloud"] = cloud.ToString()
        };
        var application = new SavaWebApplicationFactory(configuration, () => handler, credential);
        await using var applicationDisposal = application.ConfigureAwait(false);
        await application.InitializeAsync();
        var containerName = $"hns-group-overage-{Guid.NewGuid():N}";
        var endpoint = new Uri($"https://{SavaWebApplicationFactory.AccountName}.localhost");
        var sharedKey = new StorageSharedKeyCredential(
            SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey);
        var transport = new HttpClientTransport(application.Server.CreateHandler());
        var writer = new BlobServiceClient(endpoint, sharedKey, new BlobClientOptions
        {
            Transport = transport,
            Retry = { MaxRetries = 0 }
        });
        var container = writer.GetBlobContainerClient(containerName);
        await container.CreateAsync();
        await container.GetBlobClient("group.txt").UploadAsync(BinaryData.FromString("group-only"));
        await SeedGroupOnlyBlobAclAsync(application, containerName);

        var token = CreateOverageJwt(distributedClaim);
        var reader = new BlobServiceClient(endpoint, new GraphCredential(token), new BlobClientOptions
        {
            Transport = new HttpClientTransport(application.Server.CreateHandler()),
            Retry = { MaxRetries = 0 }
        });
        var downloaded = await reader.GetBlobContainerClient(containerName)
            .GetBlobClient("group.txt").DownloadContentAsync();

        Assert.Equal("group-only", downloaded.Value.Content.ToString());
        Assert.Equal(1, handler.Calls);
        Assert.NotNull(credential.Scopes);
        Assert.Equal([$"https://{host}/.default"], credential.Scopes);
    }

    private static Task SeedGroupOnlyBlobAclAsync(SavaWebApplicationFactory application, string containerName) =>
        application.Services.GetRequiredService<MetadataStore>().ApplyHierarchicalAclEntriesAsync(
        [
            new HierarchicalAclManifestEntry
            {
                Account = SavaWebApplicationFactory.AccountName,
                Container = containerName,
                Path = string.Empty,
                AccessAcl = $"user::rwx,group::---,group:{ReaderGroupId}:--x,mask::r-x,other::---"
            },
            new HierarchicalAclManifestEntry
            {
                Account = SavaWebApplicationFactory.AccountName,
                Container = containerName,
                Path = "group.txt",
                AccessAcl = $"user::rw-,group::---,group:{ReaderGroupId}:r--,mask::r--,other::---"
            }
        ], CancellationToken.None);

    private static MicrosoftGraphGroupMembershipResolver CreateResolver(
        HttpClient client, GraphCredential credential, bool enabled = true,
        MicrosoftGraphCloud cloud = MicrosoftGraphCloud.Global)
    {
        var options = Options.Create(new SavaOptions
        {
            BearerAuthentication = new BearerAuthenticationOptions
            {
                Enabled = true,
                GraphGroupResolution = new GraphGroupResolutionOptions
                {
                    Enabled = enabled,
                    TenantId = SavaWebApplicationFactory.TenantId,
                    Cloud = cloud
                }
            }
        });
        return new MicrosoftGraphGroupMembershipResolver(
            options, credential, client, NullLogger<MicrosoftGraphGroupMembershipResolver>.Instance);
    }

    private static ClaimsPrincipal Principal(params Claim[] extraClaims) => new(new ClaimsIdentity(
        [new Claim("oid", ReaderObjectId), new Claim("tid", SavaWebApplicationFactory.TenantId), .. extraClaims],
        "test"));

    private static string CreateOverageJwt(bool distributedClaim)
    {
        var key = new SymmetricSecurityKey(Convert.FromBase64String(SavaWebApplicationFactory.AccountKey))
        {
            KeyId = "test-key"
        };
        Claim overage = distributedClaim
            ? new("_claim_names", "{\"groups\":\"src1\"}")
            : new("hasgroups", "true");
        var token = new JwtSecurityToken(
            issuer: "https://issuer.mk8.test",
            audience: "https://storage.azure.com/",
            claims:
            [
                new Claim("oid", ReaderObjectId),
                new Claim("tid", SavaWebApplicationFactory.TenantId),
                overage
            ],
            notBefore: DateTime.UtcNow.AddMinutes(-1),
            expires: DateTime.UtcNow.AddMinutes(10),
            signingCredentials: new SigningCredentials(key, SecurityAlgorithms.HmacSha256));
        return new JwtSecurityTokenHandler().WriteToken(token);
    }

    private static HttpResponseMessage JsonResponse(HttpStatusCode status, string body) => new(status)
    {
        Content = new StringContent(body, Encoding.UTF8, "application/json")
    };

    private sealed class GraphHandler(Func<HttpRequestMessage, HttpResponseMessage> response) : HttpMessageHandler
    {
        public int Calls { get; private set; }

        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Calls++;
            return Task.FromResult(response(request));
        }
    }

    private sealed class GraphCredential(string token = "graph-test-token") : TokenCredential
    {
        public int Calls { get; private set; }
        public string[]? Scopes { get; private set; }

        public override AccessToken GetToken(TokenRequestContext requestContext, CancellationToken cancellationToken)
        {
            Calls++;
            Scopes = requestContext.Scopes;
            return new AccessToken(token, DateTimeOffset.UtcNow.AddMinutes(10));
        }

        public override ValueTask<AccessToken> GetTokenAsync(
            TokenRequestContext requestContext, CancellationToken cancellationToken) =>
            ValueTask.FromResult(GetToken(requestContext, cancellationToken));
    }
}
