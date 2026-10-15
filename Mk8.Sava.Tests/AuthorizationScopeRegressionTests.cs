using System.Globalization;
using System.IdentityModel.Tokens.Jwt;
using System.Net;
using System.Net.Http.Headers;
using System.Security.Claims;
using System.Security.Cryptography;
using System.Text;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Sas;
using Microsoft.IdentityModel.Tokens;
using Microsoft.Extensions.DependencyInjection;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

public sealed class AuthorizationScopeRegressionTests
{
    private const string Payload = "original authorized content";
    private const string CollisionName = "authority-collision";
    private const string Subject = SavaWebApplicationFactory.DelegatorObjectId;
    private const string Issuer = "https://issuer.example.test";
    private const string Audience = "https://storage.azure.com/";

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task DeletedContainerRestoreRequiresBothSourceAndDestinationAuthority(bool grantSource, bool grantDestination)
    {
        const string sourceName = "private-restore-source";
        const string destinationName = "allowed-restore-target";
        var factory = CreateRestoreFactory(sourceName, destinationName, grantSource, grantDestination);
        await using var lifetime = factory.ConfigureAwait(false);
        using var sdkTransport = new HttpClientTransport(factory.Server.CreateHandler());
        var owner = CreateOwner(sdkTransport);
        var metadata = factory.Services.GetRequiredService<MetadataStore>();
        var properties = await metadata.GetServicePropertiesAsync(SavaWebApplicationFactory.AccountName,
            CancellationToken.None).ConfigureAwait(true);
        await metadata.PutServicePropertiesAsync(SavaWebApplicationFactory.AccountName, properties with
        {
            ContainerSoftDeleteEnabled = true,
            ContainerSoftDeleteRetentionDays = 7,
        }, CancellationToken.None).ConfigureAwait(true);
        var blob = await SeedBlobAsync(owner, sourceName).ConfigureAwait(true);
        await owner.GetBlobContainerClient(sourceName).DeleteAsync().ConfigureAwait(true);
        var deleted = Assert.IsType<ContainerRecord>(await metadata.GetContainerAsync(SavaWebApplicationFactory.AccountName,
            sourceName, includeDeleted: true, CancellationToken.None).ConfigureAwait(true));
        Assert.NotNull(deleted.DeletedVersion);
        using var client = new HttpClient(factory.Server.CreateHandler());
        using var request = new HttpRequestMessage(HttpMethod.Put,
            new Uri(owner.Uri + "/" + destinationName + "?restype=container&comp=undelete"))
        {
            Content = new ByteArrayContent([]),
        };
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", CreateToken());
        request.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
        request.Headers.TryAddWithoutValidation("x-ms-deleted-container-name", sourceName);
        request.Headers.TryAddWithoutValidation("x-ms-deleted-container-version", deleted.DeletedVersion);
        using var response = await client.SendAsync(request).ConfigureAwait(true);
        var allowed = grantSource && grantDestination;
        Assert.Equal(allowed ? HttpStatusCode.Created : HttpStatusCode.Forbidden, response.StatusCode);
        if (!allowed)
        {
            Assert.Equal(grantDestination ? "AuthorizationPermissionMismatch" : "AuthorizationFailure",
                Assert.Single(response.Headers.GetValues("x-ms-error-code")));
            var remaining = await metadata.GetContainerAsync(SavaWebApplicationFactory.AccountName,
                sourceName, includeDeleted: true, CancellationToken.None).ConfigureAwait(true);
            Assert.Equal(System.Text.Json.JsonSerializer.Serialize(deleted), System.Text.Json.JsonSerializer.Serialize(remaining));
            Assert.False((await owner.GetBlobContainerClient(destinationName).ExistsAsync().ConfigureAwait(true)).Value);
            // The refusal must not consume the deleted source/version or alter its content.
            await owner.UndeleteBlobContainerAsync(sourceName, deleted.DeletedVersion).ConfigureAwait(true);
            Assert.Equal(Payload, (await blob.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
        }
        else
        {
            Assert.Null(await metadata.GetContainerAsync(SavaWebApplicationFactory.AccountName,
                sourceName, includeDeleted: true, CancellationToken.None).ConfigureAwait(true));
            Assert.Equal(Payload, (await owner.GetBlobContainerClient(destinationName).GetBlobClient(blob.Name)
                .DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
        }
    }

    private static SavaWebApplicationFactory CreateRestoreFactory(string sourceName, string destinationName,
        bool grantSource, bool grantDestination)
    {
        var settings = new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["Sava:BearerAuthentication:ValidIssuers:0"] = Issuer,
            ["Sava:BearerAuthentication:ValidAudiences:0"] = Audience,
            [$"Sava:BearerAuthentication:Principals:{Subject}:Permissions"] = "rw",
            [$"Sava:BearerAuthentication:Principals:{Subject}:Containers:0"] = "unrelated-container",
        };
        if (grantSource)
            settings[$"Sava:BearerAuthentication:Principals:{Subject}:Containers:1"] = sourceName;
        if (grantDestination)
        {
            var index = grantSource ? "2" : "1";
            settings[$"Sava:BearerAuthentication:Principals:{Subject}:Containers:{index}"] = destinationName;
        }
        return new SavaWebApplicationFactory(TimeProvider.System, settings, disableMaintenance: true);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task LiteGetSignatureCannotAuthorizeDelete(bool dateAlsoPresent)
    {
        var factory = CreateFactory();
        await using var lifetime = factory.ConfigureAwait(false);
        using var sdkTransport = new HttpClientTransport(factory.Server.CreateHandler());
        var blob = await SeedBlobAsync(CreateOwner(sdkTransport), "lite-method").ConfigureAwait(true);
        using var client = new HttpClient(factory.Server.CreateHandler());
        using var request = new HttpRequestMessage(HttpMethod.Get, blob.Uri);
        SignDocumentedLite(request, dateAlsoPresent);
        using (var positive = await client.SendAsync(request).ConfigureAwait(true))
        {
            Assert.Equal(HttpStatusCode.OK, positive.StatusCode);
            Assert.Equal(Payload, await positive.Content.ReadAsStringAsync().ConfigureAwait(true));
        }
        using var altered = new HttpRequestMessage(HttpMethod.Delete, blob.Uri);
        foreach (var header in request.Headers)
            altered.Headers.TryAddWithoutValidation(header.Key, header.Value);
        using var denial = await client.SendAsync(altered).ConfigureAwait(true);

        Assert.Equal(HttpStatusCode.Forbidden, denial.StatusCode);
        Assert.Equal("AuthenticationFailed", Assert.Single(denial.Headers.GetValues("x-ms-error-code")));
        Assert.Equal(Payload, (await blob.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
    }

    [Fact]
    public async Task MethodFreeLegacyMacCannotDeleteAnOwnedBlob()
    {
        var factory = CreateFactory();
        await using var lifetime = factory.ConfigureAwait(false);
        using var sdkTransport = new HttpClientTransport(factory.Server.CreateHandler());
        var blob = await SeedBlobAsync(CreateOwner(sdkTransport), "lite-legacy").ConfigureAwait(true);
        using var client = new HttpClient(factory.Server.CreateHandler());
        using var request = new HttpRequestMessage(HttpMethod.Delete, blob.Uri);
        var date = DateTimeOffset.UtcNow.ToString("R", CultureInfo.InvariantCulture);
        request.Headers.TryAddWithoutValidation("x-ms-date", date);
        request.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
        // Deliberately reproduce the rejected method-free local format, NOT the
        // supported Lite signer or an independent Azure compatibility oracle.
        SetMac(request, "\nx-ms-date:" + date + "\nx-ms-version:2023-11-03\n/" +
            SavaWebApplicationFactory.AccountName + blob.Uri.AbsolutePath);
        using var response = await client.SendAsync(request).ConfigureAwait(true);

        Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
        Assert.Equal(Payload, (await blob.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
    }

    [Theory]
    [InlineData("none", HttpStatusCode.Created)]
    [InlineData("type", HttpStatusCode.Forbidden)]
    [InlineData("digest", HttpStatusCode.Forbidden)]
    [InlineData("body", HttpStatusCode.BadRequest)]
    public async Task LiteBindsEntityHeadersAndAnAdmittedDigestChecksTheBody(string alteration, HttpStatusCode expected)
    {
        var factory = CreateFactory();
        await using var lifetime = factory.ConfigureAwait(false);
        using var sdkTransport = new HttpClientTransport(factory.Server.CreateHandler());
        var blob = await SeedBlobAsync(CreateOwner(sdkTransport), "lite-entity").ConfigureAwait(true);
        var before = (await blob.GetPropertiesAsync().ConfigureAwait(true)).Value.ETag;
        using var client = new HttpClient(factory.Server.CreateHandler());
        using var request = new HttpRequestMessage(HttpMethod.Put, blob.Uri)
        {
            Content = new ByteArrayContent("replacement"u8.ToArray()),
        };
        request.Content.Headers.ContentType = new MediaTypeHeaderValue("text/plain");
        request.Content.Headers.ContentMD5 = AzureProtocolChecksum.Md5("replacement"u8);
        request.Headers.TryAddWithoutValidation("x-ms-blob-type", "BlockBlob");
        SignDocumentedLite(request, dateAlsoPresent: false, blockBlob: true);
        if (string.Equals(alteration, "type", StringComparison.Ordinal))
            request.Content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
        if (string.Equals(alteration, "digest", StringComparison.Ordinal))
            request.Content.Headers.ContentMD5 = AzureProtocolChecksum.Md5("wrong digest"u8);
        if (string.Equals(alteration, "body", StringComparison.Ordinal))
        {
            request.Content.Dispose();
            request.Content = new ByteArrayContent("other bytes"u8.ToArray());
            request.Content.Headers.ContentType = new MediaTypeHeaderValue("text/plain");
            request.Content.Headers.ContentMD5 = AzureProtocolChecksum.Md5("replacement"u8);
        }
        using var response = await client.SendAsync(request).ConfigureAwait(true);

        Assert.Equal(expected, response.StatusCode);
        Assert.Equal(expected == HttpStatusCode.Created ? "replacement" : Payload,
            (await blob.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
        if (expected != HttpStatusCode.Created)
            Assert.Equal(before, (await blob.GetPropertiesAsync().ConfigureAwait(true)).Value.ETag);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public async Task ImplicitRootBearerUsesRootScopeBeforeAuthorization(bool hostStyle, bool grantRoot)
    {
        var factory = CreateFactory(grantRoot ? "$root" : CollisionName);
        await using var lifetime = factory.ConfigureAwait(false);
        using var sdkTransport = new HttpClientTransport(factory.Server.CreateHandler());
        var owner = CreateOwner(sdkTransport);
        var root = owner.GetBlobContainerClient("$root");
        await root.CreateAsync().ConfigureAwait(true);
        var blob = root.GetBlobClient(CollisionName);
        await blob.UploadAsync(BinaryData.FromString(Payload)).ConfigureAwait(true);
        using var client = new HttpClient(factory.Server.CreateHandler());
        using var request = new HttpRequestMessage(HttpMethod.Get, RootAlias(hostStyle));
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", CreateToken());
        request.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
        using var response = await client.SendAsync(request).ConfigureAwait(true);

        Assert.Equal(grantRoot ? HttpStatusCode.OK : HttpStatusCode.Forbidden, response.StatusCode);
        if (grantRoot)
            Assert.Equal(Payload, await response.Content.ReadAsStringAsync().ConfigureAwait(true));
        else
            Assert.Null(response.Headers.ETag);
        Assert.Equal(Payload, (await blob.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ImplicitAndExplicitRootAddressesShareTheSignedLogicalResource(bool hostStyle)
    {
        var factory = CreateFactory();
        await using var lifetime = factory.ConfigureAwait(false);
        using var sdkTransport = new HttpClientTransport(factory.Server.CreateHandler());
        var owner = CreateOwner(sdkTransport);
        var root = owner.GetBlobContainerClient("$root");
        await root.CreateAsync().ConfigureAwait(true);
        var blob = root.GetBlobClient(CollisionName);
        await blob.UploadAsync(BinaryData.FromString(Payload)).ConfigureAwait(true);
        var other = await SeedBlobAsync(owner, CollisionName).ConfigureAwait(true);
        using var client = new HttpClient(factory.Server.CreateHandler());
        foreach (var source in new[] { blob, other })
        {
            var signed = source.GenerateSasUri(BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddMinutes(5));
            using var response = await client.GetAsync(new Uri(RootAlias(hostStyle) + signed.Query)).ConfigureAwait(true);
            Assert.Equal(ReferenceEquals(source, blob) ? HttpStatusCode.OK : HttpStatusCode.Forbidden, response.StatusCode);
            if (ReferenceEquals(source, blob))
                Assert.Equal(Payload, await response.Content.ReadAsStringAsync().ConfigureAwait(true));
        }
        Assert.Equal(Payload, (await blob.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
    }

    [Theory]
    [InlineData("restricted", "list")]
    [InlineData("restricted", "properties")]
    [InlineData("restricted", "blobs")]
    [InlineData("empty", "list")]
    [InlineData("empty", "properties")]
    [InlineData("empty", "blobs")]
    [InlineData("wildcard", "list")]
    [InlineData("wildcard", "properties")]
    [InlineData("wildcard", "blobs")]
    [InlineData("role", "list")]
    [InlineData("role", "properties")]
    [InlineData("role", "blobs")]
    public async Task ServiceOperationsRequireAccountWideBearerAuthority(string scope, string operation)
    {
        var factory = CreateFactory(string.Equals(scope, "wildcard", StringComparison.Ordinal) ? "*" :
            string.Equals(scope, "empty", StringComparison.Ordinal) ? null : CollisionName, scope);
        await using var lifetime = factory.ConfigureAwait(false);
        using var sdkTransport = new HttpClientTransport(factory.Server.CreateHandler());
        var owner = CreateOwner(sdkTransport);
        var blob = await SeedBlobAsync(owner, "account-private").ConfigureAwait(true);
        await blob.SetTagsAsync(new Dictionary<string, string>(StringComparer.Ordinal) { ["purpose"] = "fixture" }).ConfigureAwait(true);
        var versionBefore = (await owner.GetPropertiesAsync().ConfigureAwait(true)).Value.DefaultServiceVersion;
        var query = "?comp=" + operation;
        if (string.Equals(operation, "blobs", StringComparison.Ordinal))
            query += "&where=" + Uri.EscapeDataString("\"purpose\" = 'fixture'");
        using var client = new HttpClient(factory.Server.CreateHandler());
        using var request = new HttpRequestMessage(string.Equals(operation, "properties", StringComparison.Ordinal) ? HttpMethod.Put : HttpMethod.Get,
            new Uri(owner.Uri + query));
        if (request.Method == HttpMethod.Put)
            request.Content = new StringContent("<StorageServiceProperties><DefaultServiceVersion>2023-11-03</DefaultServiceVersion></StorageServiceProperties>");
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", CreateToken(string.Equals(scope, "role", StringComparison.Ordinal)));
        request.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
        using var response = await client.SendAsync(request).ConfigureAwait(true);
        var restricted = string.Equals(scope, "restricted", StringComparison.Ordinal);

        Assert.Equal(restricted ? HttpStatusCode.Forbidden : request.Method == HttpMethod.Put ? HttpStatusCode.Accepted : HttpStatusCode.OK,
            response.StatusCode);
        Assert.Equal(restricted ? versionBefore : request.Method == HttpMethod.Put ? "2023-11-03" : versionBefore,
            (await owner.GetPropertiesAsync().ConfigureAwait(true)).Value.DefaultServiceVersion);
        Assert.Equal(Payload, (await blob.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
        if (restricted)
            Assert.DoesNotContain("account-private", await response.Content.ReadAsStringAsync().ConfigureAwait(true), StringComparison.Ordinal);
    }

    private static SavaWebApplicationFactory CreateFactory(string? container = null, string? scope = null)
    {
        var settings = new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["Sava:BearerAuthentication:Enabled"] = "true",
            ["Sava:BearerAuthentication:ValidIssuers:0"] = Issuer,
            ["Sava:BearerAuthentication:ValidAudiences:0"] = Audience,
            ["Sava:BearerAuthentication:SymmetricSigningKeys:fixture"] = SavaWebApplicationFactory.AccountKey,
            [$"Sava:BearerAuthentication:Principals:{Subject}:Accounts:0"] = SavaWebApplicationFactory.AccountName,
            [$"Sava:BearerAuthentication:Principals:{Subject}:Permissions"] = "rlfw",
        };
        if (container is not null)
            settings[$"Sava:BearerAuthentication:Principals:{Subject}:Containers:0"] = container;
        if (string.Equals(scope, "role", StringComparison.Ordinal))
            settings["Sava:BearerAuthentication:RolePermissions:broad"] = "rlfw";
        return new SavaWebApplicationFactory(TimeProvider.System, settings, disableMaintenance: true);
    }

    private static string CreateToken(bool role = false)
    {
        var claims = new List<Claim> { new("oid", Subject), new("tid", SavaWebApplicationFactory.TenantId) };
        if (role)
            claims.Add(new Claim("roles", "broad"));
        var token = new JwtSecurityToken(Issuer, Audience, claims,
            DateTime.UtcNow.AddMinutes(-1), DateTime.UtcNow.AddMinutes(5),
            new SigningCredentials(new SymmetricSecurityKey(Convert.FromBase64String(SavaWebApplicationFactory.AccountKey)), SecurityAlgorithms.HmacSha256));
        return new JwtSecurityTokenHandler().WriteToken(token);
    }

    private static BlobServiceClient CreateOwner(HttpClientTransport transport) =>
        new(new Uri($"http://127.0.0.1/{SavaWebApplicationFactory.AccountName}"),
            new StorageSharedKeyCredential(SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey),
            new BlobClientOptions { Transport = transport, Retry = { MaxRetries = 0 } });

    private static async Task<BlobClient> SeedBlobAsync(BlobServiceClient owner, string name)
    {
        var container = owner.GetBlobContainerClient(name);
        await container.CreateAsync().ConfigureAwait(false);
        var blob = container.GetBlobClient("present.bin");
        await blob.UploadAsync(BinaryData.FromString(Payload)).ConfigureAwait(false);
        return blob;
    }

    private static Uri RootAlias(bool hostStyle) => hostStyle
        ? new Uri($"http://{SavaWebApplicationFactory.AccountName}.localhost/{CollisionName}")
        : new Uri($"http://127.0.0.1/{SavaWebApplicationFactory.AccountName}/{CollisionName}");

    private static void SignDocumentedLite(HttpRequestMessage request, bool dateAlsoPresent, bool blockBlob = false)
    {
        var uri = request.RequestUri ?? throw new InvalidOperationException("The signature fixture requires a URI.");
        var date = DateTimeOffset.UtcNow.ToString("R", CultureInfo.InvariantCulture);
        request.Headers.TryAddWithoutValidation("x-ms-date", date);
        request.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
        if (dateAlsoPresent)
            request.Headers.Date = DateTimeOffset.UtcNow;
        // This finite independent fixture string follows Microsoft's published
        // Blob Lite format, not the production signer or legacy SDK-test helper.
        var canonicalHeaders = (blockBlob ? "x-ms-blob-type:BlockBlob\n" : string.Empty) +
            "x-ms-date:" + date + "\nx-ms-version:2023-11-03\n";
        SetMac(request, request.Method.Method + "\n" +
            (request.Content?.Headers.ContentMD5 is { } digest ? Convert.ToBase64String(digest) : string.Empty) + "\n" +
            (request.Content?.Headers.ContentType?.ToString() ?? string.Empty) + "\n\n" + canonicalHeaders +
            "/" + SavaWebApplicationFactory.AccountName + uri.AbsolutePath);
    }

    private static void SetMac(HttpRequestMessage request, string input)
    {
        using var hash = new HMACSHA256(Convert.FromBase64String(SavaWebApplicationFactory.AccountKey));
        request.Headers.Authorization = new AuthenticationHeaderValue("SharedKeyLite",
            SavaWebApplicationFactory.AccountName + ":" + Convert.ToBase64String(hash.ComputeHash(Encoding.UTF8.GetBytes(input))));
    }
}
