using System.Diagnostics;
using System.Globalization;
using System.IdentityModel.Tokens.Jwt;
using System.Net;
using System.Security.Claims;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.Json;
using Azure;
using Azure.Core;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Sas;
using Microsoft.IdentityModel.Tokens;
using Mk8.Sava.Application;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

[Trait("Category", "SplitProcess")]
public sealed class SplitProcessSecurityTests
{
    private const string IssuerId = "bd3fa379-e71d-4883-a2d7-4a6b2bbfaea2";
    private const string EndUserId = "564893d2-6eb4-49c7-bab5-c8a3fb084449";
    private const string ReaderRole = "BlobReader";

    [Fact]
    public async Task CustomerProvidedKeysCrossTheTransportAndRemainRequiredAfterApplicationRestart()
    {
        var host = await SplitProcessHost.StartAsync().ConfigureAwait(true);
        await using var lifetime = host.ConfigureAwait(false);
        using var https = await HttpsGateway.StartAsync(host).ConfigureAwait(true);
        var container = host.Client.GetBlobContainerClient("process-customer-key");
        await container.CreateAsync().ConfigureAwait(true);
        var key = RandomNumberGenerator.GetBytes(32);
        var wrongKey = RandomNumberGenerator.GetBytes(32);
        try
        {
            var bytes = RandomNumberGenerator.GetBytes(131073);
            var keyed = CreateEncryptedClient(host, key).GetBlobContainerClient(container.Name).GetBlockBlobClient("keyed");
            using (var source = new MemoryStream(bytes, writable: false))
                await keyed.UploadAsync(source).ConfigureAwait(true);
            var properties = (await keyed.GetPropertiesAsync().ConfigureAwait(true)).Value;
            Assert.Equal(Convert.ToBase64String(SHA256.HashData(key)), properties.EncryptionKeySha256, StringComparer.Ordinal);
            await AssertEncryptedReadsAsync(keyed, bytes).ConfigureAwait(true);
            var snapshot = (await keyed.CreateSnapshotAsync().ConfigureAwait(true)).Value.Snapshot;
            await AssertCustomerKeyDenialsAsync(host, container.Name, keyed.Name, wrongKey).ConfigureAwait(true);
            await AssertCustomerKeyStagedBlocksAsync(host, container.Name, key).ConfigureAwait(true);
            var gatewayProcess = host.GatewayProcessId;
            await RestartApplicationAsync(host).ConfigureAwait(true);

            Assert.Equal(gatewayProcess, host.GatewayProcessId);
            await AssertEncryptedReadsAsync(keyed, bytes).ConfigureAwait(true);
            Assert.Equal(bytes, (await keyed.WithSnapshot(snapshot).DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToArray());
            await AssertCustomerKeyDenialsAsync(host, container.Name, keyed.Name, wrongKey).ConfigureAwait(true);
            Assert.False(host.CapturedLogs.Contains(Convert.ToBase64String(key), StringComparison.Ordinal));
            Assert.False(host.CapturedLogs.Contains(Convert.ToBase64String(wrongKey), StringComparison.Ordinal));
            Assert.False(Directory.Exists(host.GatewayUnusedDataPath));
        }
        finally
        {
            CryptographicOperations.ZeroMemory(key);
            CryptographicOperations.ZeroMemory(wrongKey);
        }
    }

    [Theory]
    [InlineData(false, "")]
    [InlineData(false, "w")]
    [InlineData(true, "")]
    [InlineData(true, "w")]
    public async Task TrustedRoleKeysSurviveApplicationRestartAndRoleRemovalRevokesOnlyRoleRights(bool hns, string directPermissions)
    {
        ArgumentNullException.ThrowIfNull(directPermissions);
        var configuration = AuthenticationSettings(hns, directPermissions);
        configuration[$"Sava:BearerAuthentication:RolePermissions:{ReaderRole}"] = "r";
        var host = await SplitProcessHost.StartAsync(settings: configuration).ConfigureAwait(true);
        await using var lifetime = host.ConfigureAwait(false);
        using var https = await HttpsGateway.StartAsync(host).ConfigureAwait(true);
        var container = host.Client.GetBlobContainerClient("process-role-key");
        await container.CreateAsync().ConfigureAwait(true);
        var blob = container.GetBlobClient("parent/file.txt");
        await blob.UploadAsync(BinaryData.FromString("role-readable")).ConfigureAwait(true);
        if (hns)
            await DenyIssuerAclAsync(host, container.Name, blob.Name).ConfigureAwait(true);

        var issued = await IssueAndCheckRoleKeysAsync(host, container, directPermissions).ConfigureAwait(true);
        var gatewayProcess = host.GatewayProcessId;
        await RestartApplicationAsync(host).ConfigureAwait(true);
        Assert.Equal(gatewayProcess, host.GatewayProcessId);
        await AssertRoleSnapshotAsync(host, issued.Key).ConfigureAwait(true);
        var signed = CreateSignedBlob(host, container.Name, blob.Name, issued,
            directPermissions.Length == 0 ? "r" : "rw");
        Assert.Equal("role-readable", (await signed.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
        await AssertRoleRemovalAsync(host, https, container.Name, blob.Name, issued, directPermissions).ConfigureAwait(true);
        Assert.False(Directory.Exists(host.GatewayUnusedDataPath));
    }

    [Theory]
    [InlineData("c", false)]
    [InlineData("cw", false)]
    [InlineData("c", true)]
    [InlineData("cw", true)]
    public async Task HnsAclOnlyCreationKeepsCreateWriteAndSuoidRightsSeparateAcrossTheTransport(string permissions, bool suoid)
    {
        var host = await SplitProcessHost.StartAsync(settings: AuthenticationSettings(hns: true,
            permissions: string.Empty, ownership: suoid)).ConfigureAwait(true);
        await using var lifetime = host.ConfigureAwait(false);
        using var https = await HttpsGateway.StartAsync(host).ConfigureAwait(true);
        var container = host.Client.GetBlobContainerClient("process-acl-create");
        await container.CreateAsync().ConfigureAwait(true);
        await container.GetBlobClient("parent/sdk-existing.txt").UploadAsync(BinaryData.FromString("sdk-seed")).ConfigureAwait(true);
        await container.GetBlobClient("parent/raw-existing.txt").UploadAsync(BinaryData.FromString("raw-seed")).ConfigureAwait(true);
        var root = CreateAcl(container.Name, string.Empty, "--x", suoid ? "--x" : null);
        var parent = CreateAcl(container.Name, "parent", "-wx", suoid ? "-wx" : null);
        await ApplyAclWithOperatorAsync(host, root, parent).ConfigureAwait(true);
        var issued = await IssueKeyAsync(CreateBearerClient(host)).ConfigureAwait(true);
        var gatewayProcess = host.GatewayProcessId;
        await RestartApplicationAsync(host).ConfigureAwait(true);
        Assert.Equal(gatewayProcess, host.GatewayProcessId);

        var sdkNew = CreateSignedBlob(host, container.Name, "parent/sdk-new.txt", issued, permissions, suoid);
        var rawNew = CreateSignedBlob(host, container.Name, "parent/raw-new.txt", issued, permissions, suoid);
        await sdkNew.UploadAsync(BinaryData.FromString("sdk-created")).ConfigureAwait(true);
        Assert.Equal(HttpStatusCode.Created, await RawPutBlobAsync(host, rawNew.Uri, "raw-created").ConfigureAwait(true));
        await AssertAclOverwriteAsync(host, container, issued, permissions, suoid).ConfigureAwait(true);
        if (suoid)
        {
            await ApplyAclWithOperatorAsync(host, CreateAcl(container.Name, "parent", "-wx", "--x")).ConfigureAwait(true);
            await AssertAclCreateDeniedAsync(host, container.Name, issued, permissions, suoid, "end-user-revoked").ConfigureAwait(true);
        }
        await ApplyAclWithOperatorAsync(host, CreateAcl(container.Name, "parent", "--x", suoid ? "-wx" : null)).ConfigureAwait(true);
        await AssertAclCreateDeniedAsync(host, container.Name, issued, permissions, suoid, "issuer-revoked").ConfigureAwait(true);
        await AssertAclOverwriteDeniedAsync(host, container.Name, issued, permissions, suoid).ConfigureAwait(true);
        Assert.Equal(gatewayProcess, host.GatewayProcessId);
        Assert.Equal("sdk-created", (await container.GetBlobClient(sdkNew.Name).DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
        Assert.Equal("raw-created", (await container.GetBlobClient(rawNew.Name).DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
    }

    [Theory]
    [InlineData(false, "a", "a", true)]
    [InlineData(false, "a", "aw", true)]
    [InlineData(false, "w", "a", false)]
    [InlineData(false, "w", "aw", true)]
    [InlineData(false, "", "aw", false)]
    [InlineData(true, "a", "a", true)]
    [InlineData(true, "a", "aw", true)]
    [InlineData(true, "w", "a", false)]
    [InlineData(true, "w", "aw", true)]
    [InlineData(true, "", "aw", false)]
    public async Task DelegatedAppendUsesEffectiveAlternativeRightsAfterApplicationRestart(bool hns, string issuerPermissions,
        string signedPermissions, bool allowed)
    {
        var host = await SplitProcessHost.StartAsync(settings: AuthenticationSettings(hns, issuerPermissions)).ConfigureAwait(true);
        await using var lifetime = host.ConfigureAwait(false);
        using var https = await HttpsGateway.StartAsync(host).ConfigureAwait(true);
        var container = host.Client.GetBlobContainerClient("process-delegated-append");
        await container.CreateAsync().ConfigureAwait(true);
        var append = container.GetAppendBlobClient("parent/log.txt");
        await append.CreateAsync().ConfigureAwait(true);
        if (hns)
            await DenyIssuerAclAsync(host, container.Name, append.Name).ConfigureAwait(true);
        var issued = await IssueKeyAsync(CreateBearerClient(host)).ConfigureAwait(true);
        var gatewayProcess = host.GatewayProcessId;
        await RestartApplicationAsync(host).ConfigureAwait(true);
        Assert.Equal(gatewayProcess, host.GatewayProcessId);
        var signedUri = SignedUri(host, container.Name, append.Name, issued, signedPermissions);
        var signed = new AppendBlobClient(signedUri, host.CreateBlobClientOptions());
        using var body = BinaryData.FromString("x").ToStream();
        if (allowed)
            await signed.AppendBlockAsync(body).ConfigureAwait(true);
        else
            AssertForbidden(await Assert.ThrowsAsync<RequestFailedException>(() => signed.AppendBlockAsync(body)).ConfigureAwait(true));
        Assert.Equal(allowed ? HttpStatusCode.Created : HttpStatusCode.Forbidden,
            await RawAppendAsync(host, signedUri).ConfigureAwait(true));
        Assert.Equal(allowed ? "xy" : string.Empty,
            (await append.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
    }

    [Theory]
    [InlineData("http://0.0.0.0:0/internal/application", "requires HTTPS")]
    [InlineData("https://0.0.0.0:0/internal/application", "ApplicationHosting:CertificateFile")]
    [InlineData("https://127.0.0.1:0/internal/application", "ApplicationHosting:CertificateFile")]
    [InlineData("http://localhost:18581/internal/application", "literal IP address")]
    public async Task ApplicationRejectsUnsafeTlsBindingsBeforeOpeningItsDataRoot(string endpoint, string diagnostic)
    {
        var host = await SplitProcessHost.StartAsync(application: false).ConfigureAwait(true);
        await using var lifetime = host.ConfigureAwait(false);
        var rejectedData = Path.Combine(host.Root, "rejected-application-data");
        var start = host.CreateStart(typeof(ApplicationProgram).Assembly.Location);
        start.Environment["Sava__DataPath"] = rejectedData;
        start.Environment["ApplicationTransport__Endpoint"] = endpoint;
        start.Environment["ApplicationHosting__CertificateFile"] = string.Empty;
        start.Environment["ASPNETCORE_URLS"] = "http://127.0.0.1:0";
        start.Environment["Kestrel__Endpoints__Bypass__Url"] = "http://127.0.0.1:0";
        var result = await RunOperatorProcessAsync(start).ConfigureAwait(true);

        Assert.NotEqual(0, result.ExitCode);
        Assert.Contains(diagnostic, result.Error, StringComparison.Ordinal);
        Assert.DoesNotContain("Now listening on:", result.Output, StringComparison.Ordinal);
        Assert.False(Directory.Exists(rejectedData));
        Assert.False(Directory.Exists(host.StoragePath));
    }

    private static Dictionary<string, string?> AuthenticationSettings(bool hns, string permissions, bool ownership = false) =>
        new(StringComparer.Ordinal)
        {
            [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:HierarchicalNamespaceEnabled"] = hns ? "true" : "false",
            ["Sava:BearerAuthentication:Enabled"] = "true",
            ["Sava:BearerAuthentication:ValidAudiences:0"] = "https://storage.azure.com/",
            ["Sava:BearerAuthentication:ValidIssuers:0"] = "https://issuer.mk8.test",
            ["Sava:BearerAuthentication:SymmetricSigningKeys:test-key"] = SavaWebApplicationFactory.AccountKey,
            [$"Sava:BearerAuthentication:Principals:{IssuerId}:Accounts:0"] = SavaWebApplicationFactory.AccountName,
            [$"Sava:BearerAuthentication:Principals:{IssuerId}:Permissions"] = permissions,
            [$"Sava:BearerAuthentication:Principals:{IssuerId}:CanGenerateUserDelegationKey"] = "true",
            [$"Sava:BearerAuthentication:Principals:{IssuerId}:CanManageOwnership"] = ownership ? "true" : "false"
        };

    private static BlobServiceClient CreateEncryptedClient(SplitProcessHost host, byte[] key)
    {
        var options = host.CreateBlobClientOptions();
        options.CustomerProvidedKey = new CustomerProvidedKey(key);
        return new BlobServiceClient(new Uri(host.GatewayAddress, "/" + SavaWebApplicationFactory.AccountName),
            new StorageSharedKeyCredential(SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey), options);
    }

    private static BlobServiceClient CreateBearerClient(SplitProcessHost host, bool role = false) =>
        new(new Uri(host.GatewayAddress, "/" + SavaWebApplicationFactory.AccountName),
            new StaticTokenCredential(CreateJwt(role)), host.CreateBlobClientOptions());

    private static string CreateJwt(bool role)
    {
        var claims = new List<Claim> { new("oid", IssuerId), new("tid", SavaWebApplicationFactory.TenantId) };
        if (role)
            claims.Add(new Claim("roles", ReaderRole));
        var key = new SymmetricSecurityKey(Convert.FromBase64String(SavaWebApplicationFactory.AccountKey)) { KeyId = "test-key" };
        var token = new JwtSecurityToken("https://issuer.mk8.test", "https://storage.azure.com/", claims,
            DateTime.UtcNow.AddMinutes(-1), DateTime.UtcNow.AddMinutes(10),
            new SigningCredentials(key, SecurityAlgorithms.HmacSha256));
        return new JwtSecurityTokenHandler().WriteToken(token);
    }

    private static async Task<DelegationIssue> IssueKeyAsync(BlobServiceClient bearer)
    {
        var starts = DateTimeOffset.UtcNow.AddMinutes(-1);
        var expires = DateTimeOffset.UtcNow.AddMinutes(10);
        var key = (await bearer.GetUserDelegationKeyAsync(new BlobGetUserDelegationKeyOptions(expires)
        {
            StartsOn = starts
        }).ConfigureAwait(true)).Value;
        return new DelegationIssue(key, starts, expires);
    }

    private static BlobClient CreateSignedBlob(SplitProcessHost host, string container, string name,
        DelegationIssue issued, string permissions, bool suoid = false) =>
        new(SignedUri(host, container, name, issued, permissions, suoid), host.CreateBlobClientOptions());

    private static Uri SignedUri(SplitProcessHost host, string container, string name,
        DelegationIssue issued, string permissions, bool suoid = false)
    {
        var blob = host.Client.GetBlobContainerClient(container).GetBlobClient(name);
        if (suoid)
            return CreateSuoidUri(blob.Uri, container, name, issued, permissions);
        var sas = new BlobSasBuilder
        {
            BlobContainerName = container,
            BlobName = name,
            Resource = "b",
            StartsOn = issued.Starts,
            ExpiresOn = issued.Expires
        };
        sas.SetPermissions(permissions);
        var query = sas.ToSasQueryParameters(issued.Key, SavaWebApplicationFactory.AccountName).ToString();
        Assert.Contains("sp=" + permissions, query, StringComparison.Ordinal);
        Assert.DoesNotContain("suoid=", query, StringComparison.Ordinal);
        return new Uri(blob.Uri + "?" + query);
    }

    private static Uri CreateSuoidUri(Uri blob, string container, string name, DelegationIssue issued, string permissions)
    {
        const string version = "2023-11-03";
        static string Format(DateTimeOffset value) => value.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);
        var starts = Format(issued.Starts);
        var expires = Format(issued.Expires);
        var keyStarts = Format(issued.Key.SignedStartsOn);
        var keyExpires = Format(issued.Key.SignedExpiresOn);
        var canonical = $"/blob/{SavaWebApplicationFactory.AccountName}/{container}/{name}";
        var text = string.Join('\n', permissions, starts, expires, canonical,
            issued.Key.SignedObjectId, issued.Key.SignedTenantId, keyStarts, keyExpires,
            issued.Key.SignedService, issued.Key.SignedVersion, string.Empty, EndUserId, string.Empty, string.Empty,
            "https,http", version, "b", string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty);
        var secret = Convert.FromBase64String(issued.Key.Value);
        string signature;
        try
        {
            signature = Convert.ToBase64String(HMACSHA256.HashData(secret, Encoding.UTF8.GetBytes(text)));
        }
        finally
        {
            CryptographicOperations.ZeroMemory(secret);
        }
        var query = $"sp={permissions}&st={Uri.EscapeDataString(starts)}&se={Uri.EscapeDataString(expires)}" +
            $"&skoid={issued.Key.SignedObjectId}&sktid={issued.Key.SignedTenantId}" +
            $"&skt={Uri.EscapeDataString(keyStarts)}&ske={Uri.EscapeDataString(keyExpires)}" +
            $"&sks={issued.Key.SignedService}&skv={issued.Key.SignedVersion}&suoid={EndUserId}" +
            $"&spr=https%2Chttp&sv={version}&sr=b&sig={Uri.EscapeDataString(signature)}";
        return new Uri(blob + "?" + query);
    }

    private static async Task AssertEncryptedReadsAsync(BlobBaseClient blob, byte[] bytes)
    {
        Assert.Equal(bytes, (await blob.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToArray());
        var response = await blob.DownloadStreamingAsync(new BlobDownloadOptions { Range = new HttpRange(63, 8195) }).ConfigureAwait(true);
        using var content = response.Value.Content;
        using var destination = new MemoryStream();
        await content.CopyToAsync(destination).ConfigureAwait(true);
        Assert.Equal(bytes.AsSpan(63, 8195).ToArray(), destination.ToArray());
    }

    private static async Task AssertCustomerKeyDenialsAsync(SplitProcessHost host, string container, string name, byte[] wrongKey)
    {
        var missing = host.Client.GetBlobContainerClient(container).GetBlobClient(name);
        AssertCustomerKeyFailure(await Assert.ThrowsAsync<RequestFailedException>(() => missing.DownloadContentAsync()).ConfigureAwait(true));
        AssertCustomerKeyFailure(await Assert.ThrowsAsync<RequestFailedException>(() => missing.GetPropertiesAsync()).ConfigureAwait(true));
        var wrong = CreateEncryptedClient(host, wrongKey).GetBlobContainerClient(container).GetBlobClient(name);
        AssertCustomerKeyFailure(await Assert.ThrowsAsync<RequestFailedException>(() => wrong.DownloadContentAsync()).ConfigureAwait(true));
        AssertCustomerKeyFailure(await Assert.ThrowsAsync<RequestFailedException>(() => wrong.SetMetadataAsync(
            new Dictionary<string, string>(StringComparer.Ordinal) { ["wrong-key"] = "must-not-persist" })).ConfigureAwait(true));
    }

    private static void AssertCustomerKeyFailure(RequestFailedException failure)
    {
        Assert.Equal(409, failure.Status);
        Assert.Equal("BlobUsesCustomerSpecifiedEncryption", failure.ErrorCode);
    }

    private static async Task AssertCustomerKeyStagedBlocksAsync(SplitProcessHost host, string container, byte[] key)
    {
        var blob = CreateEncryptedClient(host, key).GetBlobContainerClient(container).GetBlockBlobClient("keyed-blocks");
        var firstId = Convert.ToBase64String("block-01"u8);
        var secondId = Convert.ToBase64String("block-02"u8);
        using (var first = BinaryData.FromString("encrypted-").ToStream())
            await blob.StageBlockAsync(firstId, first).ConfigureAwait(true);
        using (var second = BinaryData.FromString("blocks").ToStream())
            await blob.StageBlockAsync(secondId, second).ConfigureAwait(true);
        await blob.CommitBlockListAsync([firstId, secondId]).ConfigureAwait(true);
        Assert.Equal("encrypted-blocks", (await blob.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
    }

    private static async Task<DelegationIssue> IssueAndCheckRoleKeysAsync(SplitProcessHost host,
        BlobContainerClient container, string directPermissions)
    {
        var bearer = CreateBearerClient(host, role: true);
        Assert.Equal("role-readable", (await bearer.GetBlobContainerClient(container.Name).GetBlobClient("parent/file.txt")
            .DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
        var issued = await IssueKeyAsync(bearer).ConfigureAwait(true);
        var plainKey = (await CreateBearerClient(host).GetUserDelegationKeyAsync(new BlobGetUserDelegationKeyOptions(issued.Expires)
        {
            StartsOn = issued.Starts
        }).ConfigureAwait(true)).Value;
        Assert.False(string.Equals(issued.Key.Value, plainKey.Value, StringComparison.Ordinal));
        var denied = CreateSignedBlob(host, container.Name, "parent/file.txt", issued with { Key = plainKey }, "r");
        AssertForbidden(await Assert.ThrowsAsync<RequestFailedException>(() => denied.DownloadContentAsync()).ConfigureAwait(true));
        var signed = CreateSignedBlob(host, container.Name, "parent/file.txt", issued, directPermissions.Length == 0 ? "r" : "rw");
        Assert.Equal("role-readable", (await signed.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
        await AssertRoleSnapshotAsync(host, issued.Key).ConfigureAwait(true);
        return issued;
    }

    private static async Task AssertRoleSnapshotAsync(SplitProcessHost host, UserDelegationKey key)
    {
        var secret = Convert.FromBase64String(key.Value);
        string fingerprint;
        try
        {
            fingerprint = Convert.ToHexString(SHA256.HashData(secret));
        }
        finally
        {
            CryptographicOperations.ZeroMemory(secret);
        }
        using var application = host.CreateApplicationClient();
        var grants = await application.CreateProxy<IMetadataApplication>().ReadUserDelegationRoleGrantsAsync(fingerprint,
            CancellationToken.None).ConfigureAwait(true);
        Assert.NotNull(grants);
        Assert.Equal([ReaderRole], grants.Roles, StringComparer.Ordinal);
        Assert.Equal("r", grants.IssuedPermissions, StringComparer.Ordinal);
    }

    private static async Task AssertRoleRemovalAsync(SplitProcessHost host, HttpsGateway https, string container,
        string name, DelegationIssue issued, string directPermissions)
    {
        var changed = new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            [$"Sava:BearerAuthentication:RolePermissions:{ReaderRole}"] = string.Empty
        };
        await host.StopApplicationAsync().ConfigureAwait(true);
        await host.StartApplicationAsync(changed).ConfigureAwait(true);
        using (var probe = host.CreateHttpClient())
        using (var response = await probe.GetAsync(new Uri(host.GatewayAddress, "/health/ready")).ConfigureAwait(true))
            Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
        await https.RestartAsync(host, changed).ConfigureAwait(true);
        var signed = CreateSignedBlob(host, container, name, issued, directPermissions.Length == 0 ? "r" : "rw");
        AssertForbidden(await Assert.ThrowsAsync<RequestFailedException>(() => signed.DownloadContentAsync()).ConfigureAwait(true));
        if (directPermissions.Length > 0)
            await signed.UploadAsync(BinaryData.FromString("direct-write-still-granted"), overwrite: true).ConfigureAwait(true);
        else
            AssertForbidden(await Assert.ThrowsAsync<RequestFailedException>(() =>
                signed.UploadAsync(BinaryData.FromString("must-not-write"), overwrite: true)).ConfigureAwait(true));
        Assert.Equal(directPermissions.Length == 0 ? "role-readable" : "direct-write-still-granted",
            (await host.Client.GetBlobContainerClient(container).GetBlobClient(name).DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
    }

    private static HierarchicalAclManifestEntry CreateAcl(string container, string path, string issuer, string? user = null) =>
        new()
        {
            Account = SavaWebApplicationFactory.AccountName,
            Container = container,
            Path = path,
            AccessAcl = $"user::rwx,user:{IssuerId}:{issuer}" + (user is null ? string.Empty : $",user:{EndUserId}:{user}") +
                ",group::---,mask::rwx,other::---"
        };

    private static Task DenyIssuerAclAsync(SplitProcessHost host, string container, string blob) =>
        ApplyAclWithOperatorAsync(host, CreateAcl(container, string.Empty, "---"), CreateAcl(container, "parent", "---"),
            CreateAcl(container, blob, "---"));

    private static async Task ApplyAclWithOperatorAsync(SplitProcessHost host, params HierarchicalAclManifestEntry[] entries)
    {
        var gatewayProcess = host.GatewayProcessId;
        await host.StopApplicationAsync().ConfigureAwait(true);
        var manifestPath = Path.Combine(host.Root, "acl-" + Guid.NewGuid().ToString("N") + ".json");
        await File.WriteAllTextAsync(manifestPath, JsonSerializer.Serialize(
            new HierarchicalAclManifest { SchemaVersion = 1, Entries = [.. entries] }, JsonSerializerOptions.Web)).ConfigureAwait(true);
        var start = host.CreateStart(typeof(ApplicationProgram).Assembly.Location);
        start.Environment["Sava__DataPath"] = host.StoragePath;
        start.ArgumentList.Add("--hns-acl-apply");
        start.ArgumentList.Add(manifestPath);
        var result = await RunOperatorProcessAsync(start).ConfigureAwait(true);
        Assert.True(result.ExitCode == 0, result.Error);
        Assert.Contains(FormattableString.Invariant($"Applied HNS access ACLs to {entries.Length} existing targets."),
            result.Output, StringComparison.Ordinal);
        await host.StartApplicationAsync().ConfigureAwait(true);
        Assert.Equal(gatewayProcess, host.GatewayProcessId);
        await AssertGatewayReadyAsync(host).ConfigureAwait(true);
    }

    private static async Task RestartApplicationAsync(SplitProcessHost host)
    {
        await host.StopApplicationAsync().ConfigureAwait(true);
        await host.StartApplicationAsync().ConfigureAwait(true);
        await AssertGatewayReadyAsync(host).ConfigureAwait(true);
    }

    private static async Task AssertGatewayReadyAsync(SplitProcessHost host)
    {
        using var client = host.CreateHttpClient();
        using var response = await client.GetAsync(new Uri(host.GatewayAddress, "/health/ready")).ConfigureAwait(true);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    private static async Task AssertAclOverwriteAsync(SplitProcessHost host, BlobContainerClient container,
        DelegationIssue issued, string permissions, bool suoid)
    {
        var sdk = CreateSignedBlob(host, container.Name, "parent/sdk-existing.txt", issued, permissions, suoid);
        var raw = CreateSignedBlob(host, container.Name, "parent/raw-existing.txt", issued, permissions, suoid);
        var allowed = string.Equals(permissions, "cw", StringComparison.Ordinal);
        if (allowed)
            await sdk.UploadAsync(BinaryData.FromString("sdk-overwritten"), overwrite: true).ConfigureAwait(true);
        else
            AssertForbidden(await Assert.ThrowsAsync<RequestFailedException>(() => sdk.UploadAsync(
                BinaryData.FromString("must-not-overwrite"), overwrite: true)).ConfigureAwait(true));
        Assert.Equal(allowed ? HttpStatusCode.Created : HttpStatusCode.Forbidden,
            await RawPutBlobAsync(host, raw.Uri, "raw-overwritten").ConfigureAwait(true));
        Assert.Equal(allowed ? "sdk-overwritten" : "sdk-seed",
            (await container.GetBlobClient(sdk.Name).DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
        Assert.Equal(allowed ? "raw-overwritten" : "raw-seed",
            (await container.GetBlobClient(raw.Name).DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
    }

    private static async Task AssertAclCreateDeniedAsync(SplitProcessHost host, string container, DelegationIssue issued,
        string permissions, bool suoid, string suffix)
    {
        var sdk = CreateSignedBlob(host, container, "parent/sdk-" + suffix, issued, permissions, suoid);
        var raw = CreateSignedBlob(host, container, "parent/raw-" + suffix, issued, permissions, suoid);
        AssertForbidden(await Assert.ThrowsAsync<RequestFailedException>(() => sdk.UploadAsync(BinaryData.FromString("must-not-create")))
            .ConfigureAwait(true));
        Assert.Equal(HttpStatusCode.Forbidden, await RawPutBlobAsync(host, raw.Uri, "must-not-create").ConfigureAwait(true));
        Assert.False((await host.Client.GetBlobContainerClient(container).GetBlobClient(sdk.Name).ExistsAsync().ConfigureAwait(true)).Value);
        Assert.False((await host.Client.GetBlobContainerClient(container).GetBlobClient(raw.Name).ExistsAsync().ConfigureAwait(true)).Value);
    }

    private static async Task AssertAclOverwriteDeniedAsync(SplitProcessHost host, string container, DelegationIssue issued,
        string permissions, bool suoid)
    {
        var sdk = CreateSignedBlob(host, container, "parent/sdk-existing.txt", issued, permissions, suoid);
        var raw = CreateSignedBlob(host, container, "parent/raw-existing.txt", issued, permissions, suoid);
        AssertForbidden(await Assert.ThrowsAsync<RequestFailedException>(() => sdk.UploadAsync(
            BinaryData.FromString("must-not-overwrite"), overwrite: true)).ConfigureAwait(true));
        Assert.Equal(HttpStatusCode.Forbidden, await RawPutBlobAsync(host, raw.Uri, "must-not-overwrite").ConfigureAwait(true));
    }

    private static async Task<HttpStatusCode> RawPutBlobAsync(SplitProcessHost host, Uri uri, string content)
    {
        using var transport = host.CreateHttpClient();
        using var request = new HttpRequestMessage(HttpMethod.Put, uri)
        {
            Content = new ByteArrayContent(Encoding.UTF8.GetBytes(content))
        };
        request.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
        request.Headers.TryAddWithoutValidation("x-ms-blob-type", "BlockBlob");
        using var response = await transport.SendAsync(request).ConfigureAwait(true);
        return response.StatusCode;
    }

    private static async Task<HttpStatusCode> RawAppendAsync(SplitProcessHost host, Uri uri)
    {
        using var transport = host.CreateHttpClient();
        using var request = new HttpRequestMessage(HttpMethod.Put, new Uri(uri + "&comp=appendblock"))
        {
            Content = new ByteArrayContent("y"u8.ToArray())
        };
        request.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
        using var response = await transport.SendAsync(request).ConfigureAwait(true);
        return response.StatusCode;
    }

    private static void AssertForbidden(RequestFailedException failure) => Assert.Equal(403, failure.Status);

    private static async Task<OperatorResult> RunOperatorProcessAsync(ProcessStartInfo start)
    {
        using var process = Process.Start(start);
        Assert.NotNull(process);
        var result = await TestProcessRunner.ObserveAsync(process, TimeSpan.FromSeconds(45), TimeSpan.FromSeconds(5))
            .ConfigureAwait(true);
        return new OperatorResult(result.ExitCode, result.StandardOutput, result.StandardError);
    }

    private sealed record DelegationIssue(UserDelegationKey Key, DateTimeOffset Starts, DateTimeOffset Expires);
    private sealed record OperatorResult(int ExitCode, string Output, string Error);

    private sealed class StaticTokenCredential(string token) : TokenCredential
    {
        public override AccessToken GetToken(TokenRequestContext requestContext, CancellationToken cancellationToken) =>
            new(token, DateTimeOffset.UtcNow.AddMinutes(10));

        public override ValueTask<AccessToken> GetTokenAsync(TokenRequestContext requestContext, CancellationToken cancellationToken) =>
            ValueTask.FromResult(GetToken(requestContext, cancellationToken));
    }

    private sealed class HttpsGateway(X509Certificate2 certificate, Dictionary<string, string?> settings) : IDisposable
    {
        public static async Task<HttpsGateway> StartAsync(SplitProcessHost host)
        {
            using var rsa = RSA.Create(2048);
            var request = new CertificateRequest("CN=localhost", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            var names = new SubjectAlternativeNameBuilder();
            names.AddDnsName("localhost");
            names.AddIpAddress(IPAddress.Loopback);
            request.CertificateExtensions.Add(names.Build());
            request.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, false));
            request.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, false));
            request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(new OidCollection { new("1.3.6.1.5.5.7.3.1") }, false));
            X509Certificate2? certificate = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddMinutes(-1), DateTimeOffset.UtcNow.AddMinutes(20));
            HttpsGateway? fixture = null;
            byte[]? pfx = null;
            try
            {
                var password = Convert.ToBase64String(RandomNumberGenerator.GetBytes(32));
                var path = Path.Combine(host.Root, "gateway.pfx");
                pfx = certificate.Export(X509ContentType.Pfx, password);
                var creation = new FileStreamOptions { Mode = FileMode.CreateNew, Access = FileAccess.Write };
                if (!OperatingSystem.IsWindows())
                    creation.UnixCreateMode = UnixFileMode.UserRead | UnixFileMode.UserWrite;
                var file = new FileStream(path, creation);
                await using (file.ConfigureAwait(false))
                    await file.WriteAsync(pfx).ConfigureAwait(true);
                var settings = new Dictionary<string, string?>(StringComparer.Ordinal)
                {
                    ["ASPNETCORE_URLS"] = "https://127.0.0.1:0",
                    ["Kestrel:Certificates:Default:Path"] = path,
                    ["Kestrel:Certificates:Default:Password"] = password
                };
                fixture = new HttpsGateway(certificate, settings);
                certificate = null;
                await fixture.RestartAsync(host).ConfigureAwait(true);
                Assert.Equal(Uri.UriSchemeHttps, host.GatewayAddress.Scheme, StringComparer.Ordinal);
                var result = fixture;
                fixture = null;
                return result;
            }
            finally
            {
                fixture?.Dispose();
                certificate?.Dispose();
                if (pfx is not null)
                    CryptographicOperations.ZeroMemory(pfx);
            }
        }

        public async Task RestartAsync(SplitProcessHost host, IReadOnlyDictionary<string, string?>? overrides = null)
        {
            await host.StopGatewayAsync().ConfigureAwait(true);
            var configured = new Dictionary<string, string?>(settings, StringComparer.Ordinal);
            if (overrides is not null)
            {
                foreach (var pair in overrides)
                    configured[pair.Key] = pair.Value;
            }
            await host.StartGatewayAsync(configured, certificate).ConfigureAwait(true);
            await AssertGatewayReadyAsync(host).ConfigureAwait(true);
        }

        public void Dispose() => certificate.Dispose();
    }
}
