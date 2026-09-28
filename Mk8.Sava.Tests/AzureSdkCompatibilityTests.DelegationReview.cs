using Azure;
using Azure.Core.Pipeline;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Sas;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

public sealed partial class AzureSdkCompatibilityTests
{
    [Theory]
    [InlineData(false, "a", "a", true)]
    [InlineData(false, "a", "aw", true)]
    [InlineData(false, "a", "w", false)]
    [InlineData(false, "w", "a", false)]
    [InlineData(false, "w", "aw", true)]
    [InlineData(false, "w", "w", true)]
    [InlineData(false, "", "a", false)]
    [InlineData(false, "", "aw", false)]
    [InlineData(false, "", "w", false)]
    [InlineData(true, "a", "a", true)]
    [InlineData(true, "a", "aw", true)]
    [InlineData(true, "a", "w", false)]
    [InlineData(true, "w", "a", false)]
    [InlineData(true, "w", "aw", true)]
    [InlineData(true, "w", "w", true)]
    [InlineData(true, "", "a", false)]
    [InlineData(true, "", "aw", false)]
    [InlineData(true, "", "w", false)]
    public async Task DelegationAppendAcceptsEitherEffectiveSignedPermission(
        bool hns, string issuerPermissions, string signedPermissions, bool allowed)
    {
        const string issuerId = "01755f02-85bd-4e9e-9eec-a6f895c381a3";
        var application = new SavaWebApplicationFactory(new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:HierarchicalNamespaceEnabled"] = hns ? "true" : "false",
            [$"Sava:BearerAuthentication:Principals:{issuerId}:Accounts:0"] = SavaWebApplicationFactory.AccountName,
            [$"Sava:BearerAuthentication:Principals:{issuerId}:Permissions"] = issuerPermissions,
            [$"Sava:BearerAuthentication:Principals:{issuerId}:CanGenerateUserDelegationKey"] = "true"
        });
        await using var disposal = application.ConfigureAwait(true);
        await application.InitializeAsync().ConfigureAwait(true);
        var container = CreateClient(application).GetBlobContainerClient($"review-append-{Guid.NewGuid():N}");
        await container.CreateAsync().ConfigureAwait(true);
        var append = container.GetAppendBlobClient("parent/log.txt");
        await append.CreateAsync().ConfigureAwait(true);
        if (hns)
            await DenyIssuerAclAsync(application, container, append.Name, issuerId).ConfigureAwait(true);
        var bearer = CreateBearerClient(application,
            CreateJwt(SavaWebApplicationFactory.AccountKey, issuerId, SavaWebApplicationFactory.TenantId));
        var starts = DateTimeOffset.UtcNow.AddMinutes(-1);
        var expires = DateTimeOffset.UtcNow.AddMinutes(5);
        var key = (await bearer.GetUserDelegationKeyAsync(new BlobGetUserDelegationKeyOptions(expires)
        {
            StartsOn = starts
        }).ConfigureAwait(true)).Value;
        var signed = CreateDelegatedAppendClient(application, append, container.Name, key, starts, expires,
            signedPermissions);
        using var body = BinaryData.FromString("x").ToStream();
        if (allowed)
            await signed.AppendBlockAsync(body).ConfigureAwait(true);
        else
            Assert.Equal(403, (await Assert.ThrowsAsync<RequestFailedException>(() => signed.AppendBlockAsync(body))
                .ConfigureAwait(true)).Status);
        Assert.Equal(allowed ? 201 : 403, await SendRawAppendBlockAsync(application, signed.Uri).ConfigureAwait(true));
        Assert.Equal(allowed ? "xy" : "", (await append.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
    }

    [Theory]
    [InlineData(false, "")]
    [InlineData(true, "")]
    [InlineData(false, "w")]
    [InlineData(true, "w")]
    public async Task DelegationKeyRetainsTrustedRoleGrantForSasUse(bool hns, string directPermissions)
    {
        ArgumentNullException.ThrowIfNull(directPermissions);
        const string issuerId = "fc045b29-63c7-4eca-b34e-19b9d5f9963d";
        var application = new SavaWebApplicationFactory(new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:HierarchicalNamespaceEnabled"] = hns ? "true" : "false",
            [$"Sava:BearerAuthentication:Principals:{issuerId}:Accounts:0"] = SavaWebApplicationFactory.AccountName,
            [$"Sava:BearerAuthentication:Principals:{issuerId}:Permissions"] = directPermissions,
            [$"Sava:BearerAuthentication:Principals:{issuerId}:CanGenerateUserDelegationKey"] = "true",
            ["Sava:BearerAuthentication:RolePermissions:BlobReader"] = "r"
        });
        await using var disposal = application.ConfigureAwait(true);
        await application.InitializeAsync().ConfigureAwait(true);
        var container = CreateClient(application).GetBlobContainerClient($"review-role-{Guid.NewGuid():N}");
        await container.CreateAsync().ConfigureAwait(true);
        var blob = container.GetBlobClient("parent/file.txt");
        await blob.UploadAsync(BinaryData.FromString("role-readable")).ConfigureAwait(true);
        if (hns)
            await DenyIssuerAclAsync(application, container, blob.Name, issuerId).ConfigureAwait(true);
        var bearer = CreateBearerClient(application,
            CreateJwt(SavaWebApplicationFactory.AccountKey, issuerId, SavaWebApplicationFactory.TenantId,
                roles: ["BlobReader"]));
        var bearerBlob = bearer.GetBlobContainerClient(container.Name).GetBlobClient(blob.Name);
        Assert.Equal("role-readable", (await bearerBlob.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
        var starts = DateTimeOffset.UtcNow.AddMinutes(-1);
        var expires = DateTimeOffset.UtcNow.AddMinutes(5);
        var key = (await bearer.GetUserDelegationKeyAsync(new BlobGetUserDelegationKeyOptions(expires)
        {
            StartsOn = starts
        }).ConfigureAwait(true)).Value;
        var sasPermissions = directPermissions.Length == 0
            ? BlobSasPermissions.Read
            : BlobSasPermissions.Read | BlobSasPermissions.Write;
        var signed = CreateOrdinaryDelegationClient(application, container, blob, key, starts, expires,
            sasPermissions);
        Assert.Equal("role-readable", (await signed.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
        if (directPermissions.Length > 0)
        {
            await signed.UploadAsync(BinaryData.FromString("role-and-direct"), overwrite: true).ConfigureAwait(true);
            Assert.Equal("role-and-direct", (await signed.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
        }
    }

    [Theory]
    [InlineData(false, "c", "c", true, false)]
    [InlineData(false, "c", "cw", true, false)]
    [InlineData(false, "w", "c", false, false)]
    [InlineData(false, "w", "cw", true, true)]
    [InlineData(false, "", "c", false, false)]
    [InlineData(false, "", "cw", false, false)]
    [InlineData(true, "c", "c", true, false)]
    [InlineData(true, "c", "cw", true, false)]
    [InlineData(true, "w", "c", false, false)]
    [InlineData(true, "w", "cw", true, true)]
    [InlineData(true, "", "c", false, false)]
    [InlineData(true, "", "cw", false, false)]
    public async Task DelegationCreateWriteAlternativesHonorEffectiveIssuerRights(
        bool hns, string issuerPermissions, string signedPermissions, bool createAllowed, bool overwriteAllowed)
    {
        const string issuerId = "98bfd858-8555-423c-a973-4942243c327b";
        var application = new SavaWebApplicationFactory(new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:HierarchicalNamespaceEnabled"] = hns ? "true" : "false",
            [$"Sava:BearerAuthentication:Principals:{issuerId}:Accounts:0"] = SavaWebApplicationFactory.AccountName,
            [$"Sava:BearerAuthentication:Principals:{issuerId}:Permissions"] = issuerPermissions,
            [$"Sava:BearerAuthentication:Principals:{issuerId}:CanGenerateUserDelegationKey"] = "true"
        });
        await using var disposal = application.ConfigureAwait(true);
        await application.InitializeAsync().ConfigureAwait(true);
        var container = CreateClient(application).GetBlobContainerClient($"review-create-{Guid.NewGuid():N}");
        await container.CreateAsync().ConfigureAwait(true);
        var blob = container.GetBlobClient("parent/new.txt");
        await container.GetBlobClient("parent/seed.txt").UploadAsync(BinaryData.FromString("seed")).ConfigureAwait(true);
        if (hns)
            await DenyIssuerParentAclAsync(application, container, issuerId).ConfigureAwait(true);
        var bearer = CreateBearerClient(application,
            CreateJwt(SavaWebApplicationFactory.AccountKey, issuerId, SavaWebApplicationFactory.TenantId));
        var starts = DateTimeOffset.UtcNow.AddMinutes(-1);
        var expires = DateTimeOffset.UtcNow.AddMinutes(5);
        var key = (await bearer.GetUserDelegationKeyAsync(new BlobGetUserDelegationKeyOptions(expires)
        {
            StartsOn = starts
        }).ConfigureAwait(true)).Value;
        var sasPermissions = string.Equals(signedPermissions, "c", StringComparison.Ordinal)
            ? BlobSasPermissions.Create : BlobSasPermissions.Create | BlobSasPermissions.Write;
        var signed = CreateOrdinaryDelegationClient(application, container, blob, key, starts, expires, sasPermissions);
        if (createAllowed)
            await signed.UploadAsync(BinaryData.FromString("created")).ConfigureAwait(true);
        else
            Assert.Equal(403, (await Assert.ThrowsAsync<RequestFailedException>(() =>
                signed.UploadAsync(BinaryData.FromString("denied"))).ConfigureAwait(true)).Status);
        if (!createAllowed)
            await blob.UploadAsync(BinaryData.FromString("owner-created")).ConfigureAwait(true);
        if (overwriteAllowed)
            await signed.UploadAsync(BinaryData.FromString("overwritten"), overwrite: true).ConfigureAwait(true);
        else
            Assert.Equal(403, (await Assert.ThrowsAsync<RequestFailedException>(() =>
                signed.UploadAsync(BinaryData.FromString("forbidden"), overwrite: true)).ConfigureAwait(true)).Status);
        Assert.Equal(overwriteAllowed ? "overwritten" : createAllowed ? "created" : "owner-created",
            (await blob.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
    }

    [Theory]
    [InlineData("c", false)]
    [InlineData("cw", true)]
    public async Task DelegationCreateUsesIssuerParentAclWithoutPromotingCreateToWrite(
        string signedPermissions, bool overwriteAllowed)
    {
        const string issuerId = "879acd19-52b5-486b-9754-6b9c8c1eeaec";
        var application = new SavaWebApplicationFactory(new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:HierarchicalNamespaceEnabled"] = "true",
            [$"Sava:BearerAuthentication:Principals:{issuerId}:Accounts:0"] = SavaWebApplicationFactory.AccountName,
            [$"Sava:BearerAuthentication:Principals:{issuerId}:Permissions"] = "",
            [$"Sava:BearerAuthentication:Principals:{issuerId}:CanGenerateUserDelegationKey"] = "true"
        });
        await using var disposal = application.ConfigureAwait(true);
        await application.InitializeAsync().ConfigureAwait(true);
        var container = CreateClient(application).GetBlobContainerClient($"review-acl-create-{Guid.NewGuid():N}");
        await container.CreateAsync().ConfigureAwait(true);
        await container.GetBlobClient("parent/seed.txt").UploadAsync(BinaryData.FromString("seed")).ConfigureAwait(true);
        var root = new HierarchicalAclManifestEntry
        {
            Account = SavaWebApplicationFactory.AccountName,
            Container = container.Name,
            Path = "",
            AccessAcl = $"user::rwx,user:{issuerId}:--x,group::---,mask::--x,other::---"
        };
        var parent = root with
        {
            Path = "parent",
            AccessAcl = $"user::rwx,user:{issuerId}:-wx,group::---,mask::-wx,other::---"
        };
        await ApplyAclManifestAsync(application, root, parent).ConfigureAwait(true);
        var bearer = CreateBearerClient(application,
            CreateJwt(SavaWebApplicationFactory.AccountKey, issuerId, SavaWebApplicationFactory.TenantId));
        var starts = DateTimeOffset.UtcNow.AddMinutes(-1);
        var expires = DateTimeOffset.UtcNow.AddMinutes(5);
        var key = (await bearer.GetUserDelegationKeyAsync(new BlobGetUserDelegationKeyOptions(expires)
        {
            StartsOn = starts
        }).ConfigureAwait(true)).Value;
        var permissions = overwriteAllowed
            ? BlobSasPermissions.Create | BlobSasPermissions.Write : BlobSasPermissions.Create;
        BlobClient Signed(string name) => CreateOrdinaryDelegationClient(
            application, container, container.GetBlobClient(name), key, starts, expires, permissions);
        var sdkNew = Signed("parent/sdk-new.txt");
        Assert.Contains($"sp={signedPermissions}", sdkNew.Uri.Query, StringComparison.Ordinal);
        await sdkNew.UploadAsync(BinaryData.FromString("sdk-created")).ConfigureAwait(true);
        var rawNew = Signed("parent/raw-new.txt");
        Assert.Equal(201, await SendRawPutBlobAsync(application, rawNew.Uri, "raw-created").ConfigureAwait(true));
        var sdkExisting = Signed("parent/sdk-existing.txt");
        var rawExisting = Signed("parent/raw-existing.txt");
        await container.GetBlobClient(sdkExisting.Name).UploadAsync(BinaryData.FromString("sdk-seed")).ConfigureAwait(true);
        await container.GetBlobClient(rawExisting.Name).UploadAsync(BinaryData.FromString("raw-seed")).ConfigureAwait(true);
        if (overwriteAllowed)
            await sdkExisting.UploadAsync(BinaryData.FromString("sdk-overwritten"), overwrite: true).ConfigureAwait(true);
        else
            Assert.Equal(403, (await Assert.ThrowsAsync<RequestFailedException>(() =>
                sdkExisting.UploadAsync(BinaryData.FromString("forbidden"), overwrite: true)).ConfigureAwait(true)).Status);
        Assert.Equal(overwriteAllowed ? 201 : 403,
            await SendRawPutBlobAsync(application, rawExisting.Uri, "raw-overwritten").ConfigureAwait(true));
        await AssertParentAclRevocationAsync(
            application, parent, issuerId, Signed("parent/sdk-revoked.txt"),
            Signed("parent/raw-revoked.txt"), sdkExisting, rawExisting).ConfigureAwait(true);
        await AssertExistingBlobContentsAsync(container, sdkExisting, rawExisting, overwriteAllowed).ConfigureAwait(true);
    }

    private static async Task AssertExistingBlobContentsAsync(
        BlobContainerClient container, BlobClient sdkExisting, BlobClient rawExisting, bool overwritten)
    {
        Assert.Equal(overwritten ? "sdk-overwritten" : "sdk-seed",
            (await container.GetBlobClient(sdkExisting.Name).DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
        Assert.Equal(overwritten ? "raw-overwritten" : "raw-seed",
            (await container.GetBlobClient(rawExisting.Name).DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
    }

    private static async Task AssertParentAclRevocationAsync(
        SavaWebApplicationFactory application, HierarchicalAclManifestEntry parent, string issuerId,
        BlobClient sdkNew, BlobClient rawNew, BlobClient sdkExisting, BlobClient rawExisting)
    {
        await ApplyAclManifestAsync(application, parent with
        {
            AccessAcl = $"user::rwx,user:{issuerId}:--x,group::---,mask::--x,other::---"
        }).ConfigureAwait(true);
        Assert.Equal(403, (await Assert.ThrowsAsync<RequestFailedException>(() =>
            sdkNew.UploadAsync(BinaryData.FromString("forbidden"))).ConfigureAwait(true)).Status);
        Assert.Equal(403, await SendRawPutBlobAsync(application, rawNew.Uri, "forbidden").ConfigureAwait(true));
        Assert.Equal(403, (await Assert.ThrowsAsync<RequestFailedException>(() =>
            sdkExisting.UploadAsync(BinaryData.FromString("forbidden"), overwrite: true)).ConfigureAwait(true)).Status);
        Assert.Equal(403, await SendRawPutBlobAsync(application, rawExisting.Uri, "forbidden").ConfigureAwait(true));
    }

    private static async Task<int> SendRawPutBlobAsync(
        SavaWebApplicationFactory application, Uri signedUri, string body)
    {
        using var transport = new HttpClient(application.Server.CreateHandler());
        using var request = new HttpRequestMessage(HttpMethod.Put, signedUri);
        request.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
        request.Headers.TryAddWithoutValidation("x-ms-blob-type", "BlockBlob");
        request.Content = new ByteArrayContent(System.Text.Encoding.UTF8.GetBytes(body));
        using var response = await transport.SendAsync(request).ConfigureAwait(false);
        return (int)response.StatusCode;
    }

    private static AppendBlobClient CreateDelegatedAppendClient(
        SavaWebApplicationFactory application, AppendBlobClient blob, string containerName,
        UserDelegationKey key, DateTimeOffset starts, DateTimeOffset expires, string signedPermissions)
    {
        var builder = new BlobSasBuilder
        {
            BlobContainerName = containerName,
            BlobName = blob.Name,
            Resource = "b",
            StartsOn = starts,
            ExpiresOn = expires
        };
        builder.SetPermissions(signedPermissions switch
        {
            "a" => BlobSasPermissions.Add,
            "w" => BlobSasPermissions.Write,
            _ => BlobSasPermissions.Add | BlobSasPermissions.Write
        });
        var sas = builder.ToSasQueryParameters(key, SavaWebApplicationFactory.AccountName).ToString();
        Assert.Contains($"sp={signedPermissions}", sas, StringComparison.Ordinal);
        return new AppendBlobClient(new Uri(blob.Uri + "?" + sas), new BlobClientOptions
        {
            Transport = new HttpClientTransport(application.Server.CreateHandler()),
            Retry = { MaxRetries = 0 }
        });
    }

    private static Task<int> DenyIssuerAclAsync(
        SavaWebApplicationFactory application, BlobContainerClient container, string blobName, string issuerId)
    {
        var root = new HierarchicalAclManifestEntry
        {
            Account = SavaWebApplicationFactory.AccountName,
            Container = container.Name,
            Path = "",
            AccessAcl = $"user::rwx,user:{issuerId}:---,group::---,mask::---,other::---"
        };
        return ApplyAclManifestAsync(application, root, root with { Path = "parent" }, root with
        {
            Path = blobName,
            AccessAcl = $"user::rw-,user:{issuerId}:---,group::---,mask::---,other::---"
        });
    }

    private static Task<int> DenyIssuerParentAclAsync(
        SavaWebApplicationFactory application, BlobContainerClient container, string issuerId)
    {
        var root = new HierarchicalAclManifestEntry
        {
            Account = SavaWebApplicationFactory.AccountName,
            Container = container.Name,
            Path = "",
            AccessAcl = $"user::rwx,user:{issuerId}:---,group::---,mask::---,other::---"
        };
        return ApplyAclManifestAsync(application, root, root with { Path = "parent" });
    }

    private static async Task<int> SendRawAppendBlockAsync(SavaWebApplicationFactory application, Uri signedUri)
    {
        using var transport = new HttpClient(application.Server.CreateHandler());
        using var request = new HttpRequestMessage(HttpMethod.Put, new Uri(signedUri + "&comp=appendblock"));
        request.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
        request.Content = new ByteArrayContent("y"u8.ToArray());
        using var response = await transport.SendAsync(request).ConfigureAwait(false);
        return (int)response.StatusCode;
    }
}
