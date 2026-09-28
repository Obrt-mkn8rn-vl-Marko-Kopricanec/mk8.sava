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
