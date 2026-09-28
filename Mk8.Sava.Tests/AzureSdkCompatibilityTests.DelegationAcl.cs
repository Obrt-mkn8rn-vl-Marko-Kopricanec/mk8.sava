using System.Net;
using System.Text;
using Azure;
using Azure.Core.Pipeline;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Sas;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

public sealed partial class AzureSdkCompatibilityTests
{
    [Theory]
    [InlineData(true, false)]
    [InlineData(true, true)]
    [InlineData(false, false)]
    [InlineData(false, true)]
    public async Task OrdinaryUserDelegationSasUsesIssuerAclOnlyWhenHnsAndRbacDoesNotGrant(bool hns, bool rbac)
    {
        const string issuerId = "702a566c-14a5-4a9b-ae78-1a5b82119bbd";
        var application = new SavaWebApplicationFactory(new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:HierarchicalNamespaceEnabled"] = hns ? "true" : "false",
            [$"Sava:BearerAuthentication:Principals:{issuerId}:Accounts:0"] = SavaWebApplicationFactory.AccountName,
            [$"Sava:BearerAuthentication:Principals:{issuerId}:Permissions"] = rbac ? "r" : "",
            [$"Sava:BearerAuthentication:Principals:{issuerId}:CanGenerateUserDelegationKey"] = "true"
        });
        await using var disposal = application.ConfigureAwait(true);
        await application.InitializeAsync().ConfigureAwait(true);
        var container = CreateClient(application).GetBlobContainerClient($"issuer-acl-{Guid.NewGuid():N}");
        await container.CreateAsync().ConfigureAwait(true);
        var blob = container.GetBlobClient("parent/file.txt");
        await blob.UploadAsync(BinaryData.FromString("issuer-acl-content")).ConfigureAwait(true);
        var file = hns
            ? await ConfigureIssuerAclAsync(application, container, blob, issuerId).ConfigureAwait(true)
            : null;

        var bearer = CreateBearerClient(application,
            CreateJwt(SavaWebApplicationFactory.AccountKey, issuerId, SavaWebApplicationFactory.TenantId));
        var starts = DateTimeOffset.UtcNow.AddMinutes(-1);
        var expires = DateTimeOffset.UtcNow.AddMinutes(5);
        var key = (await bearer.GetUserDelegationKeyAsync(new BlobGetUserDelegationKeyOptions(expires)
        {
            StartsOn = starts
        }).ConfigureAwait(true)).Value;
        var signed = CreateOrdinaryDelegationClient(application, container, blob, key, starts, expires);
        if (!hns && !rbac)
        {
            Assert.Equal(403, (await Assert.ThrowsAsync<RequestFailedException>(() => signed.DownloadContentAsync())
                .ConfigureAwait(true)).Status);
            return;
        }
        Assert.Equal("issuer-acl-content", (await signed.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
        if (hns)
        {
            await ApplyAclManifestAsync(application, file! with
            {
                AccessAcl = $"user::rw-,user:{issuerId}:---,group::r--,mask::r--,other::---"
            }).ConfigureAwait(true);
            if (rbac)
                Assert.Equal("issuer-acl-content", (await signed.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
            else
                Assert.Equal(403, (await Assert.ThrowsAsync<RequestFailedException>(() => signed.DownloadContentAsync())
                    .ConfigureAwait(true)).Status);
        }
        Assert.Equal("issuer-acl-content", (await blob.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
    }

    private static BlobClient CreateOrdinaryDelegationClient(
        SavaWebApplicationFactory application, BlobContainerClient container, BlobClient blob,
        UserDelegationKey key, DateTimeOffset starts, DateTimeOffset expires,
        BlobSasPermissions permissions = BlobSasPermissions.Read)
    {
        var builder = new BlobSasBuilder
        {
            BlobContainerName = container.Name,
            BlobName = blob.Name,
            Resource = "b",
            StartsOn = starts,
            ExpiresOn = expires
        };
        builder.SetPermissions(permissions);
        var sas = builder.ToSasQueryParameters(key, SavaWebApplicationFactory.AccountName).ToString();
        Assert.DoesNotContain("suoid=", sas, StringComparison.Ordinal);
        return new BlobClient(new Uri(blob.Uri + "?" + sas), new BlobClientOptions
        {
            Transport = new HttpClientTransport(application.Server.CreateHandler()),
            Retry = { MaxRetries = 0 }
        });
    }

    [Fact]
    public async Task OrdinaryDelegationIssuerGroupAclUsesPinnedGraphMembership()
    {
        const string issuerId = "fc045b29-63c7-4eca-b34e-19b9d5f9963c";
        const string groupId = "22138456-bf17-4b2f-a052-1459f400a93d";
        using var graph = new IssuerGraphHandler(issuerId, groupId);
        var application = new SavaWebApplicationFactory(new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:HierarchicalNamespaceEnabled"] = "true",
            [$"Sava:BearerAuthentication:Principals:{issuerId}:Accounts:0"] = SavaWebApplicationFactory.AccountName,
            [$"Sava:BearerAuthentication:Principals:{issuerId}:Permissions"] = "",
            [$"Sava:BearerAuthentication:Principals:{issuerId}:CanGenerateUserDelegationKey"] = "true",
            ["Sava:BearerAuthentication:GraphGroupResolution:Enabled"] = "true",
            ["Sava:BearerAuthentication:GraphGroupResolution:TenantId"] = SavaWebApplicationFactory.TenantId
        }, () => graph, new StaticTokenCredential("graph-test-token"));
        await using var disposal = application.ConfigureAwait(true);
        await application.InitializeAsync().ConfigureAwait(true);
        var container = CreateClient(application).GetBlobContainerClient($"issuer-group-{Guid.NewGuid():N}");
        await container.CreateAsync().ConfigureAwait(true);
        var blob = container.GetBlobClient("parent/file.txt");
        await blob.UploadAsync(BinaryData.FromString("group-only")).ConfigureAwait(true);
        var root = new HierarchicalAclManifestEntry
        {
            Account = SavaWebApplicationFactory.AccountName,
            Container = container.Name,
            Path = "",
            AccessAcl = $"user::rwx,group::---,group:{groupId}:--x,mask::r-x,other::---"
        };
        var file = root with
        {
            Path = blob.Name,
            AccessAcl = $"user::rw-,group::---,group:{groupId}:r--,mask::r--,other::---"
        };
        await ApplyAclManifestAsync(application, root, root with { Path = "parent" }, file).ConfigureAwait(true);
        var bearer = CreateBearerClient(application,
            CreateJwt(SavaWebApplicationFactory.AccountKey, issuerId, SavaWebApplicationFactory.TenantId));
        var starts = DateTimeOffset.UtcNow.AddMinutes(-1);
        var expires = DateTimeOffset.UtcNow.AddMinutes(5);
        var key = (await bearer.GetUserDelegationKeyAsync(new BlobGetUserDelegationKeyOptions(expires)
        {
            StartsOn = starts
        }).ConfigureAwait(true)).Value;
        var signed = CreateOrdinaryDelegationClient(application, container, blob, key, starts, expires);
        Assert.Equal("group-only", (await signed.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
        Assert.True(graph.Calls >= 1);
        await ApplyAclManifestAsync(application, file with
        {
            AccessAcl = $"user::rw-,group::---,group:{groupId}:---,mask::r--,other::---"
        }).ConfigureAwait(true);
        Assert.Equal(403, (await Assert.ThrowsAsync<RequestFailedException>(() => signed.DownloadContentAsync())
            .ConfigureAwait(true)).Status);
    }

    private sealed class IssuerGraphHandler(string issuerId, string groupId) : HttpMessageHandler
    {
        public int Calls { get; private set; }

        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Assert.Equal(HttpMethod.Post, request.Method);
            Assert.Equal($"https://graph.microsoft.com/v1.0/directoryObjects/{issuerId}/getMemberGroups",
                request.RequestUri?.ToString());
            Assert.Equal("Bearer", request.Headers.Authorization?.Scheme);
            Assert.Equal("graph-test-token", request.Headers.Authorization?.Parameter);
            Calls++;
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent($"{{\"value\":[\"{groupId}\"]}}", Encoding.UTF8, "application/json")
            });
        }
    }

    [Fact]
    public async Task UserDelegationSuoidChecksBothIssuerAndEndUserAclsIndependently()
    {
        const string issuerId = "99297780-992c-4241-8815-dc0057cd6eee";
        const string endUserId = "d7796de5-de35-493f-b117-4b7438a162aa";
        const string foreignId = "7bc3bf9a-65d4-4d70-a0a9-d9853d0a4ebc";
        var application = new SavaWebApplicationFactory(new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:HierarchicalNamespaceEnabled"] = "true",
            [$"Sava:BearerAuthentication:Principals:{issuerId}:Accounts:0"] = SavaWebApplicationFactory.AccountName,
            [$"Sava:BearerAuthentication:Principals:{issuerId}:Permissions"] = "",
            [$"Sava:BearerAuthentication:Principals:{issuerId}:CanGenerateUserDelegationKey"] = "true",
            [$"Sava:BearerAuthentication:Principals:{issuerId}:CanManageOwnership"] = "true"
        });
        await using var disposal = application.ConfigureAwait(true);
        await application.InitializeAsync().ConfigureAwait(true);
        var container = CreateClient(application).GetBlobContainerClient($"issuer-suoid-{Guid.NewGuid():N}");
        await container.CreateAsync().ConfigureAwait(true);
        var blob = container.GetBlobClient("parent/file.txt");
        await blob.UploadAsync(BinaryData.FromString("both-acls")).ConfigureAwait(true);
        var file = await ConfigureIssuerAclAsync(application, container, blob, issuerId, endUserId).ConfigureAwait(true);
        var bothAcls = file with
        {
            AccessAcl = $"user::rw-,user:{issuerId}:r--,user:{endUserId}:r--,group::r--,mask::r--,other::---"
        };
        await ApplyAclManifestAsync(application, bothAcls).ConfigureAwait(true);
        var bearer = CreateBearerClient(application,
            CreateJwt(SavaWebApplicationFactory.AccountKey, issuerId, SavaWebApplicationFactory.TenantId));
        var starts = DateTimeOffset.UtcNow.AddMinutes(-1);
        var expires = DateTimeOffset.UtcNow.AddMinutes(5);
        var key = (await bearer.GetUserDelegationKeyAsync(new BlobGetUserDelegationKeyOptions(expires)
        {
            StartsOn = starts
        }).ConfigureAwait(true)).Value;
        BlobClient Signed(string userId) => CreateSuoidBlobClient(
            application, key, container.Name, blob.Name, userId, "r", starts, expires);

        Assert.Equal("both-acls", (await Signed(endUserId).DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
        Assert.Equal(403, (await Assert.ThrowsAsync<RequestFailedException>(() => Signed(foreignId).DownloadContentAsync())
            .ConfigureAwait(true)).Status);
        await ApplyAclManifestAsync(application, bothAcls with
        {
            AccessAcl = $"user::rw-,user:{issuerId}:---,user:{endUserId}:r--,group::r--,mask::r--,other::---"
        }).ConfigureAwait(true);
        Assert.Equal(403, (await Assert.ThrowsAsync<RequestFailedException>(() => Signed(endUserId).DownloadContentAsync())
            .ConfigureAwait(true)).Status);
        await ApplyAclManifestAsync(application, bothAcls with
        {
            AccessAcl = $"user::rw-,user:{issuerId}:r--,user:{endUserId}:---,group::r--,mask::r--,other::---"
        }).ConfigureAwait(true);
        Assert.Equal(403, (await Assert.ThrowsAsync<RequestFailedException>(() => Signed(endUserId).DownloadContentAsync())
            .ConfigureAwait(true)).Status);
        Assert.Equal("both-acls", (await blob.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
    }

    private static async Task<HierarchicalAclManifestEntry> ConfigureIssuerAclAsync(
        SavaWebApplicationFactory application, BlobContainerClient container, BlobClient blob, string issuerId,
        string? endUserId = null)
    {
        var endUserTraverse = endUserId is null ? "" : $"user:{endUserId}:--x,";
        var endUserRead = endUserId is null ? "" : $"user:{endUserId}:r--,";
        var root = new HierarchicalAclManifestEntry
        {
            Account = SavaWebApplicationFactory.AccountName,
            Container = container.Name,
            Path = "",
            AccessAcl = $"user::rwx,user:{issuerId}:--x,{endUserTraverse}group::r-x,mask::r-x,other::---"
        };
        var file = root with
        {
            Path = blob.Name,
            AccessAcl = $"user::rw-,user:{issuerId}:r--,{endUserRead}group::r--,mask::r--,other::---"
        };
        await ApplyAclManifestAsync(application, root, root with { Path = "parent" }, file).ConfigureAwait(false);
        return file;
    }
}
