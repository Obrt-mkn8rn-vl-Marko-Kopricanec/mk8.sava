using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

public sealed partial class AzureSdkCompatibilityTests
{
    [Fact]
    public async Task DelegationCreateKeepsIssuerAndSuoidParentAclsSeparate()
    {
        const string issuerId = "1e9d84ef-fb52-405a-95e6-d977846d1070";
        const string endUserId = "1059c5b8-f838-4954-8ba4-9119ff3d32b9";
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
        var container = CreateClient(application).GetBlobContainerClient($"review-suoid-create-{Guid.NewGuid():N}");
        await container.CreateAsync().ConfigureAwait(true);
        await container.GetBlobClient("parent/seed.txt").UploadAsync(BinaryData.FromString("seed")).ConfigureAwait(true);
        var root = new HierarchicalAclManifestEntry
        {
            Account = SavaWebApplicationFactory.AccountName,
            Container = container.Name,
            Path = "",
            AccessAcl = $"user::rwx,user:{issuerId}:--x,user:{endUserId}:--x,group::---,mask::--x,other::---"
        };
        var parent = root with
        {
            Path = "parent",
            AccessAcl = $"user::rwx,user:{issuerId}:-wx,user:{endUserId}:-wx,group::---,mask::-wx,other::---"
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
        BlobClient Signed(string name) => CreateSuoidBlobClient(
            application, key, container.Name, name, endUserId, "c", starts, expires);
        await Signed("parent/allowed.txt").UploadAsync(BinaryData.FromString("allowed")).ConfigureAwait(true);
        Assert.Equal(403, (await Assert.ThrowsAsync<RequestFailedException>(() =>
            Signed("parent/allowed.txt").UploadAsync(BinaryData.FromString("overwrite"), overwrite: true))
            .ConfigureAwait(true)).Status);
        await ApplyAclManifestAsync(application, parent with
        {
            AccessAcl = $"user::rwx,user:{issuerId}:-wx,user:{endUserId}:--x,group::---,mask::-wx,other::---"
        }).ConfigureAwait(true);
        Assert.Equal(403, (await Assert.ThrowsAsync<RequestFailedException>(() =>
            Signed("parent/end-user-denied.txt").UploadAsync(BinaryData.FromString("denied")))
            .ConfigureAwait(true)).Status);
        await ApplyAclManifestAsync(application, parent with
        {
            AccessAcl = $"user::rwx,user:{issuerId}:--x,user:{endUserId}:-wx,group::---,mask::-wx,other::---"
        }).ConfigureAwait(true);
        Assert.Equal(403, (await Assert.ThrowsAsync<RequestFailedException>(() =>
            Signed("parent/issuer-denied.txt").UploadAsync(BinaryData.FromString("denied")))
            .ConfigureAwait(true)).Status);
    }
}
