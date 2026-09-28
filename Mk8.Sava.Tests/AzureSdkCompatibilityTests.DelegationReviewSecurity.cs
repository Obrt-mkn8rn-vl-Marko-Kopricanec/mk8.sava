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
    [InlineData(false)]
    [InlineData(true)]
    public async Task DelegationKeysWithIdenticalTimesDoNotShareTrustedRoleGrants(bool hns)
    {
        const string issuerId = "d4618c6e-06d2-408c-a5d0-79b9c5df1450";
        var application = new SavaWebApplicationFactory(CreateDelegationRoleConfiguration(hns, issuerId, "", "r"));
        await using var disposal = application.ConfigureAwait(true);
        await application.InitializeAsync().ConfigureAwait(true);
        var container = CreateClient(application).GetBlobContainerClient($"role-collision-{Guid.NewGuid():N}");
        await container.CreateAsync().ConfigureAwait(true);
        var blob = container.GetBlobClient("parent/file.txt");
        await blob.UploadAsync(BinaryData.FromString("isolated")).ConfigureAwait(true);
        if (hns)
            await DenyIssuerAclAsync(application, container, blob.Name, issuerId).ConfigureAwait(true);
        var starts = DateTimeOffset.UtcNow.AddMinutes(-1);
        var expires = DateTimeOffset.UtcNow.AddMinutes(5);
        var noRole = CreateBearerClient(application,
            CreateJwt(SavaWebApplicationFactory.AccountKey, issuerId, SavaWebApplicationFactory.TenantId));
        var withRole = CreateBearerClient(application,
            CreateJwt(SavaWebApplicationFactory.AccountKey, issuerId, SavaWebApplicationFactory.TenantId,
                roles: ["BlobReader"]));
        var noRoleKey = (await noRole.GetUserDelegationKeyAsync(new BlobGetUserDelegationKeyOptions(expires)
        {
            StartsOn = starts
        }).ConfigureAwait(true)).Value;
        var roleKey = (await withRole.GetUserDelegationKeyAsync(new BlobGetUserDelegationKeyOptions(expires)
        {
            StartsOn = starts
        }).ConfigureAwait(true)).Value;
        var repeatedRoleKey = (await withRole.GetUserDelegationKeyAsync(new BlobGetUserDelegationKeyOptions(expires)
        {
            StartsOn = starts
        }).ConfigureAwait(true)).Value;
        Assert.Equal(roleKey.Value, repeatedRoleKey.Value, StringComparer.Ordinal);
        Assert.False(string.Equals(noRoleKey.Value, roleKey.Value, StringComparison.Ordinal));
        var denied = CreateOrdinaryDelegationClient(application, container, blob, noRoleKey, starts, expires);
        var permitted = CreateOrdinaryDelegationClient(application, container, blob, roleKey, starts, expires);
        Assert.Equal(403, (await Assert.ThrowsAsync<RequestFailedException>(() => denied.DownloadContentAsync())
            .ConfigureAwait(true)).Status);
        Assert.Equal("isolated", (await permitted.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
        Assert.Equal(403, (await Assert.ThrowsAsync<RequestFailedException>(() => denied.DownloadContentAsync())
            .ConfigureAwait(true)).Status);
    }

    [Theory]
    [InlineData(false, "", null)]
    [InlineData(true, "", null)]
    [InlineData(false, "w", null)]
    [InlineData(true, "w", null)]
    [InlineData(false, "", "w")]
    [InlineData(true, "", "w")]
    public async Task DelegationRoleRemovalOrChangeRevokesOnlyRoleDerivedRights(
        bool hns, string directPermissions, string? changedRolePermissions)
    {
        ArgumentNullException.ThrowIfNull(directPermissions);
        const string issuerId = "5a006544-af9f-4ef5-8335-432050f0b49e";
        var dataPath = Path.Combine(Path.GetTempPath(), $"mk8-sava-role-revoke-{Guid.NewGuid():N}");
        try
        {
            var issued = await IssueRevocableRoleKeyAsync(
                dataPath, hns, issuerId, directPermissions).ConfigureAwait(true);
            var second = new SavaWebApplicationFactory(dataPath,
                CreateDelegationRoleConfiguration(hns, issuerId, directPermissions, changedRolePermissions),
                deleteDataPath: false);
            await using var disposal = second.ConfigureAwait(true);
            await second.InitializeAsync().ConfigureAwait(true);
            var container = CreateClient(second).GetBlobContainerClient(issued.ContainerName);
            var blob = container.GetBlobClient("parent/file.txt");
            var sasPermissions = directPermissions.Length == 0
                ? BlobSasPermissions.Read : BlobSasPermissions.Read | BlobSasPermissions.Write;
            var signed = CreateOrdinaryDelegationClient(
                second, container, blob, issued.Key, issued.Starts, issued.Expires, sasPermissions);
            Assert.Equal(403, (await Assert.ThrowsAsync<RequestFailedException>(() => signed.DownloadContentAsync())
                .ConfigureAwait(true)).Status);
            if (directPermissions.Length > 0)
                await signed.UploadAsync(BinaryData.FromString("direct-still-valid"), overwrite: true).ConfigureAwait(true);
            else if (changedRolePermissions is not null)
                Assert.Equal(403, (await Assert.ThrowsAsync<RequestFailedException>(() =>
                    signed.UploadAsync(BinaryData.FromString("new-role-write"), overwrite: true))
                    .ConfigureAwait(true)).Status);
        }
        finally
        {
            if (Directory.Exists(dataPath))
                await SavaWebApplicationFactory.DeleteDataPathAsync(dataPath).ConfigureAwait(true);
        }
    }

    private static async Task<(string ContainerName, UserDelegationKey Key, DateTimeOffset Starts, DateTimeOffset Expires)>
        IssueRevocableRoleKeyAsync(string dataPath, bool hns, string issuerId, string directPermissions)
    {
        var first = new SavaWebApplicationFactory(dataPath,
            CreateDelegationRoleConfiguration(hns, issuerId, directPermissions, "r"), deleteDataPath: false);
        await using var disposal = first.ConfigureAwait(true);
        await first.InitializeAsync().ConfigureAwait(true);
        var container = CreateClient(first).GetBlobContainerClient($"role-revoke-{Guid.NewGuid():N}");
        await container.CreateAsync().ConfigureAwait(true);
        var blob = container.GetBlobClient("parent/file.txt");
        await blob.UploadAsync(BinaryData.FromString("role-current")).ConfigureAwait(true);
        if (hns)
            await DenyIssuerAclAsync(first, container, blob.Name, issuerId).ConfigureAwait(true);
        var bearer = CreateBearerClient(first,
            CreateJwt(SavaWebApplicationFactory.AccountKey, issuerId, SavaWebApplicationFactory.TenantId,
                roles: ["BlobReader"]));
        var starts = DateTimeOffset.UtcNow.AddMinutes(-1);
        var expires = DateTimeOffset.UtcNow.AddMinutes(5);
        var key = (await bearer.GetUserDelegationKeyAsync(new BlobGetUserDelegationKeyOptions(expires)
        {
            StartsOn = starts
        }).ConfigureAwait(true)).Value;
        var signed = CreateOrdinaryDelegationClient(first, container, blob, key, starts, expires);
        Assert.Equal("role-current", (await signed.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
        return (container.Name, key, starts, expires);
    }

    private static Dictionary<string, string?> CreateDelegationRoleConfiguration(
        bool hns, string issuerId, string directPermissions, string? rolePermissions)
    {
        var configuration = new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:HierarchicalNamespaceEnabled"] = hns ? "true" : "false",
            [$"Sava:BearerAuthentication:Principals:{issuerId}:Accounts:0"] = SavaWebApplicationFactory.AccountName,
            [$"Sava:BearerAuthentication:Principals:{issuerId}:Permissions"] = directPermissions,
            [$"Sava:BearerAuthentication:Principals:{issuerId}:CanGenerateUserDelegationKey"] = "true"
        };
        if (rolePermissions is not null)
            configuration["Sava:BearerAuthentication:RolePermissions:BlobReader"] = rolePermissions;
        return configuration;
    }

    [Fact]
    public async Task DelegationAppendWithSuoidRequiresEndUserAclSeparatelyFromIssuerAddGrant()
    {
        const string issuerId = "016217ec-e22a-419c-97cf-807b6f483b96";
        const string endUserId = "7770d2dd-bc93-4320-a8e1-359e475ed3e4";
        var configuration = CreateDelegationRoleConfiguration(true, issuerId, "a", null);
        configuration[$"Sava:BearerAuthentication:Principals:{issuerId}:CanManageOwnership"] = "true";
        var application = new SavaWebApplicationFactory(configuration);
        await using var disposal = application.ConfigureAwait(true);
        await application.InitializeAsync().ConfigureAwait(true);
        var container = CreateClient(application).GetBlobContainerClient($"review-suoid-{Guid.NewGuid():N}");
        await container.CreateAsync().ConfigureAwait(true);
        var append = container.GetAppendBlobClient("parent/log.txt");
        await append.CreateAsync().ConfigureAwait(true);
        var root = new HierarchicalAclManifestEntry
        {
            Account = SavaWebApplicationFactory.AccountName,
            Container = container.Name,
            Path = "",
            AccessAcl = $"user::rwx,user:{issuerId}:---,user:{endUserId}:--x,group::---,mask::--x,other::---"
        };
        var file = root with
        {
            Path = append.Name,
            AccessAcl = $"user::rw-,user:{issuerId}:---,user:{endUserId}:rw-,group::---,mask::rw-,other::---"
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
        var signedUri = CreateSuoidBlobClient(
            application, key, container.Name, append.Name, endUserId, "aw", starts, expires).Uri;
        var signed = new AppendBlobClient(signedUri, new BlobClientOptions
        {
            Transport = new HttpClientTransport(application.Server.CreateHandler()),
            Retry = { MaxRetries = 0 }
        });
        using (var body = BinaryData.FromString("x").ToStream())
            await signed.AppendBlockAsync(body).ConfigureAwait(true);
        await ApplyAclManifestAsync(application, file with
        {
            AccessAcl = $"user::rw-,user:{issuerId}:---,user:{endUserId}:r--,group::---,mask::rw-,other::---"
        }).ConfigureAwait(true);
        using var deniedBody = BinaryData.FromString("y").ToStream();
        Assert.Equal(403, (await Assert.ThrowsAsync<RequestFailedException>(() => signed.AppendBlockAsync(deniedBody))
            .ConfigureAwait(true)).Status);
        Assert.Equal("x", (await append.DownloadContentAsync().ConfigureAwait(true)).Value.Content.ToString());
    }
}
