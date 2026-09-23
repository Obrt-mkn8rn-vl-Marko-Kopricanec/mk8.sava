using Mk8.Sava.Protocol;

namespace Mk8.Sava.Tests;

public sealed class HierarchicalAclAuthorizationTests
{
    [Fact]
    public void AclReadCannotServeAReplacementOrPreviouslyMissingGeneration()
    {
        var checkedRead = new StorageAuthorization(
            StorageAuthorizationKind.Bearer,
            "r",
            AclReadChecked: true,
            AclAuthorizedGenerationId: "authorized-generation");
        HierarchicalAclAuthorization.EnsureAuthorizedGeneration(
            checkedRead,
            "authorized-generation");
        Assert.Equal(
            "AuthorizationFailure",
            Assert.Throws<AzureStorageException>(() =>
                HierarchicalAclAuthorization.EnsureAuthorizedGeneration(
                    checkedRead,
                    "replacement-generation")).ErrorCode);

        var missingAtAuthorization = checkedRead with { AclAuthorizedGenerationId = null };
        Assert.Equal(
            "AuthorizationFailure",
            Assert.Throws<AzureStorageException>(() =>
                HierarchicalAclAuthorization.EnsureAuthorizedGeneration(
                    missingAtAuthorization,
                    "new-generation")).ErrorCode);

        HierarchicalAclAuthorization.EnsureAuthorizedGeneration(
            StorageAuthorization.Owner,
            "any-generation");
    }
}
