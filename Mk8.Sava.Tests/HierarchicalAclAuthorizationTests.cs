using Mk8.Sava.Protocol;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

public sealed class HierarchicalAclAuthorizationTests
{
    [Fact]
    public void PosixAclHonorsOwnerNamedUserGroupsMaskAndOtherPrecedence()
    {
        const string acl = "user::rw-,user:named:r--,user:owner:--x,group::r--," +
                           "group:team:--x,mask::r--,other::--x";
        var noGroups = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var team = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "team" };
        var owningGroup = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "owning-group" };

        Assert.True(PosixAccessControl.Allows(acl, "owner", "owning-group", "owner", noGroups, 'w'));
        Assert.False(PosixAccessControl.Allows(acl, "owner", "owning-group", "owner", noGroups, 'x'));
        Assert.True(PosixAccessControl.Allows(acl, "owner", "owning-group", "named", noGroups, 'r'));
        Assert.False(PosixAccessControl.Allows(acl, "owner", "owning-group", "named", noGroups, 'x'));
        Assert.True(PosixAccessControl.Allows(acl, "owner", "owning-group", "member", owningGroup, 'r'));
        Assert.False(PosixAccessControl.Allows(acl, "owner", "owning-group", "member", team, 'r'));
        Assert.True(PosixAccessControl.Allows(acl, "owner", "owning-group", "stranger", noGroups, 'x'));
        Assert.Equal("rw-r----x", PosixAccessControl.FormatMode(acl));
    }

    [Fact]
    public void PosixAclIgnoresDefaultEntriesDuringAccessChecksAndRejectsMalformedAccessEntries()
    {
        const string acl = "user::rwx,group::r-x,other::---,default:user::rwx,default:other::rwx";
        Assert.False(PosixAccessControl.Allows(
            acl,
            "owner",
            "owning-group",
            "stranger",
            new HashSet<string>(StringComparer.OrdinalIgnoreCase),
            'r'));

        Assert.Throws<InvalidDataException>(() => PosixAccessControl.FormatMode("user::rwx,group::r-x"));
        Assert.Throws<InvalidDataException>(() => PosixAccessControl.FormatMode(
            "user::rwx,group::r-x,other::---,user::rwx"));
        Assert.Throws<InvalidDataException>(() => PosixAccessControl.FormatMode(
            "user::rwx,group::bad,other::---"));
    }

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
