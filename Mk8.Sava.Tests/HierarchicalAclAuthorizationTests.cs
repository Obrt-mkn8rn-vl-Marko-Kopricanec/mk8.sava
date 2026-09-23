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

        const string permissiveOther = "user::---,group::---,group:team:---,mask::---,other::rwx";
        Assert.True(PosixAccessControl.Allows(
            permissiveOther, "owner", "owning-group", "member", team, 'r'));
        Assert.True(PosixAccessControl.Allows(
            permissiveOther, "owner", "owning-group", "stranger", noGroups, 'r'));
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

        const string inherited = "user::rwx,group::r-x,other::---," +
                                 "default:user::rwx,default:user:reader:r-x," +
                                 "default:group::r-x,default:mask::r-x,default:other::---";
        PosixAccessControl.ValidateStoredAcl(inherited, isDirectory: true);
        var childFile = PosixAccessControl.InheritDefaultAcl(inherited, childIsDirectory: false);
        Assert.Equal("user::rwx,user:reader:r-x,group::r-x,mask::r-x,other::---", childFile);
        Assert.Equal(childFile + ",default:user::rwx,default:user:reader:r-x," +
                     "default:group::r-x,default:mask::r-x,default:other::---",
            PosixAccessControl.InheritDefaultAcl(inherited, childIsDirectory: true));
        Assert.Throws<InvalidDataException>(() => PosixAccessControl.ValidateStoredAcl(
            inherited, isDirectory: false));
        Assert.Throws<InvalidDataException>(() => PosixAccessControl.ValidateStoredAcl(
            "user::rwx,group::r-x,other::---,default:user::rwx,default:user::r--," +
            "default:group::r-x,default:other::---", isDirectory: true));
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
