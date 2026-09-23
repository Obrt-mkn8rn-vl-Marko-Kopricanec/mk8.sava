using Microsoft.AspNetCore.Http;
using Mk8.Sava.Protocol;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

public sealed class HierarchicalAclAuthorizationTests
{
    [Theory]
    [InlineData("PUT", "appendblock", "", true)]
    [InlineData("PUT", "AppendBlock", "", true)]
    [InlineData("PUT", "appendblock", "snapshot=2026-01-01", false)]
    [InlineData("GET", "appendblock", "", false)]
    [InlineData("PUT", "block", "", false)]
    public void AppendFallbackOnlyTargetsTheCurrentBlobAppendOperation(
        string method, string component, string additionalQuery, bool expected)
    {
        ArgumentNullException.ThrowIfNull(additionalQuery);
        var request = new DefaultHttpContext().Request;
        request.Method = method;
        request.QueryString = new QueryString(
            $"?comp={component}" + (additionalQuery.Length == 0 ? string.Empty : $"&{additionalQuery}"));
        var resource = new StorageRequestContext
        {
            RequestId = "test",
            Account = "account",
            Container = "container",
            Blob = "blob",
            ResourceKind = StorageResourceKind.Blob,
            CanonicalResourcePath = "/account/container/blob",
            ServiceVersion = "2023-11-03",
            Authorization = StorageAuthorization.Anonymous
        };
        Assert.Equal(expected, HierarchicalAclAuthorization.IsAppendOperation(request, resource));
    }

    [Theory]
    [InlineData("PUT", "", "", 'w')]
    [InlineData("PUT", "block", "", 'w')]
    [InlineData("PUT", "blocklist", "", 'w')]
    [InlineData("PUT", "metadata", "", 'w')]
    [InlineData("PUT", "properties", "", 'w')]
    [InlineData("DELETE", "", "", 'd')]
    [InlineData("GET", "", "", null)]
    [InlineData("PUT", "tags", "", null)]
    [InlineData("DELETE", "", "deletetype=permanent", null)]
    [InlineData("DELETE", "", "snapshot=2026-01-01", null)]
    public void ParentMutationFallbackTargetsCurrentBlobWritesAndDelete(
        string method, string component, string additionalQuery, char? expected)
    {
        ArgumentNullException.ThrowIfNull(additionalQuery);
        var request = new DefaultHttpContext().Request;
        request.Method = method;
        request.QueryString = new QueryString(
            $"?comp={component}" + (additionalQuery.Length == 0 ? string.Empty : $"&{additionalQuery}"));
        var resource = new StorageRequestContext
        {
            RequestId = "test",
            Account = "account",
            Container = "container",
            Blob = "blob",
            ResourceKind = StorageResourceKind.Blob,
            CanonicalResourcePath = "/account/container/blob",
            ServiceVersion = "2023-11-03",
            Authorization = StorageAuthorization.Anonymous
        };
        Assert.Equal(expected, HierarchicalAclAuthorization.GetParentMutationPermission(request, resource));
    }

    [Theory]
    [InlineData("GET", "/", "", "", "", true)]
    [InlineData("GET", "/", "one/two/", "files", "metadata", true)]
    [InlineData("GET", "", "", "", "", false)]
    [InlineData("GET", "/", "one", "", "", false)]
    [InlineData("GET", "/", "/one/", "", "", false)]
    [InlineData("GET", "/", "one//two/", "", "", false)]
    [InlineData("GET", "/", "", "deleted", "", false)]
    [InlineData("GET", "/", "", "", "deleted", false)]
    [InlineData("GET", "/", "", "", "tags", false)]
    [InlineData("POST", "/", "", "", "", false)]
    public void AclListFallbackRequiresAPlainHierarchicalDirectoryListing(
        string method, string delimiter, string prefix, string showOnly, string include, bool expected)
    {
        var request = new DefaultHttpContext().Request;
        request.Method = method;
        request.QueryString = new QueryString(
            $"?comp=list&delimiter={Uri.EscapeDataString(delimiter)}" +
            $"&prefix={Uri.EscapeDataString(prefix)}&showonly={showOnly}&include={include}");
        var resource = new StorageRequestContext
        {
            RequestId = "test",
            Account = "account",
            Container = "container",
            ResourceKind = StorageResourceKind.Container,
            CanonicalResourcePath = "/account/container",
            ServiceVersion = "2023-11-03",
            Authorization = StorageAuthorization.Anonymous
        };
        Assert.Equal(expected, HierarchicalAclAuthorization.IsDirectoryListOperation(request, resource));
    }

    [Theory]
    [InlineData("GET", "", true)]
    [InlineData("HEAD", "metadata", true)]
    [InlineData("GET", "metadata", true)]
    [InlineData("POST", "query", true)]
    [InlineData("GET", "tags", false)]
    [InlineData("GET", "blocklist", false)]
    [InlineData("PUT", "metadata", false)]
    [InlineData("POST", "queryOther", false)]
    public void AclFallbackOnlyTargetsBlobContentReadOperations(string method, string component, bool expected)
    {
        ArgumentNullException.ThrowIfNull(component);
        var request = new DefaultHttpContext().Request;
        request.Method = method;
        if (component.Length > 0)
            request.QueryString = new QueryString($"?comp={component}");
        Assert.Equal(expected, HierarchicalAclAuthorization.IsBlobReadOperation(request));
    }

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
        Assert.Equal("rw-r----t", PosixAccessControl.FormatMode(acl, stickyBit: true));
        Assert.Equal("rwxr-x--T", PosixAccessControl.FormatMode(
            "user::rwx,group::r-x,other::---", stickyBit: true));

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
