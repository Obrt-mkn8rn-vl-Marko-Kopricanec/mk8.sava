namespace Mk8.Sava.Storage;

internal static class PosixAccessControl
{
    internal static bool Allows(
        string acl,
        string owner,
        string owningGroup,
        string objectId,
        IReadOnlySet<string> groups,
        char requiredPermission)
    {
        var required = requiredPermission switch
        {
            'r' => 4,
            'w' => 2,
            'x' => 1,
            _ => throw new ArgumentOutOfRangeException(nameof(requiredPermission))
        };
        var parsed = Parse(acl);
        if (string.Equals(objectId, owner, StringComparison.OrdinalIgnoreCase))
            return Has(parsed.Owner, required);

        if (parsed.NamedUsers.TryGetValue(objectId, out var namedUser))
            return Has(namedUser & parsed.Mask, required);

        if (IsMember(groups, owningGroup) && Has(parsed.Group & parsed.Mask, required))
            return true;
        foreach (var (groupId, permissions) in parsed.NamedGroups)
        {
            if (IsMember(groups, groupId) && Has(permissions & parsed.Mask, required))
                return true;
        }

        return Has(parsed.Other, required);
    }

    internal static string FormatMode(string acl)
    {
        var parsed = Parse(acl);
        return Format(parsed.Owner) + Format(parsed.Group & parsed.Mask) + Format(parsed.Other);
    }

    internal static void ValidateStoredAcl(string acl, bool isDirectory)
    {
        var entries = acl.Split(',');
        var access = entries.Where(entry => !entry.StartsWith("default:", StringComparison.Ordinal)).ToArray();
        var defaults = entries.Where(entry => entry.StartsWith("default:", StringComparison.Ordinal))
            .Select(entry => entry["default:".Length..]).ToArray();
        if (access.Length is < 3 or > 32 || defaults.Length > 32 || defaults.Length > 0 && !isDirectory)
            throw new InvalidDataException("The hierarchical ACL has invalid access or default entries.");
        _ = Parse(string.Join(',', access));
        if (defaults.Length > 0)
            _ = Parse(string.Join(',', defaults));
    }

    internal static string? InheritDefaultAcl(string parentAcl, bool childIsDirectory)
    {
        if (!parentAcl.Contains("default:", StringComparison.Ordinal))
            return null;
        ValidateStoredAcl(parentAcl, isDirectory: true);
        var defaults = parentAcl.Split(',')
            .Where(entry => entry.StartsWith("default:", StringComparison.Ordinal))
            .ToArray();
        var access = string.Join(',', defaults.Select(entry => entry["default:".Length..]));
        return childIsDirectory ? access + "," + string.Join(',', defaults) : access;
    }

    private static ParsedAcl Parse(string acl)
    {
        var users = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        var groups = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        int? owner = null;
        int? owningGroup = null;
        int? other = null;
        int? mask = null;

        foreach (var rawEntry in acl.Split(','))
        {
            var (type, identity, permissions, isDefault) = ParseEntry(rawEntry);
            if (isDefault)
                continue;

            switch (type)
            {
                case "user" when identity.Length == 0:
                    if (owner is not null)
                        throw new InvalidDataException("The hierarchical access ACL has duplicate owner entries.");
                    owner = permissions;
                    break;
                case "user":
                    if (!users.TryAdd(identity, permissions))
                        throw new InvalidDataException("The hierarchical access ACL has duplicate named users.");
                    break;
                case "group" when identity.Length == 0:
                    if (owningGroup is not null)
                        throw new InvalidDataException("The hierarchical access ACL has duplicate owning groups.");
                    owningGroup = permissions;
                    break;
                case "group":
                    if (!groups.TryAdd(identity, permissions))
                        throw new InvalidDataException("The hierarchical access ACL has duplicate named groups.");
                    break;
                case "mask" when identity.Length == 0:
                    if (mask is not null)
                        throw new InvalidDataException("The hierarchical access ACL has duplicate masks.");
                    mask = permissions;
                    break;
                case "other" when identity.Length == 0:
                    if (other is not null)
                        throw new InvalidDataException("The hierarchical access ACL has duplicate other entries.");
                    other = permissions;
                    break;
                default:
                    throw new InvalidDataException("The hierarchical access ACL has an invalid identity entry.");
            }
        }

        if (owner is null || owningGroup is null || other is null)
            throw new InvalidDataException("The hierarchical access ACL is missing a required entry.");

        return new ParsedAcl(owner.Value, owningGroup.Value, other.Value, mask ?? 7, users, groups);
    }

    private static (string Type, string Identity, int Permissions, bool IsDefault) ParseEntry(string rawEntry)
    {
        var entry = rawEntry.Split(':');
        var isDefault = entry.Length == 4 &&
                        string.Equals(entry[0], "default", StringComparison.Ordinal);
        var offset = isDefault ? 1 : 0;
        if (entry.Length - offset != 3)
            throw new InvalidDataException("The hierarchical access ACL has an invalid entry.");
        return (entry[offset], entry[offset + 1], ParsePermissions(entry[offset + 2]), isDefault);
    }

    private static int ParsePermissions(string value)
    {
        if (value.Length != 3 ||
            value[0] is not ('r' or '-') ||
            value[1] is not ('w' or '-') ||
            value[2] is not ('x' or '-'))
        {
            throw new InvalidDataException("The hierarchical access ACL has invalid permissions.");
        }

        return (value[0] == 'r' ? 4 : 0) |
               (value[1] == 'w' ? 2 : 0) |
               (value[2] == 'x' ? 1 : 0);
    }

    private static bool Has(int permissions, int required) => (permissions & required) == required;

    private static bool IsMember(IReadOnlySet<string> groups, string groupId) =>
        groups.Any(value => string.Equals(value, groupId, StringComparison.OrdinalIgnoreCase));

    private static string Format(int permissions) => string.Create(3, permissions, static (characters, bits) =>
    {
        characters[0] = (bits & 4) != 0 ? 'r' : '-';
        characters[1] = (bits & 2) != 0 ? 'w' : '-';
        characters[2] = (bits & 1) != 0 ? 'x' : '-';
    });

    private sealed record ParsedAcl(
        int Owner,
        int Group,
        int Other,
        int Mask,
        IReadOnlyDictionary<string, int> NamedUsers,
        IReadOnlyDictionary<string, int> NamedGroups);
}
