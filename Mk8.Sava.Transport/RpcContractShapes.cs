using System.Globalization;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.Json.Serialization;

namespace Mk8.Sava.Transport;

internal static class RpcContractShapes
{
    internal static string Describe(IEnumerable<Type> roots)
    {
        var visited = new HashSet<Type>();
        var lines = new List<string>();
        foreach (var root in roots)
            AddType(root, visited, lines);
        return string.Join('\n', lines.Order(StringComparer.Ordinal));
    }

    private static void AddType(Type type, HashSet<Type> visited, List<string> lines)
    {
        if (!visited.Add(type))
            return;
        lines.Add("type:" + type.FullName);
        if (type.GetElementType() is { } element)
            AddType(element, visited, lines);
        foreach (var argument in type.GetGenericArguments())
            AddType(argument, visited, lines);
        if (type.Namespace is null || type.Namespace.StartsWith("System", StringComparison.Ordinal))
            return;
        lines.Add("converter:" + type.FullName + ":" + type.GetCustomAttribute<JsonConverterAttribute>()?.ConverterType?.FullName);
        if (type.IsEnum)
        {
            foreach (var field in type.GetFields(BindingFlags.Public | BindingFlags.Static))
                lines.Add("enum:" + type.FullName + ":" + field.Name + ":" +
                    Convert.ToString(field.GetRawConstantValue(), CultureInfo.InvariantCulture));
            return;
        }
        foreach (var property in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            if (property.GetIndexParameters().Length == 0)
                AddMember(type, property, property.PropertyType, property.CanRead, property.CanWrite, visited, lines);
        }
        foreach (var field in type.GetFields(BindingFlags.Public | BindingFlags.Instance))
            AddMember(type, field, field.FieldType, canRead: true, canWrite: !field.IsInitOnly, visited, lines);
    }

    private static void AddMember(Type owner, MemberInfo member, Type memberType,
        bool canRead, bool canWrite, HashSet<Type> visited, List<string> lines)
    {
        var ignored = member.GetCustomAttribute<JsonIgnoreAttribute>();
        if (ignored?.Condition == JsonIgnoreCondition.Always || RpcJson.IsGatewayUnusedProperty(owner, member.Name))
            return;
        var name = member.GetCustomAttribute<JsonPropertyNameAttribute>()?.Name ?? member.Name;
        var required = member.IsDefined(typeof(JsonRequiredAttribute)) || member.IsDefined(typeof(RequiredMemberAttribute));
        lines.Add("member:" + owner.FullName + ":" + name + ":" + memberType.FullName + ":" +
            canRead + ":" + canWrite + ":" + required + ":" + ignored?.Condition + ":" +
            member.GetCustomAttribute<JsonConverterAttribute>()?.ConverterType?.FullName);
        AddType(memberType, visited, lines);
    }
}
