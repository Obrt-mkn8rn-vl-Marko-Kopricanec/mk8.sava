using System.Reflection;

namespace Mk8.Sava.Transport;

internal sealed class RpcContract(Type type)
{
    internal Type Type { get; } = type;
    internal Dictionary<string, RpcMethod> Methods { get; } = type.GetMethods()
        .Where(method => method.ReturnType == typeof(Task) ||
                         (method.ReturnType.IsGenericType && method.ReturnType.GetGenericTypeDefinition() == typeof(Task<>)))
        .Select(method => new RpcMethod(method))
        .ToDictionary(method => method.Id, StringComparer.Ordinal);

    internal RpcMethod GetMethod(MethodInfo method) => Methods.Values.First(candidate =>
        string.Equals(candidate.Method.Name, method.Name, StringComparison.Ordinal) &&
        candidate.Parameters.Select(parameter => parameter.ParameterType)
            .SequenceEqual(method.GetParameters().Select(parameter => parameter.ParameterType)));
}
