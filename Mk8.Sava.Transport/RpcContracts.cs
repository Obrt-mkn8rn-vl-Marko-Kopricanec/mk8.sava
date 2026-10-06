using System.Security.Cryptography;
using System.Text;

namespace Mk8.Sava.Transport;

internal static class RpcContracts
{
    private static readonly string[] AllowedNames =
    [
        "Mk8.Sava.Application.IBlobApplication",
        "Mk8.Sava.Application.IMetadataApplication",
        "Mk8.Sava.Application.IApplicationReadSessions",
        "Mk8.Sava.Application.IApplicationReadiness",
        "Mk8.Sava.Storage.IStorageAnalyticsSink",
    ];

    private static readonly Lazy<Dictionary<string, RpcContract>> Contracts = new(() =>
        AllowedNames.Select(name => typeof(ApplicationTransportOptions).Assembly.GetType(name, throwOnError: true)!)
            .ToDictionary(type => type.FullName!, type => new RpcContract(type), StringComparer.Ordinal));

    internal static string Fingerprint { get; } = CreateFingerprint();

    private static string CreateFingerprint()
    {
        var operations = string.Join('\n', Contracts.Value.OrderBy(pair => pair.Key, StringComparer.Ordinal).SelectMany(pair =>
            pair.Value.Methods.Keys.Order(StringComparer.Ordinal).Select(method => pair.Key + ":" + method)));
        var methods = Contracts.Value.Values.SelectMany(contract => contract.Methods.Values).ToArray();
        var roots = methods.SelectMany(method => method.Parameters
            .Where((_, index) => method.IsControlParameter(index)).Select(parameter => parameter.ParameterType))
            .Concat(methods.Select(method => method.ResultType).OfType<Type>())
            .Concat([typeof(RpcRequestPayload), typeof(RpcResponsePayload), typeof(RpcError), typeof(Mk8.Sava.Storage.PageRangeDiff)]);
        var shapes = RpcContractShapes.Describe(roots);
        return Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(operations + "\n" + shapes)));
    }

    internal static RpcContract GetContract(string name) => Contracts.Value.TryGetValue(name, out var contract)
        ? contract : throw new InvalidDataException("The application contract is not supported.");

    internal static RpcContract GetContract(Type type)
    {
        var contract = GetContract(type.FullName ?? string.Empty);
        if (contract.Type != type)
            throw new InvalidOperationException("Only the fixed application contracts can be proxied.");
        return contract;
    }
}
