using System.Reflection;
using System.Runtime.ExceptionServices;
using System.Text.Json;

namespace Mk8.Sava.Transport;

// DispatchProxy requires a visible, nonsealed proxy base with a public
// parameterless constructor. Only ApplicationRpcClient configures usable instances.
public class ApplicationRpcProxy : DispatchProxy
{
    private ApplicationRpcClient? owner;
    private RpcContract? contract;
    private object? localPolicy;

    internal void Initialize(ApplicationRpcClient rpcClient, RpcContract rpcContract, object? policyImplementation)
    {
        owner = rpcClient;
        contract = rpcContract;
        localPolicy = policyImplementation;
    }

    protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
    {
        ArgumentNullException.ThrowIfNull(targetMethod);
        ArgumentNullException.ThrowIfNull(args);
        if (owner is null || contract is null)
            throw new InvalidOperationException("The application proxy is not initialized.");
        if (targetMethod.ReturnType == typeof(bool))
            return InvokeLocalPolicy(targetMethod, args);
        var method = contract.GetMethod(targetMethod);
        var tokenIndex = Array.FindIndex(method.Parameters, parameter => parameter.ParameterType == typeof(CancellationToken));
        var cancellationToken = tokenIndex >= 0 ? (CancellationToken)args[tokenIndex]! : CancellationToken.None;
        if (method.ResultType is null)
            return InvokeVoidAsync(owner, contract, method, args, cancellationToken);
        return typeof(ApplicationRpcProxy).GetMethod(nameof(InvokeResultAsync), BindingFlags.NonPublic | BindingFlags.Static)!
            .MakeGenericMethod(method.ResultType).Invoke(null, [owner, contract, method, args, cancellationToken]);
    }

    private object? InvokeLocalPolicy(MethodInfo method, object?[] arguments)
    {
        if (localPolicy is null || method.DeclaringType?.IsInstanceOfType(localPolicy) != true)
            throw new InvalidOperationException("Synchronous application policy requires a local implementation.");
        try
        {
            return method.Invoke(localPolicy, arguments);
        }
        catch (TargetInvocationException exception) when (exception.InnerException is { } inner)
        {
            ExceptionDispatchInfo.Capture(inner).Throw();
            throw;
        }
    }

    private static async Task InvokeVoidAsync(
        ApplicationRpcClient owner, RpcContract contract, RpcMethod method, object?[] arguments,
        CancellationToken cancellationToken) =>
        _ = await owner.InvokeAsync(contract, method, arguments, cancellationToken).ConfigureAwait(false);

    private static async Task<T?> InvokeResultAsync<T>(
        ApplicationRpcClient owner, RpcContract contract, RpcMethod method, object?[] arguments,
        CancellationToken cancellationToken)
    {
        var response = await owner.InvokeAsync(contract, method, arguments, cancellationToken).ConfigureAwait(false);
        return ReadResult<T>(response);
    }

    internal static T? ReadResult<T>(RpcResponse response)
    {
        if (response.Result.ValueKind == JsonValueKind.Undefined)
            throw new InvalidDataException("The application response has no result.");
        return response.Result.Deserialize<T>(RpcJson.Options);
    }
}
