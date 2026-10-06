using System.Reflection;
using Mk8.Sava.Transport;

namespace Mk8.Sava.Tests;

// Existing fault-injection/unit fixtures intentionally keep their backend in-process.
// Separate Kestrel/process tests cover the production transport and host boundary.
#pragma warning disable CA1515 // DispatchProxy emits a derived type in a separate dynamic assembly.
public class InProcessApplicationProxy : DispatchProxy
#pragma warning restore CA1515
{
    private IApplicationRpcDispatcher _dispatcher = null!;
    private Type _contract = null!;
    private object? _localPolicy;

    internal static T Create<T>(IApplicationRpcDispatcher dispatcher, object? localPolicy = null) where T : class
    {
        var instance = Create<T, InProcessApplicationProxy>();
        var proxy = (InProcessApplicationProxy)(object)instance;
        proxy._dispatcher = dispatcher;
        proxy._contract = typeof(T);
        proxy._localPolicy = localPolicy;
        return instance;
    }

    protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
    {
        var method = targetMethod ?? throw new ArgumentNullException(nameof(targetMethod));
        var arguments = args ?? [];
        if (!typeof(Task).IsAssignableFrom(method.ReturnType))
            return method.Invoke(_localPolicy ?? throw new InvalidOperationException("No local capability policy."), arguments);
        var cancellationToken = arguments.OfType<CancellationToken>().LastOrDefault();
        if (method.ReturnType == typeof(Task))
            return InvokeVoidAsync(method, arguments, cancellationToken);
        return typeof(InProcessApplicationProxy).GetMethod(nameof(InvokeResultAsync), BindingFlags.Instance | BindingFlags.NonPublic)!
            .MakeGenericMethod(method.ReturnType.GenericTypeArguments[0]).Invoke(this, [method, arguments, cancellationToken]);
    }

    private async Task InvokeVoidAsync(MethodInfo method, object?[] arguments, CancellationToken cancellationToken)
    {
        using var peer = ApplicationRpcIdentity.Enter("in-process-test-fixture");
        _ = await _dispatcher.InvokeAsync(_contract, method, arguments, cancellationToken).ConfigureAwait(false);
    }

    private async Task<T> InvokeResultAsync<T>(MethodInfo method, object?[] arguments, CancellationToken cancellationToken)
    {
        using var peer = ApplicationRpcIdentity.Enter("in-process-test-fixture");
        return (T)(await _dispatcher.InvokeAsync(_contract, method, arguments, cancellationToken).ConfigureAwait(false))!;
    }
}
