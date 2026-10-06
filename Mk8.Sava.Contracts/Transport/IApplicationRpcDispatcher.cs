using System.Reflection;

namespace Mk8.Sava.Transport;

public interface IApplicationRpcDispatcher
{
    ValueTask<object?> InvokeAsync(
        Type contract, MethodInfo method, object?[] arguments, CancellationToken cancellationToken);
}
