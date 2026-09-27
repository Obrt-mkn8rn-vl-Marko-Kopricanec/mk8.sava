using Microsoft.Extensions.Hosting;

namespace Mk8.Sava.Tests;

internal sealed class SavaTestHostLifetime : IHostLifetime
{
    private readonly TaskCompletionSource _start = new(TaskCreationOptions.RunContinuationsAsynchronously);

    internal void Release() => _start.TrySetResult();

    public Task WaitForStartAsync(CancellationToken cancellationToken) => _start.Task.WaitAsync(cancellationToken);

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
