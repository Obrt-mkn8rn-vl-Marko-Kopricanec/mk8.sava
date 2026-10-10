using System.Diagnostics;
using Mk8.Sava.Application;
using Mk8.Sava.Configuration;
using Mk8.Sava.Storage;
using Mk8.Sava.Transport;

namespace Mk8.Sava.Tests;

public sealed partial class ApplicationTransportAllocationTests
{
    [Theory]
    [InlineData("control")]
    [InlineData("stream")]
    [InlineData("page")]
    public async Task RequestPreparationHasABoundedMeasuredAllocationCost(string form)
    {
        var directory = Directory.CreateTempSubdirectory("sava-content-allocation-");
        try
        {
            var key = Path.Combine(directory.FullName, "access.key");
            await File.WriteAllTextAsync(key, Convert.ToBase64String(new byte[32])).ConfigureAwait(true);
            if (!OperatingSystem.IsWindows())
                File.SetUnixFileMode(key, UnixFileMode.UserRead | UnixFileMode.UserWrite);
            using var client = CreatePreparationClient(key);
            using var source = new MemoryStream();
            var contract = RpcContracts.GetContract(string.Equals(form, "control", StringComparison.Ordinal)
                ? typeof(IApplicationReadiness) : typeof(IBlobApplication));
            var operation = form switch
            {
                "control" => nameof(IApplicationReadiness.GetAsync),
                "stream" => nameof(IBlobApplication.StageBlockAsync),
                _ => nameof(IBlobApplication.BeginIncrementalCopyFromPageRangesAsync),
            };
            var method = contract.GetMethod(contract.Type.GetMethod(operation)!);
            object?[] arguments = form switch
            {
                "control" => [CancellationToken.None],
                "stream" => ["devstoreaccount1", "container", "blob", "YmxvY2s=", source, new BlobEncryption(null, null), CancellationToken.None],
                _ => ["devstoreaccount1", "container", "blob", 0L, "snapshot", "source", null, 0L,
                    Array.Empty<PageRange>(), new BlobWriteOptions(new BlobHttpProperties(), new Dictionary<string, string>(StringComparer.Ordinal)),
                    "https://example.invalid/container/blob", null, new EmptyPreparationPages(), CancellationToken.None],
            };
            for (var iteration = 0; iteration < 10; iteration++)
                await PrepareAndDisposeAsync(client, contract, method, arguments).ConfigureAwait(true);
            var thread = Environment.CurrentManagedThreadId;
            var watch = Stopwatch.StartNew();
            var before = GC.GetAllocatedBytesForCurrentThread();

            for (var iteration = 0; iteration < 1000; iteration++)
                await PrepareAndDisposeAsync(client, contract, method, arguments).ConfigureAwait(true);

            var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
            watch.Stop();
            Assert.Equal(thread, Environment.CurrentManagedThreadId);
            output.WriteLine("1000 warmed {0} request preparations: {1} managed bytes; {2:F3} ms", form, allocated, watch.Elapsed.TotalMilliseconds);
            Assert.InRange(allocated, 1L, 8L * 1024 * 1024);
            Assert.Equal(0, source.Position);
            Assert.True(source.CanRead);
            using var request = await client.CreateRequestAsync(contract, method, arguments, CancellationToken.None).ConfigureAwait(true);
            using var wire = new MemoryStream();
            await request.Content!.CopyToAsync(wire).ConfigureAwait(true);
            wire.Position = 0;
            var descriptor = await RpcFrames.ReadControlAsync<RpcRequest>(wire, 65536, CancellationToken.None).ConfigureAwait(true);
            Assert.Equal(!string.Equals(form, "control", StringComparison.Ordinal), descriptor.HasInput);
            Assert.Equal(string.Equals(form, "page", StringComparison.Ordinal), descriptor.PageChanges is not null);
            using var framed = new FramedReadStream(wire, 0);
            await framed.EnsureCompletedAsync(CancellationToken.None).ConfigureAwait(true);
            Assert.Equal(wire.Length, wire.Position);
        }
        finally
        {
            directory.Delete(recursive: true);
        }
    }

    private static ApplicationRpcClient CreatePreparationClient(string key)
    {
        var options = new ApplicationTransportOptions
        {
            Endpoint = new Uri("http://127.0.0.1:0/internal/application"),
            AccessKeyFile = key,
        };
        return new ApplicationRpcClient(options, new SavaOptions());
    }

    private static async Task PrepareAndDisposeAsync(ApplicationRpcClient client, RpcContract contract, RpcMethod method, object?[] arguments)
    {
        using var request = await client.CreateRequestAsync(contract, method, arguments, CancellationToken.None).ConfigureAwait(false);
        Assert.Equal(ApplicationTransportSecurity.ContentType, request.Content!.Headers.ContentType!.MediaType);
    }

    private sealed class EmptyPreparationPages : IPageCopySource
    {
        private static readonly PageRangeDiff Changes = new([], []);
        public Task<PageRangeDiff> ReadChangesAsync(string? previousSnapshot, long previousLength, CancellationToken cancellationToken) => Task.FromResult(Changes);
        public Task ReadRangeAsync(PageRange range, Func<Stream, CancellationToken, Task> consume, CancellationToken cancellationToken) =>
            throw new InvalidOperationException("Empty page preparation must not request range data.");
    }
}
