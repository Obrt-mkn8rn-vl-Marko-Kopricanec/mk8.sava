using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Mk8.Sava.Protocol;
using Mk8.Sava.Storage;
using Xunit.Abstractions;

namespace Mk8.Sava.Tests;

public sealed class BlobSeekableReadStreamTests(ITestOutputHelper output)
{
    [Theory]
    [InlineData(false, 0)]
    [InlineData(true, 0)]
    [InlineData(false, 11)]
    [InlineData(true, 11)]
    [InlineData(false, 31)]
    [InlineData(true, 31)]
    public async Task RetirementRejectsSupportedOperationsBeforeBorrowedRangeAccess(bool asynchronous, int position)
    {
        var source = "0123456789abcdefghijklmnopqrstu"u8.ToArray();
        var calls = 0;
        var stream = new BlobSeekableReadStream((offset, length, destination, token) =>
        {
            calls++;
            return destination.WriteAsync(source.AsMemory(checked((int)offset), checked((int)length)), token).AsTask();
        }, source.Length);
        await using var lifetime = stream.ConfigureAwait(false);
        stream.Position = position;
        await RetireAsync(stream, asynchronous).ConfigureAwait(true);
        var buffer = Enumerable.Repeat((byte)0xCC, 7).ToArray();
        var failure = await Record.ExceptionAsync(() => stream.ReadAsync(buffer.AsMemory()).AsTask()).ConfigureAwait(true);
        output.WriteLine($"Retired query adapter: Async={asynchronous}, Position={position}, RangeCalls={calls}, " +
            $"Failure={failure?.GetType().Name ?? "none"}, Buffer={Convert.ToHexString(buffer)}.");

        Assert.IsType<ObjectDisposedException>(failure);
        Assert.Equal(0, calls);
        Assert.Equal(Enumerable.Repeat((byte)0xCC, buffer.Length), buffer);
        Assert.False(stream.CanRead);
        Assert.False(stream.CanSeek);
        Assert.False(stream.CanWrite);
        await Assert.ThrowsAsync<ObjectDisposedException>(() => stream.ReadAsync(Memory<byte>.Empty).AsTask()).ConfigureAwait(true);
        await Assert.ThrowsAsync<ObjectDisposedException>(() => stream.ReadAsync(buffer, 0, buffer.Length, CancellationToken.None)).ConfigureAwait(true);
        await Assert.ThrowsAsync<ObjectDisposedException>(() => stream.ReadAsync(buffer, 0, 0, CancellationToken.None)).ConfigureAwait(true);
        await Assert.ThrowsAsync<ObjectDisposedException>(() => stream.FlushAsync(CancellationToken.None)).ConfigureAwait(true);
        using var destination = new MemoryStream();
        await Assert.ThrowsAsync<ObjectDisposedException>(() => stream.CopyToAsync(destination)).ConfigureAwait(true);
        Assert.Equal(0, destination.Length);
        AssertRetiredSynchronousOperations(stream, buffer);
        await RetireAsync(stream, asynchronous: !asynchronous).ConfigureAwait(true);
        Assert.Equal(0, calls);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RetiredAdapterCannotReadStoredContentAndDoesNotRetireItsBorrowedPin(bool asynchronous)
    {
        var application = new SavaWebApplicationFactory(TimeProvider.System,
            new Dictionary<string, string?>(StringComparer.Ordinal), disableMaintenance: true);
        await using var applicationLifetime = application.ConfigureAwait(true);
        await application.InitializeAsync().ConfigureAwait(true);
        Assert.DoesNotContain(application.Services.GetServices<IHostedService>(), service => service is StorageMaintenanceService);
        var chunks = application.Services.GetRequiredService<ChunkStore>();
        var encryption = new BlobEncryption(null, null);
        var bytes = new byte[32768];
        DeterministicTestBytes.Fill(0x7391, bytes);
        using var input = new MemoryStream(bytes, writable: false);
        using var stored = await chunks.StorePinnedAsync(SavaWebApplicationFactory.AccountName, encryption, input, CancellationToken.None).ConfigureAwait(true);
        var calls = 0;
        var stream = new BlobSeekableReadStream((offset, length, destination, token) =>
        {
            calls++;
            return chunks.WriteRangeAsync(stored.Manifest, encryption, offset, length, destination, token);
        }, stored.Manifest.Length);
        await using var streamLifetime = stream.ConfigureAwait(false);
        var prefix = new byte[11];
        Assert.Equal(prefix.Length, await stream.ReadAsync(prefix.AsMemory()).ConfigureAwait(true));
        Assert.Equal(bytes.AsSpan(0, prefix.Length).ToArray(), prefix);
        await RetireAsync(stream, asynchronous).ConfigureAwait(true);
        var buffer = Enumerable.Repeat((byte)0xCD, 7).ToArray();
        var failure = await Record.ExceptionAsync(() => stream.ReadAsync(buffer.AsMemory()).AsTask()).ConfigureAwait(true);
        output.WriteLine($"Retired stored-content adapter: Async={asynchronous}, RangeCalls={calls}, " +
            $"Failure={failure?.GetType().Name ?? "none"}, Buffer={Convert.ToHexString(buffer)}.");

        Assert.IsType<ObjectDisposedException>(failure);
        Assert.Equal(1, calls);
        Assert.Equal(Enumerable.Repeat((byte)0xCD, buffer.Length), buffer);
        Assert.True(input.CanRead);
        Assert.Equal(bytes, await chunks.ReadAllAsync(stored.Manifest, encryption, CancellationToken.None).ConfigureAwait(true));
        Assert.Contains("mk8_sava_storage_work_active{lane=\"reads\"} 0\n", chunks.Admission.RenderPrometheus(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task PartialRangeFailureAndCancellationKeepPositionAndPermitAValidRetry()
    {
        var bytes = "0123456789abcdefghijklmnopqrstu"u8.ToArray();
        var original = new IOException("controlled partial query read");
        var fail = true;
        CancellationToken observed = default;
        var stream = new BlobSeekableReadStream(async (offset, length, destination, token) =>
        {
            observed = token;
            token.ThrowIfCancellationRequested();
            if (fail)
            {
                await destination.WriteAsync(bytes.AsMemory(checked((int)offset), 3), token).ConfigureAwait(false);
                throw original;
            }
            await destination.WriteAsync(bytes.AsMemory(checked((int)offset), checked((int)length)), token).ConfigureAwait(false);
        }, bytes.Length);
        await using var lifetime = stream.ConfigureAwait(false);
        var buffer = Enumerable.Repeat((byte)0xCE, 7).ToArray();
        var failure = await Record.ExceptionAsync(() => stream.ReadAsync(buffer.AsMemory()).AsTask()).ConfigureAwait(true);
        Assert.Same(original, failure);
        Assert.Equal(0, stream.Position);
        Assert.Equal(bytes.AsSpan(0, 3).ToArray(), buffer.AsSpan(0, 3).ToArray());
        Assert.Equal(Enumerable.Repeat((byte)0xCE, 4), buffer.AsSpan(3).ToArray());
        fail = false;
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync().ConfigureAwait(true);
        var canceled = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => stream.ReadAsync(buffer.AsMemory(), cancellation.Token).AsTask()).ConfigureAwait(true);
        Assert.Equal(cancellation.Token, canceled.CancellationToken);
        Assert.Equal(cancellation.Token, observed);
        Assert.Equal(0, stream.Position);
        Assert.Equal(buffer.Length, await stream.ReadAsync(buffer.AsMemory()).ConfigureAwait(true));
        Assert.Equal(bytes.AsSpan(0, buffer.Length).ToArray(), buffer);
        Assert.Equal(buffer.Length, stream.Position);
        Assert.True(stream.CanRead);
        Assert.True(stream.CanSeek);
    }

    [Fact]
    public async Task LongOffsetsClippedTailsAndFailedSeeksKeepTheirExistingContract()
    {
        const long length = (1L << 33) + 31;
        var calls = new List<(long Offset, long Length)>();
        var stream = new BlobSeekableReadStream((offset, count, destination, token) =>
        {
            calls.Add((offset, count));
            return destination.WriteAsync(Enumerable.Range(0, checked((int)count))
                .Select(index => unchecked((byte)(offset + index))).ToArray(), token).AsTask();
        }, length);
        await using var lifetime = stream.ConfigureAwait(false);
        Assert.Equal(length - 3, stream.Seek(-3, SeekOrigin.End));
        var buffer = Enumerable.Repeat((byte)0xCF, 7).ToArray();
        Assert.Equal(3, await stream.ReadAsync(buffer.AsMemory()).ConfigureAwait(true));
        Assert.Equal(new byte[] { 28, 29, 30, 0xCF, 0xCF, 0xCF, 0xCF }, buffer);
        Assert.Equal([(length - 3, 3L)], calls);
        Assert.Equal(length, stream.Position);
        Assert.Equal(0, await stream.ReadAsync(buffer.AsMemory()).ConfigureAwait(true));
        Assert.Equal(length + 7, stream.Seek(7, SeekOrigin.Current));
        Assert.Equal(0, await stream.ReadAsync(buffer.AsMemory()).ConfigureAwait(true));
        Assert.Throws<IOException>(() => stream.Seek(-1, SeekOrigin.Begin));
        Assert.Throws<OverflowException>(() => stream.Seek(long.MaxValue, SeekOrigin.Current));
        Assert.Equal(length + 7, stream.Position);
        Assert.Equal(0, stream.Seek(0, SeekOrigin.Begin));
        Assert.Equal(7, await stream.ReadAsync(buffer.AsMemory()).ConfigureAwait(true));
        Assert.Equal(new byte[] { 0, 1, 2, 3, 4, 5, 6 }, buffer);
        Assert.Equal([(length - 3, 3L), (0L, 7L)], calls);
    }

    private static async Task RetireAsync(BlobSeekableReadStream stream, bool asynchronous)
    {
        if (asynchronous)
            await stream.DisposeAsync().ConfigureAwait(false);
        else
        {
#pragma warning disable CA1849, VSTHRD103 // Exercise the actual synchronous Stream.Dispose contract; the asynchronous branch is tested separately.
            stream.Dispose();
#pragma warning restore CA1849, VSTHRD103
        }
    }

    private static void AssertRetiredSynchronousOperations(BlobSeekableReadStream stream, byte[] buffer)
    {
        Assert.Throws<ObjectDisposedException>(() => stream.Read(buffer, 0, buffer.Length));
        Assert.Throws<ObjectDisposedException>(() => stream.Read(buffer.AsSpan()));
        Assert.Throws<ObjectDisposedException>(() => stream.Read(Span<byte>.Empty));
        Assert.Throws<ObjectDisposedException>(() => stream.ReadByte());
        Assert.Throws<ObjectDisposedException>(() => stream.Flush());
        Assert.Throws<ObjectDisposedException>(() => stream.Seek(0, SeekOrigin.Begin));
        Assert.Throws<ObjectDisposedException>(() => stream.Position = 0);
        Assert.Throws<ObjectDisposedException>(() => stream.Position);
        Assert.Throws<ObjectDisposedException>(() => stream.Length);
    }
}
