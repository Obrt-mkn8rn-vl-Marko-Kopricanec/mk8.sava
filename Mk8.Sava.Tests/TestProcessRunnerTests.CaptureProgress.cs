using System.Text;

namespace Mk8.Sava.Tests;

public sealed partial class TestProcessRunnerTests
{
    [Fact]
    public async Task ShortAsciiCaptureProgressDistinguishesPendingReadFromCanceledTask()
    {
        const string prefix = "short-ascii";
        using var input = new PendingSuffixStream(Encoding.UTF8.GetBytes(prefix));
        using var reader = new StreamReader(input, Encoding.UTF8, detectEncodingFromByteOrderMarks: true,
            bufferSize: 128, leaveOpen: true);
        using var cancellation = new CancellationTokenSource();
        var capture = new ProcessOutputCapture();
        var unstarted = capture.Progress;
        Assert.Equal(0, unstarted.ReadRequests);
        Assert.Equal(0, unstarted.ReadReturns);
        Assert.Equal(0, unstarted.CapturedCharacters);
        Assert.False(unstarted.ReachedEof);
#pragma warning disable CA2025 // Guaranteed cleanup cancels and joins this owned read before disposing its borrowed inputs.
        var work = capture.ReadAsync(reader, onLine: null, cancellation.Token);
#pragma warning restore CA2025
        ProcessOutputCapture.CaptureProgress pending = default;
        await RunWithCleanupAsync(async () =>
        {
            await input.PendingRead.Task.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
            pending = capture.Progress;
            Assert.Equal(prefix.Length + 1, pending.ReadRequests);
            Assert.Equal(prefix.Length, pending.ReadReturns);
            Assert.Equal(prefix.Length, pending.CapturedCharacters);
            Assert.False(pending.ReachedEof);
            Assert.False(work.IsCompleted);
            Assert.Equal(prefix, capture.Text);
            Assert.Equal(0, unstarted.CapturedCharacters);
        }, async () =>
        {
            await cancellation.CancelAsync().WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
            var failure = await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => work.WaitAsync(TimeSpan.FromSeconds(5))).ConfigureAwait(false);
            Assert.Equal(cancellation.Token, failure.CancellationToken);
        }).ConfigureAwait(true);
        Assert.True(work.IsCanceled);
        Assert.Equal(pending, capture.Progress);
        Assert.True(input.CanRead);
        output.WriteLine($"Pending and canceled reads retain the same logical counts: {pending.ToDiagnostic()}; Task={work.Status}.");
    }

    [Fact]
    public async Task HeldLineCallbackExposesCommittedProgressWithoutBlockingSnapshotsOrStartingAnotherRead()
    {
        using var input = new MemoryStream(Encoding.UTF8.GetBytes("p\ntail"), writable: false);
        using var reader = new StreamReader(input, Encoding.UTF8, detectEncodingFromByteOrderMarks: true,
            bufferSize: 128, leaveOpen: true);
        using var release = new ManualResetEventSlim();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var capture = new ProcessOutputCapture();
        var lines = new List<string>();
#pragma warning disable CA2025 // Guaranteed release and bounded join enclose the owned worker before reader/source/event disposal.
        var work = Task.Run(() => capture.ReadAsync(reader, line =>
        {
            lines.Add(line);
            if (!string.Equals(line, "p", StringComparison.Ordinal))
                return;
            entered.TrySetResult();
#pragma warning disable VSTHRD002 // Deliberately hold this synchronous line callback; guaranteed cleanup releases and joins its owned worker.
            release.Wait();
#pragma warning restore VSTHRD002
        }, CancellationToken.None));
#pragma warning restore CA2025
        Task<ProcessOutputCapture.CaptureProgress>? snapshot = null;
        await RunWithCleanupAsync(async () =>
        {
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
            // A separate bounded observer proves the callback does not own the snapshot lock.
            snapshot = Task.Run(() => capture.Progress);
            var progress = await snapshot.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
            Assert.Equal(2, progress.ReadRequests);
            Assert.Equal(2, progress.ReadReturns);
            Assert.Equal(2, progress.CapturedCharacters);
            Assert.False(progress.ReachedEof);
            Assert.False(work.IsCompleted);
            output.WriteLine($"Held callback, no next logical read: {progress.ToDiagnostic()}; Task={work.Status}.");
        }, async () =>
        {
            release.Set();
            await Task.WhenAll(work, snapshot ?? Task.CompletedTask).WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
        }).ConfigureAwait(true);
        Assert.Equal("p\ntail", capture.Text);
        Assert.Equal(["p", "tail"], lines, StringComparer.Ordinal);
        Assert.True(capture.ReachedEof);
        Assert.True(input.CanRead);
    }

    [Fact]
    public async Task CompletedCaptureProgressCountsDecodedUtf16CharactersAndTheEofReturn()
    {
        const string content = "ž🧪\nlast";
        using var input = new FragmentedCaptureStream(Encoding.UTF8.GetBytes(content));
        using var reader = new StreamReader(input, Encoding.UTF8, detectEncodingFromByteOrderMarks: true,
            bufferSize: 128, leaveOpen: true);
        var capture = new ProcessOutputCapture();
        await capture.ReadAsync(reader, onLine: null, CancellationToken.None).ConfigureAwait(true);
        var completed = capture.Progress;
        Assert.Equal(content, capture.Text);
        Assert.Equal(content.Length + 1, completed.ReadRequests);
        Assert.Equal(completed.ReadRequests, completed.ReadReturns);
        Assert.Equal(content.Length, completed.CapturedCharacters);
        Assert.True(completed.ReachedEof);
        Assert.True(input.CanRead);
        output.WriteLine($"Decoded UTF-16 characters, not wire bytes/native reads: {completed.ToDiagnostic()}.");
    }
}
