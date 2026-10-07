using System.Diagnostics;
using System.Globalization;
using System.Text;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

// Test-only observations: the production deadline and storage operations remain independent.
internal sealed class StorageWorkProgressProbe : IAsyncDisposable
{
    internal const int MaximumSamples = 32;
    private readonly Lock _gate = new();
    private readonly Queue<Snapshot> _samples = new();
    private readonly StorageWorkAdmission _admission;
    private readonly CancellationToken _operationToken;
    private readonly CopyPhase[] _copies;
    private readonly long _started = Stopwatch.GetTimestamp();
    private readonly PeriodicTimer _timer;
    private readonly Task _sampling;
    private readonly TaskCompletionSource _firstTickOrStop = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private SourcePhase _source;
    private long _sequence;

    internal StorageWorkProgressProbe(
        StorageWorkAdmission admission, int copies, TimeSpan period, CancellationToken operationToken)
    {
        ArgumentNullException.ThrowIfNull(admission);
        ArgumentOutOfRangeException.ThrowIfNegative(copies);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(period, TimeSpan.Zero);
        _admission = admission;
        _operationToken = operationToken;
        _copies = new CopyPhase[copies];
        Capture();
        _timer = new PeriodicTimer(period);
        _sampling = SampleAsync();
    }

    internal IReadOnlyList<Snapshot> Samples
    {
        get { lock (_gate) return Array.AsReadOnly(_samples.ToArray()); }
    }

    internal void SetSourcePhase(SourcePhase phase)
    {
        lock (_gate) _source = phase;
        Capture();
    }

    internal void SetCopyPhase(int copy, CopyPhase phase)
    {
        lock (_gate) _copies[copy] = phase;
        Capture();
    }

    internal Task WaitForFirstTickOrStopAsync(CancellationToken cancellationToken) =>
        _firstTickOrStop.Task.WaitAsync(cancellationToken);

    internal void Capture()
    {
        lock (_gate)
        {
            // A cancellation callback could run after storage's callbacks have already drained
            // the queues. Sample independently, and discard any capture spanning cancellation.
            if (_operationToken.IsCancellationRequested)
                return;
            var start = Stopwatch.GetElapsedTime(_started);
            var admission = _admission.RenderPrometheus();
            var end = Stopwatch.GetElapsedTime(_started);
            if (_operationToken.IsCancellationRequested)
                return;
            if (_samples.Count == MaximumSamples)
                _samples.Dequeue();
            _samples.Enqueue(new Snapshot(++_sequence, start, end, _source, Array.AsReadOnly((CopyPhase[])_copies.Clone()), admission));
        }
    }

    internal string RenderDiagnostics()
    {
        var samples = Samples;
        var builder = new StringBuilder();
        builder.Append("Retained pre-cancellation samples (operation token unset before AND after capture).\n")
            .Append("Lane counters are individually synchronized, not an atomic cross-lane/progress snapshot; samples do not diagnose a cause.\n");
        if (samples.Count == 0)
            builder.Append("No pre-cancellation sample was captured.\n");
        foreach (var sample in samples)
        {
            builder.Append(CultureInfo.InvariantCulture,
                $"sample={sample.Sequence}, elapsed_ms={sample.Start.TotalMilliseconds:F3}..{sample.End.TotalMilliseconds:F3}, source={sample.Source}");
            for (var copy = 0; copy < sample.Copies.Count; copy++)
                builder.Append(CultureInfo.InvariantCulture, $", copy[{copy}]={sample.Copies[copy]}");
            builder.Append('\n').Append(sample.Admission);
        }
        if (samples.Count != 0)
        {
            var age = Stopwatch.GetElapsedTime(_started) - samples[^1].End;
            builder.Append(CultureInfo.InvariantCulture, $"Last retained sample age_ms={age.TotalMilliseconds:F3}. Sampling can be delayed by scheduling.\n");
        }
        builder.Append("Current admission state (possibly after cancellation/cleanup; not historical proof):\n")
            .Append(_admission.RenderPrometheus());
        return builder.ToString();
    }

    public async ValueTask DisposeAsync()
    {
        _timer.Dispose();
#pragma warning disable VSTHRD003 // This probe owns its sampling task; all timer awaits avoid context capture, and disposal must join it.
        await _sampling.ConfigureAwait(false);
#pragma warning restore VSTHRD003
        GC.SuppressFinalize(this);
    }

    private async Task SampleAsync()
    {
        try
        {
            while (await _timer.WaitForNextTickAsync().ConfigureAwait(false))
            {
                Capture();
                _firstTickOrStop.TrySetResult();
                if (_operationToken.IsCancellationRequested)
                    return;
            }
        }
        finally
        {
            _firstTickOrStop.TrySetResult();
        }
    }

    internal enum SourcePhase { Pending, Storing, Stored }
    internal enum CopyPhase { Pending, Copying, Reading, Verified }

    internal sealed record Snapshot(
        long Sequence, TimeSpan Start, TimeSpan End, SourcePhase Source, IReadOnlyList<CopyPhase> Copies, string Admission);
}
