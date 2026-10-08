using System.Diagnostics;
using System.Globalization;
using System.Text;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

// Observe existing test seams without changing the storage operation or its deadline.
internal sealed class StorageCopyCheckpointProbe : IStorageFaultInjector
{
    internal const int MaximumCheckpoints = 32;
    private Observation? _observation;

    internal void Start(CancellationToken operationToken)
    {
        var next = new Observation(operationToken);
        if (Interlocked.CompareExchange(ref _observation, next, null) is not null)
            throw new InvalidOperationException("Checkpoint observation has already started.");
    }

    internal void SourceStored() => RequireObservation().SourceStored();
    internal IReadOnlyList<Checkpoint> Checkpoints =>
        Volatile.Read(ref _observation)?.Checkpoints ?? Array.Empty<Checkpoint>();

    public void Inject(StorageFaultPoint point)
    {
        if (point is StorageFaultPoint.DuringChunkStagingWrite or
            StorageFaultPoint.BeforeChunkPublication or StorageFaultPoint.DuringPackRecordAppend)
            Volatile.Read(ref _observation)?.Capture(point);
    }

    internal string RenderDiagnostics() => RequireObservation().RenderDiagnostics();

    private Observation RequireObservation() => Volatile.Read(ref _observation) ??
        throw new InvalidOperationException("Checkpoint observation has not started.");

    internal enum Work { Source, Copies }

    internal sealed record Checkpoint(
        long Sequence, TimeSpan Start, TimeSpan End, Work Work, StorageFaultPoint Point,
        long StagingReached, long BeforePublicationReached, long PackAppendReached);

    private sealed class Observation(CancellationToken operationToken)
    {
        private readonly Lock _gate = new();
        private readonly Queue<Checkpoint> _checkpoints = new();
        private readonly long _started = Stopwatch.GetTimestamp();
        private Work _work;
        private long _sequence;
        private long _staging;
        private long _beforePublication;
        private long _packAppend;

        internal IReadOnlyList<Checkpoint> Checkpoints
        {
            get { lock (_gate) return Array.AsReadOnly(_checkpoints.ToArray()); }
        }

        internal void SourceStored()
        {
            lock (_gate) _work = Work.Copies;
        }

        internal void Capture(StorageFaultPoint point)
        {
            lock (_gate)
            {
                if (operationToken.IsCancellationRequested)
                    return;
                var start = Stopwatch.GetElapsedTime(_started);
                var staging = _staging + (point == StorageFaultPoint.DuringChunkStagingWrite ? 1 : 0);
                var publication = _beforePublication + (point == StorageFaultPoint.BeforeChunkPublication ? 1 : 0);
                var packAppend = _packAppend + (point == StorageFaultPoint.DuringPackRecordAppend ? 1 : 0);
                var end = Stopwatch.GetElapsedTime(_started);
                if (operationToken.IsCancellationRequested)
                    return;
                _staging = staging;
                _beforePublication = publication;
                _packAppend = packAppend;
                if (_checkpoints.Count == MaximumCheckpoints)
                    _checkpoints.Dequeue();
                _checkpoints.Enqueue(new Checkpoint(++_sequence, start, end, _work, point,
                    staging, publication, packAppend));
            }
        }

        internal string RenderDiagnostics()
        {
            var checkpoints = Checkpoints;
            var builder = new StringBuilder();
            builder.Append("Retained storage seam reaches (operation token unset before AND after capture).\n")
                .Append("Counts are cumulative reaches, not successful writes/publications or per-copy identity.\n")
                .Append("DuringChunkStagingWrite precedes ciphertext write/flush; BeforeChunkPublication follows staging write/flush/close, but precedes publication.\n")
                .Append("Work grouping is fixture context; seam history and lane samples are not atomic or causal proof.\n");
            if (checkpoints.Count == 0)
                builder.Append("No pre-cancellation storage seam was captured.\n");
            foreach (var checkpoint in checkpoints)
            {
                builder.Append(CultureInfo.InvariantCulture,
                    $"checkpoint={checkpoint.Sequence}, elapsed_ms={checkpoint.Start.TotalMilliseconds:F3}..{checkpoint.End.TotalMilliseconds:F3}, work={checkpoint.Work}, point={checkpoint.Point}, staging_reached={checkpoint.StagingReached}, before_publication_reached={checkpoint.BeforePublicationReached}, pack_append_reached={checkpoint.PackAppendReached}\n");
            }
            if (checkpoints.Count != 0)
            {
                var age = Stopwatch.GetElapsedTime(_started) - checkpoints[^1].End;
                builder.Append(CultureInfo.InvariantCulture,
                    $"Last retained storage seam age_ms={age.TotalMilliseconds:F3}; absence of later reaches does not identify the stalled operation or cause.\n");
            }
            return builder.ToString();
        }
    }
}
