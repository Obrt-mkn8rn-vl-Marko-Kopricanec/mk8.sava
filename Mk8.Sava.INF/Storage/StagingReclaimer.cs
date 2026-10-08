using System.IO.Enumeration;
using Mk8.Sava.Protocol;

namespace Mk8.Sava.Storage;

// Owns one sampled, top-level directory cycle. Every entry, including an ineligible
// one, consumes the pass budget; keeping the cursor prevents a busy prefix starving later files.
internal sealed class StagingReclaimer(string directory) : IDisposable
{
    private static readonly string TemporaryPattern = FileSystemName.TranslateWin32Expression("*.tmp");
    private readonly Lock _gate = new();
    private readonly string _directory = Path.GetFullPath(directory);
    private IEnumerator<string>? _entries;
    private bool _disposed;

    internal StagingReclamationBatch Advance(DateTimeOffset olderThan, int maximumEntries)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maximumEntries);
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            var examined = 0;
            var deleted = 0;
            try
            {
                _entries ??= Directory.EnumerateFileSystemEntries(_directory).GetEnumerator();
                while (examined < maximumEntries)
                {
                    if (!_entries.MoveNext())
                    {
                        Reset();
                        return new StagingReclamationBatch(examined, deleted, CycleCompleted: true);
                    }
                    examined++;
                    if (TryReclaim(_entries.Current, olderThan.UtcDateTime))
                        deleted++;
                }
                return new StagingReclamationBatch(examined, deleted, CycleCompleted: false);
            }
            catch (Exception failure)
            {
                try
                {
                    Reset();
                }
                catch (Exception cleanup)
                {
                    throw new AggregateException("Staging enumeration and its retirement both failed.", failure, cleanup);
                }
                throw;
            }
        }
    }

    private static bool TryReclaim(string path, DateTime olderThan)
    {
        if (!FileSystemName.MatchesWin32Expression(TemporaryPattern, Path.GetFileName(path.AsSpan()),
                ignoreCase: OperatingSystem.IsWindows()))
            return false;
        try
        {
            if ((File.GetAttributes(path) & (FileAttributes.Directory | FileAttributes.ReparsePoint)) != FileAttributes.None ||
                File.GetLastWriteTimeUtc(path) > olderThan)
                return false;

            using var abandoned = new FileStream(path, FileMode.Open, FileAccess.ReadWrite,
                FileShare.None, bufferSize: 1, FileOptions.DeleteOnClose);
            return true;
        }
        catch (IOException failure) when (!CatastrophicExceptionPolicy.Contains(failure))
        {
            // An owner/race won the lease, or the sampled file/directory disappeared.
            return false;
        }
        catch (UnauthorizedAccessException failure) when (!CatastrophicExceptionPolicy.Contains(failure))
        {
            // Leave inaccessible files in staging and visible to physical usage.
            return false;
        }
    }

    private void Reset()
    {
        var entries = _entries;
        _entries = null;
        entries?.Dispose();
    }

    public void Dispose()
    {
        lock (_gate)
        {
            if (_disposed)
                return;
            _disposed = true;
            Reset();
        }
    }
}
