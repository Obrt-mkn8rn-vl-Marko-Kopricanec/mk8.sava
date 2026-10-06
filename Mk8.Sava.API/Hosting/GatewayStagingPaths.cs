using Microsoft.Extensions.Options;
using System.Globalization;
using Mk8.Sava.Configuration;
using Mk8.Sava.Protocol;

namespace Mk8.Sava.Hosting;

internal sealed class GatewayStagingPaths : IDisposable
{
    private readonly long _maximumBytes;
    private readonly FileStream _owner;
    private long _reservedBytes;
    private long _rejected;
    private int _disposed;

    public GatewayStagingPaths(IHostEnvironment environment, IOptions<GatewayOptions> configuredOptions,
        IOptions<SavaOptions> storageOptions)
    {
        var options = configuredOptions.Value;
        _maximumBytes = options.MaximumStagingBytes;
        var root = Path.GetFullPath(options.StagingPath, environment.ContentRootPath);
        var dataRoot = Path.GetFullPath(storageOptions.Value.DataPath, environment.ContentRootPath);
        var comparison = OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
        if (string.Equals(root, dataRoot, comparison) ||
            root.StartsWith(Path.TrimEndingDirectorySeparator(dataRoot) + Path.DirectorySeparatorChar, comparison) ||
            dataRoot.StartsWith(Path.TrimEndingDirectorySeparator(root) + Path.DirectorySeparatorChar, comparison))
            throw new InvalidOperationException("Gateway staging must be separate from the Application storage root.");
        CreatePrivateDirectory(root);
        GatewayStagingCleanup.RemoveOrphans(root);
        Staging = Path.Combine(root, "gw-" + Guid.NewGuid().ToString("N"));
        CreatePrivateDirectory(Staging);
        _owner = new FileStream(Path.Combine(Staging, ".owner"), FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None);
        _owner.Write(GatewayStagingCleanup.OwnerMarker);
        _owner.Flush(flushToDisk: true);
    }

    public string Staging { get; }
    internal long ReservedBytes => Interlocked.Read(ref _reservedBytes);

    internal string RenderMetrics() => string.Create(CultureInfo.InvariantCulture, $"""
        # HELP mk8_sava_gateway_staging_bytes Quota-accounted bytes in active Gateway temporary files.
        # TYPE mk8_sava_gateway_staging_bytes gauge
        mk8_sava_gateway_staging_bytes {ReservedBytes}
        # HELP mk8_sava_gateway_staging_limit_bytes Configured per-process Gateway staging budget.
        # TYPE mk8_sava_gateway_staging_limit_bytes gauge
        mk8_sava_gateway_staging_limit_bytes {_maximumBytes}
        # HELP mk8_sava_gateway_staging_rejections_total Writes rejected by the Gateway staging budget.
        # TYPE mk8_sava_gateway_staging_rejections_total counter
        mk8_sava_gateway_staging_rejections_total {Interlocked.Read(ref _rejected)}

        """);

    public Stream OpenTemporaryFile(string path)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        if (!string.Equals(Path.GetDirectoryName(Path.GetFullPath(path)), Staging, StringComparison.Ordinal))
            throw new ArgumentException("A Gateway temporary file must belong to this process's private staging directory.", nameof(path));
        return new QuotaFileStream(path, this);
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;
        // Keep ownership locked until payload cleanup finishes. If shutdown
        // cleanup fails, the next instance can identify and reclaim the orphan.
        try
        {
            foreach (var path in Directory.EnumerateFiles(Staging))
            {
                if (!string.Equals(Path.GetFileName(path), ".owner", StringComparison.Ordinal))
                    File.Delete(path);
            }
        }
        finally
        {
            _owner.Dispose();
        }
        File.Delete(Path.Combine(Staging, ".owner"));
        Directory.Delete(Staging, recursive: false);
        GC.SuppressFinalize(this);
    }

    private static void CreatePrivateDirectory(string path)
    {
        for (var directory = new DirectoryInfo(path); directory is not null; directory = directory.Parent)
        {
            if (directory.Exists && directory.Attributes.HasFlag(FileAttributes.ReparsePoint))
                throw new InvalidOperationException("Gateway staging cannot traverse a symbolic link or reparse point.");
        }
        if (OperatingSystem.IsWindows())
            Directory.CreateDirectory(path);
        else
        {
            Directory.CreateDirectory(path, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
            if ((File.GetUnixFileMode(path) & (UnixFileMode.GroupRead | UnixFileMode.GroupWrite | UnixFileMode.GroupExecute |
                UnixFileMode.OtherRead | UnixFileMode.OtherWrite | UnixFileMode.OtherExecute)) != UnixFileMode.None)
                throw new InvalidOperationException("Gateway staging requires an owner-only private directory.");
        }
    }

    private void Reserve(int count)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        var total = Interlocked.Add(ref _reservedBytes, count);
        if (total <= _maximumBytes)
            return;
        Interlocked.Add(ref _reservedBytes, -count);
        Interlocked.Increment(ref _rejected);
        throw new AzureStorageException(503, "ServerBusy", "Gateway temporary storage is at its configured limit.");
    }

    private sealed class QuotaFileStream(string path, GatewayStagingPaths paths) : Stream
    {
        private readonly FileStream _file = new(path, FileMode.CreateNew, FileAccess.ReadWrite,
            FileShare.None, 128 * 1024, FileOptions.Asynchronous | FileOptions.SequentialScan);
        private readonly string _temporaryPath = path;
        private long _bytes;
        private int _released;

        public override bool CanRead => _file.CanRead;
        public override bool CanWrite => _file.CanWrite;
        public override bool CanSeek => _file.CanSeek;
        public override long Length => _file.Length;
        public override long Position
        {
            get => _file.Position;
            set
            {
                if (value < 0 || value > Length)
                    throw new ArgumentOutOfRangeException(nameof(value), "Gateway temporary files cannot contain unaccounted sparse gaps.");
                _file.Position = value;
            }
        }

        public override void Flush() => _file.Flush();
        public override Task FlushAsync(CancellationToken cancellationToken) => _file.FlushAsync(cancellationToken);
        public override int Read(byte[] buffer, int offset, int count) => _file.Read(buffer, offset, count);
        public override int Read(Span<byte> buffer) => _file.Read(buffer);
        public override int ReadByte() => _file.ReadByte();
        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
            _file.ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) =>
            _file.ReadAsync(buffer, cancellationToken);
        public override long Seek(long offset, SeekOrigin origin)
        {
            var basis = origin switch
            {
                SeekOrigin.Begin => 0,
                SeekOrigin.Current => Position,
                SeekOrigin.End => Length,
                _ => throw new ArgumentOutOfRangeException(nameof(origin))
            };
            Position = checked(basis + offset);
            return Position;
        }

        public override void Write(byte[] buffer, int offset, int count) => Write(buffer.AsSpan(offset, count));

        public override void WriteByte(byte value)
        {
            Span<byte> buffer = stackalloc byte[1];
            buffer[0] = value;
            Write(buffer);
        }

        public override void Write(ReadOnlySpan<byte> buffer)
        {
            Reserve(buffer.Length);
            _file.Write(buffer);
        }

        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
            WriteAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            Reserve(buffer.Length);
            return _file.WriteAsync(buffer, cancellationToken);
        }

        public override void SetLength(long value)
        {
            if (value > Length)
                throw new NotSupportedException("Gateway temporary files grow only through quota-accounted writes.");
            _file.SetLength(value);
            if (_file.Position > value)
                _file.Position = value;
        }

        protected override void Dispose(bool disposing)
        {
            try
            {
                if (disposing)
                {
                    try
                    {
                        _file.Dispose();
                    }
                    finally
                    {
                        Release();
                    }
                }
            }
            finally
            {
                base.Dispose(disposing);
            }
        }

        public override async ValueTask DisposeAsync()
        {
            try
            {
                await _file.DisposeAsync().ConfigureAwait(false);
            }
            finally
            {
                // Stream.DisposeAsync calls this wrapper's Dispose(bool), so
                // deletion/refund stays in one idempotent ownership path.
                await base.DisposeAsync().ConfigureAwait(false);
            }
            GC.SuppressFinalize(this);
        }

        private void Reserve(int count)
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _released) != 0, this);
            paths.Reserve(count);
            Interlocked.Add(ref _bytes, count);
        }

        private void Release()
        {
            if (Interlocked.Exchange(ref _released, 1) != 0)
                return;
            File.Delete(_temporaryPath);
            // A failed unlink conservatively retains the reservation. The
            // private owner cleanup can reclaim it on shutdown/restart, but
            // another upload must not reuse quota while those bytes remain.
            Interlocked.Add(ref paths._reservedBytes, -Interlocked.Read(ref _bytes));
        }
    }
}
