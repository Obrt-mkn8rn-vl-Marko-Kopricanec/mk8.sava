using System.Diagnostics;
using System.Globalization;
using System.Security.Cryptography;
using Microsoft.Extensions.Logging.Abstractions;
using MSAVA_INF.Contexts;
using MSAVA_INF.Managers;
using MSAVA_INF.Models;

const string expectedRevision = "17ccf1cd1a43d21360fd21b434a82ea2011b4f8f";
var sourceRoot = Path.GetFullPath(Environment.GetEnvironmentVariable("MSAVA_SOURCE_ROOT")
    ?? throw new InvalidOperationException("Set MSAVA_SOURCE_ROOT to a local MSAVA checkout."));
var actualRevision = ReadGit(sourceRoot, "rev-parse", "HEAD").Trim();
if (!string.Equals(actualRevision, expectedRevision, StringComparison.Ordinal) ||
    !string.IsNullOrEmpty(ReadGit(sourceRoot, "status", "--porcelain")))
{
    throw new InvalidOperationException("The MSAVA baseline requires the clean, pinned 17ccf1c source revision.");
}
var dataRoot = Path.Combine(AppContext.BaseDirectory, "Data");
if (Directory.Exists(dataRoot))
    throw new InvalidOperationException($"Refusing to reuse an existing MSAVA data root: {dataRoot}");

using var metadata = new MetadataStore(Path.Combine(dataRoot, "file_metadata.db"));
var files = new FileManager(metadata, NullLogger<FileManager>.Instance);
var runner = new BaselineRunner(files, dataRoot);
Console.WriteLine($"source_root={sourceRoot}");
Console.WriteLine($"source_revision={actualRevision}");
Console.WriteLine($"data_root={dataRoot}");
Console.WriteLine("workload,logical_bytes,msava_inf_allocated_bytes,content_allocated_bytes,litedb_allocated_bytes,content_file_count,stage_register_ms,read_ms,process_cpu_ms,working_set_bytes");
await runner.RunAsync().ConfigureAwait(false);

static string ReadGit(string sourceRoot, params string[] arguments)
{
    var start = new ProcessStartInfo("git")
    {
        RedirectStandardOutput = true,
        RedirectStandardError = true,
        UseShellExecute = false
    };
    start.ArgumentList.Add("-C");
    start.ArgumentList.Add(sourceRoot);
    foreach (var argument in arguments)
        start.ArgumentList.Add(argument);
    using var process = Process.Start(start) ?? throw new InvalidOperationException("Failed to start git.");
    var output = process.StandardOutput.ReadToEnd();
    var error = process.StandardError.ReadToEnd();
    process.WaitForExit();
    if (process.ExitCode != 0)
        throw new InvalidOperationException($"Failed to verify MSAVA source revision: {error}");
    return output;
}

internal sealed class BaselineRunner(FileManager files, string dataRoot)
{
    private long _logicalBytes;

    public async Task RunAsync()
    {
        var duplicate = new byte[256 * 1024];
        Fill(0x5100, duplicate);
        await MeasureAsync("eight_exact_duplicates", async () =>
        {
            for (var index = 0; index < 8; index++)
                await SaveAsync(duplicate).ConfigureAwait(false);
            return duplicate;
        }).ConfigureAwait(false);

        var sharedBase = new byte[2 * 1024 * 1024];
        Fill(0x5101, sharedBase);
        await MeasureAsync("five_shifted_partials", async () =>
        {
            byte[] latest = sharedBase;
            for (var index = 0; index < 5; index++)
            {
                latest = new byte[sharedBase.Length + 4096];
                sharedBase.AsSpan(0, 2048).CopyTo(latest);
                Fill(0x5200 + index, latest.AsSpan(2048, 4096));
                sharedBase.AsSpan(2048).CopyTo(latest.AsSpan(6144));
                await SaveAsync(latest).ConfigureAwait(false);
            }
            return latest;
        }).ConfigureAwait(false);

        await MeasureAsync("eight_versions", async () =>
        {
            byte[] latest = [];
            for (var index = 0; index < 8; index++)
            {
                latest = new byte[128 * 1024];
                latest.AsSpan().Fill((byte)('A' + index));
                await SaveAsync(latest).ConfigureAwait(false);
            }
            return latest;
        }).ConfigureAwait(false);

        await MeasureAsync("one_hundred_twenty_eight_small", async () =>
        {
            byte[] latest = [];
            for (var index = 0; index < 128; index++)
            {
                latest = new byte[80];
                Fill(0x5300 + index, latest);
                await SaveAsync(latest).ConfigureAwait(false);
            }
            return latest;
        }).ConfigureAwait(false);

        await MeasureAsync("four_incompressible", async () =>
        {
            byte[] latest = [];
            for (var index = 0; index < 4; index++)
            {
                latest = new byte[512 * 1024];
                Fill(0x5400 + index, latest);
                await SaveAsync(latest).ConfigureAwait(false);
            }
            return latest;
        }).ConfigureAwait(false);
    }

    private async Task MeasureAsync(string workload, Func<Task<byte[]>> write)
    {
        using var process = Process.GetCurrentProcess();
        var beforeCpu = process.TotalProcessorTime;
        var watch = Stopwatch.StartNew();
        var latest = await write().ConfigureAwait(false);
        watch.Stop();
        var uploadMs = watch.Elapsed.TotalMilliseconds;

        watch.Restart();
        var hash = SHA256.HashData(latest);
        await using (var stream = files.GetFileStream(hash, "txt"))
        {
            using var output = new MemoryStream();
            await stream.CopyToAsync(output).ConfigureAwait(false);
            if (!output.ToArray().AsSpan().SequenceEqual(latest))
                throw new InvalidDataException("MSAVA-INF returned bytes different from those saved.");
        }
        watch.Stop();
        process.Refresh();

        var liteDbBytes = MeasureAllocatedBytes(Path.Combine(dataRoot, "file_metadata.db"));
        var totalBytes = MeasureAllocatedBytes(dataRoot);
        var contentFiles = Directory.EnumerateFiles(dataRoot)
            .Where(path => path.EndsWith(".txt", StringComparison.Ordinal)).ToArray();
        var expected = workload switch
        {
            "eight_exact_duplicates" => (LogicalBytes: 2097152L, FileCount: 1),
            "five_shifted_partials" => (LogicalBytes: 12603392L, FileCount: 6),
            "eight_versions" => (LogicalBytes: 13651968L, FileCount: 14),
            "one_hundred_twenty_eight_small" => (LogicalBytes: 13662208L, FileCount: 142),
            "four_incompressible" => (LogicalBytes: 15759360L, FileCount: 146),
            _ => throw new InvalidOperationException($"Unknown fixture '{workload}'.")
        };
        if (_logicalBytes != expected.LogicalBytes || contentFiles.Length != expected.FileCount)
            throw new InvalidDataException($"MSAVA-INF did not produce the expected '{workload}' fixture.");
        var contentBytes = contentFiles.Sum(MeasureAllocatedBytes);
        Console.WriteLine(string.Join(',',
            workload,
            _logicalBytes.ToString(CultureInfo.InvariantCulture),
            totalBytes.ToString(CultureInfo.InvariantCulture),
            contentBytes.ToString(CultureInfo.InvariantCulture),
            liteDbBytes.ToString(CultureInfo.InvariantCulture),
            contentFiles.Length.ToString(CultureInfo.InvariantCulture),
            uploadMs.ToString("F3", CultureInfo.InvariantCulture),
            watch.Elapsed.TotalMilliseconds.ToString("F3", CultureInfo.InvariantCulture),
            (process.TotalProcessorTime - beforeCpu).TotalMilliseconds.ToString("F3", CultureInfo.InvariantCulture),
            process.WorkingSet64.ToString(CultureInfo.InvariantCulture)));
    }

    private async Task SaveAsync(byte[] content)
    {
        var record = new SavedFileMetaRecord
        {
            RefId = Guid.NewGuid(),
            FileHash = SHA256.HashData(content),
            FileExtension = "txt",
            AccessGroupId = Guid.Empty,
            PublicDownload = false,
            CreatedAt = DateTime.UtcNow
        };
        var staged = Path.Combine(AppContext.BaseDirectory, $"stage-{record.RefId:N}.tmp");
        await File.WriteAllBytesAsync(staged, content).ConfigureAwait(false);
        await files.SaveTempFileAsync(record, staged).ConfigureAwait(false);
        _logicalBytes = checked(_logicalBytes + content.Length);
    }

    private static void Fill(int seed, Span<byte> bytes) => new Random(seed).NextBytes(bytes);

    private static long MeasureAllocatedBytes(string path)
    {
        var start = new ProcessStartInfo("du")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false
        };
        start.ArgumentList.Add("-s");
        start.ArgumentList.Add("--block-size=1");
        start.ArgumentList.Add("--");
        start.ArgumentList.Add(path);
        using var process = Process.Start(start) ?? throw new InvalidOperationException("Failed to start du.");
        var output = process.StandardOutput.ReadToEnd();
        var error = process.StandardError.ReadToEnd();
        process.WaitForExit();
        if (process.ExitCode != 0)
            throw new InvalidOperationException($"du failed: {error}");
        var separator = output.IndexOf('\t', StringComparison.Ordinal);
        if (separator < 0 || !long.TryParse(output.AsSpan(0, separator), CultureInfo.InvariantCulture, out var bytes))
            throw new InvalidDataException("du returned an invalid allocated-byte measurement.");
        return bytes;
    }
}
