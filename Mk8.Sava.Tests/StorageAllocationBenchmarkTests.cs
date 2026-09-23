using System.Diagnostics;
using System.Globalization;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Mk8.Sava.Storage;
using Xunit.Abstractions;

namespace Mk8.Sava.Tests;

[Collection("Storage allocation benchmark")]
public sealed class StorageAllocationBenchmarkTests(ITestOutputHelper output)
{
    [Fact]
    public async Task OrdinarySdkWorkloadsReportFilesystemAllocationAgainstRawFiles()
    {
        if (!OperatingSystem.IsLinux())
        {
            output.WriteLine("Allocated-byte benchmark requires GNU du on Linux; the other platform lanes are not measured here.");
            return;
        }

        var rawRoot = Path.Combine(Path.GetTempPath(), $"mk8-sava-raw-baseline-{Guid.NewGuid():N}");
        Directory.CreateDirectory(rawRoot);
        try
        {
            var application = new SavaWebApplicationFactory(new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:VersioningEnabled"] = "true",
                ["Sava:MaintenanceScanInterval"] = "01:00:00",
                ["Sava:MinimumChunkBytes"] = "65536",
                ["Sava:TargetChunkBytes"] = "262144",
                ["Sava:MaximumChunkBytes"] = "1048576",
                ["Sava:SmallChunkPackingThresholdBytes"] = "49152",
                ["Logging:LogLevel:Default"] = "Warning"
            });
            await using var applicationDisposal1 = application.ConfigureAwait(false);
            await application.InitializeAsync();
            var account = SavaWebApplicationFactory.AccountName;
            var endpoint = new Uri($"http://{account}.localhost");
            var client = new BlobServiceClient(
                endpoint,
                new StorageSharedKeyCredential(account, SavaWebApplicationFactory.AccountKey),
                new BlobClientOptions
                {
                    Transport = new HttpClientTransport(application.Server.CreateHandler()),
                    Retry = { MaxRetries = 0 }
                });
            var container = client.GetBlobContainerClient($"allocation-{Guid.NewGuid():N}");
            await container.CreateAsync();
            var benchmark = new AllocationBenchmarkScenario(application, container, rawRoot, output);
            await benchmark.RunAsync().ConfigureAwait(true);
        }
        finally
        {
            Directory.Delete(rawRoot, recursive: true);
        }
    }

    private sealed class AllocationBenchmarkScenario(
        SavaWebApplicationFactory application,
        BlobContainerClient container,
        string rawRoot,
        ITestOutputHelper output)
    {
        private long _logicalBytes;
        private long _previousSavaAllocatedBytes;
        private long _previousRawAllocatedBytes;

        public async Task RunAsync()
        {
            output.WriteLine("workload,logical_bytes,sava_allocated_bytes,raw_allocated_bytes,metadata_allocated_bytes,chunks_allocated_bytes,packs_allocated_bytes,staging_allocated_bytes,upload_ms,read_ms,process_cpu_ms,working_set_bytes,sampled_peak_staging_temp_allocated_bytes,sampled_peak_working_set_bytes");
            await RunDuplicatesAsync().ConfigureAwait(false);
            await RunShiftedPartialsAsync().ConfigureAwait(false);
            await RunVersionsAsync().ConfigureAwait(false);
            await RunSmallBlobsAsync().ConfigureAwait(false);
            await RunIncompressibleAsync().ConfigureAwait(false);
        }

        private async Task RecordAsync(string workload, Func<Task<(double UploadMs, double ReadMs)>> operation)
        {
            using var process = Process.GetCurrentProcess();
            var cpuBefore = process.TotalProcessorTime;
            var stagingPath = Path.Combine(application.DataPath, "staging");
            var samples = new AllocationSamples(StorageAllocationMeter.MeasureRoot(stagingPath));
            using var samplingCancellation = new CancellationTokenSource();
            var sampler = Task.Run(() => SampleUsageAsync(stagingPath, samples, samplingCancellation.Token));
            (double UploadMs, double ReadMs) timing;
            try
            {
                timing = await operation().ConfigureAwait(false);
            }
            finally
            {
                await samplingCancellation.CancelAsync().ConfigureAwait(false);
                await sampler.ConfigureAwait(false);
            }
            process.Refresh();
            samples.PeakWorkingSet = Math.Max(samples.PeakWorkingSet, process.WorkingSet64);
            Assert.True(samples.PeakWorkingSet > 0);
            await WriteRowAsync(workload, timing, process, cpuBefore, samples).ConfigureAwait(false);
        }

        private static async Task SampleUsageAsync(string stagingPath, AllocationSamples samples, CancellationToken cancellationToken)
        {
            using var process = Process.GetCurrentProcess();
            while (!cancellationToken.IsCancellationRequested)
            {
                var allocated = StorageAllocationMeter.MeasureRoot(stagingPath);
                if (allocated is { } measured && samples.StagingBaseline is { } baseline)
                {
                    samples.PeakTemporaryAllocation = Math.Max(
                        samples.PeakTemporaryAllocation!.Value, Math.Max(0, measured - baseline));
                }
                process.Refresh();
                samples.PeakWorkingSet = Math.Max(samples.PeakWorkingSet, process.WorkingSet64);
                try
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(10), cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
            }
        }

        private async Task WriteRowAsync(
            string workload,
            (double UploadMs, double ReadMs) timing,
            Process process,
            TimeSpan cpuBefore,
            AllocationSamples samples)
        {
            var metadataBytes = await MeasureAllocatedBytesAsync(Path.Combine(application.DataPath, "metadata.db"))
                .ConfigureAwait(false) +
                await MeasureAllocatedBytesAsync(Path.Combine(application.DataPath, "metadata.db-wal")).ConfigureAwait(false) +
                await MeasureAllocatedBytesAsync(Path.Combine(application.DataPath, "metadata.db-shm")).ConfigureAwait(false);
            var savaAllocatedBytes = await MeasureAllocatedBytesAsync(application.DataPath).ConfigureAwait(false);
            var rawAllocatedBytes = await MeasureAllocatedBytesAsync(rawRoot).ConfigureAwait(false);
            AssertAllocationBudgets(workload, savaAllocatedBytes, rawAllocatedBytes, samples);
            output.WriteLine(string.Join(',',
                workload,
                _logicalBytes.ToString(CultureInfo.InvariantCulture),
                savaAllocatedBytes.ToString(CultureInfo.InvariantCulture),
                rawAllocatedBytes.ToString(CultureInfo.InvariantCulture),
                metadataBytes.ToString(CultureInfo.InvariantCulture),
                (await MeasureAllocatedBytesAsync(Path.Combine(application.DataPath, "chunks")).ConfigureAwait(false))
                    .ToString(CultureInfo.InvariantCulture),
                (await MeasureAllocatedBytesAsync(Path.Combine(application.DataPath, "packs")).ConfigureAwait(false))
                    .ToString(CultureInfo.InvariantCulture),
                (await MeasureAllocatedBytesAsync(Path.Combine(application.DataPath, "staging")).ConfigureAwait(false))
                    .ToString(CultureInfo.InvariantCulture),
                timing.UploadMs.ToString("F3", CultureInfo.InvariantCulture),
                timing.ReadMs.ToString("F3", CultureInfo.InvariantCulture),
                (process.TotalProcessorTime - cpuBefore).TotalMilliseconds.ToString("F3", CultureInfo.InvariantCulture),
                process.WorkingSet64.ToString(CultureInfo.InvariantCulture),
                samples.PeakTemporaryAllocation?.ToString(CultureInfo.InvariantCulture) ?? "unavailable",
                samples.PeakWorkingSet.ToString(CultureInfo.InvariantCulture)));
        }

        private void AssertAllocationBudgets(
            string workload,
            long savaAllocatedBytes,
            long rawAllocatedBytes,
            AllocationSamples samples)
        {
            var maximumRatio = workload switch
            {
                "eight_exact_duplicates" => 0.80D,
                "five_shifted_partials" => 0.60D,
                "eight_versions" => 0.60D,
                "one_hundred_twenty_eight_small" => 0.70D,
                "four_incompressible" => 0.75D,
                _ => throw new InvalidOperationException($"No allocation budget is defined for '{workload}'.")
            };
            Assert.True(rawAllocatedBytes > 0);
            Assert.True(savaAllocatedBytes <= rawAllocatedBytes * maximumRatio,
                $"{workload}: {savaAllocatedBytes} allocated bytes exceeds {maximumRatio:P0} of raw {rawAllocatedBytes}.");
            if (string.Equals(workload, "four_incompressible", StringComparison.Ordinal))
            {
                var savaIncrement = savaAllocatedBytes - _previousSavaAllocatedBytes;
                var rawIncrement = rawAllocatedBytes - _previousRawAllocatedBytes;
                Assert.True(rawIncrement > 0 && savaIncrement >= 0 && savaIncrement <= rawIncrement * 1.50D,
                    $"Incompressible bytes added {savaIncrement} allocated bytes versus {rawIncrement} raw bytes.");
            }
            Assert.True(samples.PeakTemporaryAllocation is >= 0 and <= 4 * 1024 * 1024,
                $"{workload}: sampled staging peak exceeded 4 MiB or was unavailable.");
            Assert.InRange(samples.PeakWorkingSet, 1, 512L * 1024 * 1024);
            _previousSavaAllocatedBytes = savaAllocatedBytes;
            _previousRawAllocatedBytes = rawAllocatedBytes;
        }

        private async Task<double> UploadAsync(string blobName, string rawName, byte[] content)
        {
            var watch = Stopwatch.StartNew();
            await container.GetBlobClient(blobName).UploadAsync(BinaryData.FromBytes(content), overwrite: true)
                .ConfigureAwait(false);
            watch.Stop();
            await File.WriteAllBytesAsync(Path.Combine(rawRoot, rawName), content).ConfigureAwait(false);
            _logicalBytes = checked(_logicalBytes + content.Length);
            return watch.Elapsed.TotalMilliseconds;
        }

        private async Task<double> VerifyAsync(string blobName, byte[] expected)
        {
            var watch = Stopwatch.StartNew();
            var actual = await container.GetBlobClient(blobName).DownloadContentAsync().ConfigureAwait(false);
            watch.Stop();
            Assert.Equal(expected, actual.Value.Content.ToArray());
            return watch.Elapsed.TotalMilliseconds;
        }

        private async Task RunDuplicatesAsync()
        {
            var duplicate = new byte[256 * 1024];
            DeterministicTestBytes.Fill(0x5100, duplicate);
            await RecordAsync("eight_exact_duplicates", async () =>
            {
                double write = 0;
                for (var index = 0; index < 8; index++)
                    write += await UploadAsync($"duplicate-{index}.bin", $"duplicate-{index}.bin", duplicate).ConfigureAwait(false);
                return (write, await VerifyAsync("duplicate-7.bin", duplicate).ConfigureAwait(false));
            }).ConfigureAwait(false);
        }

        private async Task RunShiftedPartialsAsync()
        {
            var sharedBase = new byte[2 * 1024 * 1024];
            DeterministicTestBytes.Fill(0x5101, sharedBase);
            await RecordAsync("five_shifted_partials", async () =>
            {
                double write = 0;
                byte[] latest = sharedBase;
                for (var index = 0; index < 5; index++)
                {
                    latest = new byte[sharedBase.Length + 4096];
                    sharedBase.AsSpan(0, 2048).CopyTo(latest);
                    DeterministicTestBytes.Fill(0x5200 + index, latest.AsSpan(2048, 4096));
                    sharedBase.AsSpan(2048).CopyTo(latest.AsSpan(6144));
                    write += await UploadAsync($"partial-{index}.bin", $"partial-{index}.bin", latest).ConfigureAwait(false);
                }
                return (write, await VerifyAsync("partial-4.bin", latest).ConfigureAwait(false));
            }).ConfigureAwait(false);
        }

        private async Task RunVersionsAsync()
        {
            await RecordAsync("eight_versions", async () =>
            {
                double write = 0;
                byte[] latest = [];
                for (var index = 0; index < 8; index++)
                {
                    latest = new byte[128 * 1024];
                    latest.AsSpan().Fill((byte)('A' + index));
                    write += await UploadAsync("versioned.bin", $"version-{index}.bin", latest).ConfigureAwait(false);
                }
                return (write, await VerifyAsync("versioned.bin", latest).ConfigureAwait(false));
            }).ConfigureAwait(false);
        }

        private async Task RunSmallBlobsAsync()
        {
            await RecordAsync("one_hundred_twenty_eight_small", async () =>
            {
                double write = 0;
                byte[] latest = [];
                for (var index = 0; index < 128; index++)
                {
                    latest = new byte[80];
                    DeterministicTestBytes.Fill(0x5300 + index, latest);
                    write += await UploadAsync($"small-{index}.bin", $"small-{index}.bin", latest).ConfigureAwait(false);
                }
                return (write, await VerifyAsync("small-127.bin", latest).ConfigureAwait(false));
            }).ConfigureAwait(false);
        }

        private async Task RunIncompressibleAsync()
        {
            await RecordAsync("four_incompressible", async () =>
            {
                double write = 0;
                byte[] latest = [];
                for (var index = 0; index < 4; index++)
                {
                    latest = new byte[512 * 1024];
                    DeterministicTestBytes.Fill(0x5400 + index, latest);
                    write += await UploadAsync($"random-{index}.bin", $"random-{index}.bin", latest).ConfigureAwait(false);
                }
                return (write, await VerifyAsync("random-3.bin", latest).ConfigureAwait(false));
            }).ConfigureAwait(false);
        }

        private sealed class AllocationSamples(long? stagingBaseline)
        {
            public long? StagingBaseline { get; } = stagingBaseline;

            public long? PeakTemporaryAllocation { get; set; } = stagingBaseline.HasValue ? 0 : null;

            public long PeakWorkingSet { get; set; }
        }
    }

    private static async Task<long> MeasureAllocatedBytesAsync(string path)
    {
        if (!File.Exists(path) && !Directory.Exists(path))
            return 0;
        var process = new Process
        {
            StartInfo = new ProcessStartInfo("du")
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false
            }
        };
        process.StartInfo.ArgumentList.Add("-s");
        process.StartInfo.ArgumentList.Add("--block-size=1");
        process.StartInfo.ArgumentList.Add("--");
        process.StartInfo.ArgumentList.Add(path);
        using (process)
        {
            process.Start();
            var output = await process.StandardOutput.ReadToEndAsync().ConfigureAwait(false);
            var error = await process.StandardError.ReadToEndAsync().ConfigureAwait(false);
            await process.WaitForExitAsync().ConfigureAwait(false);
            if (process.ExitCode != 0)
                throw new InvalidOperationException($"du failed while measuring filesystem allocation: {error}");
            var separator = output.IndexOf('\t', StringComparison.Ordinal);
            if (separator < 0 || !long.TryParse(output.AsSpan(0, separator), CultureInfo.InvariantCulture, out var bytes))
                throw new InvalidDataException("du returned an invalid allocated-byte measurement.");
            return bytes;
        }
    }
}
