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

            long logicalBytes = 0;
            output.WriteLine("workload,logical_bytes,sava_allocated_bytes,raw_allocated_bytes,metadata_allocated_bytes,chunks_allocated_bytes,packs_allocated_bytes,staging_allocated_bytes,upload_ms,read_ms,process_cpu_ms,working_set_bytes,sampled_peak_staging_temp_allocated_bytes,sampled_peak_working_set_bytes");

            async Task RecordAsync(string workload, Func<Task<(double UploadMs, double ReadMs)>> operation)
            {
                var cpuBefore = Process.GetCurrentProcess().TotalProcessorTime;
                var stagingPath = Path.Combine(application.DataPath, "staging");
                var stagingBaseline = StorageAllocationMeter.MeasureRoot(stagingPath);
                long? peakTemporaryAllocation = stagingBaseline.HasValue ? 0 : null;
                long peakWorkingSet = 0;
                using var samplingCancellation = new CancellationTokenSource();
                var sampler = Task.Run(async () =>
                {
                    using var sampledProcess = Process.GetCurrentProcess();
                    while (!samplingCancellation.IsCancellationRequested)
                    {
                        var allocated = StorageAllocationMeter.MeasureRoot(stagingPath);
                        if (allocated is { } measured && stagingBaseline is { } baseline)
                        {
                            peakTemporaryAllocation = Math.Max(
                                peakTemporaryAllocation!.Value,
                                Math.Max(0, measured - baseline));
                        }
                        sampledProcess.Refresh();
                        peakWorkingSet = Math.Max(peakWorkingSet, sampledProcess.WorkingSet64);
                        try
                        {
                            await Task.Delay(TimeSpan.FromMilliseconds(10), samplingCancellation.Token).ConfigureAwait(false);
                        }
                        catch (OperationCanceledException) when (samplingCancellation.IsCancellationRequested)
                        {
                            break;
                        }
                    }
                });
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
                var process = Process.GetCurrentProcess();
                peakWorkingSet = Math.Max(peakWorkingSet, process.WorkingSet64);
                Assert.True(peakWorkingSet > 0);
                output.WriteLine(string.Join(',',
                    workload,
                    logicalBytes.ToString(CultureInfo.InvariantCulture),
                    (await MeasureAllocatedBytesAsync(application.DataPath).ConfigureAwait(false)).ToString(CultureInfo.InvariantCulture),
                    (await MeasureAllocatedBytesAsync(rawRoot).ConfigureAwait(false)).ToString(CultureInfo.InvariantCulture),
                    (await MeasureAllocatedBytesAsync(Path.Combine(application.DataPath, "metadata.db")).ConfigureAwait(false) +
                     await MeasureAllocatedBytesAsync(Path.Combine(application.DataPath, "metadata.db-wal")).ConfigureAwait(false) +
                     await MeasureAllocatedBytesAsync(Path.Combine(application.DataPath, "metadata.db-shm")).ConfigureAwait(false))
                    .ToString(CultureInfo.InvariantCulture),
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
                    peakTemporaryAllocation?.ToString(CultureInfo.InvariantCulture) ?? "unavailable",
                    peakWorkingSet.ToString(CultureInfo.InvariantCulture)));
            }

            async Task<double> UploadAsync(string blobName, string rawName, byte[] content)
            {
                var watch = Stopwatch.StartNew();
                await container.GetBlobClient(blobName).UploadAsync(BinaryData.FromBytes(content), overwrite: true).ConfigureAwait(false);
                watch.Stop();
                await File.WriteAllBytesAsync(Path.Combine(rawRoot, rawName), content).ConfigureAwait(false);
                logicalBytes = checked(logicalBytes + content.Length);
                return watch.Elapsed.TotalMilliseconds;
            }

            async Task<double> VerifyAsync(string blobName, byte[] expected)
            {
                var watch = Stopwatch.StartNew();
                var actual = await container.GetBlobClient(blobName).DownloadContentAsync().ConfigureAwait(false);
                watch.Stop();
                Assert.Equal(expected, actual.Value.Content.ToArray());
                return watch.Elapsed.TotalMilliseconds;
            }

            var duplicate = new byte[256 * 1024];
            DeterministicTestBytes.Fill(0x5100, duplicate);
            await RecordAsync("eight_exact_duplicates", async () =>
            {
                double write = 0;
                for (var index = 0; index < 8; index++)
                    write += await UploadAsync($"duplicate-{index}.bin", $"duplicate-{index}.bin", duplicate).ConfigureAwait(false);
                return (write, await VerifyAsync("duplicate-7.bin", duplicate).ConfigureAwait(false));
            });

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
            });

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
            });

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
            });

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
            });
        }
        finally
        {
            Directory.Delete(rawRoot, recursive: true);
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
