using System.Buffers.Binary;
using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using Azure;
using Azure.Core;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Sas;
using Mk8.Sava.Protocol;

namespace Mk8.Sava.Tests;

public sealed class KestrelUploadTests
{
    private const int LargeContentLength = 30 * 1024 * 1024;

    [Theory]
    [InlineData("sdk")]
    [InlineData("raw")]
    [InlineData("md5")]
    [InlineData("crc64")]
    [InlineData("structured")]
    public async Task SingleRequestUploadsAboveKestrelDefaultPreserveContent(string mode)
    {
        var server = await RunningServer.StartAsync();
        await using (server.ConfigureAwait(false))
        {
            var container = server.Client.GetBlobContainerClient("large-uploads");
            await container.CreateAsync();
            var blob = container.GetBlobClient(mode);
            var content = CreateContent(LargeContentLength);
            if (string.Equals(mode, "sdk", StringComparison.Ordinal))
            {
                using var source = new MemoryStream(content, writable: false);
                var upload = await container.GetBlockBlobClient(mode).UploadAsync(source);
                Assert.Equal(201, upload.GetRawResponse().Status);
            }
            else
            {
                using var transport = new HttpClient();
                using var request = CreateUpload(blob, content, mode);
                using var response = await transport.SendAsync(request);
                Assert.Equal(HttpStatusCode.Created, response.StatusCode);
            }
            await AssertContentAsync(blob, content);
        }
    }

    [Fact]
    public async Task LargeStagedBlocksAndAppendBlocksUseTheProtocolLimits()
    {
        var server = await RunningServer.StartAsync();
        await using (server.ConfigureAwait(false))
        {
            var container = server.Client.GetBlobContainerClient("large-blocks");
            await container.CreateAsync();
            var content = CreateContent(LargeContentLength);
            var block = container.GetBlockBlobClient("staged");
            var blockId = Convert.ToBase64String("block-01"u8);
            using (var source = new MemoryStream(content, writable: false))
                Assert.Equal(201, (await block.StageBlockAsync(blockId, source)).GetRawResponse().Status);
            Assert.Equal(201, (await block.CommitBlockListAsync([blockId])).GetRawResponse().Status);
            await AssertContentAsync(container.GetBlobClient("staged"), content);

            var append = container.GetAppendBlobClient("append");
            await append.CreateAsync();
            using (var source = new MemoryStream(content, writable: false))
                Assert.Equal(201, (await append.AppendBlockAsync(source)).GetRawResponse().Status);
            await AssertContentAsync(container.GetBlobClient("append"), content);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ConfiguredLogicalBoundaryAllowsFramingButRejectsOversizedWrites(bool structured)
    {
        var server = await RunningServer.StartAsync(LargeContentLength);
        await using (server.ConfigureAwait(false))
        {
            var container = server.Client.GetBlobContainerClient("logical-limit");
            await container.CreateAsync();
            var blob = container.GetBlobClient("existing");
            var content = CreateContent(LargeContentLength);
            var mode = structured ? "structured" : "raw";
            using var transport = new HttpClient();
            using (var request = CreateUpload(blob, content, mode))
            using (var response = await transport.SendAsync(request))
                Assert.Equal(HttpStatusCode.Created, response.StatusCode);
            await AssertContentAsync(blob, content);
            var before = (await blob.GetPropertiesAsync()).Value;

            var oversized = CreateContent(LargeContentLength + 1);
            using (var request = CreateUpload(blob, oversized, mode))
            using (var response = await transport.SendAsync(request))
                await AssertBodyTooLargeAsync(response);
            Assert.Equal(before.ETag, (await blob.GetPropertiesAsync()).Value.ETag);
            await AssertContentAsync(blob, content);

            var absent = container.GetBlobClient("absent");
            using (var request = CreateUpload(absent, oversized, mode))
            using (var response = await transport.SendAsync(request))
                await AssertBodyTooLargeAsync(response);
            Assert.False((await absent.ExistsAsync()).Value);
        }
    }

    [Fact]
    public async Task ConfiguredLimitStillProtectsStagingAppendingAndPages()
    {
        const int limit = 1024;
        var server = await RunningServer.StartAsync(limit);
        await using (server.ConfigureAwait(false))
        {
            var container = server.Client.GetBlobContainerClient("small-limit");
            await container.CreateAsync();
            var block = container.GetBlockBlobClient("staged");
            var blockId = Convert.ToBase64String("block-01"u8);
            using (var source = new MemoryStream(CreateContent(limit), writable: false))
                await block.StageBlockAsync(blockId, source);
            using (var source = new MemoryStream(CreateContent(limit + 1), writable: false))
                AssertBodyTooLarge(await Assert.ThrowsAsync<RequestFailedException>(
                    () => block.StageBlockAsync(Convert.ToBase64String("block-02"u8), source)));
            Assert.Equal([blockId], (await block.GetBlockListAsync(BlockListTypes.Uncommitted))
                .Value.UncommittedBlocks.Select(item => item.Name), StringComparer.Ordinal);

            var append = container.GetAppendBlobClient("append");
            await append.CreateAsync();
            using (var source = new MemoryStream(CreateContent(limit), writable: false))
                await append.AppendBlockAsync(source);
            var appendBefore = (await append.GetPropertiesAsync()).Value;
            using (var source = new MemoryStream(CreateContent(limit + 1), writable: false))
                AssertBodyTooLarge(await Assert.ThrowsAsync<RequestFailedException>(() => append.AppendBlockAsync(source)));
            Assert.Equal(appendBefore.ETag, (await append.GetPropertiesAsync()).Value.ETag);
            Assert.Equal(limit, (await append.GetPropertiesAsync()).Value.ContentLength);

            var page = container.GetPageBlobClient("page");
            await page.CreateAsync(2048);
            using (var source = new MemoryStream(CreateContent(limit), writable: false))
                await page.UploadPagesAsync(source, offset: 0);
            var pageBefore = (await page.GetPropertiesAsync()).Value;
            using (var source = new MemoryStream(CreateContent(limit + 512), writable: false))
                AssertBodyTooLarge(await Assert.ThrowsAsync<RequestFailedException>(
                    () => page.UploadPagesAsync(source, offset: 0)));
            Assert.Equal(pageBefore.ETag, (await page.GetPropertiesAsync()).Value.ETag);
            Assert.Equal([new HttpRange(0, limit)], (await page.GetPageRangesAsync()).Value.PageRanges);
        }
    }

    private static byte[] CreateContent(int length)
    {
        var content = new byte[length];
        RandomNumberGenerator.Fill(content);
        return content;
    }

    [Fact]
    public async Task StructuredUploadsAllowAlternativeSegmentLayoutsAtTheLogicalLimit()
    {
        const int limit = 1024;
        var server = await RunningServer.StartAsync(limit);
        await using (server.ConfigureAwait(false))
        {
            var container = server.Client.GetBlobContainerClient("segmented-upload");
            await container.CreateAsync();
            var blob = container.GetBlobClient("segmented");
            var content = CreateContent(limit);
            using var transport = new HttpClient();
            using var request = CreateUpload(blob, content, "structured");
            request.Content!.Dispose();
            request.Content = new ByteArrayContent(EncodeStructuredBody(content, segmentCount: 64));
            using var response = await transport.SendAsync(request);
            Assert.Equal(HttpStatusCode.Created, response.StatusCode);
            Assert.Equal(content, (await blob.DownloadContentAsync()).Value.Content.ToArray());
        }
    }

    [Fact]
    public async Task RemainingTransport413OnNonUploadPathIsAnAzureErrorWithoutMutation()
    {
        var server = await RunningServer.StartAsync();
        await using (server.ConfigureAwait(false))
        {
            var container = server.Client.GetBlobContainerClient("transport-error");
            await container.CreateAsync();
            var blob = container.GetBlobClient("existing");
            await blob.UploadAsync(BinaryData.FromString("unchanged"));
            var before = (await blob.GetPropertiesAsync()).Value;
            using var transport = new HttpClient();
            using var request = new HttpRequestMessage(HttpMethod.Post,
                new Uri(blob.GenerateSasUri(BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddMinutes(10)) + "&comp=query"))
            {
                Content = new ByteArrayContent(new byte[LargeContentLength])
            };
            request.Headers.ExpectContinue = true;
            request.Headers.Add("x-ms-version", "2026-06-06");
            using var response = await transport.SendAsync(request);
            await AssertBodyTooLargeAsync(response);
            Assert.Equal(before.ETag, (await blob.GetPropertiesAsync()).Value.ETag);
            Assert.Empty((await blob.GetTagsAsync()).Value.Tags);
            Assert.Equal("unchanged", (await blob.DownloadContentAsync()).Value.Content.ToString());
        }
    }

    private static HttpRequestMessage CreateUpload(BlobClient blob, byte[] content, string mode)
    {
        var structured = string.Equals(mode, "structured", StringComparison.Ordinal);
        var request = new HttpRequestMessage(HttpMethod.Put,
            blob.GenerateSasUri(BlobSasPermissions.Create | BlobSasPermissions.Write,
                DateTimeOffset.UtcNow.AddMinutes(10)))
        {
            Content = new ByteArrayContent(structured ? EncodeStructuredBody(content) : content)
        };
        request.Headers.ExpectContinue = true;
        request.Headers.Add("x-ms-version", "2026-06-06");
        request.Headers.Add("x-ms-blob-type", "BlockBlob");
        if (structured)
        {
            request.Headers.Add("x-ms-structured-body", StructuredBodyDecoder.ContentType);
            request.Headers.Add("x-ms-structured-content-length", content.Length.ToString(CultureInfo.InvariantCulture));
        }
        else if (string.Equals(mode, "md5", StringComparison.Ordinal))
        {
#pragma warning disable CA5351 // MD5 is the Azure transactional checksum under test, not a security or deduplication primitive.
            request.Content.Headers.ContentMD5 = MD5.HashData(content);
#pragma warning restore CA5351
        }
        else if (string.Equals(mode, "crc64", StringComparison.Ordinal))
        {
            var crc64 = new StorageCrc64();
            crc64.Append(content);
            request.Headers.Add("x-ms-content-crc64", Convert.ToBase64String(crc64.GetHash()));
        }
        return request;
    }

    private static byte[] EncodeStructuredBody(ReadOnlySpan<byte> content, ushort segmentCount = 1)
    {
        // Encode the wire independently of the production response encoder.
        var encoded = new byte[content.Length + 21 + segmentCount * 18];
        encoded[0] = 1;
        BinaryPrimitives.WriteUInt64LittleEndian(encoded.AsSpan(1), (ulong)encoded.Length);
        BinaryPrimitives.WriteUInt16LittleEndian(encoded.AsSpan(9), 1);
        BinaryPrimitives.WriteUInt16LittleEndian(encoded.AsSpan(11), segmentCount);
        var encodedOffset = 13;
        var contentOffset = 0;
        for (var index = 1; index <= segmentCount; index++)
        {
            var length = (content.Length - contentOffset) / (segmentCount - index + 1);
            BinaryPrimitives.WriteUInt16LittleEndian(encoded.AsSpan(encodedOffset), (ushort)index);
            BinaryPrimitives.WriteUInt64LittleEndian(encoded.AsSpan(encodedOffset + 2), (ulong)length);
            var segment = content.Slice(contentOffset, length);
            segment.CopyTo(encoded.AsSpan(encodedOffset + 10));
            var segmentCrc64 = new StorageCrc64();
            segmentCrc64.Append(segment);
            segmentCrc64.GetHash().CopyTo(encoded.AsSpan(encodedOffset + 10 + length));
            encodedOffset += 18 + length;
            contentOffset += length;
        }
        var crc64 = new StorageCrc64();
        crc64.Append(content);
        crc64.GetHash().CopyTo(encoded.AsSpan(encodedOffset));
        return encoded;
    }

    private static async Task AssertContentAsync(BlobClient blob, byte[] content)
    {
        var downloaded = (await blob.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToMemory();
        Assert.Equal(content.Length, downloaded.Length);
        Assert.Equal(SHA256.HashData(content), SHA256.HashData(downloaded.Span));
        var range = (await blob.DownloadContentAsync(new BlobDownloadOptions
        {
            Range = new HttpRange(4093, 8195)
        }).ConfigureAwait(false)).Value.Content.ToMemory();
        Assert.Equal(content.AsSpan(4093, 8195).ToArray(), range.ToArray());
    }

    private static async Task AssertBodyTooLargeAsync(HttpResponseMessage response)
    {
        Assert.Equal("RequestBodyTooLarge", response.Headers.GetValues("x-ms-error-code").Single());
        Assert.Equal(HttpStatusCode.RequestEntityTooLarge, response.StatusCode);
        Assert.Contains("<Code>RequestBodyTooLarge</Code>", await response.Content.ReadAsStringAsync().ConfigureAwait(false),
            StringComparison.Ordinal);
    }

    private static void AssertBodyTooLarge(RequestFailedException exception)
    {
        Assert.Equal(413, exception.Status);
        Assert.Equal("RequestBodyTooLarge", exception.ErrorCode);
    }

    private sealed class RunningServer : IAsyncDisposable
    {
        private readonly Process _process;
        private readonly string _dataPath;
        private readonly Task<string> _output;
        private readonly Task<string> _error;
        private readonly TaskCompletionSource<Uri> _address = new(TaskCreationOptions.RunContinuationsAsynchronously);

        private RunningServer(Process process, string dataPath)
        {
            _process = process;
            _dataPath = dataPath;
            _output = CaptureOutputAsync();
            _error = process.StandardError.ReadToEndAsync();
        }

        public BlobServiceClient Client { get; private set; } = null!;

        public static async Task<RunningServer> StartAsync(long? maximumBodyBytes = null)
        {
            var dataPath = Directory.CreateTempSubdirectory("mk8-sava-kestrel-").FullName;
            var start = new ProcessStartInfo(Environment.GetEnvironmentVariable("DOTNET_HOST_PATH") ?? "dotnet")
            {
                WorkingDirectory = AppContext.BaseDirectory,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false
            };
            start.ArgumentList.Add(typeof(Program).Assembly.Location);
            foreach (var key in start.Environment.Keys.Where(key => key.StartsWith("Sava__", StringComparison.OrdinalIgnoreCase))
                         .ToArray())
                start.Environment.Remove(key);
            start.Environment["ASPNETCORE_ENVIRONMENT"] = "Production";
            start.Environment["DOTNET_ENVIRONMENT"] = "Production";
            start.Environment["ASPNETCORE_URLS"] = "http://127.0.0.1:0";
            start.Environment["Logging__LogLevel__Microsoft.Hosting.Lifetime"] = "Information";
            start.Environment["Sava__DataPath"] = dataPath;
            start.Environment["Sava__DefaultAccount"] = SavaWebApplicationFactory.AccountName;
            start.Environment["Sava__Accounts__devstoreaccount1"] = SavaWebApplicationFactory.AccountKey;
            if (maximumBodyBytes.HasValue)
                start.Environment["Sava__MaximumRequestBodyBytes"] = maximumBodyBytes.Value.ToString(CultureInfo.InvariantCulture);
            var process = Process.Start(start) ?? throw new InvalidOperationException("The Kestrel test host could not start.");
            var server = new RunningServer(process, dataPath);
            try
            {
                var address = await server._address.Task.WaitAsync(TimeSpan.FromSeconds(60)).ConfigureAwait(false);
                using var transport = new HttpClient();
                using var readiness = await transport.GetAsync(new Uri(address, "/health/ready")).ConfigureAwait(false);
                Assert.Equal(HttpStatusCode.OK, readiness.StatusCode);
                var options = new BlobClientOptions(BlobClientOptions.ServiceVersion.V2023_11_03);
                options.Retry.MaxRetries = 0;
                server.Client = new BlobServiceClient(new Uri(address, "/" + SavaWebApplicationFactory.AccountName),
                    new StorageSharedKeyCredential(SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey),
                    options);
                return server;
            }
            catch
            {
                await server.DisposeAsync().ConfigureAwait(false);
                throw;
            }
        }

        private async Task<string> CaptureOutputAsync()
        {
            var output = new StringBuilder();
            while (await _process.StandardOutput.ReadLineAsync().ConfigureAwait(false) is { } line)
            {
                output.AppendLine(line);
                const string prefix = "Now listening on: ";
                var start = line.IndexOf(prefix, StringComparison.Ordinal);
                if (start >= 0 && Uri.TryCreate(line[(start + prefix.Length)..].Trim(), UriKind.Absolute, out var uri))
                    _address.TrySetResult(uri);
            }
            _address.TrySetException(new InvalidOperationException($"The Kestrel test host exited before publishing an address: {output}"));
            return output.ToString();
        }

        public async ValueTask DisposeAsync()
        {
            try
            {
                if (!_process.HasExited)
                    _process.Kill(entireProcessTree: true);
                await _process.WaitForExitAsync().ConfigureAwait(false);
                _ = await Task.WhenAll(_output, _error).WaitAsync(TimeSpan.FromSeconds(30)).ConfigureAwait(false);
            }
            finally
            {
                _process.Dispose();
                await SavaWebApplicationFactory.DeleteDataPathAsync(_dataPath).ConfigureAwait(false);
            }
        }
    }
}
