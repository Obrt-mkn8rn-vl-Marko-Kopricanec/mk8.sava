using Azure;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using System.Threading.RateLimiting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Mk8.Sava.Protocol;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

public sealed class StorageAdmissionSdkTests
{
    [Fact]
    public async Task RejectedUploadLeavesPublishedBytesAndPropertiesUnchanged() =>
        await AssertRejectedUploadAsync().ConfigureAwait(true);

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SaturatedReadAndQueryLanesReturnRetryableAzureErrors(bool query) =>
        await AssertRejectedReadAsync(query).ConfigureAwait(true);

    [Theory]
    [InlineData("md5")]
    [InlineData("crc64")]
    [InlineData("structured")]
    public async Task BufferedAndStructuredResponsesKeepTheReadPermitUntilOutputCompletes(string format) =>
        await AssertResponsePermitAsync(format).ConfigureAwait(true);

    private static async Task AssertRejectedUploadAsync()
    {
        var application = CreateApplication();
        await using var disposal = application.ConfigureAwait(false);
        var container = CreateClient(application).GetBlobContainerClient($"admission-{Guid.NewGuid():N}");
        await container.CreateAsync().ConfigureAwait(false);
        var blob = container.GetBlobClient("payload.bin");
        await blob.UploadAsync(BinaryData.FromString("original"), new BlobUploadOptions
        {
            Metadata = new Dictionary<string, string>(StringComparer.Ordinal) { ["marker"] = "original" }
        }).ConfigureAwait(false);
        var before = (await blob.GetPropertiesAsync().ConfigureAwait(false)).Value;
        var chunks = application.Services.GetRequiredService<ChunkStore>();
        using (await chunks.Admission.AcquireWriteAsync(CancellationToken.None).ConfigureAwait(false))
        {
            var rejected = await Assert.ThrowsAsync<RequestFailedException>(() =>
                blob.UploadAsync(BinaryData.FromString("replacement"), overwrite: true)).ConfigureAwait(false);
            AssertBusy(rejected);
            var after = (await blob.GetPropertiesAsync().ConfigureAwait(false)).Value;
            Assert.Equal(before.ETag, after.ETag);
            Assert.Equal(before.LastModified, after.LastModified);
            Assert.Equal("original", after.Metadata["marker"]);
            Assert.Equal("original", (await blob.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToString());
            using var monitoring = application.CreateClient();
            var metrics = await monitoring.GetStringAsync(new Uri("/metrics", UriKind.Relative)).ConfigureAwait(false);
            Assert.Contains("mk8_sava_storage_work_active{lane=\"writes\"} 1\n", metrics, StringComparison.Ordinal);
            Assert.Contains("mk8_sava_storage_work_rejected_total{lane=\"writes\"} 1\n", metrics, StringComparison.Ordinal);
        }
        await blob.UploadAsync(BinaryData.FromString("replacement"), overwrite: true).ConfigureAwait(false);
        Assert.Equal("replacement", (await blob.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToString());
        Assert.NotEqual(before.ETag, (await blob.GetPropertiesAsync().ConfigureAwait(false)).Value.ETag);
    }

    private static async Task AssertRejectedReadAsync(bool query)
    {
        var application = CreateApplication();
        await using var disposal = application.ConfigureAwait(false);
        var container = CreateClient(application).GetBlobContainerClient($"admission-{Guid.NewGuid():N}");
        await container.CreateAsync().ConfigureAwait(false);
        var blob = container.GetBlockBlobClient("payload.csv");
        await blob.UploadAsync(BinaryData.FromString("1,first\n2,second\n").ToStream()).ConfigureAwait(false);
        var before = (await blob.GetPropertiesAsync().ConfigureAwait(false)).Value;
        var chunks = application.Services.GetRequiredService<ChunkStore>();
        using (await AcquireReadOrQueryAsync(chunks.Admission, query).ConfigureAwait(false))
        {
            var rejected = query
                ? await Assert.ThrowsAsync<RequestFailedException>(() => blob.QueryAsync("SELECT _2 FROM BlobStorage;")).ConfigureAwait(false)
                : await Assert.ThrowsAsync<RequestFailedException>(() => blob.DownloadContentAsync()).ConfigureAwait(false);
            AssertBusy(rejected);
            Assert.Equal(before.ETag, (await blob.GetPropertiesAsync().ConfigureAwait(false)).Value.ETag);
        }
        var response = await blob.QueryAsync("SELECT _2 FROM BlobStorage;").ConfigureAwait(false);
        var content = response.Value.Content;
        await using var contentDisposal = content.ConfigureAwait(false);
        using var reader = new StreamReader(content);
        Assert.Equal("first\nsecond\n", await reader.ReadToEndAsync().ConfigureAwait(false));
        Assert.Equal("1,first\n2,second\n", (await blob.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToString());
    }

    private static void AssertBusy(RequestFailedException exception)
    {
        Assert.Equal(503, exception.Status);
        Assert.Equal("ServerBusy", exception.ErrorCode);
        var response = exception.GetRawResponse();
        Assert.NotNull(response);
        Assert.True(response.Headers.TryGetValue("Retry-After", out var value));
        Assert.Equal("1", value);
    }

    private static async Task AssertResponsePermitAsync(string format)
    {
        var application = CreateApplication();
        await using var disposal = application.ConfigureAwait(false);
        var container = CreateClient(application).GetBlobContainerClient($"admission-{Guid.NewGuid():N}");
        await container.CreateAsync().ConfigureAwait(false);
        var blob = container.GetBlobClient("payload.bin");
        var bytes = System.Security.Cryptography.RandomNumberGenerator.GetBytes(8192);
        await blob.UploadAsync(BinaryData.FromBytes(bytes)).ConfigureAwait(false);
        var output = new PausedResponseStream();
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var http = CreateDownloadContext(application, container.Name, blob.Name, output, cancellation.Token);
        var structured = string.Equals(format, "structured", StringComparison.Ordinal);
        http.Request.Headers[structured ? "x-ms-structured-body" : $"x-ms-range-get-content-{format}"] =
            structured ? StructuredBodyDecoder.ContentType : "true";
        var response = BlobProtocolEndpoint.HandleAsync(http);
        try
        {
            await output.Entered.WaitAsync(cancellation.Token).ConfigureAwait(false);
            AssertBusy(await Assert.ThrowsAsync<RequestFailedException>(() => blob.DownloadContentAsync()).ConfigureAwait(false));
            output.Release();
            await response.ConfigureAwait(false);
            await AssertResponseBytesAsync(format, bytes, http, output, cancellation.Token).ConfigureAwait(false);
            Assert.Equal(bytes, (await blob.DownloadContentAsync(cancellationToken: cancellation.Token).ConfigureAwait(false)).Value.Content.ToArray());
        }
        finally
        {
            await cancellation.CancelAsync().ConfigureAwait(false);
            try
            {
                await response.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
            await output.DisposeAsync().ConfigureAwait(false);
        }
    }

    private static DefaultHttpContext CreateDownloadContext(
        SavaWebApplicationFactory application, string container, string blob, Stream output, CancellationToken cancellationToken)
    {
        var http = new DefaultHttpContext { RequestServices = application.Services, RequestAborted = cancellationToken };
        http.Request.Method = HttpMethods.Get;
        http.Request.Host = new HostString($"{SavaWebApplicationFactory.AccountName}.localhost");
        http.Request.Path = $"/{container}/{blob}";
        http.Request.Headers["x-ms-range"] = "bytes=0-8191";
        http.Response.Body = output;
        StorageRequestContext.Set(http, new StorageRequestContext
        {
            RequestId = Guid.NewGuid().ToString("N"),
            Account = SavaWebApplicationFactory.AccountName,
            Container = container,
            Blob = blob,
            CanonicalResourcePath = $"/{SavaWebApplicationFactory.AccountName}/{container}/{blob}",
            ResourceKind = StorageResourceKind.Blob,
            ServiceVersion = "2026-06-06",
            Authorization = StorageAuthorization.Owner
        });
        return http;
    }

    private static async Task AssertResponseBytesAsync(
        string format, byte[] bytes, DefaultHttpContext http, MemoryStream output, CancellationToken cancellationToken)
    {
        Assert.Equal(StatusCodes.Status206PartialContent, http.Response.StatusCode);
        if (string.Equals(format, "structured", StringComparison.Ordinal))
        {
            output.Position = 0;
            using var decoded = new MemoryStream();
            await StructuredBodyDecoder.DecodeAsync(output, decoded, output.Length, bytes.Length, bytes.Length, cancellationToken).ConfigureAwait(false);
            Assert.Equal(bytes, decoded.ToArray());
        }
        else
        {
            Assert.Equal(bytes, output.ToArray());
            var md5 = string.Equals(format, "md5", StringComparison.Ordinal);
            var crc64 = new StorageCrc64();
            crc64.Append(bytes);
            var expected = md5 ? AzureProtocolChecksum.Md5(bytes) : crc64.GetHash();
            Assert.Equal(Convert.ToBase64String(expected), http.Response.Headers[md5 ? "Content-MD5" : "x-ms-content-crc64"].ToString());
        }
    }

    private static ValueTask<RateLimitLease> AcquireReadOrQueryAsync(StorageWorkAdmission admission, bool query)
    {
        if (query)
            return admission.AcquireQueryAsync(CancellationToken.None);
        return admission.AcquireReadAsync(CancellationToken.None);
    }

    private static SavaWebApplicationFactory CreateApplication() => new(new Dictionary<string, string?>(StringComparer.Ordinal)
    {
        ["Sava:MaximumConcurrentStorageReads"] = "1",
        ["Sava:MaximumConcurrentStorageWrites"] = "1",
        ["Sava:MaximumConcurrentBlobQueries"] = "1",
        ["Sava:MaximumQueuedStorageOperations"] = "0",
        ["Sava:MaintenanceScanInterval"] = "01:00:00"
    });

    private static BlobServiceClient CreateClient(SavaWebApplicationFactory application)
    {
        var options = new BlobClientOptions(BlobClientOptions.ServiceVersion.V2023_11_03)
        {
            Transport = new HttpClientTransport(application.CreateClient())
        };
        options.Retry.MaxRetries = 0;
        return new BlobServiceClient(new Uri($"http://{SavaWebApplicationFactory.AccountName}.localhost"),
            new StorageSharedKeyCredential(SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey), options);
    }

    private sealed class PausedResponseStream : MemoryStream
    {
        private readonly TaskCompletionSource _entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public Task Entered => _entered.Task;
        public void Release() => _release.TrySetResult();

        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            _entered.TrySetResult();
            await _release.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            await base.WriteAsync(buffer, cancellationToken).ConfigureAwait(false);
        }
    }
}
