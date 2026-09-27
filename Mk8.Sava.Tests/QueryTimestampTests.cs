using System.Globalization;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Mk8.Sava.Protocol;

namespace Mk8.Sava.Tests;

public sealed class QueryTimestampTests(SavaWebApplicationFactory application) : IClassFixture<SavaWebApplicationFactory>
{
    [Theory]
    [InlineData("2007T", "2007-01-01T00:00:00.0000000+00:00")]
    [InlineData("2010-01-01T", "2010-01-01T00:00:00.0000000+00:00")]
    [InlineData("2026", "2026-01-01T00:00:00.0000000+00:00")]
    [InlineData("2026-09", "2026-09-01T00:00:00.0000000+00:00")]
    [InlineData("2024-02-29", "2024-02-29T00:00:00.0000000+00:00")]
    [InlineData("2024-02-29T10:15+02:30", "2024-02-29T10:15:00.0000000+02:30")]
    [InlineData("2024-02-29T10:15:30Z", "2024-02-29T10:15:30.0000000+00:00")]
    [InlineData("2024-02-29T10:15:30.1234567-05:30", "2024-02-29T10:15:30.1234567-05:30")]
    public void DocumentedTimestampGranularitiesUseExplicitDefaultsAndPreserveOffset(string text, string expected)
    {
        var plan = BlobQueryPlan.Parse($"SELECT TO_TIMESTAMP('{text}') FROM BlobStorage;");
        var result = plan.Select(new QueryRow([], []));
        Assert.NotNull(result);
        var timestamp = Assert.IsType<DateTimeOffset>(Assert.Single(result.Values).Value);
        Assert.Equal(expected, timestamp.ToString("O", CultureInfo.InvariantCulture));
    }

    [Theory]
    [InlineData("09/27/2026")]
    [InlineData("September 27, 2026")]
    [InlineData("12:34:56")]
    [InlineData("2026-09-27T10:15:30.Z")]
    public void NonIsoTimestampsDoNotGainCultureOrCurrentDateDependentMeaning(string text)
    {
        var plan = BlobQueryPlan.Parse($"SELECT TO_TIMESTAMP('{text}') FROM BlobStorage;");
        var error = Assert.Throws<BlobQueryDataException>(() => plan.Select(new QueryRow([], [])));
        Assert.Equal("InvalidType", error.Name);
    }

    [Theory]
    [InlineData("2007T", "2007-01-01T00:00:00.0000000+00:00", false)]
    [InlineData("2010-01-01T", "2010-01-01T00:00:00.0000000+00:00", false)]
    [InlineData("2007T", "2007-01-01T00:00:00.0000000+00:00", true)]
    [InlineData("2010-01-01T", "2010-01-01T00:00:00.0000000+00:00", true)]
    public async Task OfficialSdkReconstructsPartialTimestampsInCsvAndArrow(string text, string expected, bool arrow) =>
        await AssertSdkTimestampAsync(application, text, expected, arrow).ConfigureAwait(true);

    [Theory]
    [InlineData("09/27/2026")]
    [InlineData("September 27, 2026")]
    [InlineData("12:34:56")]
    [InlineData("2026-09-27T10:15:30.Z")]
    public async Task OfficialSdkReceivesFatalQueryErrorInsteadOfInventingATimestamp(string text) =>
        await AssertSdkTimestampErrorAsync(application, text).ConfigureAwait(true);

    private static async Task AssertSdkTimestampErrorAsync(SavaWebApplicationFactory application, string text)
    {
        var options = new BlobClientOptions(BlobClientOptions.ServiceVersion.V2023_11_03)
        {
            Transport = new HttpClientTransport(application.CreateClient())
        };
        var service = new BlobServiceClient(new Uri($"http://{SavaWebApplicationFactory.AccountName}.localhost"),
            new StorageSharedKeyCredential(SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey), options);
        var container = service.GetBlobContainerClient($"query-iso-error-{Guid.NewGuid():N}");
        await container.CreateAsync().ConfigureAwait(false);
        var blob = container.GetBlockBlobClient("dates.csv");
        using var upload = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(text + "\n"), writable: false);
        await blob.UploadAsync(upload).ConfigureAwait(false);
        var before = (await blob.GetPropertiesAsync().ConfigureAwait(false)).Value;
        var errors = new List<BlobQueryError>();
        var query = new BlobQueryOptions();
        query.ErrorHandler += errors.Add;
        var result = await blob.QueryAsync("SELECT TO_TIMESTAMP(_1) FROM BlobStorage;", query).ConfigureAwait(false);
        var content = result.Value.Content;
        await using var disposal = content.ConfigureAwait(false);
        using var reader = new StreamReader(content, leaveOpen: true);
        Assert.Empty(await reader.ReadToEndAsync().ConfigureAwait(false));
        var error = Assert.Single(errors);
        Assert.True(error.IsFatal);
        Assert.Equal("InvalidType", error.Name);
        Assert.Equal(before.ETag, (await blob.GetPropertiesAsync().ConfigureAwait(false)).Value.ETag);
        Assert.Equal(text + "\n", (await blob.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToString());
    }

    private static async Task AssertSdkTimestampAsync(SavaWebApplicationFactory application,
        string text, string expected, bool arrow)
    {
        var options = new BlobClientOptions(BlobClientOptions.ServiceVersion.V2023_11_03)
        {
            Transport = new HttpClientTransport(application.CreateClient())
        };
        var service = new BlobServiceClient(new Uri($"http://{SavaWebApplicationFactory.AccountName}.localhost"),
            new StorageSharedKeyCredential(SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey), options);
        var container = service.GetBlobContainerClient($"query-iso-{Guid.NewGuid():N}");
        await container.CreateAsync().ConfigureAwait(false);
        var blob = container.GetBlockBlobClient("dates.csv");
        using var upload = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(text + "\n"), writable: false);
        await blob.UploadAsync(upload).ConfigureAwait(false);
        var query = new BlobQueryOptions();
        if (arrow)
        {
            query.OutputTextConfiguration = new BlobQueryArrowOptions
            {
                Schema = { new BlobQueryArrowField { Name = "date", Type = BlobQueryArrowFieldType.Timestamp } }
            };
        }
        var result = await blob.QueryAsync(arrow ? "SELECT _1 FROM BlobStorage;" :
            "SELECT TO_TIMESTAMP(_1) FROM BlobStorage;", query).ConfigureAwait(false);
        var content = result.Value.Content;
        await using var disposal = content.ConfigureAwait(false);
        if (arrow)
        {
            using var reader = new ArrowStreamReader(content, leaveOpen: true);
            using var batch = await reader.ReadNextRecordBatchAsync().ConfigureAwait(false);
            Assert.NotNull(batch);
            var timestamp = Assert.IsType<TimestampArray>(batch.Column(0)).GetTimestamp(0);
            Assert.NotNull(timestamp);
            Assert.Equal(expected, timestamp.Value.ToString("O", CultureInfo.InvariantCulture));
            Assert.Null(await reader.ReadNextRecordBatchAsync().ConfigureAwait(false));
        }
        else
        {
            using var reader = new StreamReader(content, leaveOpen: true);
            Assert.Equal(expected + "\n", await reader.ReadToEndAsync().ConfigureAwait(false));
        }
    }
}
