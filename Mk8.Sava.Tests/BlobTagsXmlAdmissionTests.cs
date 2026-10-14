using System.Net;
using System.Text;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Sas;

namespace Mk8.Sava.Tests;

public sealed class BlobTagsXmlAdmissionTests
{
    [Theory]
    [InlineData("<NotTags/>")]
    [InlineData("<Tags/>")]
    [InlineData("<Tags><Other/></Tags>")]
    [InlineData("<Tags><TagSet/><TagSet/></Tags>")]
    [InlineData("<Tags><TagSet><Other/></TagSet></Tags>")]
    [InlineData("<Tags><Wrapper><TagSet/></Wrapper></Tags>")]
    [InlineData("<Tags><TagSet><Tag><Key>a</Key><Key>b</Key><Value>v</Value></Tag></TagSet></Tags>")]
    [InlineData("<Tags><TagSet><Tag><Key>a</Key><Value>v</Value><Other/></Tag></TagSet></Tags>")]
    [InlineData("<Tags><TagSet><Tag><Key><Nested>a</Nested></Key><Value>v</Value></Tag></TagSet></Tags>")]
    [InlineData("<Tags><TagSet><Tag><Key>a</Key></Tag></TagSet></Tags>")]
    [InlineData("<Tags><TagSet>unexpected text</TagSet></Tags>")]
    public Task MalformedTagsCannotClearOrReplaceExistingState(string xml) => VerifyAsync(xml, refused: true, empty: false);

    [Theory]
    [InlineData("<Tags><TagSet/></Tags>", true)]
    [InlineData("<Tags><TagSet><Tag><Key>new</Key><Value>value</Value></Tag></TagSet></Tags>", false)]
    public Task AValidTagSetCanClearOrReplaceTags(string xml, bool empty) => VerifyAsync(xml, refused: false, empty);

    private static async Task VerifyAsync(string xml, bool refused, bool empty)
    {
        var application = new SavaWebApplicationFactory(TimeProvider.System,
            new Dictionary<string, string?>(StringComparer.Ordinal), disableMaintenance: true);
        await using var applicationLifetime = application.ConfigureAwait(false);
        using var transport = new HttpClientTransport(application.Server.CreateHandler());
        var owner = new BlobServiceClient(new Uri($"http://127.0.0.1/{SavaWebApplicationFactory.AccountName}"),
            new StorageSharedKeyCredential(SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey),
            new BlobClientOptions { Transport = transport, Retry = { MaxRetries = 0 } });
        var container = owner.GetBlobContainerClient("tags-admission");
        await container.CreateAsync().ConfigureAwait(false);
        var blob = container.GetBlobClient("present.bin");
        await blob.UploadAsync(BinaryData.FromString("unchanged bytes")).ConfigureAwait(false);
        await blob.SetTagsAsync(new Dictionary<string, string>(StringComparer.Ordinal) { ["old"] = "retained" }).ConfigureAwait(false);
        var before = (await blob.GetPropertiesAsync().ConfigureAwait(false)).Value.ETag;
        var sas = blob.GenerateSasUri(BlobSasPermissions.Tag, DateTimeOffset.UtcNow.AddMinutes(5));
        using var client = new HttpClient(application.Server.CreateHandler());
        using var request = new HttpRequestMessage(HttpMethod.Put, new Uri(sas.AbsoluteUri + "&comp=tags"))
        {
            Content = new StringContent(xml, Encoding.UTF8, "application/xml"),
        };
        request.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
        using var response = await client.SendAsync(request).ConfigureAwait(false);
        Assert.Equal(refused ? HttpStatusCode.BadRequest : HttpStatusCode.NoContent, response.StatusCode);
        if (refused)
            Assert.Equal("InvalidXmlDocument", Assert.Single(response.Headers.GetValues("x-ms-error-code")));
        var tags = (await blob.GetTagsAsync().ConfigureAwait(false)).Value.Tags;
        if (refused)
            Assert.Equal(new KeyValuePair<string, string>("old", "retained"), Assert.Single(tags));
        else if (empty)
            Assert.Empty(tags);
        else
            Assert.Equal(new KeyValuePair<string, string>("new", "value"), Assert.Single(tags));
        Assert.Equal(before, (await blob.GetPropertiesAsync().ConfigureAwait(false)).Value.ETag);
        Assert.Equal("unchanged bytes", (await blob.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToString());
    }
}
