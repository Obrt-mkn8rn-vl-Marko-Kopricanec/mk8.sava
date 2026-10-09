using System.Net;

namespace Mk8.Sava.Tests;

public sealed partial class KestrelUploadTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task LargeOrdinaryConditionalRejectionsCompleteWithoutChangingTheBlob(
        bool ifNoneMatch, bool configuredBoundary)
    {
        var server = await RunningServer.StartAsync(configuredBoundary ? LargeContentLength : null)
            .ConfigureAwait(true);
        Exception? primary = null;
        var cleanupCompleted = false;
        try
        {
            try
            {
                await AssertRejectedUploadAsync(server, ifNoneMatch).ConfigureAwait(true);
            }
            catch (Exception failure)
            {
                primary = failure;
                throw;
            }
            finally
            {
                await server.DisposeAsync().ConfigureAwait(true);
                cleanupCompleted = true;
            }
        }
        catch (Exception failure)
        {
            if (primary is not null && !cleanupCompleted)
                throw new AggregateException("Conditional upload verification and owned host retirement both failed.", primary, failure);
            throw;
        }
    }

    private static async Task AssertRejectedUploadAsync(RunningServer server, bool ifNoneMatch)
    {
        var container = server.Client.GetBlobContainerClient("conditional-large-uploads");
        await container.CreateAsync().ConfigureAwait(false);
        var blob = container.GetBlobClient("existing");
        var content = CreateContent(LargeContentLength);
        var source = new MemoryStream(content, writable: false);
        await using (source.ConfigureAwait(false))
            await blob.UploadAsync(source).ConfigureAwait(false);
        var before = (await blob.GetPropertiesAsync().ConfigureAwait(false)).Value;

        var rejectedContent = CreateContent(LargeContentLength);
        using var transport = new HttpClient();
        using var request = CreateUpload(blob, rejectedContent, "raw");
        // Match clients that send immediately: a conditional rejection must not rely on withholding the body.
        request.Headers.ExpectContinue = false;
        request.Headers.Add(ifNoneMatch ? "If-None-Match" : "If-Match", ifNoneMatch ? "*" : "\"unmatched-etag\"");
        using var response = await transport.SendAsync(request).ConfigureAwait(false);

        Assert.Equal(HttpStatusCode.PreconditionFailed, response.StatusCode);
        Assert.Equal("ConditionNotMet", response.Headers.GetValues("x-ms-error-code").Single());
        var error = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
        Assert.Contains("<Code>ConditionNotMet</Code>", error, StringComparison.Ordinal);
        Assert.Equal(before.ETag, (await blob.GetPropertiesAsync().ConfigureAwait(false)).Value.ETag);
        await AssertContentAsync(blob, content).ConfigureAwait(false);
    }
}
