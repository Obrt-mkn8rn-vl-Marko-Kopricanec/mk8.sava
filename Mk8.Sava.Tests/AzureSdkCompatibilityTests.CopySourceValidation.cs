using System.Net;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Sas;
using Microsoft.AspNetCore.WebUtilities;

namespace Mk8.Sava.Tests;

public sealed partial class AzureSdkCompatibilityTests
{
    [Fact]
    public async Task InternalCopyShortcutRejectsUserInfoAndHonorsSourceAuthority()
    {
        var container = CreateClient(factory).GetBlobContainerClient($"copy-authority-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var source = container.GetBlobClient("source.bin");
        await source.UploadAsync(BinaryData.FromString("verified source"));
        var sourceSas = source.GenerateSasUri(BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddMinutes(10));
        var withUserInfo = new UriBuilder(sourceSas)
        {
            UserName = "source-user",
            Password = "source-password"
        }.Uri;
        var otherPort = new UriBuilder(sourceSas) { Port = sourceSas.Port + 1 }.Uri;
        var otherScheme = new UriBuilder(sourceSas) { Scheme = Uri.UriSchemeHttps, Port = 443 }.Uri;
        var otherProtocol = new UriBuilder(sourceSas) { Scheme = "ftp", Port = 21 }.Uri;
        var sourceSignature = QueryHelpers.ParseQuery(sourceSas.Query)["sig"].ToString();
        using var transport = new HttpClient(factory.Server.CreateHandler());

        foreach (var (name, sourceUri, synchronous, expectedStatus, expectedError) in new[]
                 {
                     ("userinfo-async", withUserInfo, false, HttpStatusCode.BadRequest, "InvalidHeaderValue"),
                     ("userinfo-sync", withUserInfo, true, HttpStatusCode.BadRequest, "InvalidHeaderValue"),
                     ("different-port", otherPort, false, HttpStatusCode.InternalServerError, "CannotVerifyCopySource"),
                     ("different-scheme", otherScheme, false, HttpStatusCode.InternalServerError, "CannotVerifyCopySource"),
                     ("non-http", otherProtocol, false, HttpStatusCode.BadRequest, "InvalidHeaderValue")
                 })
        {
            var destination = container.GetBlobClient($"{name}.bin");
            using var request = CreateRawCopyRequest(destination.GenerateSasUri(
                BlobSasPermissions.Create | BlobSasPermissions.Write,
                DateTimeOffset.UtcNow.AddMinutes(10)), sourceUri, synchronous);

            using var response = await transport.SendAsync(request);

            Assert.Equal(expectedStatus, response.StatusCode);
            Assert.Equal(expectedError, response.Headers.GetValues("x-ms-error-code").Single());
            var body = await response.Content.ReadAsStringAsync();
            Assert.DoesNotContain("source-user", body, StringComparison.Ordinal);
            Assert.DoesNotContain("source-password", body, StringComparison.Ordinal);
            Assert.DoesNotContain(sourceSignature, body, StringComparison.Ordinal);
            Assert.False((await destination.ExistsAsync()).Value);
        }

        var accepted = container.GetBlobClient("accepted.bin");
        var fragmentSource = new UriBuilder(sourceSas) { Fragment = "fragment-secret" }.Uri;
        using var acceptedRequest = CreateRawCopyRequest(accepted.GenerateSasUri(
            BlobSasPermissions.Create | BlobSasPermissions.Write,
            DateTimeOffset.UtcNow.AddMinutes(10)), fragmentSource, synchronous: true);
        using var acceptedResponse = await transport.SendAsync(acceptedRequest);
        Assert.Equal(HttpStatusCode.Accepted, acceptedResponse.StatusCode);
        var copySource = (await accepted.GetPropertiesAsync()).Value.CopySource.AbsoluteUri;
        Assert.DoesNotContain("sig=", copySource, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(sourceSignature, copySource, StringComparison.Ordinal);
        Assert.DoesNotContain("fragment-secret", copySource, StringComparison.Ordinal);
        Assert.Equal("verified source", (await accepted.DownloadContentAsync()).Value.Content.ToString());
    }

    [Fact]
    public async Task IncrementalCopyRejectsUserInfoAndDifferentSourceAuthority()
    {
        var container = CreateClient(factory).GetBlobContainerClient($"incremental-authority-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var source = container.GetPageBlobClient("source.vhd");
        await source.CreateAsync(512);
        var snapshot = (await source.CreateSnapshotAsync()).Value.Snapshot;
        var sourceSas = source.WithSnapshot(snapshot).GenerateSasUri(
            BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddMinutes(10));
        var withUserInfo = new UriBuilder(sourceSas)
        {
            UserName = "source-user",
            Password = "source-password"
        }.Uri;
        var otherPort = new UriBuilder(sourceSas) { Port = sourceSas.Port + 1 }.Uri;
        var otherScheme = new UriBuilder(sourceSas) { Scheme = Uri.UriSchemeHttps, Port = 443 }.Uri;
        var sourceSignature = QueryHelpers.ParseQuery(sourceSas.Query)["sig"].ToString();
        using var transport = new HttpClient(factory.Server.CreateHandler());

        foreach (var (name, sourceUri, expectedStatus, expectedError) in new[]
                 {
                     ("userinfo", withUserInfo, HttpStatusCode.BadRequest, "InvalidHeaderValue"),
                     ("different-port", otherPort, HttpStatusCode.InternalServerError, "CannotVerifyCopySource"),
                     ("different-scheme", otherScheme, HttpStatusCode.InternalServerError, "CannotVerifyCopySource")
                 })
        {
            PageBlobClient destination = container.GetPageBlobClient($"{name}.vhd");
            var destinationSas = destination.GenerateSasUri(
                BlobSasPermissions.Create | BlobSasPermissions.Write,
                DateTimeOffset.UtcNow.AddMinutes(10));
            using var request = CreateRawCopyRequest(
                AppendQuery(destinationSas, "comp=incrementalcopy"), sourceUri, synchronous: false);

            using var response = await transport.SendAsync(request);

            Assert.Equal(expectedStatus, response.StatusCode);
            Assert.Equal(expectedError, response.Headers.GetValues("x-ms-error-code").Single());
            var body = await response.Content.ReadAsStringAsync();
            Assert.DoesNotContain("source-user", body, StringComparison.Ordinal);
            Assert.DoesNotContain("source-password", body, StringComparison.Ordinal);
            Assert.DoesNotContain(sourceSignature, body, StringComparison.Ordinal);
            Assert.False((await destination.ExistsAsync()).Value);
        }

        var accepted = container.GetPageBlobClient("accepted.vhd");
        var acceptedSas = accepted.GenerateSasUri(
            BlobSasPermissions.Create | BlobSasPermissions.Write,
            DateTimeOffset.UtcNow.AddMinutes(10));
        using var acceptedRequest = CreateRawCopyRequest(
            AppendQuery(acceptedSas, "comp=incrementalcopy"), sourceSas, synchronous: false);
        using var acceptedResponse = await transport.SendAsync(acceptedRequest);
        Assert.Equal(HttpStatusCode.Accepted, acceptedResponse.StatusCode);
        var acceptedCopySource = (await accepted.GetPropertiesAsync()).Value.CopySource.AbsoluteUri;
        Assert.DoesNotContain(sourceSignature, acceptedCopySource, StringComparison.Ordinal);
        Assert.DoesNotContain("sig=", acceptedCopySource, StringComparison.OrdinalIgnoreCase);
    }

    private static HttpRequestMessage CreateRawCopyRequest(Uri destination, Uri source, bool synchronous)
    {
        var request = new HttpRequestMessage(HttpMethod.Put, destination)
        {
            Content = new ByteArrayContent([])
        };
        request.Headers.TryAddWithoutValidation("x-ms-version", "2025-07-05");
        request.Headers.TryAddWithoutValidation("x-ms-copy-source", source.AbsoluteUri);
        if (synchronous)
            request.Headers.TryAddWithoutValidation("x-ms-requires-sync", "true");
        return request;
    }
}
