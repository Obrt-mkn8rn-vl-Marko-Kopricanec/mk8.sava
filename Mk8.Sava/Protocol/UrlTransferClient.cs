using System.Net;
using System.Net.Http.Headers;
using Microsoft.Extensions.Options;
using Mk8.Sava.Configuration;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Protocol;

internal sealed record UrlSource(
    Stream Content,
    long? ContentLength,
    BlobHttpProperties Http,
    string? ETag);

internal sealed class UrlTransferClient(
    HttpClient client,
    StoragePaths paths,
    IOptions<SavaOptions> configuredOptions)
{
    private readonly SavaOptions _options = configuredOptions.Value;

    public async Task<TResult> ReadAsync<TResult>(
        HttpRequest destinationRequest,
        string sourceValue,
        string? sourceRange,
        Func<UrlSource, Task<TResult>> consume,
        CancellationToken cancellationToken)
    {
        if (!Uri.TryCreate(sourceValue, UriKind.Absolute, out var sourceUri) ||
            sourceUri.Scheme is not ("http" or "https"))
        {
            throw AzureStorageException.InvalidHeader("x-ms-copy-source", sourceValue);
        }

        using var sourceRequest = new HttpRequestMessage(HttpMethod.Get, sourceUri);
        AddSourceAuthorization(destinationRequest, sourceRequest);
        AddSourceConditions(destinationRequest, sourceRequest);
        if (sourceRange is not null)
            sourceRequest.Headers.TryAddWithoutValidation("Range", sourceRange);

        using var response = await client.SendAsync(
            sourceRequest,
            HttpCompletionOption.ResponseHeadersRead,
            cancellationToken);
        if (!response.IsSuccessStatusCode)
        {
            throw new AzureStorageException(
                StatusCodes.Status400BadRequest,
                "CannotVerifyCopySource",
                $"The source returned HTTP status {(int)response.StatusCode} ({response.ReasonPhrase}).");
        }
        if (sourceRange is not null && response.StatusCode != HttpStatusCode.PartialContent)
        {
            throw new AzureStorageException(
                StatusCodes.Status400BadRequest,
                "CannotVerifyCopySource",
                "The source did not honor the requested byte range.");
        }

        await using var source = await response.Content.ReadAsStreamAsync(cancellationToken);
        var sourceInfo = new UrlSource(
            source,
            response.Content.Headers.ContentRange?.Length ?? response.Content.Headers.ContentLength,
            ReadHttpProperties(response),
            response.Headers.ETag?.ToString());
        return await ConsumeWithChecksumValidationAsync(
            destinationRequest,
            sourceInfo,
            consume,
            cancellationToken);
    }

    private async Task<TResult> ConsumeWithChecksumValidationAsync<TResult>(
        HttpRequest request,
        UrlSource source,
        Func<UrlSource, Task<TResult>> consume,
        CancellationToken cancellationToken)
    {
        var expectedMd5Text = ProtocolParsing.First(request.Headers, "x-ms-source-content-md5");
        var expectedCrc64Text = ProtocolParsing.First(request.Headers, "x-ms-source-content-crc64");
        if (expectedMd5Text is not null && expectedCrc64Text is not null)
        {
            throw new AzureStorageException(
                StatusCodes.Status400BadRequest,
                "BothCrc64AndMd5Specified",
                "Both CRC64 and MD5 were specified for the source. Specify only one checksum.");
        }
        if (expectedMd5Text is null && expectedCrc64Text is null)
            return await consume(source);

        var headerName = expectedMd5Text is null ? "x-ms-source-content-crc64" : "x-ms-source-content-md5";
        var expected = DecodeChecksum(expectedMd5Text ?? expectedCrc64Text!, expectedMd5Text is null ? 8 : 16, headerName);
        var temporaryPath = Path.Combine(paths.Staging, $"url-source-{Guid.NewGuid():N}.tmp");
        try
        {
            await using var temporary = new FileStream(
                temporaryPath,
                FileMode.CreateNew,
                FileAccess.ReadWrite,
                FileShare.None,
                128 * 1024,
                FileOptions.Asynchronous | FileOptions.SequentialScan);
            using var md5 = expectedMd5Text is null ? null : IncrementalHash.CreateHash(HashAlgorithmName.MD5);
            var crc64 = expectedCrc64Text is null ? null : new StorageCrc64();
            var buffer = new byte[128 * 1024];
            long length = 0;
            while (true)
            {
                var read = await source.Content.ReadAsync(buffer, cancellationToken);
                if (read == 0)
                    break;
                length = checked(length + read);
                if (length > _options.MaximumRequestBodyBytes)
                    throw new RequestBodyTooLargeException(_options.MaximumRequestBodyBytes);
                md5?.AppendData(buffer, 0, read);
                crc64?.Append(buffer.AsSpan(0, read));
                await temporary.WriteAsync(buffer.AsMemory(0, read), cancellationToken);
            }

            var actual = md5?.GetHashAndReset() ?? crc64!.GetHash();
            if (!CryptographicOperations.FixedTimeEquals(expected, actual))
            {
                throw new AzureStorageException(
                    StatusCodes.Status400BadRequest,
                    expectedMd5Text is null ? "Crc64Mismatch" : "Md5Mismatch",
                    "The checksum specified for the source did not match its content.");
            }

            temporary.Position = 0;
            return await consume(source with { Content = temporary, ContentLength = length });
        }
        finally
        {
            if (File.Exists(temporaryPath))
                File.Delete(temporaryPath);
        }
    }

    private static void AddSourceAuthorization(HttpRequest destination, HttpRequestMessage source)
    {
        var value = ProtocolParsing.First(destination.Headers, "x-ms-copy-source-authorization");
        if (value is null)
            return;
        if (!AuthenticationHeaderValue.TryParse(value, out var authorization) ||
            !string.Equals(authorization.Scheme, "Bearer", StringComparison.OrdinalIgnoreCase))
        {
            throw AzureStorageException.InvalidHeader("x-ms-copy-source-authorization", value);
        }
        source.Headers.Authorization = authorization;
    }

    private static void AddSourceConditions(HttpRequest destination, HttpRequestMessage source)
    {
        Copy("x-ms-source-if-match", "If-Match");
        Copy("x-ms-source-if-none-match", "If-None-Match");
        Copy("x-ms-source-if-modified-since", "If-Modified-Since");
        Copy("x-ms-source-if-unmodified-since", "If-Unmodified-Since");
        Copy("x-ms-source-if-tags", "x-ms-if-tags");
        return;

        void Copy(string sourceName, string destinationName)
        {
            if (destination.Headers.TryGetValue(sourceName, out var values))
                source.Headers.TryAddWithoutValidation(destinationName, values.ToArray());
        }
    }

    private static BlobHttpProperties ReadHttpProperties(HttpResponseMessage response) => new()
    {
        ContentType = response.Content.Headers.ContentType?.ToString() ?? "application/octet-stream",
        ContentEncoding = Join(response.Content.Headers.ContentEncoding),
        ContentLanguage = Join(response.Content.Headers.ContentLanguage),
        CacheControl = response.Headers.CacheControl?.ToString(),
        ContentDisposition = response.Content.Headers.ContentDisposition?.ToString(),
        ContentMd5 = response.Content.Headers.ContentMD5 is { Length: > 0 } md5 ? Convert.ToBase64String(md5) : null
    };

    private static string? Join(IEnumerable<string> values)
    {
        var value = string.Join(',', values);
        return string.IsNullOrEmpty(value) ? null : value;
    }

    private static byte[] DecodeChecksum(string value, int requiredLength, string headerName)
    {
        try
        {
            var bytes = Convert.FromBase64String(value);
            if (bytes.Length == requiredLength)
                return bytes;
        }
        catch (FormatException)
        {
        }
        throw AzureStorageException.InvalidHeader(headerName, value);
    }
}
