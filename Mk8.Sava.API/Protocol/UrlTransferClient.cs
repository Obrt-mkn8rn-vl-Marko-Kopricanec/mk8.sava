using System.Globalization;
using System.Net;
using System.Net.Http.Headers;
using System.Xml;
using System.Xml.Linq;
using Microsoft.Extensions.Options;
using Mk8.Sava.Configuration;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Protocol;

internal sealed record UrlSource(
    Stream Content,
    long? ContentLength,
    BlobHttpProperties Http,
    Dictionary<string, string> Metadata,
    Dictionary<string, string> Tags,
    BlobKind? Kind,
    string? ETag,
    long SequenceNumber,
    bool IsSealed,
    int AppendBlockCount,
    IReadOnlyList<CopySourceBlock> CommittedBlocks,
    IReadOnlyList<PageRange> PageRanges);

internal sealed record UrlTransferResult<TResult>(TResult Value, TransactionalChecksums Checksums);

internal sealed class SourceCustomerProvidedKey(
    string encodedKey,
    string encodedHash,
    string algorithm)
{
    public string EncodedKey { get; } = encodedKey;
    public string EncodedHash { get; } = encodedHash;
    public string Algorithm { get; } = algorithm;
}

internal sealed class UrlTransferClient(
    HttpClient client,
    StoragePaths paths,
    IOptions<SavaOptions> configuredOptions)
{
    private readonly SavaOptions _options = configuredOptions.Value;

    public async Task<UrlTransferResult<TResult>> ReadAsync<TResult>(
        HttpRequest destinationRequest,
        string sourceValue,
        string? sourceRange,
        bool allowSourceCustomerProvidedKey,
        long maximumBytes,
        bool sourceLengthConflict,
        Func<UrlSource, Task<TResult>> consume,
        CancellationToken cancellationToken,
        bool copySourceTags = false,
        bool preserveSourceShape = false)
    {
        var effectiveMaximumBytes = Math.Min(maximumBytes, _options.MaximumRequestBodyBytes);
        if (effectiveMaximumBytes <= 0)
            throw new ArgumentOutOfRangeException(nameof(maximumBytes));
        if (ProtocolParsing.First(destinationRequest.Headers, "x-ms-source-content-crc64") is not null &&
            !IsServiceVersionAtLeast(destinationRequest, new DateOnly(2019, 2, 2)))
        {
            throw new AzureStorageException(
                StatusCodes.Status400BadRequest,
                "FeatureVersionMismatch",
                "Source transactional CRC64 checksums require service version 2019-02-02 or later.");
        }
        if (!Uri.TryCreate(sourceValue, UriKind.Absolute, out var sourceUri) ||
            sourceUri.Scheme is not ("http" or "https"))
        {
            throw AzureStorageException.InvalidHeader("x-ms-copy-source");
        }

        var sourceTags = copySourceTags
            ? await ReadSourceTagsAsync(destinationRequest, sourceUri, cancellationToken)
            : new Dictionary<string, string>(StringComparer.Ordinal);

        using var sourceRequest = new HttpRequestMessage(HttpMethod.Get, sourceUri);
        AddSourceAuthorization(destinationRequest, sourceRequest);
        AddSourceConditions(destinationRequest, sourceRequest);
        sourceRequest.Headers.TryAddWithoutValidation(
            "x-ms-version",
            StorageRequestContext.Get(destinationRequest.HttpContext).ServiceVersion);
        AddSourceCustomerProvidedKey(
            destinationRequest,
            sourceRequest,
            sourceUri,
            allowSourceCustomerProvidedKey);
        if (sourceRange is not null)
            sourceRequest.Headers.TryAddWithoutValidation("Range", sourceRange);

        HttpResponseMessage response;
        try
        {
            response = await client.SendAsync(
                sourceRequest,
                HttpCompletionOption.ResponseHeadersRead,
                cancellationToken);
        }
        catch (HttpRequestException)
        {
            throw CannotVerifyCopySource("The source could not be reached or did not complete a valid HTTP response.");
        }
        catch (TaskCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            throw CannotVerifyCopySource("The source did not complete the request before the transfer timeout.");
        }
        using (response)
        {
            if (!response.IsSuccessStatusCode)
            {
                if (HasSourceConditions(destinationRequest) &&
                    response.StatusCode is HttpStatusCode.NotModified or HttpStatusCode.PreconditionFailed)
                {
                    throw AzureStorageException.SourceConditionNotMet();
                }
                throw await CreateSourceFailureAsync(
                    destinationRequest,
                    response,
                    cancellationToken);
            }
            if (sourceRange is not null && response.StatusCode != HttpStatusCode.PartialContent)
            {
                throw CannotVerifyCopySource("The source did not honor the requested byte range.");
            }

            var contentLength = response.Content.Headers.ContentLength;
            if (!contentLength.HasValue &&
                response.Content.Headers.ContentRange is { From: { } from, To: { } to })
            {
                contentLength = checked(to - from + 1);
            }
            if (!contentLength.HasValue)
                throw CannotVerifyCopySource("The source did not return a valid Content-Length value.");
            if (contentLength.Value > effectiveMaximumBytes)
            {
                if (sourceLengthConflict)
                {
                    throw new AzureStorageException(
                        StatusCodes.Status409Conflict,
                        "CannotVerifyCopySource",
                        $"The source exceeds the maximum permitted length of {effectiveMaximumBytes} bytes.");
                }
                throw new RequestBodyTooLargeException(effectiveMaximumBytes);
            }
            var kind = ReadBlobKind(response);
            var etag = response.Headers.ETag?.ToString();
            var sourceShape = preserveSourceShape
                ? await ReadSourceShapeAsync(
                    destinationRequest,
                    sourceUri,
                    response,
                    kind,
                    etag,
                    contentLength.Value,
                    cancellationToken)
                : UrlSourceShape.Empty;
            await using var source = await response.Content.ReadAsStreamAsync(cancellationToken);
            var sourceInfo = new UrlSource(
                new LengthLimitedReadStream(source, effectiveMaximumBytes),
                contentLength,
                ReadHttpProperties(response),
                ReadMetadata(response),
                sourceTags,
                kind,
                etag,
                sourceShape.SequenceNumber,
                sourceShape.IsSealed,
                sourceShape.AppendBlockCount,
                sourceShape.CommittedBlocks,
                sourceShape.PageRanges);
            return await ConsumeWithChecksumValidationAsync(
                destinationRequest,
                sourceInfo,
                effectiveMaximumBytes,
                consume,
                cancellationToken);
        }
    }

    private async Task<UrlTransferResult<TResult>> ConsumeWithChecksumValidationAsync<TResult>(
        HttpRequest request,
        UrlSource source,
        long maximumBytes,
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
        {
            using var hashingSource = new TransactionalChecksumReadStream(source.Content);
            var value = await consume(source with { Content = hashingSource });
            return new UrlTransferResult<TResult>(value, hashingSource.Complete());
        }

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
            using var md5 = IncrementalHash.CreateHash(HashAlgorithmName.MD5);
            var crc64 = new StorageCrc64();
            var buffer = new byte[128 * 1024];
            long length = 0;
            while (true)
            {
                var read = await source.Content.ReadAsync(buffer, cancellationToken);
                if (read == 0)
                    break;
                length = checked(length + read);
                if (length > maximumBytes)
                    throw new RequestBodyTooLargeException(maximumBytes);
                md5.AppendData(buffer, 0, read);
                crc64.Append(buffer.AsSpan(0, read));
                await temporary.WriteAsync(buffer.AsMemory(0, read), cancellationToken);
            }

            var checksums = new TransactionalChecksums(md5.GetHashAndReset(), crc64.GetHash());
            var actual = expectedMd5Text is null ? checksums.Crc64 : checksums.Md5;
            if (!CryptographicOperations.FixedTimeEquals(expected, actual))
            {
                throw new AzureStorageException(
                    StatusCodes.Status400BadRequest,
                    expectedMd5Text is null ? "Crc64Mismatch" : "Md5Mismatch",
                    "The checksum specified for the source did not match its content.");
            }

            temporary.Position = 0;
            var value = await consume(source with { Content = temporary, ContentLength = length });
            return new UrlTransferResult<TResult>(value, checksums);
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
        if (!IsServiceVersionAtLeast(destination, new DateOnly(2020, 10, 2)))
        {
            throw new AzureStorageException(
                StatusCodes.Status400BadRequest,
                "FeatureVersionMismatch",
                "Copy source authorization requires service version 2020-10-02 or later.");
        }
        if (!AuthenticationHeaderValue.TryParse(value, out var authorization) ||
            !string.Equals(authorization.Scheme, "Bearer", StringComparison.OrdinalIgnoreCase))
        {
            throw AzureStorageException.InvalidHeader("x-ms-copy-source-authorization");
        }
        source.Headers.Authorization = authorization;
    }

    private static void AddSourceConditions(HttpRequest destination, HttpRequestMessage source)
    {
        Copy("x-ms-source-if-match", "If-Match");
        Copy("x-ms-source-if-none-match", "If-None-Match");
        Copy("x-ms-source-if-modified-since", "If-Modified-Since");
        Copy("x-ms-source-if-unmodified-since", "If-Unmodified-Since");
        if (destination.Headers.ContainsKey("x-ms-source-if-tags") &&
            !IsServiceVersionAtLeast(destination, new DateOnly(2019, 12, 12)))
        {
            throw new AzureStorageException(
                StatusCodes.Status400BadRequest,
                "FeatureVersionMismatch",
                "Source tag conditions require service version 2019-12-12 or later.");
        }
        Copy("x-ms-source-if-tags", "x-ms-if-tags");
        return;

        void Copy(string sourceName, string destinationName)
        {
            if (destination.Headers.TryGetValue(sourceName, out var values))
                source.Headers.TryAddWithoutValidation(destinationName, values.ToArray());
        }
    }

    private static void AddSourceCustomerProvidedKey(
        HttpRequest destination,
        HttpRequestMessage source,
        Uri sourceUri,
        bool allowed)
    {
        var encryption = ReadSourceCustomerProvidedKey(destination, sourceUri, allowed);
        if (encryption is null)
            return;
        source.Headers.TryAddWithoutValidation("x-ms-encryption-key", encryption.EncodedKey);
        source.Headers.TryAddWithoutValidation("x-ms-encryption-key-sha256", encryption.EncodedHash);
        source.Headers.TryAddWithoutValidation("x-ms-encryption-algorithm", encryption.Algorithm);
    }

    private static SourceCustomerProvidedKey? ReadSourceCustomerProvidedKey(
        HttpRequest request,
        Uri sourceUri,
        bool allowed)
    {
        var encodedKey = ProtocolParsing.First(request.Headers, "x-ms-source-encryption-key");
        var encodedHash = ProtocolParsing.First(request.Headers, "x-ms-source-encryption-key-sha256");
        var algorithm = ProtocolParsing.First(request.Headers, "x-ms-source-encryption-algorithm");
        var present = encodedKey is not null || encodedHash is not null || algorithm is not null;
        if (!present)
            return null;
        if (!allowed)
        {
            throw new AzureStorageException(
                StatusCodes.Status400BadRequest,
                "InvalidHeaderValue",
                "Source customer-provided key headers are not supported for this operation.",
                "x-ms-source-encryption-key",
                null);
        }

        var context = StorageRequestContext.Get(request.HttpContext);
        if (!DateOnly.TryParseExact(
                context.ServiceVersion,
                "yyyy-MM-dd",
                CultureInfo.InvariantCulture,
                DateTimeStyles.None,
                out var version) || version < new DateOnly(2026, 2, 6))
        {
            throw new AzureStorageException(
                StatusCodes.Status400BadRequest,
                "FeatureVersionMismatch",
                "Source customer-provided keys require service version 2026-02-06 or later.");
        }
        if (!request.IsHttps || !string.Equals(sourceUri.Scheme, Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase))
        {
            throw new AzureStorageException(
                StatusCodes.Status400BadRequest,
                "InvalidRequest",
                "Source customer-provided encryption keys require HTTPS for both request hops.");
        }
        if (encodedKey is null)
            throw AzureStorageException.InvalidHeader("x-ms-source-encryption-key");
        if (encodedHash is null)
            throw AzureStorageException.InvalidHeader("x-ms-source-encryption-key-sha256");
        if (!string.Equals(algorithm, "AES256", StringComparison.Ordinal))
            throw AzureStorageException.InvalidHeader("x-ms-source-encryption-algorithm", algorithm);

        byte[] key;
        byte[] suppliedHash;
        try
        {
            key = Convert.FromBase64String(encodedKey);
        }
        catch (FormatException)
        {
            throw AzureStorageException.InvalidHeader("x-ms-source-encryption-key");
        }
        try
        {
            try
            {
                suppliedHash = Convert.FromBase64String(encodedHash);
            }
            catch (FormatException)
            {
                throw AzureStorageException.InvalidHeader("x-ms-source-encryption-key-sha256", encodedHash);
            }
            if (key.Length != 32)
                throw AzureStorageException.InvalidHeader("x-ms-source-encryption-key");
            var actualHash = SHA256.HashData(key);
            if (suppliedHash.Length != actualHash.Length ||
                !CryptographicOperations.FixedTimeEquals(suppliedHash, actualHash))
            {
                throw AzureStorageException.InvalidHeader("x-ms-source-encryption-key-sha256", encodedHash);
            }
        }
        finally
        {
            CryptographicOperations.ZeroMemory(key);
        }
        return new SourceCustomerProvidedKey(encodedKey, encodedHash, "AES256");
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

    private static Dictionary<string, string> ReadMetadata(HttpResponseMessage response)
    {
        var headers = new HeaderDictionary();
        foreach (var header in response.Headers.Concat(response.Content.Headers))
        {
            if (header.Key.StartsWith("x-ms-meta-", StringComparison.OrdinalIgnoreCase))
                headers.Append(header.Key, header.Value.ToArray());
        }
        return ProtocolParsing.ReadMetadata(headers);
    }

    private async Task<Dictionary<string, string>> ReadSourceTagsAsync(
        HttpRequest destinationRequest,
        Uri sourceUri,
        CancellationToken cancellationToken)
    {
        var query = Microsoft.AspNetCore.WebUtilities.QueryHelpers.ParseQuery(sourceUri.Query);
        var values = query
            .Where(pair => !string.Equals(pair.Key, "comp", StringComparison.OrdinalIgnoreCase))
            .SelectMany(pair => pair.Value.Select(value => new KeyValuePair<string, string?>(pair.Key, value)))
            .Append(new KeyValuePair<string, string?>("comp", "tags"));
        var builder = new UriBuilder(sourceUri)
        {
            Query = QueryString.Create(values).Value?.TrimStart('?') ?? string.Empty
        };
        using var request = new HttpRequestMessage(HttpMethod.Get, builder.Uri);
        AddSourceAuthorization(destinationRequest, request);
        request.Headers.TryAddWithoutValidation(
            "x-ms-version",
            StorageRequestContext.Get(destinationRequest.HttpContext).ServiceVersion);

        HttpResponseMessage response;
        try
        {
            response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
        }
        catch (HttpRequestException)
        {
            throw CannotVerifyCopySource("The source tags could not be read.");
        }
        catch (TaskCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            throw CannotVerifyCopySource("The source tag request did not complete before the transfer timeout.");
        }

        using (response)
        {
            if (!response.IsSuccessStatusCode)
                throw await CreateSourceFailureAsync(destinationRequest, response, cancellationToken);
            await using var body = await response.Content.ReadAsStreamAsync(cancellationToken);
            return await ProtocolParsing.ReadTagsBodyAsync(body, cancellationToken);
        }
    }

    private async Task<UrlSourceShape> ReadSourceShapeAsync(
        HttpRequest destinationRequest,
        Uri sourceUri,
        HttpResponseMessage sourceResponse,
        BlobKind? kind,
        string? etag,
        long contentLength,
        CancellationToken cancellationToken)
    {
        switch (kind)
        {
            case BlobKind.BlockBlob:
                {
                    var blocks = await ReadCommittedBlocksAsync(
                        destinationRequest,
                        sourceUri,
                        etag,
                        cancellationToken);
                    long blockLength;
                    try
                    {
                        blockLength = blocks.Aggregate(0L, (total, block) => checked(total + block.Length));
                    }
                    catch (OverflowException)
                    {
                        throw CannotVerifyCopySource("The source committed block list length is invalid.");
                    }
                    if (blockLength != contentLength)
                        throw CannotVerifyCopySource("The source committed block list does not match its content length.");
                    return new UrlSourceShape(0, false, 0, blocks, []);
                }
            case BlobKind.AppendBlob:
                return new UrlSourceShape(
                    0,
                    ReadBooleanHeader("x-ms-blob-sealed", defaultValue: false),
                    ReadIntegerHeader("x-ms-blob-committed-block-count"),
                    [],
                    []);
            case BlobKind.PageBlob:
                {
                    if (contentLength % 512 != 0)
                        throw CannotVerifyCopySource("The source page blob length is not 512-byte aligned.");
                    var ranges = await ReadPageRangesAsync(
                        destinationRequest,
                        sourceUri,
                        etag,
                        contentLength,
                        cancellationToken);
                    return new UrlSourceShape(
                        ReadLongHeader("x-ms-blob-sequence-number"),
                        false,
                        0,
                        [],
                        ranges);
                }
            default:
                return UrlSourceShape.Empty;
        }

        bool ReadBooleanHeader(string name, bool defaultValue)
        {
            var value = CurrentResponseHeader(name);
            if (value is null)
                return defaultValue;
            return bool.TryParse(value, out var parsed)
                ? parsed
                : throw CannotVerifyCopySource($"The source returned an invalid {name} header.");
        }

        int ReadIntegerHeader(string name)
        {
            var value = CurrentResponseHeader(name);
            return int.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var parsed) && parsed >= 0
                ? parsed
                : throw CannotVerifyCopySource($"The source returned an invalid {name} header.");
        }

        long ReadLongHeader(string name)
        {
            var value = CurrentResponseHeader(name);
            return long.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var parsed) && parsed >= 0
                ? parsed
                : throw CannotVerifyCopySource($"The source returned an invalid {name} header.");
        }

        string? CurrentResponseHeader(string name)
        {
            if (sourceResponse.Headers.TryGetValues(name, out var values) ||
                sourceResponse.Content.Headers.TryGetValues(name, out values))
            {
                return values.SingleOrDefault();
            }
            return null;
        }
    }

    private async Task<IReadOnlyList<CopySourceBlock>> ReadCommittedBlocksAsync(
        HttpRequest destinationRequest,
        Uri sourceUri,
        string? etag,
        CancellationToken cancellationToken)
    {
        var document = await ReadSourceXmlAsync(
            destinationRequest,
            BuildComponentUri(
                sourceUri,
                new KeyValuePair<string, string?>("comp", "blocklist"),
                new KeyValuePair<string, string?>("blocklisttype", "committed")),
            etag,
            cancellationToken);
        var committed = document.Descendants()
            .FirstOrDefault(element => element.Name.LocalName == "CommittedBlocks");
        if (committed is null)
            throw CannotVerifyCopySource("The source did not return a committed block list.");

        var blocks = new List<CopySourceBlock>();
        int? blockIdLength = null;
        foreach (var block in committed.Elements().Where(element => element.Name.LocalName == "Block"))
        {
            var name = block.Elements().FirstOrDefault(element => element.Name.LocalName == "Name")?.Value;
            var sizeText = block.Elements().FirstOrDefault(element => element.Name.LocalName == "Size")?.Value;
            if (string.IsNullOrEmpty(name) ||
                !long.TryParse(sizeText, NumberStyles.None, CultureInfo.InvariantCulture, out var size) ||
                size < 0)
            {
                throw CannotVerifyCopySource("The source returned an invalid committed block list.");
            }
            try
            {
                var decoded = Convert.FromBase64String(name);
                if (decoded.Length is 0 or > BlobServiceLimits.MaximumBlockIdBytes)
                    throw new FormatException();
                if (blockIdLength.HasValue && blockIdLength.Value != decoded.Length)
                    throw new FormatException();
                blockIdLength = decoded.Length;
            }
            catch (FormatException)
            {
                throw CannotVerifyCopySource("The source returned an invalid committed block ID.");
            }
            blocks.Add(new CopySourceBlock(name, size));
            if (blocks.Count > BlobServiceLimits.MaximumCommittedBlockCount)
                throw CannotVerifyCopySource("The source committed block count exceeds the service limit.");
        }
        return blocks;
    }

    private async Task<IReadOnlyList<PageRange>> ReadPageRangesAsync(
        HttpRequest destinationRequest,
        Uri sourceUri,
        string? etag,
        long contentLength,
        CancellationToken cancellationToken)
    {
        var ranges = new List<PageRange>();
        var markers = new HashSet<string>(StringComparer.Ordinal);
        string? marker = null;
        do
        {
            var parameters = new List<KeyValuePair<string, string?>>
            {
                new("comp", "pagelist")
            };
            if (!string.IsNullOrEmpty(marker))
                parameters.Add(new KeyValuePair<string, string?>("marker", marker));
            var document = await ReadSourceXmlAsync(
                destinationRequest,
                BuildComponentUri(sourceUri, parameters.ToArray()),
                etag,
                cancellationToken);
            foreach (var range in document.Descendants().Where(element => element.Name.LocalName == "PageRange"))
            {
                var startText = range.Elements().FirstOrDefault(element => element.Name.LocalName == "Start")?.Value;
                var endText = range.Elements().FirstOrDefault(element => element.Name.LocalName == "End")?.Value;
                if (!long.TryParse(startText, NumberStyles.None, CultureInfo.InvariantCulture, out var start) ||
                    !long.TryParse(endText, NumberStyles.None, CultureInfo.InvariantCulture, out var end) ||
                    start < 0 || end < start || end == long.MaxValue ||
                    start % 512 != 0 || (end + 1) % 512 != 0 || end >= contentLength)
                {
                    throw CannotVerifyCopySource("The source returned an invalid page range list.");
                }
                ranges.Add(new PageRange(start, end));
            }
            marker = document.Descendants()
                .FirstOrDefault(element => element.Name.LocalName == "NextMarker")
                ?.Value;
            if (!string.IsNullOrEmpty(marker) && !markers.Add(marker))
                throw CannotVerifyCopySource("The source page range continuation marker repeated.");
        } while (!string.IsNullOrEmpty(marker));
        return ranges;
    }

    private async Task<XDocument> ReadSourceXmlAsync(
        HttpRequest destinationRequest,
        Uri uri,
        string? etag,
        CancellationToken cancellationToken)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, uri);
        AddSourceAuthorization(destinationRequest, request);
        request.Headers.TryAddWithoutValidation(
            "x-ms-version",
            StorageRequestContext.Get(destinationRequest.HttpContext).ServiceVersion);
        if (!string.IsNullOrEmpty(etag))
            request.Headers.TryAddWithoutValidation("If-Match", etag);

        HttpResponseMessage response;
        try
        {
            response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
        }
        catch (HttpRequestException)
        {
            throw CannotVerifyCopySource("The source shape could not be read.");
        }
        catch (TaskCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            throw CannotVerifyCopySource("The source shape request did not complete before the transfer timeout.");
        }

        using (response)
        {
            if (!response.IsSuccessStatusCode)
                throw await CreateSourceFailureAsync(destinationRequest, response, cancellationToken);
            try
            {
                await using var body = await response.Content.ReadAsStreamAsync(cancellationToken);
                using var reader = XmlReader.Create(body, new XmlReaderSettings
                {
                    Async = true,
                    DtdProcessing = DtdProcessing.Prohibit,
                    MaxCharactersInDocument = 32L * 1024 * 1024
                });
                return await XDocument.LoadAsync(reader, LoadOptions.None, cancellationToken);
            }
            catch (Exception exception) when (exception is XmlException or InvalidOperationException)
            {
                throw CannotVerifyCopySource("The source returned an invalid shape document.");
            }
        }
    }

    private static Uri BuildComponentUri(
        Uri sourceUri,
        params KeyValuePair<string, string?>[] componentParameters)
    {
        var replaced = componentParameters.Select(parameter => parameter.Key).ToHashSet(StringComparer.OrdinalIgnoreCase);
        var query = Microsoft.AspNetCore.WebUtilities.QueryHelpers.ParseQuery(sourceUri.Query);
        var values = query
            .Where(pair => !replaced.Contains(pair.Key))
            .SelectMany(pair => pair.Value.Select(value => new KeyValuePair<string, string?>(pair.Key, value)))
            .Concat(componentParameters);
        var builder = new UriBuilder(sourceUri)
        {
            Query = QueryString.Create(values).Value?.TrimStart('?') ?? string.Empty
        };
        return builder.Uri;
    }

    private sealed record UrlSourceShape(
        long SequenceNumber,
        bool IsSealed,
        int AppendBlockCount,
        IReadOnlyList<CopySourceBlock> CommittedBlocks,
        IReadOnlyList<PageRange> PageRanges)
    {
        public static UrlSourceShape Empty { get; } = new(0, false, 0, [], []);
    }

    private static BlobKind? ReadBlobKind(HttpResponseMessage response)
    {
        if (!response.Headers.TryGetValues("x-ms-blob-type", out var values))
            return null;
        return values.SingleOrDefault() switch
        {
            "BlockBlob" => BlobKind.BlockBlob,
            "AppendBlob" => BlobKind.AppendBlob,
            "PageBlob" => BlobKind.PageBlob,
            _ => null
        };
    }

    private static string? Join(IEnumerable<string> values)
    {
        var value = string.Join(',', values);
        return string.IsNullOrEmpty(value) ? null : value;
    }

    private static bool HasSourceConditions(HttpRequest request) =>
        request.Headers.ContainsKey("x-ms-source-if-match") ||
        request.Headers.ContainsKey("x-ms-source-if-none-match") ||
        request.Headers.ContainsKey("x-ms-source-if-modified-since") ||
        request.Headers.ContainsKey("x-ms-source-if-unmodified-since") ||
        request.Headers.ContainsKey("x-ms-source-if-tags");

    private static async Task<AzureStorageException> CreateSourceFailureAsync(
        HttpRequest destinationRequest,
        HttpResponseMessage response,
        CancellationToken cancellationToken)
    {
        var sourceStatus = ((int)response.StatusCode).ToString(CultureInfo.InvariantCulture);
        var sourceErrorCode = response.Headers.TryGetValues("x-ms-error-code", out var errorCodes)
            ? errorCodes.FirstOrDefault()
            : null;
        var sourceError = await ReadSourceErrorAsync(response.Content, cancellationToken);
        sourceErrorCode ??= sourceError.Code;
        var sourceErrorMessage = sourceError.Message;
        var message = sourceErrorMessage ??
                      $"Could not verify the copy source because it returned HTTP status {sourceStatus} ({response.ReasonPhrase}).";
        if (!IsServiceVersionAtLeast(destinationRequest, new DateOnly(2024, 2, 4)))
            return CannotVerifyCopySource(message);

        var details = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["CopySourceStatusCode"] = sourceStatus
        };
        var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["x-ms-copy-source-status-code"] = sourceStatus
        };
        if (!string.IsNullOrEmpty(sourceErrorCode))
        {
            details["CopySourceErrorCode"] = sourceErrorCode;
            headers["x-ms-copy-source-error-code"] = sourceErrorCode;
        }
        if (!string.IsNullOrEmpty(sourceErrorMessage))
            details["CopySourceErrorMessage"] = sourceErrorMessage;

        return new AzureStorageException(
            StatusCodes.Status500InternalServerError,
            "CannotVerifyCopySource",
            message,
            responseHeaders: headers,
            details: details);
    }

    private static async Task<(string? Code, string? Message)> ReadSourceErrorAsync(
        HttpContent content,
        CancellationToken cancellationToken)
    {
        const int maximumErrorBodyBytes = 64 * 1024;
        try
        {
            await using var source = await content.ReadAsStreamAsync(cancellationToken);
            using var buffer = new MemoryStream();
            var bytes = new byte[4096];
            while (buffer.Length <= maximumErrorBodyBytes)
            {
                var read = await source.ReadAsync(bytes, cancellationToken);
                if (read == 0)
                    break;
                await buffer.WriteAsync(bytes.AsMemory(0, read), cancellationToken);
            }
            if (buffer.Length == 0 || buffer.Length > maximumErrorBodyBytes)
                return (null, null);

            buffer.Position = 0;
            using var reader = XmlReader.Create(buffer, new XmlReaderSettings
            {
                DtdProcessing = DtdProcessing.Prohibit,
                MaxCharactersInDocument = maximumErrorBodyBytes
            });
            var document = XDocument.Load(reader, LoadOptions.None);
            var code = document.Descendants()
                .FirstOrDefault(element => element.Name.LocalName == "Code")
                ?.Value;
            var message = document.Descendants()
                .FirstOrDefault(element => element.Name.LocalName == "Message")
                ?.Value;
            return (code, message);
        }
        catch (Exception exception) when (exception is XmlException or InvalidOperationException or IOException)
        {
            return (null, null);
        }
    }

    private static AzureStorageException CannotVerifyCopySource(string message) => new(
        StatusCodes.Status500InternalServerError,
        "CannotVerifyCopySource",
        message);

    private static bool IsServiceVersionAtLeast(HttpRequest request, DateOnly minimum) =>
        DateOnly.TryParseExact(
            StorageRequestContext.Get(request.HttpContext).ServiceVersion,
            "yyyy-MM-dd",
            CultureInfo.InvariantCulture,
            DateTimeStyles.None,
            out var version) && version >= minimum;

    private sealed class LengthLimitedReadStream(Stream inner, long maximumBytes) : Stream
    {
        private long _read;

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();

        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override int Read(byte[] buffer, int offset, int count) =>
            Read(buffer.AsSpan(offset, count));

        public override int Read(Span<byte> buffer)
        {
            if (buffer.IsEmpty)
                return 0;
            var read = inner.Read(Limit(buffer));
            Record(read);
            return read;
        }

        public override async ValueTask<int> ReadAsync(
            Memory<byte> buffer,
            CancellationToken cancellationToken = default)
        {
            if (buffer.IsEmpty)
                return 0;
            var read = await inner.ReadAsync(Limit(buffer), cancellationToken);
            Record(read);
            return read;
        }

        public override Task<int> ReadAsync(
            byte[] buffer,
            int offset,
            int count,
            CancellationToken cancellationToken) =>
            ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

        public override void Flush() => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            // The owning response controls the source stream lifetime.
            base.Dispose(disposing);
        }

        private Span<byte> Limit(Span<byte> buffer)
        {
            var remaining = maximumBytes - _read;
            if (remaining <= 0)
            {
                var probe = inner.ReadByte();
                if (probe >= 0)
                    throw new RequestBodyTooLargeException(maximumBytes);
                return Span<byte>.Empty;
            }
            return buffer[..(int)Math.Min(buffer.Length, remaining)];
        }

        private Memory<byte> Limit(Memory<byte> buffer)
        {
            var remaining = maximumBytes - _read;
            if (remaining <= 0)
                return ProbeAsync(buffer);
            return buffer[..(int)Math.Min(buffer.Length, remaining)];
        }

        private Memory<byte> ProbeAsync(Memory<byte> buffer)
        {
            // Read one byte asynchronously on the next call so a source that lies
            // about Content-Length cannot publish more than the operation limit.
            return buffer[..1];
        }

        private void Record(int read)
        {
            if (read == 0)
                return;
            _read = checked(_read + read);
            if (_read > maximumBytes)
                throw new RequestBodyTooLargeException(maximumBytes);
        }
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
