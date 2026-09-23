using System.Globalization;
using System.Text;
using Microsoft.AspNetCore.WebUtilities;
using Microsoft.Extensions.Primitives;
using Microsoft.Net.Http.Headers;

namespace Mk8.Sava.Protocol;

internal static class BlobBatchProtocol
{
    public const int MaximumBodyBytes = 4 * 1024 * 1024;
    public const int MaximumSubrequests = 256;

    public static async Task<IReadOnlyList<BlobBatchSubrequest>> ReadAsync(
        HttpRequest request,
        CancellationToken cancellationToken)
    {
        if (request.ContentLength is null)
            throw InvalidBatch("A Content-Length header is required for a blob batch request.");
        if (request.ContentLength <= 0)
            throw InvalidBatch("A blob batch request cannot be empty.");
        if (request.ContentLength > MaximumBodyBytes)
            throw BatchTooLarge();

        if (!MediaTypeHeaderValue.TryParse(request.ContentType, out var mediaType) ||
            !string.Equals(mediaType.MediaType.Value, "multipart/mixed", StringComparison.OrdinalIgnoreCase))
        {
            throw AzureStorageException.InvalidHeader("Content-Type", request.ContentType);
        }

        var boundary = HeaderUtilities.RemoveQuotes(mediaType.Boundary).Value;
        if (!IsValidBoundary(boundary))
            throw AzureStorageException.InvalidHeader("Content-Type", request.ContentType);

        var body = new byte[request.ContentLength.Value];
        var consumed = 0;
        while (consumed < body.Length)
        {
            var read = await request.Body.ReadAsync(body.AsMemory(consumed), cancellationToken).ConfigureAwait(false);
            if (read == 0)
                throw InvalidBatch("The blob batch payload ended before Content-Length bytes were received.");
            consumed += read;
        }
        if (await request.Body.ReadAsync(new byte[1], cancellationToken).ConfigureAwait(false) != 0)
            throw BatchTooLarge();

        var payload = Encoding.Latin1.GetString(body);
        ValidateCrLf(payload);
        var reader = new BatchLineReader(payload);
        var delimiter = $"--{boundary}";
        var finalDelimiter = $"--{boundary}--";
        if (!string.Equals(reader.ReadLine(), delimiter, StringComparison.Ordinal))
            throw InvalidBatch("The multipart payload does not begin with its declared boundary.");

        var operations = new List<BlobBatchSubrequest>();
        BlobBatchOperationKind? commonKind = null;
        while (true)
        {
            var partHeaders = ReadHeaders(reader);
            if (!partHeaders.TryGetValue("Content-Type", out var partContentType) ||
                !string.Equals(partContentType, "application/http", StringComparison.OrdinalIgnoreCase) ||
                !partHeaders.TryGetValue("Content-Transfer-Encoding", out var transferEncoding) ||
                !string.Equals(transferEncoding, "binary", StringComparison.OrdinalIgnoreCase))
            {
                throw InvalidBatch("Each batch part must be application/http with binary transfer encoding.");
            }

            partHeaders.TryGetValue("Content-ID", out var contentId);
            var requestLine = reader.ReadLine();
            var firstSpace = requestLine.IndexOf(' ', StringComparison.Ordinal);
            var lastSpace = requestLine.LastIndexOf(' ');
            if (firstSpace <= 0 || lastSpace <= firstSpace + 1 ||
                !string.Equals(requestLine[(lastSpace + 1)..], "HTTP/1.1", StringComparison.Ordinal))
            {
                throw InvalidBatch("A batch subrequest has an invalid HTTP request line.");
            }

            var method = requestLine[..firstSpace];
            var target = requestLine[(firstSpace + 1)..lastSpace];
            var kind = method switch
            {
                "DELETE" => BlobBatchOperationKind.Delete,
                "PUT" => BlobBatchOperationKind.SetTier,
                _ => throw InvalidBatch("Blob Batch only supports Delete Blob and Set Blob Tier subrequests.")
            };
            if (commonKind.HasValue && commonKind.Value != kind)
                throw InvalidBatch("All blob batch subrequests must use the same operation type.");
            commonKind = kind;

            if (!target.StartsWith('/') ||
                target.StartsWith("//", StringComparison.Ordinal) ||
                target.Contains("://", StringComparison.Ordinal) ||
                target.Contains('#', StringComparison.Ordinal))
                throw InvalidBatch("A batch subrequest URI must contain only an absolute path and query.");
            var queryOffset = target.IndexOf('?', StringComparison.Ordinal);
            var rawPath = queryOffset < 0 ? target : target[..queryOffset];
            var queryString = queryOffset < 0 ? QueryString.Empty : new QueryString(target[queryOffset..]);
            var subrequestHeaders = ReadHeaders(reader);
            if (subrequestHeaders.ContainsKey("x-ms-version"))
                throw InvalidBatch("Batch subrequests inherit the service version and cannot specify x-ms-version.");
            if (subrequestHeaders.TryGetValue("Content-Length", out var contentLength) &&
                (!long.TryParse(contentLength, NumberStyles.None, CultureInfo.InvariantCulture, out var parsedLength) || parsedLength != 0))
            {
                throw InvalidBatch("Nested batch request bodies are not supported.");
            }
            if (subrequestHeaders.ContainsKey("Transfer-Encoding"))
                throw InvalidBatch("Nested batch request bodies are not supported.");

            var query = QueryHelpers.ParseQuery(queryString.Value ?? string.Empty);
            var component = query.TryGetValue("comp", out var componentValue) ? componentValue.ToString() : string.Empty;
            if ((kind == BlobBatchOperationKind.SetTier && !string.Equals(component, "tier", StringComparison.OrdinalIgnoreCase)) ||
                (kind == BlobBatchOperationKind.Delete && !string.IsNullOrEmpty(component)))
            {
                throw InvalidBatch("A batch subrequest does not identify the operation declared by its HTTP method.");
            }

            operations.Add(new BlobBatchSubrequest(
                kind,
                method,
                rawPath,
                queryString,
                subrequestHeaders,
                StringValues.IsNullOrEmpty(contentId) ? null : contentId.ToString()));
            if (operations.Count > MaximumSubrequests)
                throw InvalidBatch($"A blob batch cannot contain more than {MaximumSubrequests} subrequests.");

            var nextBoundary = reader.ReadLine();
            if (string.Equals(nextBoundary, finalDelimiter, StringComparison.Ordinal))
                break;
            if (!string.Equals(nextBoundary, delimiter, StringComparison.Ordinal))
                throw InvalidBatch("A batch subrequest contains a nested body or an invalid multipart boundary.");
        }

        if (!reader.End)
            throw InvalidBatch("Unexpected data follows the final multipart boundary.");
        if (operations.Count == 0)
            throw InvalidBatch("A blob batch request cannot be empty.");
        return operations;
    }

    public static async Task WriteAsync(
        HttpResponse response,
        IReadOnlyList<BlobBatchSubresponse> subresponses,
        CancellationToken cancellationToken)
    {
        var boundary = $"batchresponse_{Guid.NewGuid():D}";
        response.StatusCode = StatusCodes.Status202Accepted;
        response.ContentType = $"multipart/mixed; boundary={boundary}";

        foreach (var subresponse in subresponses)
        {
            await WriteLatin1Async(response.Body, $"--{boundary}\r\n", cancellationToken).ConfigureAwait(false);
            await WriteLatin1Async(response.Body, "Content-Type: application/http\r\n", cancellationToken).ConfigureAwait(false);
            if (subresponse.ContentId is not null)
                await WriteLatin1Async(response.Body, $"Content-ID: {subresponse.ContentId}\r\n", cancellationToken).ConfigureAwait(false);
            await WriteLatin1Async(
                response.Body,
                $"\r\nHTTP/1.1 {subresponse.StatusCode} {ReasonPhrases.GetReasonPhrase(subresponse.StatusCode)}\r\n",
                cancellationToken).ConfigureAwait(false);
            foreach (var header in subresponse.Headers)
                await WriteLatin1Async(response.Body, $"{header.Key}: {header.Value}\r\n", cancellationToken).ConfigureAwait(false);
            if (subresponse.Body.Length > 0 && !subresponse.Headers.ContainsKey("Content-Length"))
                await WriteLatin1Async(response.Body, $"Content-Length: {subresponse.Body.Length}\r\n", cancellationToken).ConfigureAwait(false);
            await WriteLatin1Async(response.Body, "\r\n", cancellationToken).ConfigureAwait(false);
            if (subresponse.Body.Length > 0)
                await response.Body.WriteAsync(subresponse.Body, cancellationToken).ConfigureAwait(false);
            await WriteLatin1Async(response.Body, "\r\n", cancellationToken).ConfigureAwait(false);
        }
        await WriteLatin1Async(response.Body, $"--{boundary}--\r\n", cancellationToken).ConfigureAwait(false);
    }

    private static Dictionary<string, StringValues> ReadHeaders(BatchLineReader reader)
    {
        var headers = new Dictionary<string, StringValues>(StringComparer.OrdinalIgnoreCase);
        while (true)
        {
            var line = reader.ReadLine();
            if (line.Length == 0)
                return headers;
            var colon = line.IndexOf(':', StringComparison.Ordinal);
            if (colon <= 0 || line[..colon].Any(character => !char.IsAsciiLetterOrDigit(character) && character != '-'))
                throw InvalidBatch("A batch part contains a malformed HTTP header.");
            var name = line[..colon];
            var value = line[(colon + 1)..].Trim();
            if (value.Any(char.IsControl))
                throw InvalidBatch("A batch part contains an invalid HTTP header value.");
            if (headers.TryGetValue(name, out var existing))
                headers[name] = StringValues.Concat(existing, value);
            else
                headers.Add(name, value);
        }
    }

    private static bool IsValidBoundary(string? boundary) =>
        !string.IsNullOrEmpty(boundary) &&
        boundary.Length <= 70 &&
        boundary.All(character => char.IsAsciiLetterOrDigit(character) || "'()+_,-./:=?".Contains(character, StringComparison.Ordinal));

    private static void ValidateCrLf(string payload)
    {
        for (var index = 0; index < payload.Length; index++)
        {
            if (payload[index] == '\r' && (index + 1 >= payload.Length || payload[index + 1] != '\n') ||
                payload[index] == '\n' && (index == 0 || payload[index - 1] != '\r'))
            {
                throw InvalidBatch("Each line in a blob batch payload must end with CRLF.");
            }
        }
    }

    private static Task WriteLatin1Async(Stream stream, string value, CancellationToken cancellationToken) =>
        stream.WriteAsync(Encoding.Latin1.GetBytes(value), cancellationToken).AsTask();

    private static AzureStorageException InvalidBatch(string message) => new(
        StatusCodes.Status400BadRequest,
        "InvalidInput",
        message);

    private static AzureStorageException BatchTooLarge() => new(
        StatusCodes.Status413PayloadTooLarge,
        "RequestBodyTooLarge",
        "The blob batch request body cannot exceed 4 MiB.");

    private sealed class BatchLineReader(string payload)
    {
        private int _position;

        public bool End => _position == payload.Length;

        public string ReadLine()
        {
            if (_position >= payload.Length)
                throw InvalidBatch("The blob batch payload ended unexpectedly.");
            var lineEnd = payload.IndexOf("\r\n", _position, StringComparison.Ordinal);
            if (lineEnd < 0)
                throw InvalidBatch("The blob batch payload ended with an incomplete line.");
            var line = payload[_position..lineEnd];
            _position = lineEnd + 2;
            return line;
        }
    }
}
