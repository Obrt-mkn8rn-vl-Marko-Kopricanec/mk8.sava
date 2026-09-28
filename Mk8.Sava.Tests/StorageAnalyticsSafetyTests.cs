using Microsoft.AspNetCore.Http;
using Mk8.Sava.Protocol;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

public sealed class StorageAnalyticsSafetyTests
{
    [Fact]
    public void LoggedRequestUrlMasksEveryEncodedOrCaseVariedSignature()
    {
        var context = new DefaultHttpContext();
        context.Request.Scheme = "https";
        context.Request.Host = new HostString("account.blob.example");
        context.Request.Path = "/container/blob";
        context.Request.QueryString = new QueryString("?%73ig=first&SIG=second&safe=keep");

        var logged = StorageTelemetryMiddleware.BuildLoggedUrl(context.Request);

        Assert.Contains("%73ig=XXXXX&SIG=XXXXX&safe=keep", logged, StringComparison.Ordinal);
        Assert.DoesNotContain("first", logged, StringComparison.Ordinal);
        Assert.DoesNotContain("second", logged, StringComparison.Ordinal);
    }

    [Fact]
    public void ReferrerRedactionRemovesUserInfoSignatureAndFragment()
    {
        var redacted = StorageTelemetryMiddleware.RedactReferrer(
            "https://user:password@example.test/path?%73ig=secret&safe=keep#fragment-secret");

        Assert.NotNull(redacted);
        Assert.StartsWith("https://example.test/path?", redacted, StringComparison.Ordinal);
        Assert.Contains("sig=XXXXX", redacted, StringComparison.Ordinal);
        Assert.Contains("safe=keep", redacted, StringComparison.Ordinal);
        Assert.DoesNotContain("password", redacted, StringComparison.Ordinal);
        Assert.DoesNotContain("secret", redacted, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("/relative?sig=secret")]
    [InlineData("not a URL with sig=secret")]
    [InlineData("file:///tmp/secret?sig=secret")]
    public void UnrecognizedReferrerIsOmitted(string value)
    {
        Assert.Null(StorageTelemetryMiddleware.RedactReferrer(value));
    }

    [Theory]
    [InlineData("1.0", 30)]
    [InlineData("2.0", 38)]
    public void AnalyticsRecordIsOneBoundedParseableLine(string version, int expectedFields)
    {
        var request = new StorageAnalyticsRequest
        {
            Account = "account",
            StartedAt = DateTimeOffset.Parse("2026-09-28T12:00:00Z", System.Globalization.CultureInfo.InvariantCulture),
            CompletedAt = DateTimeOffset.Parse("2026-09-28T12:00:01Z", System.Globalization.CultureInfo.InvariantCulture),
            Operation = "GetBlob",
            Category = StorageAnalyticsOperationCategory.Read,
            RequestStatus = "Success",
            StatusCode = StatusCodes.Status200OK,
            EndToEndLatencyMilliseconds = 1000,
            ServerLatencyMilliseconds = 900,
            AuthenticationType = "sas",
            RequestUrl = "https://account.blob.example/container/blob?sig=XXXXX",
            RequestedObjectKey = "/account/container/blob",
            RequestId = "request-id",
            RequestVersion = "2025-07-05",
            RequestHeaderSize = 200,
            RequestPacketSize = 0,
            ResponseHeaderSize = 300,
            ResponsePacketSize = 5,
            RequestContentLength = 0,
            Conditions = "If-Match=\"a;b\"\r\ncontinued",
            UserAgent = new string('u', 8192) + "\r\nforged-record",
            Referrer = StorageTelemetryMiddleware.RedactReferrer(
                "https://user:password@example.test/path?sig=secret&safe=keep"),
            ClientRequestId = "client\trequest\0id"
        };

        var record = StorageAnalyticsService.FormatRecord(version, request);

        Assert.EndsWith("\n", record, StringComparison.Ordinal);
        Assert.Equal(1, record.Count(character => character == '\n'));
        Assert.DoesNotContain('\r', record);
        Assert.DoesNotContain('\t', record);
        Assert.DoesNotContain('\0', record);
        Assert.DoesNotContain("forged-record", record, StringComparison.Ordinal);
        Assert.DoesNotContain("password", record, StringComparison.Ordinal);
        Assert.DoesNotContain("secret", record, StringComparison.Ordinal);
        Assert.Contains(new string('u', 4096), record, StringComparison.Ordinal);
        Assert.DoesNotContain(new string('u', 4097), record, StringComparison.Ordinal);

        var separators = 0;
        var quoted = false;
        foreach (var character in record)
        {
            if (character == '"')
                quoted = !quoted;
            else if (character == ';' && !quoted)
                separators++;
        }
        Assert.False(quoted);
        Assert.Equal(expectedFields - 1, separators);
    }
}
