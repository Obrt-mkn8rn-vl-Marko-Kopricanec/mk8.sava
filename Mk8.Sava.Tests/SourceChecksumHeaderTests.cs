using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.Extensions.Primitives;
using Mk8.Sava.Protocol;

namespace Mk8.Sava.Tests;

public sealed class SourceChecksumHeaderTests
{
    private static readonly string[] MultipleValues = ["AA==", "AA=="];
    private static readonly string?[] NullValue = [null];

    [Theory]
    [InlineData("x-ms-source-content-md5", "zero-values")]
    [InlineData("x-ms-source-content-md5", "empty")]
    [InlineData("x-ms-source-content-md5", "null")]
    [InlineData("x-ms-source-content-md5", "multiple")]
    [InlineData("x-ms-source-content-crc64", "zero-values")]
    [InlineData("x-ms-source-content-crc64", "empty")]
    [InlineData("x-ms-source-content-crc64", "null")]
    [InlineData("x-ms-source-content-crc64", "multiple")]
    public void PresentButUnusableChecksumHeadersCannotBecomeAbsent(string name, string shape)
    {
        var value = shape switch
        {
            "zero-values" => StringValues.Empty,
            "empty" => new StringValues(string.Empty),
            "null" => new StringValues(NullValue),
            "multiple" => new StringValues(MultipleValues),
            _ => throw new ArgumentOutOfRangeException(nameof(shape)),
        };
        var headers = new HeaderDictionary(new Dictionary<string, StringValues>(StringComparer.OrdinalIgnoreCase)
        {
            [name] = value,
        });
        var context = new DefaultHttpContext();
        context.Features.Set<IHttpRequestFeature>(new HttpRequestFeature { Headers = headers });
        Assert.True(context.Request.Headers.ContainsKey(name));
        var refusal = Assert.Throws<AzureStorageException>(() => UrlTransferClient.ReadSourceChecksumHeaders(context.Request));
        Assert.Equal(400, refusal.StatusCode);
        Assert.Equal("InvalidHeaderValue", refusal.ErrorCode);
        Assert.Equal(name, refusal.HeaderName);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void SingleWellFormedChecksumHasItsExactDecodedIdentity(bool crc64)
    {
        var expected = new byte[crc64 ? 8 : 16];
        var context = new DefaultHttpContext();
        context.Request.Headers[crc64 ? "x-ms-source-content-crc64" : "x-ms-source-content-md5"] =
            Convert.ToBase64String(expected);
        var (actual, verifyCrc64) = UrlTransferClient.ReadSourceChecksumHeaders(context.Request);
        Assert.Equal(expected, actual);
        Assert.Equal(crc64, verifyCrc64);
    }

    [Fact]
    public void AbsenceOfBothHeadersDoesNotInventChecksumAdmission()
    {
        var (expected, verifyCrc64) = UrlTransferClient.ReadSourceChecksumHeaders(new DefaultHttpContext().Request);
        Assert.Null(expected);
        Assert.False(verifyCrc64);
    }
}
