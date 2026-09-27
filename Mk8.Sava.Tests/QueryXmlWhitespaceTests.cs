using System.Text;
using Mk8.Sava.Protocol;

namespace Mk8.Sava.Tests;

public sealed class QueryXmlWhitespaceTests
{
    [Theory]
    [InlineData("csv", "\n", "\n")]
    [InlineData("csv", "\t", "\t")]
    [InlineData("csv", " ", " ")]
    [InlineData("csv", "\r\n", "\n")]
    [InlineData("csv", "&#xD;&#xA;", "\r\n")]
    [InlineData("json", "\n", "\n")]
    [InlineData("json", "\t", "\t")]
    [InlineData("json", " ", " ")]
    public async Task LiteralAndEscapedSeparatorsRemainDataInBothSerializationDirections(
        string kind, string xmlSeparator, string expected)
    {
        using var input = Body(RequestXml(kind, xmlSeparator));
        var request = await BlobQueryProtocol.ReadRequestAsync(input, CancellationToken.None).ConfigureAwait(true);
        Assert.Equal(expected, request.Input.RecordSeparator);
        Assert.Equal(expected, request.Output.RecordSeparator);
    }

    [Fact]
    public async Task WhitespaceColumnQuoteAndEscapeCharactersRemainData()
    {
        var xml = RequestXml("csv", "\n").Replace(
            "<RecordSeparator>", "<ColumnSeparator>\t</ColumnSeparator><FieldQuote> </FieldQuote><EscapeChar>\t</EscapeChar><RecordSeparator>",
            StringComparison.Ordinal);
        using var input = Body(xml);
        var request = await BlobQueryProtocol.ReadRequestAsync(input, CancellationToken.None).ConfigureAwait(true);
        Assert.Equal("\t", request.Input.ColumnSeparator);
        Assert.Equal(' ', request.Input.Quote);
        Assert.Equal('\t', request.Input.Escape);
        Assert.Equal("\t", request.Output.ColumnSeparator);
        Assert.Equal(' ', request.Output.Quote);
        Assert.Equal('\t', request.Output.Escape);
    }

    [Theory]
    [InlineData("csv")]
    [InlineData("json")]
    public async Task ExplicitlyEmptySeparatorUsesTheRecordedAzureDefault(string kind)
    {
        using var input = Body(RequestXml(kind, string.Empty));
        var request = await BlobQueryProtocol.ReadRequestAsync(input, CancellationToken.None).ConfigureAwait(true);
        Assert.Equal("\n", request.Input.RecordSeparator);
        Assert.Equal("\n", request.Output.RecordSeparator);
    }

    [Fact]
    public async Task EmptyOptionalCsvCharactersUseDefaultsWithoutTrimmingActualWhitespace()
    {
        var xml = RequestXml("csv", "").Replace("<RecordSeparator>",
            "<ColumnSeparator/><FieldQuote/><EscapeChar/><RecordSeparator>", StringComparison.Ordinal);
        using var input = Body(xml);
        var request = await BlobQueryProtocol.ReadRequestAsync(input, CancellationToken.None).ConfigureAwait(true);
        Assert.Equal(",", request.Input.ColumnSeparator);
        Assert.Equal('"', request.Input.Quote);
        Assert.Equal('\\', request.Input.Escape);
        Assert.Equal(request.Input.ColumnSeparator, request.Output.ColumnSeparator);
        Assert.Equal(request.Input.Quote, request.Output.Quote);
        Assert.Equal(request.Input.Escape, request.Output.Escape);
    }

    [Fact]
    public async Task PreservingWhitespaceDoesNotPermitDocumentTypesOrExternalEntities()
    {
        using var input = Body("<!DOCTYPE QueryRequest [<!ENTITY external SYSTEM 'http://127.0.0.1:1/never-requested'>]>" +
                               RequestXml("json", "\n"));
        var error = await Assert.ThrowsAsync<AzureStorageException>(() =>
            BlobQueryProtocol.ReadRequestAsync(input, CancellationToken.None)).ConfigureAwait(true);
        Assert.Equal("InvalidXmlDocument", error.ErrorCode);
    }

    [Fact]
    public async Task WhitespaceStillCountsTowardTheExistingXmlDocumentLimit()
    {
        var xml = RequestXml("json", "\n").Replace("<QueryType>", new string(' ', 4 * 1024 * 1024) + "<QueryType>",
            StringComparison.Ordinal);
        using var input = Body(xml);
        var error = await Assert.ThrowsAsync<AzureStorageException>(() =>
            BlobQueryProtocol.ReadRequestAsync(input, CancellationToken.None)).ConfigureAwait(true);
        Assert.Equal("InvalidXmlDocument", error.ErrorCode);
    }

    private static MemoryStream Body(string xml) => new(Encoding.UTF8.GetBytes(xml), writable: false);

    private static string RequestXml(string kind, string separator)
    {
        var configuration = string.Equals(kind, "json", StringComparison.Ordinal)
            ? "JsonTextConfiguration"
            : "DelimitedTextConfiguration";
        var format = $"<Format><Type>{kind}</Type><{configuration}><RecordSeparator>{separator}</RecordSeparator></{configuration}></Format>";
        return $"<QueryRequest>\n  <QueryType>SQL</QueryType>\n  <Expression>SELECT * FROM BlobStorage;</Expression>\n" +
               $"  <InputSerialization>{format}</InputSerialization>\n  <OutputSerialization>{format}</OutputSerialization>\n</QueryRequest>";
    }
}
