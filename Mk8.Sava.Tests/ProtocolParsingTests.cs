using System.Text;
using System.Net;
using System.Xml;
using Mk8.Sava.Protocol;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

public sealed class ProtocolParsingTests
{
    [Fact]
    public async Task BlobQueryParserAcceptsTheDocumentedCsvFormatAlias()
    {
        const string xml = """
            <QueryRequest>
              <QueryType>SQL</QueryType>
              <Expression>SELECT * FROM BlobStorage</Expression>
              <InputSerialization>
                <Format>
                  <Type>csv</Type>
                  <DelimitedTextConfiguration>
                    <HasHeaders>true</HasHeaders>
                  </DelimitedTextConfiguration>
                </Format>
              </InputSerialization>
            </QueryRequest>
            """;
        await using var body = new MemoryStream(Encoding.UTF8.GetBytes(xml), writable: false);

        var request = await BlobQueryProtocol.ReadRequestAsync(body, CancellationToken.None);

        Assert.Equal(BlobQueryFormatKind.Delimited, request.Input.Kind);
        Assert.True(request.Input.HasHeaders);
    }

    [Fact]
    public async Task BlobQueryParserAcceptsParquetOnlyAsAnInputFormat()
    {
        const string parquetInput = """
            <QueryRequest>
              <QueryType>SQL</QueryType>
              <Expression>SELECT * FROM BlobStorage</Expression>
              <InputSerialization><Format><Type>parquet</Type></Format></InputSerialization>
              <OutputSerialization><Format><Type>json</Type></Format></OutputSerialization>
            </QueryRequest>
            """;
        await using var acceptedBody = new MemoryStream(Encoding.UTF8.GetBytes(parquetInput), writable: false);

        var request = await BlobQueryProtocol.ReadRequestAsync(acceptedBody, CancellationToken.None);

        Assert.Equal(BlobQueryFormatKind.Parquet, request.Input.Kind);
        Assert.Equal(BlobQueryFormatKind.Json, request.Output.Kind);

        const string parquetOutput = """
            <QueryRequest>
              <QueryType>SQL</QueryType>
              <Expression>SELECT * FROM BlobStorage</Expression>
              <OutputSerialization><Format><Type>parquet</Type></Format></OutputSerialization>
            </QueryRequest>
            """;
        await using var rejectedBody = new MemoryStream(Encoding.UTF8.GetBytes(parquetOutput), writable: false);

        var exception = await Assert.ThrowsAsync<AzureStorageException>(
            () => BlobQueryProtocol.ReadRequestAsync(rejectedBody, CancellationToken.None));

        Assert.Equal((int)HttpStatusCode.BadRequest, exception.StatusCode);
        Assert.Equal("BlobQueryError", exception.ErrorCode);
    }

    [Fact]
    public async Task BlockListParserAcceptsMaximumCountWithMaximumLengthIdentifiers()
    {
        var blockId = Convert.ToBase64String(Enumerable.Repeat((byte)0x5a, BlobServiceLimits.MaximumBlockIdBytes).ToArray());
        var xml = new StringBuilder(6 * 1024 * 1024);
        xml.Append("<?xml version=\"1.0\" encoding=\"utf-8\"?><BlockList>");
        for (var index = 0; index < BlobServiceLimits.MaximumCommittedBlockCount; index++)
            xml.Append("<Uncommitted>").Append(blockId).Append("</Uncommitted>");
        xml.Append("</BlockList>");

        await using var body = new MemoryStream(Encoding.UTF8.GetBytes(xml.ToString()), writable: false);
        var blocks = await ProtocolParsing.ReadBlockListAsync(body, CancellationToken.None);

        Assert.Equal(BlobServiceLimits.MaximumCommittedBlockCount, blocks.Count);
        Assert.Equal(new BlockListEntry(blockId, BlockListMode.Uncommitted), blocks[0]);
        Assert.Equal(new BlockListEntry(blockId, BlockListMode.Uncommitted), blocks[^1]);
    }

    [Fact]
    public async Task BlockListParserRejectsMoreThanMaximumCountBeforeBusinessMutation()
    {
        var xml = new StringBuilder(1024 * 1024);
        xml.Append("<BlockList>");
        for (var index = 0; index <= BlobServiceLimits.MaximumCommittedBlockCount; index++)
            xml.Append("<Latest>AA==</Latest>");
        xml.Append("</BlockList>");

        await using var body = new MemoryStream(Encoding.UTF8.GetBytes(xml.ToString()), writable: false);
        var exception = await Assert.ThrowsAsync<AzureStorageException>(
            () => ProtocolParsing.ReadBlockListAsync(body, CancellationToken.None));

        Assert.Equal((int)HttpStatusCode.Conflict, exception.StatusCode);
        Assert.Equal("BlockCountExceedsLimit", exception.ErrorCode);
    }

    [Fact]
    public async Task BlockListParserRejectsNestedOrTrailingContent()
    {
        foreach (var xml in new[]
                 {
                     "<BlockList><Latest><Id>AA==</Id></Latest></BlockList>",
                     "<BlockList><Latest>AA==</Latest></BlockList><Other />"
                 })
        {
            await using var body = new MemoryStream(Encoding.UTF8.GetBytes(xml), writable: false);
            await Assert.ThrowsAsync<XmlException>(
                () => ProtocolParsing.ReadBlockListAsync(body, CancellationToken.None));
        }
    }

    [Theory]
    [InlineData("<SignedIdentifiers><Unknown /></SignedIdentifiers>")]
    [InlineData("<SignedIdentifiers><SignedIdentifier><Id>a</Id><Unknown /></SignedIdentifier></SignedIdentifiers>")]
    [InlineData("<SignedIdentifiers><SignedIdentifier><Id>a</Id><Id>b</Id><AccessPolicy /></SignedIdentifier></SignedIdentifiers>")]
    [InlineData("<SignedIdentifiers><SignedIdentifier><Id>a</Id></SignedIdentifier></SignedIdentifiers>")]
    [InlineData("<SignedIdentifiers><SignedIdentifier><Id>a</Id><AccessPolicy><Permission>r</Permission><Permission>w</Permission></AccessPolicy></SignedIdentifier></SignedIdentifiers>")]
    [InlineData("<SignedIdentifiers><SignedIdentifier><Id>a</Id><AccessPolicy><Unknown /></AccessPolicy></SignedIdentifier></SignedIdentifiers>")]
    [InlineData("<SignedIdentifiers><SignedIdentifier><Id>a</Id><AccessPolicy><Start>September 22, 2026</Start></AccessPolicy></SignedIdentifier></SignedIdentifiers>")]
    public async Task AccessPolicyParserRejectsUnknownDuplicateAndNonIsoContent(string xml)
    {
        await using var body = new MemoryStream(Encoding.UTF8.GetBytes(xml), writable: false);
        var exception = await Assert.ThrowsAsync<AzureStorageException>(
            () => ProtocolParsing.ReadAclAsync(body, CancellationToken.None));

        Assert.Equal((int)HttpStatusCode.BadRequest, exception.StatusCode);
        Assert.Equal("InvalidXmlDocument", exception.ErrorCode);
    }

    [Fact]
    public async Task AccessPolicyParserAcceptsDocumentedIsoDateShapesAndEmptyFields()
    {
        const string xml = """
            <SignedIdentifiers>
              <SignedIdentifier>
                <Id>complete</Id>
                <AccessPolicy>
                  <Start>2026-09-22T08:30:00.1234567Z</Start>
                  <Expiry>2026-09-23T08:30+00:00</Expiry>
                  <Permission>racwdl</Permission>
                </AccessPolicy>
              </SignedIdentifier>
              <SignedIdentifier>
                <Id>partial</Id>
                <AccessPolicy />
              </SignedIdentifier>
            </SignedIdentifiers>
            """;
        await using var body = new MemoryStream(Encoding.UTF8.GetBytes(xml), writable: false);

        var policies = await ProtocolParsing.ReadAclAsync(body, CancellationToken.None);

        Assert.Equal(new DateTimeOffset(2026, 9, 22, 8, 30, 0, 123, TimeSpan.Zero).AddTicks(4567), policies["complete"].StartsAt);
        Assert.Equal(new DateTimeOffset(2026, 9, 23, 8, 30, 0, TimeSpan.Zero), policies["complete"].ExpiresAt);
        Assert.Equal("racwdl", policies["complete"].Permission);
        Assert.Null(policies["partial"].StartsAt);
        Assert.Null(policies["partial"].ExpiresAt);
        Assert.Empty(policies["partial"].Permission);
    }
}
