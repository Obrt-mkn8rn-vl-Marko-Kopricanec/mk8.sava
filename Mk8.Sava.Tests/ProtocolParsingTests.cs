using System.Text;
using System.Net;
using System.Xml;
using Mk8.Sava.Protocol;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

public sealed class ProtocolParsingTests
{
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
}
