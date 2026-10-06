using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Mk8.Sava.Storage;
using Mk8.Sava.Transport;

namespace Mk8.Sava.Tests;

public sealed class ApplicationTransportFramingTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(65536)]
    [InlineData(131073)]
    public async Task FramedStreamReturnsEofOnlyAfterSuccessfulProof(int length)
    {
        var data = RandomNumberGenerator.GetBytes(length);
        using var wire = new MemoryStream();
        using (var writer = new FramedWriteStream(wire, length))
        {
            await writer.WriteAsync(data);
            await writer.CompleteAsync(CancellationToken.None);
        }
        wire.Position = 0;
        using var input = new FramedReadStream(wire, length);
        using var received = new MemoryStream();

        await input.CopyToAsync(received);
        await input.EnsureCompletedAsync(CancellationToken.None);

        Assert.Equal(data, received.ToArray());
        Assert.Equal(wire.Length, wire.Position);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(65537)]
    public async Task MissingProducerSuccessNeverBecomesEofEvenAfterAllPayloadBytes(int length)
    {
        using var wire = new MemoryStream();
        using (var writer = new FramedWriteStream(wire, length))
            await writer.WriteAsync(RandomNumberGenerator.GetBytes(length));
        wire.Position = 0;
        using var input = new FramedReadStream(wire, length);

        await Assert.ThrowsAsync<EndOfStreamException>(() => input.CopyToAsync(Stream.Null));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(4096)]
    public async Task CorruptedProofNeverBecomesEof(int length)
    {
        using var wire = await CreateWireAsync(RandomNumberGenerator.GetBytes(length));
        wire.GetBuffer()[checked((int)wire.Length) - 1] ^= 1;
        using var input = new FramedReadStream(wire, length);

        await Assert.ThrowsAsync<InvalidDataException>(() => input.CopyToAsync(Stream.Null));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    [InlineData(31)]
    public async Task TruncatedProofNeverBecomesEof(int missingBytes)
    {
        using var wire = await CreateWireAsync(new byte[4096]);
        wire.SetLength(wire.Length - missingBytes);
        using var input = new FramedReadStream(wire, 4096);

        await Assert.ThrowsAsync<EndOfStreamException>(() => input.CopyToAsync(Stream.Null));
    }

    [Fact]
    public async Task BytesAfterSuccessfulTerminalAreRejected()
    {
        using var wire = await CreateWireAsync([]);
        wire.Position = wire.Length;
        wire.WriteByte(1);
        wire.Position = 0;
        using var input = new FramedReadStream(wire, 0);

        await Assert.ThrowsAsync<InvalidDataException>(() => input.EnsureCompletedAsync(CancellationToken.None));
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(65537)]
    public async Task OversizedOrNegativeDataFrameIsRejectedBeforeAllocation(int length)
    {
        var header = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(header, length);
        using var wire = new MemoryStream(header);
        using var input = new FramedReadStream(wire, long.MaxValue);

        await Assert.ThrowsAsync<InvalidDataException>(() => input.ReadAsync(new byte[1]).AsTask());
    }

    [Fact]
    public async Task AggregateInputLimitCannotBeBypassedWithMultipleFrames()
    {
        using var wire = await CreateWireAsync(new byte[65537]);
        using var input = new FramedReadStream(wire, 65536);

        await Assert.ThrowsAsync<InvalidDataException>(() => input.CopyToAsync(Stream.Null));
    }

    [Fact]
    public async Task ControlEncoderAndDecoderEnforceTheirConfiguredBound()
    {
        using var wire = new MemoryStream();

        await Assert.ThrowsAsync<InvalidDataException>(() =>
            RpcFrames.WriteControlAsync(wire, new string('x', 4096), 4096, CancellationToken.None));
        Assert.Equal(0, wire.Length);
        var header = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(header, 4097);
        await wire.WriteAsync(header);
        wire.Position = 0;
        await Assert.ThrowsAsync<InvalidDataException>(() =>
            RpcFrames.ReadControlAsync<string>(wire, 4096, CancellationToken.None));
    }

    [Fact]
    public async Task PhysicalChunkInventoryCannotExpandOrLeakThroughControlFrames()
    {
        var physical = new ChunkReference("private-physical-chunk-identifier", 0, 1024);
        var manifest = new ContentManifest("logical-domain", 10_000_000_000L, "logical-digest",
            Enumerable.Repeat(physical, 250_000).ToArray());
        using var wire = new MemoryStream();

        await RpcFrames.WriteControlAsync(wire, manifest, 4096, CancellationToken.None);

        Assert.True(wire.Length < 512);
        Assert.DoesNotContain(physical.Id, Encoding.UTF8.GetString(wire.ToArray()), StringComparison.Ordinal);
        wire.Position = 0;
        var decoded = await RpcFrames.ReadControlAsync<ContentManifest>(wire, 4096, CancellationToken.None);
        Assert.Equal(manifest.Domain, decoded.Domain);
        Assert.Equal(manifest.Length, decoded.Length);
        Assert.Equal(manifest.Sha256, decoded.Sha256);
        Assert.Empty(decoded.Chunks);
    }

    [Fact]
    public void WireCannotSupplyPhysicalChunkReferences()
    {
        const string json = "{\"Domain\":\"domain\",\"Length\":512,\"Sha256\":\"digest\",\"Chunks\":[{\"Id\":\"private\",\"Offset\":0,\"Length\":512}]}";

        Assert.Throws<JsonException>(() => JsonSerializer.Deserialize<ContentManifest>(json, RpcJson.Options));
    }

    [Fact]
    public async Task ListingControlFramesExcludeUnusedBlockAndPageInventoriesButGetRetainsBlockIds()
    {
        var content = new ContentManifest("domain", 512, "digest", []);
        var record = new BlobRecord
        {
            Account = "devstoreaccount1",
            Container = "container",
            Name = "blob",
            GenerationId = "generation",
            Revision = "revision",
            Kind = BlobKind.BlockBlob,
            Content = content,
            ETag = "etag",
            CreatedAt = DateTimeOffset.UnixEpoch,
            LastModified = DateTimeOffset.UnixEpoch,
            CommittedBlocks = Enumerable.Repeat(new CommittedBlockRecord("retained-block-id", content), 50_000).ToArray(),
            PendingCopyCommittedBlocks = Enumerable.Repeat(new CommittedBlockRecord("pending-block-id", content), 50_000).ToArray(),
            PageRanges = Enumerable.Repeat(new PageRange(0, 511), 50_000).ToArray(),
            PageMutationRanges = Enumerable.Repeat(new PageMutationRange(0, 511, 1), 50_000).ToArray(),
            PendingCopyContent = content,
            Copy = new CopyState { Id = "copy-id", Source = "https://source/blob", Status = "pending", BytesCopied = 0, TotalBytes = 512 },
        };
        var contract = RpcContracts.GetContract(typeof(Mk8.Sava.Application.IBlobApplication));
        var listMethod = contract.GetMethod(typeof(Mk8.Sava.Application.IBlobApplication)
            .GetMethod(nameof(Mk8.Sava.Application.IBlobApplication.ListBlobsPageAsync))!);
        var projection = Assert.IsType<BlobListPage>(RpcListingProjection.Apply(listMethod,
            new BlobListPage([new BlobListEntry(record, null)], HasMore: true)));
        using var wire = new MemoryStream();

        await RpcFrames.WriteControlAsync(wire, projection, 4096, CancellationToken.None);

        var listed = Assert.IsType<BlobRecord>(Assert.Single(projection.Items).Blob);
        Assert.Empty(listed.CommittedBlocks);
        Assert.Empty(listed.PageRanges);
        Assert.Null(listed.PendingCopyContent);
        Assert.Equal(record.Copy, listed.Copy);
        Assert.True(projection.HasMore);
        var getMethod = contract.GetMethod(typeof(Mk8.Sava.Application.IBlobApplication)
            .GetMethod(nameof(Mk8.Sava.Application.IBlobApplication.GetBlobAsync))!);
        Assert.Same(record, RpcListingProjection.Apply(getMethod, record));
        Assert.Equal(50_000, record.CommittedBlocks.Count);
    }

    [Fact]
    public async Task GlobalWireMetadataOmitsBackendHistoriesButRetainsActiveInventoriesAndPublicState()
    {
        var content = new ContentManifest("domain", 512, "digest", []);
        var record = new BlobRecord
        {
            Account = "devstoreaccount1",
            Container = "container",
            Name = "blob",
            GenerationId = "generation",
            Revision = "revision",
            Kind = BlobKind.PageBlob,
            Content = content,
            ETag = "etag",
            CreatedAt = DateTimeOffset.UnixEpoch,
            LastModified = DateTimeOffset.UnixEpoch,
            CommittedBlocks = [new CommittedBlockRecord("active-block-id", content)],
            PageRanges = [new PageRange(0, 511)],
            PendingCopyCommittedBlocks = Enumerable.Repeat(new CommittedBlockRecord("private-pending-block", content), 50_000).ToArray(),
            PageMutationRanges = Enumerable.Repeat(new PageMutationRange(0, 511, 1), 250_000).ToArray(),
            PendingCopyContent = content,
            PendingCopyPageRanges = [new PageRange(512, 1023)],
            PendingCopyAppendBlockCount = 999,
            PendingCopyIsSealed = true,
            PageMutationSequence = 999,
            PageBlobIncarnationId = "private-incarnation",
            IncrementalCopySourceIncarnationId = "private-source-incarnation",
            AppendBlockCount = 3,
            IsSealed = true,
            Copy = new CopyState { Id = "copy-id", Source = "https://source/blob", Status = "pending", BytesCopied = 0, TotalBytes = 512 },
        };
        using var wire = new MemoryStream();

        await RpcFrames.WriteControlAsync(wire, new[] { record }, 4096, CancellationToken.None);

        Assert.True(wire.Length < 4096);
        var json = Encoding.UTF8.GetString(wire.GetBuffer(), 4, checked((int)wire.Length) - 4);
        foreach (var hidden in new[]
        {
            nameof(BlobRecord.PendingCopyContent), nameof(BlobRecord.PendingCopyCommittedBlocks),
            nameof(BlobRecord.PendingCopyAppendBlockCount), nameof(BlobRecord.PendingCopyIsSealed),
            nameof(BlobRecord.PendingCopyPageRanges), nameof(BlobRecord.PageMutationRanges),
            nameof(BlobRecord.PageMutationSequence), nameof(BlobRecord.PageBlobIncarnationId),
            nameof(BlobRecord.IncrementalCopySourceIncarnationId),
        })
            Assert.DoesNotContain('"' + hidden + '"', json, StringComparison.Ordinal);
        Assert.DoesNotContain("private-", json, StringComparison.Ordinal);
        wire.Position = 0;
        var decoded = Assert.Single(await RpcFrames.ReadControlAsync<BlobRecord[]>(wire, 4096, CancellationToken.None));
        Assert.Equal("active-block-id", Assert.Single(decoded.CommittedBlocks).Id);
        Assert.Equal(new PageRange(0, 511), Assert.Single(decoded.PageRanges));
        Assert.Equal(record.Copy, decoded.Copy);
        Assert.Equal(3, decoded.AppendBlockCount);
        Assert.True(decoded.IsSealed);
        Assert.Null(decoded.PageMutationRanges);
        Assert.Equal(250_000, record.PageMutationRanges.Count);
    }

    [Fact]
    public async Task LastPageCallbackCannotReturnBeforeSuccessfulTerminal()
    {
        var changes = new PageRangeDiff([new PageRange(0, 511)], []);
        using var wire = new MemoryStream();
        using (var writer = new FramedWriteStream(wire, 512))
            await writer.WriteAsync(new byte[512]);
        wire.Position = 0;
        using var input = new FramedReadStream(wire, 512);
        var plan = PageCopyPlan.Create(changes, 512);
        using var source = new PageCopyFrames.ServerSource(plan, input);
        Assert.Same(plan.Descriptor, await source.ReadChangesAsync(null, 0, CancellationToken.None));
        var consumed = false;

        await Assert.ThrowsAsync<EndOfStreamException>(() => source.ReadRangeAsync(new PageRange(0, 511),
            async (stream, token) =>
            {
                await stream.CopyToAsync(Stream.Null, token).ConfigureAwait(false);
                consumed = true;
            }, CancellationToken.None));

        Assert.True(consumed);
    }

    [Fact]
    public async Task ZeroPageCopyRequiresSuccessfulTerminalBeforeReturningDescriptor()
    {
        using var wire = new MemoryStream();
        using var input = new FramedReadStream(wire, 0);
        using var source = new PageCopyFrames.ServerSource(
            PageCopyPlan.Create(new PageRangeDiff([], [new PageRange(0, 511)]), 0), input);

        await Assert.ThrowsAsync<EndOfStreamException>(() => source.ReadChangesAsync(null, 512, CancellationToken.None));
    }

    [Fact]
    public async Task EmptyPageCopyAcceptsOnlyVerifiedSuccessfulProducer()
    {
        using var wire = await CreateWireAsync([]);
        using var input = new FramedReadStream(wire, 0);
        var changes = new PageRangeDiff([], [new PageRange(0, 511)]);
        var plan = PageCopyPlan.Create(changes, 0);
        using var source = new PageCopyFrames.ServerSource(plan, input);

        Assert.Same(plan.Descriptor, await source.ReadChangesAsync(null, 512, CancellationToken.None));
        Assert.Equal(wire.Length, wire.Position);
    }

    [Fact]
    public void BlobCopyLengthIsNotMistakenForThePutBlobBodyLimit()
    {
        var contract = RpcContracts.GetContract(typeof(Mk8.Sava.Application.IBlobApplication));
        var method = contract.GetMethod(typeof(Mk8.Sava.Application.IBlobApplication)
            .GetMethod(nameof(Mk8.Sava.Application.IBlobApplication.BeginCopyFromStreamAsync))!);
        var arguments = new object?[method.Parameters.Length];
        var contentLengthIndex = Array.FindIndex(method.Parameters, parameter =>
            string.Equals(parameter.Name, "contentLength", StringComparison.Ordinal));
        arguments[contentLengthIndex] = 9L * 1024 * 1024 * 1024 * 1024;

        Assert.Equal(9L * 1024 * 1024 * 1024 * 1024, RpcStreamLimits.InputLimit(method, arguments, 5_000L * 1024 * 1024));
        Assert.True(RpcStreamLimits.MaximumBlobBytes > 9L * 1024 * 1024 * 1024 * 1024);
    }

    [Fact]
    public async Task SuccessfulPageCopyRequiresExactOrderedRangesAndVerifiedFinalProof()
    {
        var changes = new PageRangeDiff([new PageRange(0, 511), new PageRange(1024, 1535)], []);
        var bytes = RandomNumberGenerator.GetBytes(1024);
        using var wire = await CreateWireAsync(bytes);
        using var input = new FramedReadStream(wire, 1024);
        using var source = new PageCopyFrames.ServerSource(PageCopyPlan.Create(changes, 1536), input);
        await source.ReadChangesAsync(null, 0, CancellationToken.None);
        using var received = new MemoryStream();

        foreach (var range in changes.PageRanges)
            await source.ReadRangeAsync(range, (stream, token) => stream.CopyToAsync(received, token), CancellationToken.None);

        Assert.Equal(bytes, received.ToArray());
        Assert.Equal(wire.Length, wire.Position);
    }

    [Fact]
    public async Task OutOfOrderPageRangeCannotConsumeInput()
    {
        using var wire = await CreateWireAsync(new byte[1024]);
        using var input = new FramedReadStream(wire, 1024);
        using var source = new PageCopyFrames.ServerSource(
            PageCopyPlan.Create(new PageRangeDiff([new PageRange(0, 511), new PageRange(1024, 1535)], []), 1536), input);
        await source.ReadChangesAsync(null, 0, CancellationToken.None);

        await Assert.ThrowsAsync<InvalidDataException>(() => source.ReadRangeAsync(new PageRange(1024, 1535),
            (stream, token) => stream.CopyToAsync(Stream.Null, token), CancellationToken.None));

        Assert.Equal(0, wire.Position);
    }

    private static async Task<MemoryStream> CreateWireAsync(byte[] bytes)
    {
        var wire = new MemoryStream();
        try
        {
            using var writer = new FramedWriteStream(wire, bytes.Length);
            await writer.WriteAsync(bytes).ConfigureAwait(false);
            await writer.CompleteAsync(CancellationToken.None).ConfigureAwait(false);
            wire.Position = 0;
            return wire;
        }
        catch
        {
            await wire.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }
}
