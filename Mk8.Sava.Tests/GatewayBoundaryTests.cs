using System.Globalization;
using System.Reflection;
using Microsoft.Extensions.FileProviders;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Mk8.Sava.Application;
using Mk8.Sava.Configuration;
using Mk8.Sava.Hosting;
using Mk8.Sava.Protocol;
using Mk8.Sava.Storage;
using Mk8.Sava.Transport;

namespace Mk8.Sava.Tests;

[Trait("Category", "GatewayBoundary")]
public sealed class GatewayBoundaryTests
{
    [Fact]
    public void SynchronousWritesAndWriteByteShareTheQuotaAndRefundExactlyOnce()
    {
        using var fixture = new StagingFixture();
        using var paths = fixture.Create(maximum: 8);
        var firstPath = Path.Combine(paths.Staging, "first");
        var secondPath = Path.Combine(paths.Staging, "second");
        using var first = paths.OpenTemporaryFile(firstPath);
        using var second = paths.OpenTemporaryFile(secondPath);
        first.Write(new byte[3], 0, 3);
        first.Write(new byte[4].AsSpan());
        first.WriteByte(7);
        Assert.Equal(8, first.Length);
        AssertStagingMetrics(paths, bytes: 8, maximum: 8, rejections: 0);

        AssertQuotaFailure(Assert.Throws<AzureStorageException>(() => second.WriteByte(1)));
        Assert.Equal(0, second.Length);
        AssertStagingMetrics(paths, bytes: 8, maximum: 8, rejections: 1);
        first.Dispose();
        first.Dispose();
        Assert.False(File.Exists(firstPath));
        AssertStagingMetrics(paths, bytes: 0, maximum: 8, rejections: 1);
        second.Write(new byte[8], 0, 8);
        AssertStagingMetrics(paths, bytes: 8, maximum: 8, rejections: 1);
        second.Dispose();
        second.Dispose();
        Assert.False(File.Exists(secondPath));
        AssertStagingMetrics(paths, bytes: 0, maximum: 8, rejections: 1);
    }

    [Fact]
    public async Task BothAsyncWriteOverloadsShareTheQuotaAndRefundOnAsyncDisposal()
    {
        using var fixture = new StagingFixture();
        using var paths = fixture.Create(maximum: 8);
        var firstPath = Path.Combine(paths.Staging, "first");
        var secondPath = Path.Combine(paths.Staging, "second");
        var first = paths.OpenTemporaryFile(firstPath);
        await using var firstLifetime = first.ConfigureAwait(false);
        var second = paths.OpenTemporaryFile(secondPath);
        await using var secondLifetime = second.ConfigureAwait(false);
#pragma warning disable CA1835 // Intentionally exercise quota accounting through the legacy byte-array overload.
        await first.WriteAsync(new byte[3], 0, 3, CancellationToken.None).ConfigureAwait(true);
#pragma warning restore CA1835
        await second.WriteAsync(new byte[5].AsMemory(), CancellationToken.None).ConfigureAwait(true);
        AssertStagingMetrics(paths, bytes: 8, maximum: 8, rejections: 0);

        AssertQuotaFailure(await Assert.ThrowsAsync<AzureStorageException>(() => first.WriteAsync(
            new byte[1], 0, 1, CancellationToken.None)).ConfigureAwait(true));
        AssertQuotaFailure(await Assert.ThrowsAsync<AzureStorageException>(() => second.WriteAsync(
            new byte[1].AsMemory(), CancellationToken.None).AsTask()).ConfigureAwait(true));
        Assert.Equal(3, first.Length);
        Assert.Equal(5, second.Length);
        AssertStagingMetrics(paths, bytes: 8, maximum: 8, rejections: 2);
        await first.DisposeAsync().ConfigureAwait(true);
        Assert.False(File.Exists(firstPath));
        AssertStagingMetrics(paths, bytes: 5, maximum: 8, rejections: 2);
        await second.WriteAsync(new byte[3].AsMemory(), CancellationToken.None).ConfigureAwait(true);
        AssertStagingMetrics(paths, bytes: 8, maximum: 8, rejections: 2);
        await second.DisposeAsync().ConfigureAwait(true);
        await second.DisposeAsync().ConfigureAwait(true);
        Assert.False(File.Exists(secondPath));
        AssertStagingMetrics(paths, bytes: 0, maximum: 8, rejections: 2);
    }

    [Fact]
    public void FailedTemporaryUnlinkNeverRefundsQuotaForBytesStillOnDisk()
    {
        using var fixture = new StagingFixture();
        using var paths = fixture.Create(maximum: 8);
        var path = Path.Combine(paths.Staging, "payload");
        using var file = paths.OpenTemporaryFile(path);
        file.Write(new byte[8], 0, 8);
        if (OperatingSystem.IsWindows())
            File.SetAttributes(path, FileAttributes.ReadOnly);
        else
            File.SetUnixFileMode(paths.Staging, UnixFileMode.UserRead | UnixFileMode.UserExecute);
        try
        {
            var failure = Record.Exception(() => file.Dispose());
            Assert.True(failure is IOException or UnauthorizedAccessException);
            Assert.True(File.Exists(path));
            AssertStagingMetrics(paths, bytes: 8, maximum: 8, rejections: 0);
        }
        finally
        {
            if (OperatingSystem.IsWindows())
                File.SetAttributes(path, FileAttributes.Normal);
            else
                File.SetUnixFileMode(paths.Staging, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
        }
    }

    [Fact]
    public void TemporaryFilesCannotEscapeTheProcessDirectoryOrGrowWithoutAccounting()
    {
        using var fixture = new StagingFixture();
        using var paths = fixture.Create(maximum: 8);
        var outside = Path.Combine(fixture.StagingRoot, "not-owned-by-this-process");
        Assert.Throws<ArgumentException>(() => paths.OpenTemporaryFile(outside));
        Assert.False(File.Exists(outside));
        using var file = paths.OpenTemporaryFile(Path.Combine(paths.Staging, "payload"));
        Assert.Throws<NotSupportedException>(() => file.SetLength(9));
        Assert.Throws<ArgumentOutOfRangeException>(() => file.Position = 9);
        Assert.Throws<ArgumentOutOfRangeException>(() => file.Seek(1, SeekOrigin.End));
        Assert.Equal(0, file.Length);
        AssertStagingMetrics(paths, bytes: 0, maximum: 8, rejections: 0);
    }

    [Fact]
    public async Task QuotaStreamPreservesSeekableContentThroughBothReadOverloads()
    {
        using var fixture = new StagingFixture();
        using var paths = fixture.Create(maximum: 8);
        var payload = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 };
        await using var file = paths.OpenTemporaryFile(Path.Combine(paths.Staging, "payload"));
        await file.WriteAsync(payload.AsMemory(), CancellationToken.None).ConfigureAwait(true);
        await file.FlushAsync(CancellationToken.None).ConfigureAwait(true);
        Assert.Equal(0, file.Seek(0, SeekOrigin.Begin));
        var read = new byte[8];
        Assert.Equal(4, ReadSynchronousHalf(file, read));
        Assert.Equal(4, await file.ReadAsync(read.AsMemory(4, 4), CancellationToken.None).ConfigureAwait(true));
        Assert.Equal(payload, read);
        Assert.Equal(0, file.Seek(-8, SeekOrigin.Current));
        Assert.Equal(1, file.ReadByte());
        AssertStagingMetrics(paths, bytes: 8, maximum: 8, rejections: 0);
        file.SetLength(4);
        Assert.Equal(4, file.Length);
        Assert.Equal(3, file.Seek(-1, SeekOrigin.End));
        Assert.Equal(4, file.ReadByte());
    }

    private static int ReadSynchronousHalf(Stream file, byte[] destination) => file.Read(destination, 0, 4);

    [Fact]
    public void LiveOwnersSurvivePeerStartupAndEachOwnerHasItsOwnBudget()
    {
        using var fixture = new StagingFixture();
        using var first = fixture.Create(maximum: 8);
        using var firstFile = first.OpenTemporaryFile(Path.Combine(first.Staging, "live-payload"));
        firstFile.Write(new byte[8], 0, 8);
        GatewayStagingCleanup.RemoveOrphans(fixture.StagingRoot);
        using var second = fixture.Create(maximum: 8);
        using var secondFile = second.OpenTemporaryFile(Path.Combine(second.Staging, "peer-payload"));
        secondFile.Write(new byte[8], 0, 8);

        Assert.NotEqual(first.Staging, second.Staging, StringComparer.Ordinal);
        Assert.True(Directory.Exists(first.Staging));
        Assert.True(File.Exists(Path.Combine(first.Staging, "live-payload")));
        Assert.True(File.Exists(Path.Combine(first.Staging, ".owner")));
        GatewayStagingCleanup.RemoveOrphans(fixture.StagingRoot);
        Assert.Equal(8, firstFile.Length);
        Assert.Equal(8, secondFile.Length);
        AssertStagingMetrics(first, bytes: 8, maximum: 8, rejections: 0);
        AssertStagingMetrics(second, bytes: 8, maximum: 8, rejections: 0);
    }

    [Fact]
    public void CleanupReclaimsOnlyRecognizedUnlockedFlatOrphans()
    {
        using var fixture = new StagingFixture();
        fixture.CreateStagingRoot();
        var orphan = fixture.CreateOwnedDirectory();
        var invalidName = fixture.CreateOwnedDirectory("gw-not-a-guid");
        var wrongMarker = fixture.CreateOwnedDirectory();
        File.WriteAllBytes(Path.Combine(wrongMarker, ".owner"), new byte[GatewayStagingCleanup.OwnerMarker.Length]);
        var noOwner = fixture.CreateOwnedDirectory();
        File.Delete(Path.Combine(noOwner, ".owner"));
        var nested = fixture.CreateOwnedDirectory();
        Directory.CreateDirectory(Path.Combine(nested, "nested"));
        var unrelated = fixture.CreateOwnedDirectory("user-uploads");
        var unrelatedFile = Path.Combine(fixture.StagingRoot, "unrelated-file");
        File.WriteAllText(unrelatedFile, "must remain");

        GatewayStagingCleanup.RemoveOrphans(fixture.StagingRoot);

        Assert.False(Directory.Exists(orphan));
        foreach (var directory in new[] { invalidName, wrongMarker, noOwner, nested, unrelated })
        {
            Assert.True(Directory.Exists(directory));
            Assert.Equal("must remain", File.ReadAllText(Path.Combine(directory, "payload")));
        }
        Assert.Equal("must remain", File.ReadAllText(unrelatedFile));
    }

    [Fact]
    public void UnixCleanupNeverFollowsDirectoryPayloadOrOwnerLinks()
    {
        if (OperatingSystem.IsWindows())
            return;
        using var fixture = new StagingFixture();
        fixture.CreateStagingRoot();
        var external = Path.Combine(fixture.Root, "external");
        Directory.CreateDirectory(external);
        var sentinel = Path.Combine(external, "sentinel");
        var externalOwner = Path.Combine(external, ".owner");
        File.WriteAllText(sentinel, "must remain");
        File.WriteAllBytes(externalOwner, GatewayStagingCleanup.OwnerMarker.ToArray());
        var directoryLink = Path.Combine(fixture.StagingRoot, "gw-" + Guid.NewGuid().ToString("N"));
        Directory.CreateSymbolicLink(directoryLink, external);
        var payloadLink = fixture.CreateOwnedDirectory();
        File.CreateSymbolicLink(Path.Combine(payloadLink, "linked-payload"), sentinel);
        var ownerLink = fixture.CreateOwnedDirectory();
        File.Delete(Path.Combine(ownerLink, ".owner"));
        File.CreateSymbolicLink(Path.Combine(ownerLink, ".owner"), externalOwner);

        GatewayStagingCleanup.RemoveOrphans(fixture.StagingRoot);

        Assert.True(File.GetAttributes(directoryLink).HasFlag(FileAttributes.ReparsePoint));
        Assert.True(File.Exists(Path.Combine(payloadLink, "linked-payload")));
        Assert.True(File.GetAttributes(Path.Combine(ownerLink, ".owner")).HasFlag(FileAttributes.ReparsePoint));
        Assert.Equal("must remain", File.ReadAllText(sentinel));
        Assert.Equal(GatewayStagingCleanup.OwnerMarker.ToArray(), File.ReadAllBytes(externalOwner));
        Assert.True(File.Exists(Path.Combine(payloadLink, "payload")));
        Assert.True(File.Exists(Path.Combine(ownerLink, "payload")));
    }

    [Theory]
    [InlineData("storage", "storage")]
    [InlineData("storage/gateway", "storage")]
    [InlineData("scratch", "scratch/storage")]
    public void StagingAndApplicationDataRootsCannotOverlap(string staging, string data)
    {
        using var fixture = new StagingFixture();
        Assert.Throws<InvalidOperationException>(() => fixture.Create(staging: staging, data: data));
        Assert.False(Directory.Exists(Path.GetFullPath(staging, fixture.Root)));
        Assert.False(Directory.Exists(Path.GetFullPath(data, fixture.Root)));
    }

    [Fact]
    public void UnixStagingRejectsNonprivateDirectoriesWithoutChangingPermissionsOrContents()
    {
        if (OperatingSystem.IsWindows())
            return;
        using var fixture = new StagingFixture();
        fixture.CreateStagingRoot();
        var mode = UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute | UnixFileMode.GroupRead;
        File.SetUnixFileMode(fixture.StagingRoot, mode);
        var sentinel = Path.Combine(fixture.StagingRoot, "sentinel");
        File.WriteAllText(sentinel, "must remain");

        Assert.Throws<InvalidOperationException>(() => fixture.Create());

        Assert.Equal(mode, File.GetUnixFileMode(fixture.StagingRoot));
        Assert.Equal("must remain", File.ReadAllText(sentinel));
        Assert.Empty(Directory.EnumerateDirectories(fixture.StagingRoot));
    }

    [Fact]
    public void UnixStagingRejectsAnAncestorDirectoryLink()
    {
        if (OperatingSystem.IsWindows())
            return;
        using var fixture = new StagingFixture();
        var external = Path.Combine(fixture.Root, "external");
        Directory.CreateDirectory(external);
        var link = Path.Combine(fixture.Root, "linked");
        Directory.CreateSymbolicLink(link, external);

        Assert.Throws<InvalidOperationException>(() => fixture.Create(staging: "linked/gateway"));

        Assert.False(Directory.Exists(Path.Combine(external, "gateway")));
        Assert.True(File.GetAttributes(link).HasFlag(FileAttributes.ReparsePoint));
    }

    [Fact]
    public void PrivateStagingAndOwnerCleanupAreIdempotentAndNeverCreateApplicationData()
    {
        using var fixture = new StagingFixture();
        using var paths = fixture.Create();
        var staging = paths.Staging;
        Assert.False(Directory.Exists(Path.Combine(fixture.Root, "application-data")));
        if (!OperatingSystem.IsWindows())
        {
            var privateMode = UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute;
            Assert.Equal(privateMode, File.GetUnixFileMode(fixture.StagingRoot));
            Assert.Equal(privateMode, File.GetUnixFileMode(staging));
        }
        paths.Dispose();
        paths.Dispose();
        Assert.False(Directory.Exists(staging));
        Assert.True(Directory.Exists(fixture.StagingRoot));
    }

    [Theory]
    [InlineData(129)]
    [InlineData(200)]
    [InlineData(5000)]
    public async Task ContainerListingPreservesPublicPagesWhileFetchingOnlyBoundedPrivateBatches(int maximum)
    {
        var dispatcher = new ListingDispatcher();
        var application = InProcessApplicationProxy.Create<IBlobApplication>(dispatcher);
        var observed = new List<ContainerRecord>();
        var marker = string.Empty;
        while (true)
        {
            var page = await GatewayListing.ListContainersAsync(application, "account", true, true,
                "container-", marker, maximum, CancellationToken.None).ConfigureAwait(true);
            Assert.Equal(Math.Min(maximum, ListingDispatcher.Total - observed.Count), page.Items.Count);
            observed.AddRange(page.Items);
            Assert.Equal(observed.Count < ListingDispatcher.Total, page.HasMore);
            if (!page.HasMore)
                break;
            marker = page.Items[^1].Name;
        }
        Assert.Equal(dispatcher.Containers.Select(item => item.Name), observed.Select(item => item.Name), StringComparer.Ordinal);
        AssertPrivateBatches(dispatcher, maximum, nameof(IBlobApplication.ListContainersPageAsync));
    }

    [Theory]
    [InlineData(129)]
    [InlineData(200)]
    [InlineData(5000)]
    public async Task BlobListingPreservesVersionsAndSnapshotsWithTheFullContinuationCursor(int maximum)
    {
        var dispatcher = new ListingDispatcher();
        var application = InProcessApplicationProxy.Create<IBlobApplication>(dispatcher);
        var observed = new List<BlobListEntry>();
        var marker = new BlobListingMarker(null, LegacyOffset: 0);
        while (true)
        {
            var page = await GatewayListing.ListBlobsAsync(application, "account", "container", BlobListShowOnly.None,
                true, true, true, true, "blob-", string.Empty, string.Empty, string.Empty,
                marker, maximum, CancellationToken.None).ConfigureAwait(true);
            Assert.Equal(Math.Min(maximum, ListingDispatcher.Total - observed.Count), page.Items.Count);
            observed.AddRange(page.Items);
            Assert.Equal(observed.Count < ListingDispatcher.Total, page.HasMore);
            if (!page.HasMore)
                break;
            marker = new BlobListingMarker(page.Items[^1].Cursor, LegacyOffset: 0);
        }
        Assert.Equal(dispatcher.Blobs.Select(item => item.Cursor), observed.Select(item => item.Cursor));
        AssertPrivateBatches(dispatcher, maximum, nameof(IBlobApplication.ListBlobsPageAsync));
    }

    [Theory]
    [InlineData(129)]
    [InlineData(200)]
    [InlineData(5000)]
    public async Task TagListingPreservesEveryIdentityAndPublicContinuation(int maximum)
    {
        var dispatcher = new ListingDispatcher();
        var application = InProcessApplicationProxy.Create<IBlobApplication>(dispatcher);
        var filter = new BlobTagFilter(null, [new BlobTagPredicate("kind", BlobTagComparison.Equal, "test")]);
        var observed = new List<BlobRecord>();
        BlobTagCursor? cursor = null;
        while (true)
        {
            var page = await GatewayListing.FindByTagsAsync(application, "account", filter, cursor, maximum,
                CancellationToken.None).ConfigureAwait(true);
            Assert.Equal(Math.Min(maximum, ListingDispatcher.Total - observed.Count), page.Items.Count);
            observed.AddRange(page.Items);
            Assert.Equal(observed.Count < ListingDispatcher.Total, page.HasMore);
            if (!page.HasMore)
                break;
            cursor = TagCursor(page.Items[^1]);
        }
        Assert.Equal(dispatcher.Tagged.Select(TagCursor), observed.Select(TagCursor));
        AssertPrivateBatches(dispatcher, maximum, nameof(IBlobApplication.FindBlobsByTagsPageAsync));
    }

    [Theory]
    [InlineData("container", "empty")]
    [InlineData("container", "repeat")]
    [InlineData("container", "cycle")]
    [InlineData("container", "overfill")]
    [InlineData("blob", "empty")]
    [InlineData("blob", "repeat")]
    [InlineData("blob", "cycle")]
    [InlineData("blob", "overfill")]
    [InlineData("tag", "empty")]
    [InlineData("tag", "repeat")]
    [InlineData("tag", "cycle")]
    [InlineData("tag", "overfill")]
    public async Task ListingsRejectEmptyRepeatedCyclicOrOversizedPrivateContinuation(string kind, string fault)
    {
        var dispatcher = new ListingDispatcher(fault);
        var application = InProcessApplicationProxy.Create<IBlobApplication>(dispatcher);

        await Assert.ThrowsAsync<InvalidDataException>(() => kind switch
        {
            "container" => GatewayListing.ListContainersAsync(application, "account", true, true, "container-",
                dispatcher.Containers[0].Name, 300, CancellationToken.None),
            "blob" => GatewayListing.ListBlobsAsync(application, "account", "container", BlobListShowOnly.None,
                true, true, true, true, "blob-", string.Empty, string.Empty, string.Empty,
                new BlobListingMarker(dispatcher.Blobs[0].Cursor, LegacyOffset: 0), 300, CancellationToken.None),
            "tag" => GatewayListing.FindByTagsAsync(application, "account",
                new BlobTagFilter(null, [new BlobTagPredicate("kind", BlobTagComparison.Equal, "test")]),
                TagCursor(dispatcher.Tagged[0]), 300, CancellationToken.None),
            _ => throw new InvalidOperationException("Unknown listing kind.")
        }).ConfigureAwait(true);

        Assert.Equal(string.Equals(fault, "cycle", StringComparison.Ordinal) ? 3 : 1, dispatcher.Calls.Count);
        Assert.All(dispatcher.Calls, call => Assert.InRange(call.Maximum, 1, GatewayListing.MaximumBatchSize));
    }

    private static void AssertQuotaFailure(AzureStorageException failure)
    {
        Assert.Equal(503, failure.StatusCode);
        Assert.Equal("ServerBusy", failure.ErrorCode);
    }

    private static void AssertStagingMetrics(GatewayStagingPaths paths, long bytes, long maximum, long rejections)
    {
        Assert.Equal(bytes, paths.ReservedBytes);
        var lines = paths.RenderMetrics().Split('\n', StringSplitOptions.TrimEntries);
        Assert.Contains(FormattableString.Invariant($"mk8_sava_gateway_staging_bytes {bytes}"), lines, StringComparer.Ordinal);
        Assert.Contains(FormattableString.Invariant($"mk8_sava_gateway_staging_limit_bytes {maximum}"), lines, StringComparer.Ordinal);
        Assert.Contains(FormattableString.Invariant($"mk8_sava_gateway_staging_rejections_total {rejections}"), lines, StringComparer.Ordinal);
    }

    private static void AssertPrivateBatches(ListingDispatcher dispatcher, int maximum, string operation)
    {
        var expected = new List<int>();
        for (var offset = 0; offset < ListingDispatcher.Total; offset += maximum)
        {
            var publicCount = Math.Min(maximum, ListingDispatcher.Total - offset);
            var consumed = 0;
            while (consumed < publicCount)
            {
                var batch = Math.Min(GatewayListing.MaximumBatchSize, maximum - consumed);
                expected.Add(batch);
                consumed += Math.Min(batch, publicCount - consumed);
            }
        }
        Assert.Equal(expected, dispatcher.Calls.Select(call => call.Maximum));
        Assert.All(dispatcher.Calls, call => Assert.Equal(operation, call.Operation));
        Assert.All(dispatcher.Calls, call => Assert.InRange(call.Maximum, 1, GatewayListing.MaximumBatchSize));
    }

    private static BlobTagCursor TagCursor(BlobRecord record) => new(record.Container, record.Name, record.GenerationId);

    private sealed class StagingFixture : IDisposable
    {
        private readonly DirectoryInfo _root = Directory.CreateTempSubdirectory("sava-gateway-boundary-");

        public string Root => _root.FullName;
        public string StagingRoot => Path.Combine(Root, "scratch");

        public GatewayStagingPaths Create(long maximum = 8, string staging = "scratch", string data = "application-data") =>
            new(new TestEnvironment(Root), Options.Create(new GatewayOptions { StagingPath = staging, MaximumStagingBytes = maximum }),
                Options.Create(new SavaOptions { DataPath = data }));

        public void CreateStagingRoot()
        {
            if (OperatingSystem.IsWindows())
                Directory.CreateDirectory(StagingRoot);
            else
                Directory.CreateDirectory(StagingRoot, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
        }

        public string CreateOwnedDirectory(string? name = null)
        {
            var path = Path.Combine(StagingRoot, name ?? "gw-" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(path);
            File.WriteAllBytes(Path.Combine(path, ".owner"), GatewayStagingCleanup.OwnerMarker.ToArray());
            File.WriteAllText(Path.Combine(path, "payload"), "must remain");
            return path;
        }

        public void Dispose() => _root.Delete(recursive: true);
    }

    private sealed class TestEnvironment(string root) : IHostEnvironment
    {
        public string EnvironmentName { get; set; } = Environments.Production;
        public string ApplicationName { get; set; } = nameof(GatewayBoundaryTests);
        public string ContentRootPath { get; set; } = root;
        public IFileProvider ContentRootFileProvider { get; set; } = new NullFileProvider();
    }

    private sealed record ListingCall(string Operation, int Maximum);

    private sealed class ListingDispatcher(string? fault = null) : IApplicationRpcDispatcher
    {
        internal const int Total = 300;
        private readonly ContainerRecord[] _containers = Enumerable.Range(0, Total).Select(CreateContainer).ToArray();
        private readonly BlobListEntry[] _blobs = Enumerable.Range(0, Total).Select(index => new BlobListEntry(CreateBlob(index, variants: true), null)).ToArray();
        private readonly BlobRecord[] _tagged = Enumerable.Range(0, Total).Select(index => CreateBlob(index, variants: false)).ToArray();
        private readonly List<ListingCall> _calls = [];

        public ContainerRecord[] Containers => _containers;
        public BlobListEntry[] Blobs => _blobs;
        public BlobRecord[] Tagged => _tagged;
        public List<ListingCall> Calls => _calls;

        public ValueTask<object?> InvokeAsync(Type contract, MethodInfo method, object?[] arguments, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Assert.Equal(typeof(IBlobApplication), contract);
            Assert.Equal("account", arguments[0]);
            var maximum = (int)arguments[method.Name switch
            {
                nameof(IBlobApplication.ListContainersPageAsync) => 5,
                nameof(IBlobApplication.ListBlobsPageAsync) => 12,
                nameof(IBlobApplication.FindBlobsByTagsPageAsync) => 3,
                _ => throw new InvalidOperationException("Unexpected listing operation.")
            }]!;
            Assert.InRange(maximum, 1, GatewayListing.MaximumBatchSize);
            _calls.Add(new ListingCall(method.Name, maximum));
            object result = method.Name switch
            {
                nameof(IBlobApplication.ListContainersPageAsync) => ListContainers(arguments, maximum),
                nameof(IBlobApplication.ListBlobsPageAsync) => ListBlobs(arguments, maximum),
                nameof(IBlobApplication.FindBlobsByTagsPageAsync) => ListTags(arguments, maximum),
                _ => throw new InvalidOperationException("Unexpected listing operation.")
            };
            return ValueTask.FromResult<object?>(result);
        }

        private ContainerListPage ListContainers(object?[] arguments, int maximum)
        {
            Assert.True(Assert.IsType<bool>(arguments[1]));
            Assert.True(Assert.IsType<bool>(arguments[2]));
            Assert.Equal("container-", arguments[3]);
            var marker = (string)arguments[4]!;
            var index = Array.FindIndex(_containers, item => string.Equals(item.Name, marker, StringComparison.Ordinal));
            Assert.True(marker.Length == 0 || index >= 0);
            var (start, count, more) = Page(index, maximum);
            return new ContainerListPage(_containers.AsSpan(start, count).ToArray(), more);
        }

        private BlobListPage ListBlobs(object?[] arguments, int maximum)
        {
            Assert.Equal("container", arguments[1]);
            Assert.Equal(BlobListShowOnly.None, arguments[2]);
            Assert.All(arguments.AsSpan(3, 4).ToArray(), flag => Assert.True(Assert.IsType<bool>(flag)));
            Assert.Equal("blob-", arguments[7]);
            Assert.All(arguments.AsSpan(8, 3).ToArray(), text => Assert.Equal(string.Empty, text));
            var marker = (BlobListingMarker)arguments[11]!;
            var index = marker.Cursor is null ? marker.LegacyOffset - 1 : Array.FindIndex(_blobs, item => item.Cursor == marker.Cursor);
            Assert.True(marker.Cursor is null || index >= 0);
            var (start, count, more) = Page(index, maximum);
            return new BlobListPage(_blobs.AsSpan(start, count).ToArray(), more);
        }

        private TaggedBlobPage ListTags(object?[] arguments, int maximum)
        {
            var filter = (BlobTagFilter)arguments[1]!;
            Assert.Null(filter.Container);
            Assert.Equal(new BlobTagPredicate("kind", BlobTagComparison.Equal, "test"), Assert.Single(filter.Predicates));
            var cursor = (BlobTagCursor?)arguments[2];
            var index = cursor is null ? -1 : Array.FindIndex(_tagged, item => TagCursor(item) == cursor);
            Assert.True(cursor is null || index >= 0);
            var (start, count, more) = Page(index, maximum);
            return new TaggedBlobPage(_tagged.AsSpan(start, count).ToArray(), more);
        }

        private (int Start, int Count, bool More) Page(int previous, int maximum)
        {
            if (fault is null)
            {
                var first = previous + 1;
                var total = Math.Min(maximum, Total - first);
                return (first, total, first + total < Total);
            }
            var start = fault switch
            {
                "repeat" => Math.Max(previous, 0),
                "cycle" => ((_calls.Count - 1) % 2) + 1,
                _ => previous + 1
            };
            var count = fault switch
            {
                "empty" => 0,
                "repeat" or "cycle" => 1,
                "overfill" => maximum + 1,
                _ => throw new InvalidOperationException("Unknown listing fault.")
            };
            return (start, count, true);
        }

        private static ContainerRecord CreateContainer(int index)
        {
            var label = index.ToString("D4", CultureInfo.InvariantCulture);
            return new ContainerRecord
            {
                Account = "account",
                Name = "container-" + label,
                Revision = "revision-" + label,
                ETag = "\"etag-" + label + "\"",
                CreatedAt = DateTimeOffset.UnixEpoch,
                LastModified = DateTimeOffset.UnixEpoch
            };
        }

        private static BlobRecord CreateBlob(int index, bool variants)
        {
            var label = index.ToString("D4", CultureInfo.InvariantCulture);
            var name = (variants ? index / 3 : index).ToString("D4", CultureInfo.InvariantCulture);
            return new BlobRecord
            {
                Account = "account",
                Container = "container",
                Name = "blob-" + name,
                GenerationId = "generation-" + label,
                Revision = "revision-" + label,
                ETag = "\"etag-" + label + "\"",
                Kind = BlobKind.BlockBlob,
                Content = ContentManifest.Empty("account"),
                IsCurrent = !variants || index % 3 == 0,
                VersionId = variants && index % 3 == 1 ? "version-" + label : null,
                Snapshot = variants && index % 3 == 2 ? "snapshot-" + label : null,
                CreatedAt = DateTimeOffset.UnixEpoch,
                LastModified = DateTimeOffset.UnixEpoch
            };
        }
    }
}
