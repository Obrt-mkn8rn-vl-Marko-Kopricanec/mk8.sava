using System.Runtime.ExceptionServices;
using System.Text.Json;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Mk8.Sava.Protocol;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

public sealed class StagedBlockAdmissionFenceTests
{
    private const string Account = SavaWebApplicationFactory.AccountName;
    private const string Container = "staged-admission";
    private const string Target = "held.bin";
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    [Theory]
    [InlineData("staged-mismatch")]
    [InlineData("committed-mismatch")]
    [InlineData("compatible")]
    [InlineData("capacity")]
    [InlineData("replacement")]
    public async Task StageCommitRechecksIdShapeAndCapacityAfterBodyAdmission(string mode)
    {
        var factory = new SavaWebApplicationFactory(TimeProvider.System,
            new Dictionary<string, string?>(StringComparer.Ordinal), disableMaintenance: true);
        await using var lifetime = factory.ConfigureAwait(false);
        using var transport = new HttpClientTransport(factory.Server.CreateHandler());
        var owner = new BlobServiceClient(new Uri($"http://127.0.0.1/{Account}"),
            new StorageSharedKeyCredential(Account, SavaWebApplicationFactory.AccountKey),
            new BlobClientOptions { Transport = transport, Retry = { MaxRetries = 0 } });
        await owner.GetBlobContainerClient(Container).CreateAsync().ConfigureAwait(true);
        var service = factory.Services.GetRequiredService<BlobService>();
        var metadata = factory.Services.GetRequiredService<MetadataStore>();
        var id = IncomingId(mode);
        var body = new HeldBody();
        await using var bodyLifetime = body.ConfigureAwait(false);
        using var cancellation = new CancellationTokenSource();
#pragma warning disable CA2025 // finally releases, cancels and joins this actual context-free store before body/token/factory retirement; Record.ExceptionAsync hides that ownership from analysis.
        var operation = service.StageBlockAsync(Account, Container, Target, id, body,
            new BlobEncryption(Scope: null, CustomerProvidedKeySha256: null), cancellation.Token);
#pragma warning restore CA2025
        Exception? primary = null;
        Exception? observed = null;
        Exception? canceled = null;
        Exception? joined = null;
        try
        {
            primary = await Record.ExceptionAsync(async () =>
            {
                await body.Entered.Task.WaitAsync(TimeSpan.FromSeconds(15), CancellationToken.None).ConfigureAwait(true);
                await SeedWhileHeldAsync(factory, service, metadata, mode).ConfigureAwait(true);
                var before = await metadata.GetBlobAsync(Account, Container, Target, versionId: null, snapshot: null,
                    includeDeleted: false, CancellationToken.None).ConfigureAwait(true);
                body.Release();
                observed = await Record.ExceptionAsync(() => operation.WaitAsync(TimeSpan.FromSeconds(15), CancellationToken.None)).ConfigureAwait(true);
                VerifyOutcome(mode, observed);
                await VerifyStoredAsync(factory, metadata, mode, id, before).ConfigureAwait(true);
            }).ConfigureAwait(true);
        }
        finally
        {
            body.Release();
            canceled = await Record.ExceptionAsync(() => cancellation.CancelAsync()).ConfigureAwait(true);
#pragma warning disable VSTHRD003 // Join this test's actual operation after release even when verification fails, before any dependency disposal.
            joined = await Record.ExceptionAsync(() => operation).ConfigureAwait(true);
#pragma warning restore VSTHRD003
        }
        ThrowFailures(primary, canceled, ReferenceEquals(joined, observed) ? null : joined);
    }

    private static string IncomingId(string mode) => mode switch
    {
        "capacity" => "99999999",
        "replacement" => "00000001",
        _ => "YWE=", // two decoded bytes, versus YQ== (one byte).
    };

    private static async Task SeedWhileHeldAsync(SavaWebApplicationFactory factory, BlobService service,
        MetadataStore metadata, string mode)
    {
        var capacity = mode is "capacity" or "replacement";
        var id = capacity ? "00000001" : string.Equals(mode, "compatible", StringComparison.Ordinal) ? "YWI=" : "YQ==";
        var name = capacity ? "seed.bin" : Target;
        var empty = new MemoryStream();
        await using var emptyLifetime = empty.ConfigureAwait(false);
        _ = await service.StageBlockAsync(Account, Container, name, id, empty,
            new BlobEncryption(Scope: null, CustomerProvidedKeySha256: null), CancellationToken.None).ConfigureAwait(false);
        if (capacity)
        {
            var seed = Assert.Single(await metadata.ListStagedBlocksAsync(Account, Container, name,
                CancellationToken.None).ConfigureAwait(false));
            Assert.Empty(seed.Content.Chunks);
            await SeedCapacityAsync(factory.DataPath, seed with { BlobName = Target }).ConfigureAwait(false);
        }
        else if (string.Equals(mode, "committed-mismatch", StringComparison.Ordinal))
        {
            _ = await service.CommitBlockListAsync(Account, Container, Target, [new BlockListEntry(id, BlockListMode.Uncommitted)],
                new BlobWriteOptions(new BlobHttpProperties(), new Dictionary<string, string>(StringComparer.Ordinal)),
                expectedGeneration: null, expectedRevision: null, CancellationToken.None).ConfigureAwait(false);
        }
    }

    private static async Task SeedCapacityAsync(string dataPath, StagedBlockRecord seed)
    {
        var connection = CreateConnection(dataPath);
        await using var connectionLifetime = connection.ConfigureAwait(false);
        await connection.OpenAsync(CancellationToken.None).ConfigureAwait(false);
        var command = connection.CreateCommand();
        await using var commandLifetime = command.ConfigureAwait(false);
        // A finite fixture of 100000 valid, equal-length Base64 IDs and empty
        // manifests (no chunk references). No concurrent task can reach its
        // metadata commit while this test still holds the body gate.
        command.CommandText = """
            WITH RECURSIVE ids(id) AS (
                SELECT 1 UNION ALL SELECT id + 1 FROM ids WHERE id < 100000
            )
            INSERT INTO staged_blocks(account, container, blob_name, block_id, created_ticks, logical_length, data)
            SELECT $account, $container, $blob, printf('%08d', id), $created, 0,
                   json_set($data, '$.blockId', printf('%08d', id)) FROM ids;
            """;
        command.Parameters.AddWithValue("$account", Account);
        command.Parameters.AddWithValue("$container", Container);
        command.Parameters.AddWithValue("$blob", Target);
        command.Parameters.AddWithValue("$created", seed.CreatedAt.UtcTicks);
        command.Parameters.AddWithValue("$data", JsonSerializer.Serialize(seed, JsonOptions));
        Assert.Equal(100000, await command.ExecuteNonQueryAsync(CancellationToken.None).ConfigureAwait(false));
    }

    private static void VerifyOutcome(string mode, Exception? observed)
    {
        if (mode is "compatible" or "replacement")
        {
            Assert.Null(observed);
        }
        else
        {
            var error = StorageExceptionMapper.Map(Assert.IsType<Exception>(observed, exactMatch: false));
            Assert.Equal(string.Equals(mode, "capacity", StringComparison.Ordinal) ? 409 : 400, error.StatusCode);
            Assert.Equal(string.Equals(mode, "capacity", StringComparison.Ordinal) ? "BlockCountExceedsLimit" : "InvalidBlobOrBlock", error.ErrorCode);
        }
    }

    private static async Task VerifyStoredAsync(SavaWebApplicationFactory factory, MetadataStore metadata,
        string mode, string id, BlobRecord? before)
    {
        var current = await metadata.GetBlobAsync(Account, Container, Target, versionId: null, snapshot: null,
            includeDeleted: false, CancellationToken.None).ConfigureAwait(false);
        Assert.Equal(JsonSerializer.Serialize(before), JsonSerializer.Serialize(current));
        var connection = CreateConnection(factory.DataPath);
        await using var connectionLifetime = connection.ConfigureAwait(false);
        await connection.OpenAsync(CancellationToken.None).ConfigureAwait(false);
        var count = connection.CreateCommand();
        await using var countLifetime = count.ConfigureAwait(false);
        count.CommandText = "SELECT COUNT(*) FROM staged_blocks WHERE account = $account AND container = $container AND blob_name = $blob;";
        AddTarget(count);
        var expected = mode switch { "capacity" or "replacement" => 100000L, "compatible" => 2L, "committed-mismatch" => 0L, _ => 1L };
        Assert.Equal(expected, (long?)await count.ExecuteScalarAsync(CancellationToken.None).ConfigureAwait(false));
        var selected = connection.CreateCommand();
        await using var selectedLifetime = selected.ConfigureAwait(false);
        selected.CommandText = "SELECT data FROM staged_blocks WHERE account = $account AND container = $container AND blob_name = $blob AND block_id = $id;";
        AddTarget(selected);
        selected.Parameters.AddWithValue("$id", id);
        var json = (string?)await selected.ExecuteScalarAsync(CancellationToken.None).ConfigureAwait(false);
        if (mode is "compatible" or "replacement")
        {
            Assert.NotNull(json);
            var written = Assert.IsType<StagedBlockRecord>(JsonSerializer.Deserialize<StagedBlockRecord>(json, JsonOptions));
            Assert.Equal(512, written.Content.Length);
            var bytes = new MemoryStream();
            await using var bytesLifetime = bytes.ConfigureAwait(false);
            await factory.Services.GetRequiredService<ChunkStore>().WriteRangeAsync(written.Content,
                new BlobEncryption(Scope: null, CustomerProvidedKeySha256: null), 0, 512, bytes,
                CancellationToken.None).ConfigureAwait(false);
            Assert.Equal(new byte[512], bytes.ToArray());
        }
        else
        {
            Assert.Null(json);
        }
    }

    private static SqliteConnection CreateConnection(string dataPath) => new(new SqliteConnectionStringBuilder
    {
        DataSource = Path.Combine(dataPath, "metadata.db"),
        Mode = SqliteOpenMode.ReadWrite,
        Pooling = false,
    }.ToString());

    private static void AddTarget(SqliteCommand command)
    {
        command.Parameters.AddWithValue("$account", Account);
        command.Parameters.AddWithValue("$container", Container);
        command.Parameters.AddWithValue("$blob", Target);
    }

    private static void ThrowFailures(params Exception?[] failures)
    {
        var retained = failures.OfType<Exception>().ToArray();
        foreach (var failure in retained.Where(CatastrophicExceptionPolicy.Contains))
            ExceptionDispatchInfo.Throw(failure);
        if (retained.Length == 1)
            ExceptionDispatchInfo.Throw(retained[0]);
        if (retained.Length > 1)
            throw new AggregateException("Verification and independent owned-operation failures.", retained);
    }

    private sealed class HeldBody() : MemoryStream(new byte[512], writable: false)
    {
        private readonly TaskCompletionSource _released = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal void Release() => _released.TrySetResult();
        public override bool CanSeek => false;

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            Entered.TrySetResult();
            await _released.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            return await base.ReadAsync(buffer, cancellationToken).ConfigureAwait(false);
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
            ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();
    }
}
