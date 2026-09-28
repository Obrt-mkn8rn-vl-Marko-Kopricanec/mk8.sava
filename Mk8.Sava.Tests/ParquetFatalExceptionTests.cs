using Apache.Arrow;
using Apache.Arrow.Ipc;
using Mk8.Sava.Protocol;

namespace Mk8.Sava.Tests;

public sealed class ParquetFatalExceptionTests
{
    [Theory]
    [MemberData(nameof(FatalFailures))]
    public async Task NativeBatchReadDoesNotNormalizeFatalFailure(Exception failure)
    {
        using var stream = new ThrowingArrowStream(failure);

        var escaped = await Assert.ThrowsAnyAsync<Exception>(async () =>
            await ParquetQueryBatchReader.ReadBatchAsync(stream, CancellationToken.None).ConfigureAwait(false));

        Assert.Same(failure, escaped);
    }

    [Theory]
    [MemberData(nameof(FatalFailures))]
    public async Task ParquetPreparationDoesNotNormalizeFatalReadFailure(Exception failure)
    {
        using var input = new ThrowingReadStream(failure);

        var escaped = await Assert.ThrowsAnyAsync<Exception>(() =>
            BlobQueryProtocol.PrepareParquetInputAsync(input, 8L * 1024 * 1024, CancellationToken.None));

        Assert.Same(failure, escaped);
    }

    [Fact]
    public async Task OrdinaryNativeBatchFailureStillBecomesInvalidParquetInput()
    {
        using var stream = new ThrowingArrowStream(new InvalidOperationException("bad native row group"));

        var error = await Assert.ThrowsAsync<BlobQueryDataException>(async () =>
            await ParquetQueryBatchReader.ReadBatchAsync(stream, CancellationToken.None).ConfigureAwait(false));

        Assert.Equal("InvalidParquetFile", error.Name);
    }

    public static TheoryData<Exception> FatalFailures => new()
    {
#pragma warning disable CA2201 // Runtime-reserved exceptions are deliberately injected into the native-reader boundary.
        new OutOfMemoryException(),
        new AccessViolationException(),
        new InvalidOperationException("wrapped", new OutOfMemoryException())
#pragma warning restore CA2201
    };

    private sealed class ThrowingArrowStream(Exception failure) : IArrowArrayStream
    {
        public Schema Schema => throw new NotSupportedException();

        public ValueTask<RecordBatch> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default) =>
            ValueTask.FromException<RecordBatch>(failure);

        public void Dispose() { }
    }

    private sealed class ThrowingReadStream(Exception failure) : MemoryStream(new byte[12], writable: false)
    {
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) =>
            ValueTask.FromException<int>(failure);
    }
}
