namespace Mk8.Sava.Protocol;

internal sealed record UrlTransferResult<TResult>(TResult Value, TransactionalChecksums Checksums);
