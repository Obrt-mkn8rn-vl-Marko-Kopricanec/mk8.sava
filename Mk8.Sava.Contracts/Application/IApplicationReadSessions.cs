using Mk8.Sava.Storage;

namespace Mk8.Sava.Application;

public interface IApplicationReadSessions
{
    Task<ApplicationReadSession> OpenAsync(BlobRecord record, bool query, CancellationToken cancellationToken);
    Task WriteRangeAsync(string token, BlobEncryption encryption, long offset, long length,
        Stream destination, CancellationToken cancellationToken);
    Task TouchAsync(string token, CancellationToken cancellationToken);
    Task CloseAsync(string token, CancellationToken cancellationToken);
}
