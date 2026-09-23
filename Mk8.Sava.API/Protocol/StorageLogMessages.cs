namespace Mk8.Sava.Protocol;

internal static partial class StorageLogMessages
{
    [LoggerMessage(EventId = 1001, Level = LogLevel.Error, Message = "Storage operation failed for request {RequestId}.")]
    internal static partial void StorageOperationFailed(ILogger logger, Exception exception, string requestId);

    [LoggerMessage(EventId = 1002, Level = LogLevel.Information, Message = "Storage request {RequestId} {Method} {ResourceKind} completed with {StatusCode} in {ElapsedMilliseconds:F3} ms.")]
    internal static partial void StorageRequestCompleted(
        ILogger logger,
        string requestId,
        string method,
        string resourceKind,
        int statusCode,
        double elapsedMilliseconds);

    [LoggerMessage(EventId = 1003, Level = LogLevel.Warning, Message = "Storage Analytics could not persist request {RequestId}.")]
    internal static partial void StorageAnalyticsPersistenceFailed(ILogger logger, Exception exception, string requestId);

    [LoggerMessage(EventId = 1004, Level = LogLevel.Error, Message = "Blob batch subrequest {RequestId} failed unexpectedly.")]
    internal static partial void BlobBatchSubrequestFailed(ILogger logger, Exception exception, string requestId);

    [LoggerMessage(EventId = 1005, Level = LogLevel.Warning, Message = "SAS request {RequestId} for account {Account} is not bound to an end-user identity.")]
    internal static partial void UserBoundSasMissing(ILogger logger, string requestId, string account);

    [LoggerMessage(EventId = 1006, Level = LogLevel.Warning, Message = "SAS request {RequestId} for account {Account} {Violation}.")]
    internal static partial void SasExpirationPolicyViolated(ILogger logger, string requestId, string account, string violation);

    [LoggerMessage(EventId = 1007, Level = LogLevel.Information, Message = "Pruned {DirectoryCount} legacy empty chunk directories.")]
    internal static partial void LegacyChunkDirectoriesPruned(ILogger logger, int directoryCount);
}
