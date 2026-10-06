using System.Xml;
using System.Runtime.ExceptionServices;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Protocol;

public static class StorageExceptionMapper
{
    public static AzureStorageException Map(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);
        if (CatastrophicExceptionPolicy.Contains(exception))
            ExceptionDispatchInfo.Capture(exception).Throw();
        return exception switch
        {
            AzureStorageException storage => storage,
            RequestBodyTooLargeException oversized => new AzureStorageException(413, "RequestBodyTooLarge", oversized.Message),
            StorageConcurrencyException => AzureStorageException.ConditionNotMet(),
            StorageImmutabilityException immutable => new AzureStorageException(409,
                immutable.LegalHold ? "BlobImmutableDueToLegalHold" : "BlobImmutableDueToPolicy", immutable.Message),
            StoragePendingCopyException pending => new AzureStorageException(409, "PendingCopyOperation", pending.Message),
            StorageBlobTypeMismatchException mismatch => new AzureStorageException(409, "InvalidBlobType", mismatch.Message),
            StoragePathConflictException => AzureStorageException.PathAlreadyExists(),
            XmlException => new AzureStorageException(400, "InvalidXmlDocument", "The specified XML is not syntactically valid."),
            _ => new AzureStorageException(500, "InternalError", "The server encountered an internal error. Please retry the request."),
        };
    }

    public static bool IsKnown(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);
        return exception is AzureStorageException or RequestBodyTooLargeException or StorageConcurrencyException or
            StorageImmutabilityException or StoragePendingCopyException or StorageBlobTypeMismatchException or
            StoragePathConflictException or XmlException;
    }
}
