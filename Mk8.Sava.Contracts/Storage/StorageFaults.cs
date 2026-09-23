namespace Mk8.Sava.Storage;

public enum StorageFaultPoint
{
    DuringChunkStagingWrite,
    BeforeChunkPublication,
    DuringPackRecordAppend,
    BeforeBlobMetadataCommit,
    AfterBlobMetadataCommit,
    BeforePackMetadataCommit,
    AfterPackMetadataCommit,
    BeforeGarbageCollectionDelete
}

public interface IStorageFaultInjector
{
    void Inject(StorageFaultPoint point);
}
