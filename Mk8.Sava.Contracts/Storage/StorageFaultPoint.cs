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
