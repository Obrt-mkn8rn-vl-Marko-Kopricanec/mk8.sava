using Mk8.Sava.Storage;

namespace Mk8.Sava.Application;

public sealed record ApplicationReadiness(bool Ready, bool MetadataReady, StorageIntegritySnapshot Integrity);
