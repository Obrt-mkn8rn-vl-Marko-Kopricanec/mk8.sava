using Mk8.Sava.Storage;

namespace Mk8.Sava.Application;

public sealed record BlobLeaseUpdate(BlobRecord Record, int? RemainingSeconds);
