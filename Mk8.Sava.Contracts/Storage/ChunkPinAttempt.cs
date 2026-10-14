namespace Mk8.Sava.Storage;

// A declined attempt owns no pins. The caller may wait for the borrowed mutation
// completion only after releasing any metadata write gate it currently owns.
#pragma warning disable MA0182 // This internal transfer shape is consumed by the existing friend DAL/INF assemblies, not within Contracts itself.
internal readonly record struct ChunkPinAttempt(IDisposable? Pins, Task? PendingMutation);
#pragma warning restore MA0182
