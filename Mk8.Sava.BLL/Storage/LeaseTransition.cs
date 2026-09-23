namespace Mk8.Sava.Storage;

public readonly record struct LeaseTransition(LeaseRecord Lease, int? RemainingSeconds);
