namespace Mk8.Sava.Tests;

[CollectionDefinition("Isolated ENOSPC harness", DisableParallelization = true)]
// xUnit collection definitions must be public for discovery.
#pragma warning disable CA1515
public sealed class StorageEnospcHarnessTestGroup;
#pragma warning restore CA1515
