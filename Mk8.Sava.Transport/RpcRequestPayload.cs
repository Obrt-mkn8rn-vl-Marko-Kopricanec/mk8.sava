using Mk8.Sava.Storage;

namespace Mk8.Sava.Transport;

internal sealed record RpcRequestPayload(
    string Contract, string Method, object?[] Arguments, bool HasInput, PageRangeDiff? PageChanges);
