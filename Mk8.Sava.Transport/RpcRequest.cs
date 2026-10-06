using System.Text.Json;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Transport;

internal readonly record struct RpcRequest(
    string Contract, string Method, JsonElement[] Arguments, bool HasInput, PageRangeDiff? PageChanges);
