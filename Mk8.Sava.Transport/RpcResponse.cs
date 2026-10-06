using System.Text.Json;

namespace Mk8.Sava.Transport;

internal readonly record struct RpcResponse(JsonElement Result, RpcError? Error, bool HasOutput);
