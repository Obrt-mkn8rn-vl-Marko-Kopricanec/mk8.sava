namespace Mk8.Sava.Transport;

internal sealed record RpcResponsePayload(object? Result, RpcError? Error, bool HasOutput);
