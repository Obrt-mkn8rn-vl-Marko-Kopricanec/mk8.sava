namespace Mk8.Sava.Storage;

public sealed record StorageAnalyticsRequest
{
    public required string Account { get; init; }
    public required DateTimeOffset StartedAt { get; init; }
    public required DateTimeOffset CompletedAt { get; init; }
    public required string Operation { get; init; }
    public required StorageAnalyticsOperationCategory Category { get; init; }
    public required string RequestStatus { get; init; }
    public required int StatusCode { get; init; }
    public required long EndToEndLatencyMilliseconds { get; init; }
    public required long ServerLatencyMilliseconds { get; init; }
    public required string AuthenticationType { get; init; }
    public string? RequesterAccountName { get; init; }
#pragma warning disable CA1056 // Preserve the redacted, escaped request target exactly as logged.
    public required string RequestUrl { get; init; }
#pragma warning restore CA1056
    public required string RequestedObjectKey { get; init; }
    public required string RequestId { get; init; }
    public string? RequesterIpAddress { get; init; }
    public required string RequestVersion { get; init; }
    public required long RequestHeaderSize { get; init; }
    public required long RequestPacketSize { get; init; }
    public required long ResponseHeaderSize { get; init; }
    public required long ResponsePacketSize { get; init; }
    public required long RequestContentLength { get; init; }
    public string? RequestMd5 { get; init; }
    public string? ServerMd5 { get; init; }
    public string? ETag { get; init; }
    public string? LastModified { get; init; }
    public string? Conditions { get; init; }
    public string? UserAgent { get; init; }
    public string? Referrer { get; init; }
    public string? ClientRequestId { get; init; }
    public string? UserObjectId { get; init; }
    public string? TenantId { get; init; }
    public string? ApplicationId { get; init; }
    public string? Audience { get; init; }
    public string? Issuer { get; init; }
    public string? UserPrincipalName { get; init; }
}
