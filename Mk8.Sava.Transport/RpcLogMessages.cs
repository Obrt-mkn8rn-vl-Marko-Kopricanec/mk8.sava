using Microsoft.Extensions.Logging;

namespace Mk8.Sava.Transport;

internal static partial class RpcLogMessages
{
    [LoggerMessage(EventId = 4201, Level = LogLevel.Error,
        Message = "Application RPC failed with {ExceptionType}; transport request {RequestId}.")]
    internal static partial void OperationFailed(ILogger logger, string exceptionType, string requestId);
}
