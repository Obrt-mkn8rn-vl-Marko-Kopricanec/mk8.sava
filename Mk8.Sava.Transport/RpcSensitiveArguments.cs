using System.Security.Cryptography;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Transport;

internal static class RpcSensitiveArguments
{
    internal static void Clear(object?[] arguments)
    {
        foreach (var argument in arguments)
        {
            var key = argument switch
            {
                BlobEncryption encryption => encryption.CustomerProvidedKey,
                BlobWriteOptions options => options.CustomerProvidedKey,
                _ => null,
            };
            if (key is not null)
                CryptographicOperations.ZeroMemory(key);
        }
    }
}
