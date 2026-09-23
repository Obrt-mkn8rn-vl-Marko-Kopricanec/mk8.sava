using System.Security.Cryptography;

namespace Mk8.Sava.Tests;

internal static class AzureProtocolChecksum
{
    public static byte[] Md5(ReadOnlySpan<byte> payload)
    {
        // Azure Blob uses MD5 as a wire checksum; this must never be used for signatures or content identity.
#pragma warning disable CA5351
        return MD5.HashData(payload);
#pragma warning restore CA5351
    }
}
