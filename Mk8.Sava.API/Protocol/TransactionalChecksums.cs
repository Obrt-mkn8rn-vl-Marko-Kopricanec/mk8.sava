namespace Mk8.Sava.Protocol;

internal sealed record TransactionalChecksums(byte[] Md5, byte[] Crc64)
{
    public static TransactionalChecksums Empty { get; } =
        new(Convert.FromHexString("D41D8CD98F00B204E9800998ECF8427E"), new StorageCrc64().GetHash());

    public string Md5Base64 => Convert.ToBase64String(Md5);
    public string Crc64Base64 => Convert.ToBase64String(Crc64);
}
