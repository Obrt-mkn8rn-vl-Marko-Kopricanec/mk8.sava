namespace Mk8.Sava.Protocol;

internal sealed class SourceCustomerProvidedKey(
    string encodedKey,
    string encodedHash,
    string algorithm)
{
    public string EncodedKey { get; } = encodedKey;
    public string EncodedHash { get; } = encodedHash;
    public string Algorithm { get; } = algorithm;
}
