namespace Mk8.Sava.Tests;

internal static class DeterministicTestBytes
{
    public static void Fill(int seed, Span<byte> destination)
    {
        // Fixed seeds make differential and space-efficiency fixtures repeatable; these bytes are never secrets.
#pragma warning disable CA5394
        new Random(seed).NextBytes(destination);
#pragma warning restore CA5394
    }
}
