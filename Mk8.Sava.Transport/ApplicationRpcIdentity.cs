namespace Mk8.Sava.Transport;

public static class ApplicationRpcIdentity
{
    private static readonly AsyncLocal<string?> PeerFingerprint = new();

    public static string? CurrentPeerFingerprint => PeerFingerprint.Value;

    internal static IDisposable Enter(string fingerprint)
    {
        var previous = PeerFingerprint.Value;
        PeerFingerprint.Value = fingerprint;
        return new RestoreScope(previous);
    }

    private sealed class RestoreScope(string? previous) : IDisposable
    {
        public void Dispose() => PeerFingerprint.Value = previous;
    }
}
