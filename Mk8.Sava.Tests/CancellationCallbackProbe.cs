namespace Mk8.Sava.Tests;

// One controlled callback body, not the complete CancelAsync callback chain or a native operation.
internal sealed class CancellationCallbackProbe
{
    private readonly Lock gate = new();
    private readonly Action callback;
    private bool entered;
    private bool bodyExited;
    private bool returnedNormally;

    internal CancellationCallbackProbe(Action callback)
    {
        ArgumentNullException.ThrowIfNull(callback);
        this.callback = callback;
    }

    internal CallbackProgress Progress
    {
        get { lock (gate) return new(entered, bodyExited, returnedNormally); }
    }

    internal void Invoke()
    {
        lock (gate)
        {
            if (entered)
                throw new InvalidOperationException("The controlled callback probe is single-use.");
            entered = true;
        }

        var returned = false;
        try
        {
            // The callback never owns the snapshot lock, even when synchronous work is held.
            callback();
            returned = true;
        }
        finally
        {
            lock (gate)
            {
                bodyExited = true;
                returnedNormally = returned;
            }
        }
    }

    internal readonly record struct CallbackProgress(bool Entered, bool BodyExited, bool ReturnedNormally)
    {
        internal string ToDiagnostic() => $"Entered={Entered}, BodyExited={BodyExited}, ReturnedNormally={ReturnedNormally}";
    }
}
