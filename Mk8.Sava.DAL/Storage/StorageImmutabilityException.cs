namespace Mk8.Sava.Storage;

// The legal-hold discriminator is required to select the Blob protocol error.
#pragma warning disable CA1032, RCS1194
public sealed class StorageImmutabilityException(bool legalHold) : Exception(
    legalHold
        ? "The blob is protected by a legal hold."
        : "The blob is protected by a time-based retention policy.")
#pragma warning restore CA1032, RCS1194
{
    public bool LegalHold { get; } = legalHold;
}
