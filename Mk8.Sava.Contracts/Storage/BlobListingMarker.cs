namespace Mk8.Sava.Storage;

internal sealed record BlobListingMarker(BlobListCursor? Cursor, int LegacyOffset);
