namespace Mk8.Sava.Storage;

public sealed record BlobListingMarker(BlobListCursor? Cursor, int LegacyOffset);
