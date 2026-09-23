namespace Mk8.Sava.Protocol;

internal sealed record BlobQueryRequest(
    string Expression,
    BlobQueryTextFormat Input,
    BlobQueryTextFormat Output);
