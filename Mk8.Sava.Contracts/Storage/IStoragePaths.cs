namespace Mk8.Sava.Storage;

public interface IStoragePaths
{
    string Root { get; }

    string Chunks { get; }

    string Packs { get; }

    string Staging { get; }

    string Database { get; }
}
