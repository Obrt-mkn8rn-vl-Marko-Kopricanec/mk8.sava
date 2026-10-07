using System.Reflection;
using System.Security.Cryptography;

namespace Mk8.Sava.Tests;

internal sealed class RepositoryTestSource
{
    internal const string RootMetadataKey = "Mk8.Sava.Tests.RepositoryRoot";
    private const string ResourcePrefix = "Mk8.Sava.Tests.Source.";

    private RepositoryTestSource(string root)
    {
        Root = root;
        BuildPolicyPath = Path.Combine(root, "Directory.Build.targets");
    }

    public string Root { get; }
    public string BuildPolicyPath { get; }

    public static RepositoryTestSource FromAssembly(Assembly assembly)
    {
        ArgumentNullException.ThrowIfNull(assembly);
        var roots = assembly.GetCustomAttributes<AssemblyMetadataAttribute>()
            .Where(attribute => string.Equals(attribute.Key, RootMetadataKey, StringComparison.Ordinal))
            .Select(attribute => attribute.Value).ToArray();
        var root = roots.Length == 1 ? roots[0] : null;
        if (string.IsNullOrEmpty(root))
            throw new InvalidOperationException("The architecture tests need exactly one compiled repository source binding. Rebuild the test project.");
        return Open(root, assembly);
    }

    internal static RepositoryTestSource Open(string root, Assembly assembly)
    {
        ArgumentNullException.ThrowIfNull(root);
        ArgumentNullException.ThrowIfNull(assembly);
        if (!Path.IsPathFullyQualified(root))
            throw new ArgumentException("The compiled repository source path must be absolute.", nameof(root));
        var fullRoot = Path.GetFullPath(root);
        VerifyInput(fullRoot, "Mk8.Sava.slnx", assembly);
        VerifyInput(fullRoot, "Directory.Build.targets", assembly);
        foreach (var project in new[] { "API", "Application", "BLL", "Contracts", "DAL", "Gateway", "INF", "Tests", "Transport" })
        {
            var name = "Mk8.Sava." + project;
            var path = Path.Combine(fullRoot, name, name + ".csproj");
            if (!File.Exists(path))
                throw new FileNotFoundException("The compiled repository source is missing a solution project. Restore the checkout and rebuild the tests.", path);
        }
        return new RepositoryTestSource(fullRoot);
    }

    private static void VerifyInput(string root, string name, Assembly assembly)
    {
        using var snapshot = assembly.GetManifestResourceStream(ResourcePrefix + name)
            ?? throw new InvalidOperationException("The architecture test assembly is missing its compiled source snapshot. Rebuild the test project.");
        var path = Path.Combine(root, name);
        using var current = File.OpenRead(path);
        if (!SHA256.HashData(snapshot).AsSpan().SequenceEqual(SHA256.HashData(current)))
            throw new InvalidDataException($"Architecture input {name} no longer matches the compiled tests. Rebuild from the intended checkout.");
    }
}
