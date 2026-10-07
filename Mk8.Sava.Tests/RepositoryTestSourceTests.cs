using System.Reflection;

namespace Mk8.Sava.Tests;

public sealed class RepositoryTestSourceTests
{
    [Fact]
    public void CompiledSourceBindingDoesNotDependOnBinaryAncestors()
    {
        var assembly = typeof(RepositoryTestSourceTests).Assembly;
        var recorded = Assert.Single(assembly.GetCustomAttributes<AssemblyMetadataAttribute>(),
            attribute => string.Equals(attribute.Key, RepositoryTestSource.RootMetadataKey, StringComparison.Ordinal));

        var source = RepositoryTestSource.FromAssembly(assembly);

        Assert.Equal(Path.GetFullPath(recorded.Value!), source.Root);
        Assert.True(File.Exists(source.BuildPolicyPath));
        Assert.True(File.Exists(Path.Combine(source.Root, "Mk8.Sava.slnx")));
    }

    [Fact]
    public void MissingBindingCannotFallBackToAnAmbientCheckout()
    {
        var failure = Assert.Throws<InvalidOperationException>(() => RepositoryTestSource.FromAssembly(typeof(string).Assembly));
        Assert.Contains("source binding", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void RelativeSourceCannotDependOnCurrentWorkingDirectory()
    {
        var failure = Assert.Throws<ArgumentException>(() => RepositoryTestSource.Open(".", typeof(RepositoryTestSourceTests).Assembly));
        Assert.Equal("root", failure.ParamName);
    }

    [Theory]
    [InlineData("Mk8.Sava.slnx")]
    [InlineData("Directory.Build.targets")]
    public void MissingSourceInputCannotProduceAValidSource(string missing)
    {
        var fixture = new SourceFixture();
        using var disposal = fixture;
        fixture.CopyInput(string.Equals(missing, "Mk8.Sava.slnx", StringComparison.Ordinal) ? "Directory.Build.targets" : "Mk8.Sava.slnx");

        var failure = Assert.Throws<FileNotFoundException>(() => RepositoryTestSource.Open(fixture.Root, typeof(RepositoryTestSourceTests).Assembly));

        Assert.Equal(Path.Combine(fixture.Root, missing), failure.FileName);
    }

    [Theory]
    [InlineData("Mk8.Sava.slnx")]
    [InlineData("Directory.Build.targets")]
    public void SameNamedButDifferentInputsAreRejected(string changed)
    {
        var fixture = new SourceFixture();
        using var disposal = fixture;
        fixture.CopyInput("Mk8.Sava.slnx");
        fixture.CopyInput("Directory.Build.targets");
        File.AppendAllText(Path.Combine(fixture.Root, changed), "\n<!-- controlled different source -->\n");

        var failure = Assert.Throws<InvalidDataException>(() => RepositoryTestSource.Open(fixture.Root, typeof(RepositoryTestSourceTests).Assembly));

        Assert.Contains(changed, failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void PolicyAndSolutionMarkersAloneCannotStandInForTheSourceTree()
    {
        var fixture = new SourceFixture();
        using var disposal = fixture;
        fixture.CopyInput("Mk8.Sava.slnx");
        fixture.CopyInput("Directory.Build.targets");

        var failure = Assert.Throws<FileNotFoundException>(() => RepositoryTestSource.Open(fixture.Root, typeof(RepositoryTestSourceTests).Assembly));

        Assert.EndsWith("Mk8.Sava.API.csproj", failure.FileName, StringComparison.Ordinal);
    }

    private sealed class SourceFixture : IDisposable
    {
        private readonly DirectoryInfo _directory = Directory.CreateTempSubdirectory("mk8-sava-source with spaces-");

        public string Root => _directory.FullName;

        public void CopyInput(string name)
        {
            using var snapshot = typeof(RepositoryTestSourceTests).Assembly.GetManifestResourceStream("Mk8.Sava.Tests.Source." + name)
                ?? throw new InvalidOperationException("Missing controlled source fixture input.");
            using var destination = File.Create(Path.Combine(Root, name));
            snapshot.CopyTo(destination);
        }

        public void Dispose() => _directory.Delete(recursive: true);
    }
}
