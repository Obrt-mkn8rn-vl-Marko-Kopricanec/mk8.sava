using System.Diagnostics;
using System.Reflection;
using System.Text.Json;
using System.Xml.Linq;

namespace Mk8.Sava.Tests;

public sealed class ArchitectureBoundaryTests
{
    [Theory]
    [InlineData("Mk8.Sava.Gateway", "Mk8.Sava.BLL")]
    [InlineData("Mk8.Sava.API", "Mk8.Sava.DAL")]
    [InlineData("Mk8.Sava.Application", "Mk8.Sava.API")]
    [InlineData("Mk8.Sava.Transport", "Mk8.Sava.INF")]
    [InlineData("Mk8.Sava.Contracts", "Mk8.Sava.Transport")]
    [InlineData("Mk8.Sava.INF", "Mk8.Sava.BLL")]
    [InlineData("Mk8.Sava.DAL", "Mk8.Sava.INF")]
    public async Task EvaluatedBuildRejectsForbiddenDependencies(string project, string dependency)
    {
        var result = await CheckBuildPolicyAsync(project, dependency);

        Assert.NotEqual(0, result.ExitCode);
        Assert.Contains("MK8ARCH002", result.Output, StringComparison.Ordinal);
        Assert.Contains(project, result.Output, StringComparison.Ordinal);
        Assert.Contains(dependency, result.Output, StringComparison.Ordinal);
    }

    [Fact]
    public async Task EvaluatedBuildRejectsAnUnclassifiedProductionProject()
    {
        var result = await CheckBuildPolicyAsync("Mk8.Sava.Unclassified", "Mk8.Sava.Contracts");

        Assert.NotEqual(0, result.ExitCode);
        Assert.Contains("MK8ARCH001", result.Output, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ImportedConditionalReferencesHonorEvaluation(bool enabled)
    {
        var result = await CheckBuildPolicyAsync("Mk8.Sava.Gateway", "Mk8.Sava.INF", imported: true, enabled: enabled);

        if (enabled)
        {
            Assert.NotEqual(0, result.ExitCode);
            Assert.Contains("MK8ARCH002", result.Output, StringComparison.Ordinal);
        }
        else
            Assert.True(result.ExitCode == 0, result.Output);
    }

    [Fact]
    public async Task AForeignProjectCannotSpoofAnAllowedReferenceFileName()
    {
        var result = await CheckBuildPolicyAsync("Mk8.Sava.Gateway", "Mk8.Sava.API", foreign: true);

        Assert.NotEqual(0, result.ExitCode);
        Assert.Contains("MK8ARCH002", result.Output, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AProductionProjectCannotSpoofAnotherAssemblyIdentity()
    {
        var result = await CheckBuildPolicyAsync("Mk8.Sava.Gateway", "Mk8.Sava.API", assemblyName: "Mk8.Sava.Application");

        Assert.NotEqual(0, result.ExitCode);
        Assert.Contains("MK8ARCH003", result.Output, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ApprovedReferencesKeepBothHostsAndLowerLayersBuildable()
    {
        foreach (var (project, dependency) in new[]
        {
            ("Mk8.Sava.Gateway", "Mk8.Sava.API"), ("Mk8.Sava.API", "Mk8.Sava.Transport"),
            ("Mk8.Sava.Transport", "Mk8.Sava.Contracts"), ("Mk8.Sava.Application", "Mk8.Sava.BLL"),
            ("Mk8.Sava.Application", "Mk8.Sava.Transport"), ("Mk8.Sava.BLL", "Mk8.Sava.INF"),
            ("Mk8.Sava.INF", "Mk8.Sava.DAL"), ("Mk8.Sava.DAL", "Mk8.Sava.Contracts"),
        })
        {
            var result = await CheckBuildPolicyAsync(project, dependency);
            Assert.True(result.ExitCode == 0, result.Output);
        }
    }

    [Fact]
    public void GatewayCompiledClosureHasNoDurableStorageOrApplicationImplementation()
    {
        var closure = ReadCompiledClosure(typeof(Program).Assembly);

        var allowed = new HashSet<string>(StringComparer.Ordinal)
        {
            "Mk8.Sava.Gateway", "Mk8.Sava.API", "Mk8.Sava.Transport", "Mk8.Sava.Contracts",
        };
        Assert.All(closure, dependency => Assert.Contains(dependency, allowed));
        Assert.Contains("Mk8.Sava.API", closure);
        Assert.Contains("Mk8.Sava.Transport", closure);
    }

    [Fact]
    public void ApplicationCompiledClosureHasNoGatewayOrProtocolImplementation()
    {
        var closure = ReadCompiledClosure(typeof(Mk8.Sava.Application.ApplicationProgram).Assembly);

        var allowed = new HashSet<string>(StringComparer.Ordinal)
        {
            "Mk8.Sava.Application", "Mk8.Sava.BLL", "Mk8.Sava.DAL", "Mk8.Sava.INF", "Mk8.Sava.Contracts", "Mk8.Sava.Transport",
        };
        Assert.All(closure, dependency => Assert.Contains(dependency, allowed));
        Assert.Contains("Mk8.Sava.BLL", closure);
        Assert.Contains("Mk8.Sava.DAL", closure);
        Assert.Contains("Mk8.Sava.INF", closure);
    }

    [Theory]
    [InlineData("Mk8.Sava.Gateway")]
    [InlineData("Mk8.Sava.Application")]
    public void RuntimeDependencyManifestPreservesHostIsolation(string host)
    {
        var path = Path.Combine(AppContext.BaseDirectory, host + ".deps.json");
        using var manifest = JsonDocument.Parse(File.ReadAllText(path));
        var dependencies = manifest.RootElement.GetProperty("libraries").EnumerateObject()
            .Select(property => property.Name.Split('/')[0]).ToArray();

        Assert.Contains(host, dependencies, StringComparer.Ordinal);
        var forbidden = string.Equals(host, "Mk8.Sava.Gateway", StringComparison.Ordinal)
            ? new[] { "Mk8.Sava.Application", "Mk8.Sava.BLL", "Mk8.Sava.DAL", "Mk8.Sava.INF", "Microsoft.Data.Sqlite", "Microsoft.Data.Sqlite.Core", "SQLitePCLRaw.core" }
            : new[] { "Mk8.Sava.Gateway", "Mk8.Sava.API" };
        foreach (var name in forbidden)
            Assert.DoesNotContain(name, dependencies, StringComparer.Ordinal);
    }

    private static HashSet<string> ReadCompiledClosure(Assembly root)
    {
        var visited = new HashSet<string>(StringComparer.Ordinal);
        var pending = new Queue<Assembly>();
        pending.Enqueue(root);
        while (pending.TryDequeue(out var current))
        {
            if (!visited.Add(current.GetName().Name!))
                continue;
            foreach (var reference in current.GetReferencedAssemblies())
            {
                if (reference.Name!.StartsWith("Mk8.Sava.", StringComparison.Ordinal))
                    pending.Enqueue(Assembly.Load(reference));
            }
        }
        return visited;
    }

    private static async Task<(int ExitCode, string Output)> CheckBuildPolicyAsync(
        string project, string dependency, bool imported = false, bool foreign = false, bool enabled = true,
        string? assemblyName = null)
    {
        var directory = Directory.CreateTempSubdirectory("sava-architecture-");
        try
        {
            var root = FindRepositoryRoot();
            var referencePath = Path.Combine(foreign ? directory.FullName : root, dependency, dependency + ".csproj");
            var references = new XElement("ItemGroup", new XElement("ProjectReference", new XAttribute("Include", referencePath)));
            var document = new XDocument(new XElement("Project",
                new XElement("PropertyGroup", new XElement("AssemblyName", assemblyName ?? project), new XElement("EnableLeak", enabled ? "true" : "false")),
                new XElement("Import", new XAttribute("Project", Path.Combine(root, "Directory.Build.targets")))));
            if (imported)
            {
                references.SetAttributeValue("Condition", "'$(EnableLeak)' == 'true'");
                var importPath = Path.Combine(directory.FullName, "late.targets");
                await File.WriteAllTextAsync(importPath, new XDocument(new XElement("Project", references)).ToString()).ConfigureAwait(false);
                document.Root!.Add(new XElement("Import", new XAttribute("Project", importPath)));
            }
            else
                document.Root!.Add(references);
            var projectPath = Path.Combine(directory.FullName, project + ".proj");
            await File.WriteAllTextAsync(projectPath, document.ToString()).ConfigureAwait(false);
            return await RunBuildPolicyAsync(projectPath).ConfigureAwait(false);
        }
        finally
        {
            directory.Delete(recursive: true);
        }
    }

    private static async Task<(int ExitCode, string Output)> RunBuildPolicyAsync(string projectPath)
    {
        var start = new ProcessStartInfo(Environment.GetEnvironmentVariable("DOTNET_HOST_PATH") ?? "dotnet")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        foreach (var argument in new[] { "msbuild", projectPath, "-t:Mk8ValidateProjectBoundaries", "-nologo", "-v:quiet", "-nr:false" })
            start.ArgumentList.Add(argument);
        using var process = Process.Start(start) ?? throw new InvalidOperationException("Could not start MSBuild.");
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var stdout = process.StandardOutput.ReadToEndAsync();
        var stderr = process.StandardError.ReadToEndAsync();
        try
        {
            await process.WaitForExitAsync(timeout.Token).ConfigureAwait(false);
            return (process.ExitCode, await stdout.ConfigureAwait(false) + await stderr.ConfigureAwait(false));
        }
        finally
        {
            if (!process.HasExited)
            {
                process.Kill(entireProcessTree: true);
                await process.WaitForExitAsync(CancellationToken.None).ConfigureAwait(false);
            }
            await Task.WhenAll(stdout, stderr).ConfigureAwait(false);
        }
    }

    private static string FindRepositoryRoot()
    {
        for (var directory = new DirectoryInfo(AppContext.BaseDirectory); directory is not null; directory = directory.Parent)
        {
            if (File.Exists(Path.Combine(directory.FullName, "Mk8.Sava.slnx")))
                return directory.FullName;
        }
        throw new DirectoryNotFoundException("The architecture test needs its repository's solution and build policy.");
    }
}
