using System.Diagnostics;

namespace Mk8.Sava.Tests;

// Only the Windows closed-pipe service fixture needs native imports. Other service
// modes keep their existing shell protocols; this command never compiles at runtime.
internal static class SplitServiceClosedFixtureProgram
{
    internal static ProcessStartInfo CreateStartInfo(string directory, string fault = "None")
    {
        var start = new ProcessStartInfo("dotnet")
        {
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        start.ArgumentList.Add(typeof(SplitServiceClosedFixtureProgram).Assembly.Location);
        start.ArgumentList.Add("--split-service-closed-fixture");
        start.ArgumentList.Add(directory);
        start.ArgumentList.Add(fault);
        return start;
    }

    internal static async Task<int> RunAsync(string[] arguments)
    {
        if (arguments.Length != 3 || arguments[0] is not "--split-service-closed-fixture" ||
            !Path.IsPathFullyQualified(arguments[1]) || !Directory.Exists(arguments[1]) ||
            arguments[2] is not ("None" or "RejectedPreparation"))
        {
            await Console.Error.WriteLineAsync("Invalid controlled closed-service invocation.").ConfigureAwait(false);
            return 64;
        }
        var directory = arguments[1];
        var rejectPreparation = arguments[2] is "RejectedPreparation";
        if (!OperatingSystem.IsWindows() && !rejectPreparation)
        {
            await Console.Error.WriteLineAsync("Actual closed-service handle preparation requires Windows.").ConfigureAwait(false);
            return 65;
        }

        await MarkAsync(directory, "producer-started").ConfigureAwait(false);
        await Console.Out.WriteAsync("root-output-without-newline").ConfigureAwait(false);
        await Console.Out.FlushAsync().ConfigureAwait(false);
        await Console.Error.WriteAsync("root-error-without-newline").ConfigureAwait(false);
        await Console.Error.FlushAsync().ConfigureAwait(false);
        await MarkAsync(directory, "pipe-preparation-started").ConfigureAwait(false);
        if (rejectPreparation)
        {
            // Portable failure injection is BEFORE any native lookup/closure, not simulated EOF.
            await Console.Error.WriteLineAsync("injected-closed-service-preparation-error").ConfigureAwait(false);
            return 96;
        }
        TestProcessFixtureProgram.CloseStandardHandle(-11);
        TestProcessFixtureProgram.CloseStandardHandle(-12);
        // No console access follows closure. Completion/ready markers cannot precede
        // both checked native returns; they do not prove atomic capture or root exit.
        await MarkAsync(directory, "pipe-preparation-completed").ConfigureAwait(false);
        await MarkAsync(directory, "ready-publish-started").ConfigureAwait(false);
        await MarkAsync(directory, "ready").ConfigureAwait(false);
        await MarkAsync(directory, "ready-publish-completed").ConfigureAwait(false);
        for (var attempt = 0; attempt < 1200; attempt++)
        {
            if (File.Exists(Path.Combine(directory, "release-root")))
                return 23;
            await Task.Delay(50).ConfigureAwait(false);
        }
        return 92;
    }

    private static Task MarkAsync(string directory, string marker)
        => File.WriteAllTextAsync(Path.Combine(directory, marker), string.Empty);
}
