using System.ComponentModel;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.InteropServices;

namespace Mk8.Sava.Tests;

// Fixtures reuse the executable test assembly. Native imports are compiled with the normal analyzer gate,
// rather than compiling the same C# fragment in each PowerShell fixture process.
internal static partial class TestProcessFixtureProgram
{
    internal static ProcessStartInfo CreateStartInfo(string role, string directory, string inherited, int exitCode, string fault)
    {
        var start = new ProcessStartInfo("dotnet") { UseShellExecute = false };
        start.ArgumentList.Add(typeof(TestProcessFixtureProgram).Assembly.Location);
        start.ArgumentList.Add("--process-fixture");
        start.ArgumentList.Add(role);
        start.ArgumentList.Add(directory);
        start.ArgumentList.Add(inherited);
        start.ArgumentList.Add(exitCode.ToString(CultureInfo.InvariantCulture));
        start.ArgumentList.Add(fault);
        return start;
    }

    private static async Task<int> Main(string[] arguments)
    {
        if (arguments.Length > 0 && arguments[0] is "--split-service-closed-fixture")
            return await SplitServiceClosedFixtureProgram.RunAsync(arguments).ConfigureAwait(false);

        if (arguments.Length != 6 || arguments[0] is not "--process-fixture" ||
            arguments[1] is not ("root" or "descendant") ||
            !Path.IsPathFullyQualified(arguments[2]) || !Directory.Exists(arguments[2]) ||
            arguments[3] is not ("stdout" or "stderr" or "both" or "neither" or "ordinary" or "closed" or "flood") ||
            !int.TryParse(arguments[4], NumberStyles.Integer, CultureInfo.InvariantCulture, out var exitCode) ||
            arguments[5] is not ("None" or "WithheldDescendantReadiness" or "ExitedDescendantBeforeReadiness" or
                "RejectedNativePreparation" or "ExitedDescendantAfterPreparation"))
        {
            await Console.Error.WriteLineAsync("Invalid controlled process-fixture invocation.").ConfigureAwait(false);
            return 64;
        }

        var role = arguments[1];
        var directory = arguments[2];
        var inherited = arguments[3];
        var fault = arguments[5];
        if (role is "descendant")
        {
            await MarkAsync(directory, "descendant-starting").ConfigureAwait(false);
            if (fault is "ExitedDescendantBeforeReadiness")
            {
                await WriteErrorAsync("injected-descendant-startup-error").ConfigureAwait(false);
                return 97;
            }
        }

        if (!await PrepareNativeAsync(role, directory, inherited, fault).ConfigureAwait(false))
            return 96;
        return role is "descendant"
            ? await RunDescendantAsync(directory, inherited, fault).ConfigureAwait(false)
            : await RunRootAsync(directory, inherited, exitCode, fault).ConfigureAwait(false);
    }

    private static async Task<bool> PrepareNativeAsync(string role, string directory, string inherited, string fault)
    {
        var needsNativeHandles = inherited is "closed" || (role is "descendant" && inherited is not "both");
        if (needsNativeHandles)
        {
            await MarkAsync(directory, role + "-native-type-started").ConfigureAwait(false);
            if (fault is "RejectedNativePreparation")
            {
                await WriteErrorAsync("injected-native-preparation-error").ConfigureAwait(false);
                return false;
            }
            // Historical marker names are retained; this is compiled import availability, not runtime C# compilation.
            await MarkAsync(directory, role + "-native-type-completed").ConfigureAwait(false);
        }
        else
        {
            await MarkAsync(directory, role + "-native-type-skipped").ConfigureAwait(false);
        }
        return true;
    }

    private static async Task<int> RunDescendantAsync(string directory, string inherited, string fault)
    {
        if (inherited is not ("stdout" or "both"))
            CloseStandardHandle(-11);
        if (inherited is not ("stderr" or "both"))
            CloseStandardHandle(-12);
        await MarkAsync(directory, "descendant-pipes-prepared").ConfigureAwait(false);
        if (fault is "ExitedDescendantAfterPreparation")
        {
            await WriteErrorAsync("injected-after-preparation-error").ConfigureAwait(false);
            return 98;
        }
        if (fault is not "WithheldDescendantReadiness")
            await MarkAsync(directory, "descendant-ready").ConfigureAwait(false);
        return await WaitForReleaseAsync(directory, "release-descendant", 91).ConfigureAwait(false);
    }

    private static async Task<int> RunRootAsync(string directory, string inherited, int exitCode, string fault)
    {
        await Console.Out.WriteAsync("root-output-without-newline").ConfigureAwait(false);
        await Console.Out.FlushAsync().ConfigureAwait(false);
        await WriteErrorAsync("root-error-without-newline").ConfigureAwait(false);
        if (inherited is "stdout" or "stderr" or "both" or "neither")
        {
            using var child = Process.Start(CreateStartInfo("descendant", directory, inherited, 0, fault))
                ?? throw new InvalidOperationException("The controlled descendant did not start.");
            await File.WriteAllTextAsync(Path.Combine(directory, "descendant-pid"),
                child.Id.ToString(CultureInfo.InvariantCulture)).ConfigureAwait(false);
            if (fault is "WithheldDescendantReadiness")
                await MarkAsync(directory, "ready").ConfigureAwait(false);
            for (var attempt = 0; attempt < 400; attempt++)
            {
                if (File.Exists(Path.Combine(directory, "descendant-ready")))
                    break;
                if (child.HasExited)
                {
                    await WriteErrorAsync($"Descendant fixture exited before readiness ({child.ExitCode}).").ConfigureAwait(false);
                    return 93;
                }
                await Task.Delay(50).ConfigureAwait(false);
            }
            if (!File.Exists(Path.Combine(directory, "descendant-ready")))
            {
                await WriteErrorAsync("Descendant fixture never became ready.").ConfigureAwait(false);
                return 94;
            }
        }
        if (inherited is "closed")
        {
            CloseStandardHandle(-11);
            CloseStandardHandle(-12);
        }
        await MarkAsync(directory, "root-pipes-prepared").ConfigureAwait(false);
        await MarkAsync(directory, "ready").ConfigureAwait(false);
        var result = await WaitForReleaseAsync(directory, "release-root", 92).ConfigureAwait(false);
        if (result != 0)
            return result;
        if (inherited is "flood")
        {
            await Console.Out.WriteAsync(new string('o', 262144)).ConfigureAwait(false);
            await Console.Out.FlushAsync().ConfigureAwait(false);
            await WriteErrorAsync(new string('e', 262144)).ConfigureAwait(false);
        }
        return exitCode;
    }

    private static Task MarkAsync(string directory, string marker)
        => File.WriteAllTextAsync(Path.Combine(directory, marker), string.Empty);

    private static async Task WriteErrorAsync(string text)
    {
        await Console.Error.WriteAsync(text).ConfigureAwait(false);
        await Console.Error.FlushAsync().ConfigureAwait(false);
    }

    private static async Task<int> WaitForReleaseAsync(string directory, string marker, int timeoutExitCode)
    {
        for (var attempt = 0; attempt < 1200; attempt++)
        {
            if (File.Exists(Path.Combine(directory, marker)))
                return 0;
            await Task.Delay(50).ConfigureAwait(false);
        }
        return timeoutExitCode;
    }

    internal static void CloseStandardHandle(int kind)
    {
        if (!OperatingSystem.IsWindows())
            throw new PlatformNotSupportedException("Actual standard-handle closure is a Windows-only fixture operation.");
        var handle = GetStdHandle(kind);
        if (handle == 0 || handle == -1 || !CloseHandle(handle))
            throw new Win32Exception(Marshal.GetLastPInvokeError(), "The controlled standard handle could not be closed.");
    }

    [LibraryImport("kernel32.dll", SetLastError = true)]
    [DefaultDllImportSearchPaths(DllImportSearchPath.System32)]
    private static partial nint GetStdHandle(int kind);

    [LibraryImport("kernel32.dll", SetLastError = true)]
    [DefaultDllImportSearchPaths(DllImportSearchPath.System32)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static partial bool CloseHandle(nint handle);
}
