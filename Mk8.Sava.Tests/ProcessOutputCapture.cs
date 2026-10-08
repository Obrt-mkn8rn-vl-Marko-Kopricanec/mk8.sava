using System.Text;

namespace Mk8.Sava.Tests;

// One reader owns each capture. Snapshots may be observed independently by diagnostics.
internal sealed class ProcessOutputCapture
{
    private readonly Lock gate = new();
    private readonly StringBuilder text = new();
    private bool reachedEof;

    internal string Text { get { lock (gate) return text.ToString(); } }
    internal bool ReachedEof { get { lock (gate) return reachedEof; } }

    internal async Task ReadAsync(StreamReader reader, Action<string>? onLine, CancellationToken cancellationToken)
    {
        // A larger character request can consume a full byte buffer and then wait for more
        // multibyte text. Commit each returned character before another byte fill can fail.
        var buffer = new char[1];
        var line = new StringBuilder();
        while (true)
        {
            var count = await reader.ReadAsync(buffer.AsMemory(), cancellationToken).ConfigureAwait(false);
            lock (gate)
            {
                text.Append(buffer, 0, count);
                reachedEof = count == 0;
            }
            if (count == 0)
            {
                if (line.Length > 0)
                    onLine?.Invoke(line.ToString());
                return;
            }
            if (onLine is not null)
                PublishCompleteLines(buffer.AsSpan(0, count), line, onLine);
        }
    }

    private static void PublishCompleteLines(ReadOnlySpan<char> buffer, StringBuilder line, Action<string> onLine)
    {
        foreach (var character in buffer)
        {
            if (character == '\n')
            {
                onLine(line.ToString());
                line.Clear();
            }
            else
            {
                line.Append(character);
            }
        }
    }
}
