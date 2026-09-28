namespace Mk8.Sava.Protocol;

internal static class CatastrophicExceptionPolicy
{
    internal static bool Contains(Exception exception)
    {
        if (exception is OutOfMemoryException or AccessViolationException)
            return true;

        var remaining = new Stack<Exception>();
        var visited = new HashSet<Exception>(ReferenceEqualityComparer.Instance);
        remaining.Push(exception);
        while (remaining.TryPop(out var current))
        {
            if (!visited.Add(current))
                continue;
            if (current is OutOfMemoryException or AccessViolationException)
                return true;
            if (current is AggregateException aggregate)
            {
                foreach (var inner in aggregate.InnerExceptions)
                    remaining.Push(inner);
            }
            else if (current.InnerException is { } inner)
            {
                remaining.Push(inner);
            }
        }

        return false;
    }
}
