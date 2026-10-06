namespace Mk8.Sava.Application;

public interface IApplicationReadiness
{
    Task<ApplicationReadiness> GetAsync(CancellationToken cancellationToken);
    Task<string> RenderStorageMetricsAsync(CancellationToken cancellationToken);
}
