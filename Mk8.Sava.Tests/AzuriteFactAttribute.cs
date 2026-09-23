namespace Mk8.Sava.Tests;

internal sealed class AzuriteFactAttribute : FactAttribute
{
    public const string ConnectionStringVariable = "MK8_SAVA_AZURITE_CONNECTION_STRING";

    public AzuriteFactAttribute()
    {
        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(ConnectionStringVariable)))
            Skip = $"Set {ConnectionStringVariable} to a disposable Azurite Blob endpoint.";
    }
}
