namespace Mk8.Sava.Tests;

internal sealed class LiveAzureFactAttribute : FactAttribute
{
    public const string DefaultConnectionStringVariable = "MK8_SAVA_LIVE_AZURE_BLOB_CONNECTION_STRING";
    public const string HnsConnectionStringVariable = "MK8_SAVA_LIVE_AZURE_HNS_CONNECTION_STRING";

    public LiveAzureFactAttribute(string connectionStringVariable = DefaultConnectionStringVariable)
    {
        ConnectionStringVariable = connectionStringVariable;
        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(connectionStringVariable)))
            Skip = $"Set {connectionStringVariable} to a disposable Azure test account.";
    }

    public string ConnectionStringVariable { get; }
}
