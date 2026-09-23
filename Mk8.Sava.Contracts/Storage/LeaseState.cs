using System.Text.Json.Serialization;

namespace Mk8.Sava.Storage;

[JsonConverter(typeof(JsonStringEnumConverter<LeaseState>))]
public enum LeaseState
{
    Available,
    Leased,
    Expired,
    Breaking,
    Broken
}
