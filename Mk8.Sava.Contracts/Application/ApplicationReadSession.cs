using Mk8.Sava.Storage;

namespace Mk8.Sava.Application;

public sealed record ApplicationReadSession(string Token, BlobRecord Record, TimeSpan IdleTimeout);
