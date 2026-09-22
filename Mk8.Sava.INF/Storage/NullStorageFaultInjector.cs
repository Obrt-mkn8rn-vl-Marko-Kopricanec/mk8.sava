namespace Mk8.Sava.Storage;

public sealed class NullStorageFaultInjector : IStorageFaultInjector
{
    public void Inject(StorageFaultPoint point)
    {
    }
}
