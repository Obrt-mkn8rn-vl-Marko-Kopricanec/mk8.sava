namespace Mk8.Sava.Storage;

public interface IStorageFaultInjector
{
    void Inject(StorageFaultPoint point);
}
