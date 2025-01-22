namespace QueueServicesPoc.Interfaces
{
    public interface IDependency
    {
        string GetGuid();
        void SetKey(string key);
        string GetKey();
    }
}
