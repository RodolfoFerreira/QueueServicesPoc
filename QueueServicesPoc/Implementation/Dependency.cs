using QueueServicesPoc.Interfaces;

namespace QueueServicesPoc.Implementation
{
    public class Dependency : IDependency
    {
        public string Guid { get; set; }

        public string Key { get; set; }

        public Dependency()
        {
            Guid = System.Guid.NewGuid().ToString();
            Key = string.Empty;
        }

        public string GetKey() => Key;

        public void SetKey(string key) => Key = key;

        public string GetGuid() => Guid;

    }
}
