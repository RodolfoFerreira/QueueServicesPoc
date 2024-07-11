namespace QueueServicesPoc.Data
{
    public record FunctionWithKey(string Key, Func<CancellationToken, Task> Function);
}
