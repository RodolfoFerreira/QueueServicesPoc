using QueueServicesPoc.Data;

namespace QueueServicesPoc.Interfaces
{
    public interface IFunctionQueuedProcessor
    {
        Task ScheduleProcessing(FunctionWithKey functionWithKey);
    }
}
