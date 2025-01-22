using QueueServicesPoc.Data;
using QueueServicesPoc.Interfaces;
using System.Threading.Channels;

namespace QueueServicesPoc.Implementation
{
    public class KeySpecificQueuedProcessor : IFunctionQueuedProcessor
    {
        public string ProcessorKey { get; }

        public DateTime LastProcessingTimestamp => _processingFinishedTimestamp ?? DateTime.UtcNow;

        private DateTime? _processingFinishedTimestamp = DateTime.UtcNow;

        private bool Processing
        {
            set
            {
                if (!value)
                {
                    _processingFinishedTimestamp = DateTime.UtcNow;
                }
                else
                {
                    _processingFinishedTimestamp = null;
                }
            }
        }

        private readonly Channel<FunctionWithKey> _internalQueue = Channel.CreateUnbounded<FunctionWithKey>(new UnboundedChannelOptions { SingleReader = true, SingleWriter = true });

        private readonly ILogger _logger;

        private Task? _processingTask;

        private KeySpecificQueuedProcessor(string processorKey, ILogger logger)
        {
            ProcessorKey = processorKey;
            _logger = logger;
        }

        private void StartProcessing(CancellationToken cancellationToken = default)
        {
            _processingTask = Task.Factory.StartNew(
                async () =>
                {
                    await foreach (var function in _internalQueue.Reader.ReadAllAsync(cancellationToken))
                    {
                        Processing = true;
                        _logger.LogInformation("Received function: {Key}", function.Key);
                        
                        await function.Function(cancellationToken);

                        Processing = _internalQueue.Reader.TryPeek(out _);
                    }
                }, cancellationToken, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        }

        public async Task ScheduleProcessing(FunctionWithKey functionWithKey)
        {
            if (functionWithKey.Key != ProcessorKey)
            {
                throw new InvalidOperationException($"Function with key {functionWithKey.Key} scheduled for KeySpecificProcessor with key {ProcessorKey}");
            }

            Processing = true;
            await _internalQueue.Writer.WriteAsync(functionWithKey);
        }

        public async Task StopProcessing()
        {
            _internalQueue.Writer.Complete();
            if (_processingTask != null)
            {
                await _processingTask;
            }
        }

        public static KeySpecificQueuedProcessor CreateAndStartProcessing(string processorKey, ILogger logger, CancellationToken processingCancellationToken = default)
        {
            var instance = new KeySpecificQueuedProcessor(processorKey, logger);
            instance.StartProcessing(processingCancellationToken);
            return instance;
        }
    }
}
