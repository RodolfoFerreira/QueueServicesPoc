using QueueServicesPoc.Data;
using QueueServicesPoc.Extensions;
using QueueServicesPoc.Interfaces;
using System.Threading.Channels;

namespace QueueServicesPoc.Implementation
{
    public partial class BackgroundQueuedProcessor : BackgroundService, IFunctionQueuedProcessor
    {
        private readonly Channel<FunctionWithKey> _internalQueue = Channel.CreateUnbounded<FunctionWithKey>(new UnboundedChannelOptions { SingleReader = true });

        private readonly Dictionary<string, KeySpecificQueuedProcessor> _dataProcessors = new();

        private readonly SemaphoreSlim _processorsLock = new(1, 1);        

        private readonly ILoggerFactory _loggerFactory;

        private readonly ILogger<BackgroundQueuedProcessor> _logger;

        public BackgroundQueuedProcessor(ILoggerFactory loggerFactory)
        {
            _loggerFactory = loggerFactory;
            _logger = _loggerFactory.CreateLogger<BackgroundQueuedProcessor>();
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await foreach (var function in _internalQueue.Reader.ReadAllAsync(stoppingToken))
            {
                if (!await _processorsLock.WaitWithCancellation(stoppingToken))
                {
                    break;
                }

                var processor = GetOrCreateQueuedProcessor(function.Key, stoppingToken);
                await processor.ScheduleProcessing(function);

                _processorsLock.Release();
                _logger.LogInformation("Scheduled new function '{Function}' for processor with key '{Key}'", function, function.Key);
            }
        }

        private KeySpecificQueuedProcessor GetOrCreateQueuedProcessor(string key, CancellationToken newProcessorCancellationToken = default)
        {
            if (!_dataProcessors.TryGetValue(key, out var deviceProcessor))
            {
                var processor = CreateNewProcessor(key, newProcessorCancellationToken);
                _dataProcessors[key] = processor;
                deviceProcessor = processor;
                _logger.LogInformation("Created new processor for function key: {Key}", key);
            }

            return deviceProcessor;
        }

        private KeySpecificQueuedProcessor CreateNewProcessor(string key, CancellationToken processorCancellationToken = default)
        {
            var logger = _loggerFactory.CreateLogger($"{typeof(KeySpecificQueuedProcessor).FullName}-{key}");
            return KeySpecificQueuedProcessor.CreateAndStartProcessing(key, logger, processorCancellationToken);
        }

        public async Task ScheduleProcessing(FunctionWithKey functionWithKey) => await _internalQueue.Writer.WriteAsync(functionWithKey);
    }
}
