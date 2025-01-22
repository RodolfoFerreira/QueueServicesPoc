using QueueServicesPoc.Extensions;

namespace QueueServicesPoc.Implementation
{
    public class BackgroundQueuedProcessorMonitor
    {
        readonly TimeSpan _processorExpiryThreshold = TimeSpan.FromSeconds(30);

        private readonly TimeSpan _processorExpiryScanningPeriod = TimeSpan.FromSeconds(5);

        private MonitoringTask? _monitoringTask;

        private readonly SemaphoreSlim _processorsLock;

        private readonly Dictionary<string, KeySpecificQueuedProcessor> _dataProcessors;

        private readonly ILogger<BackgroundQueuedProcessorMonitor> _logger;

        private BackgroundQueuedProcessorMonitor(SemaphoreSlim processorsLock, Dictionary<string, KeySpecificQueuedProcessor> dataProcessors, ILogger<BackgroundQueuedProcessorMonitor> logger)
        {
            _processorsLock = processorsLock;
            _dataProcessors = dataProcessors;
            _logger = logger;
        }

        private void StartMonitoring(CancellationToken cancellationToken = default)
        {
            var tokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            var task = Task.Factory.StartNew(async () =>
            {
                using var timer = new PeriodicTimer(_processorExpiryScanningPeriod);
                while (!tokenSource.IsCancellationRequested && await timer.WaitForNextTickAsync(tokenSource.Token))
                {
                    if (!await _processorsLock.WaitWithCancellation(tokenSource.Token))
                    {
                        continue;
                    }

                    var expiredProcessors = _dataProcessors.Values.Where(IsExpired).ToArray();
                    foreach (var expiredProcessor in expiredProcessors)
                    {
                        await expiredProcessor.StopProcessing();
                        _dataProcessors.Remove(expiredProcessor.ProcessorKey);

                        _logger.LogInformation("Removed data processor for data key {Key}", expiredProcessor.ProcessorKey);
                    }

                    _processorsLock.Release();
                }
            }, tokenSource.Token, TaskCreationOptions.LongRunning, TaskScheduler.Default);
            _monitoringTask = new MonitoringTask(task, tokenSource);
        }

        private bool IsExpired(KeySpecificQueuedProcessor processorInfo) => (DateTime.UtcNow - processorInfo.LastProcessingTimestamp) > _processorExpiryThreshold;

        public async Task StopMonitoring()
        {
            if (_monitoringTask.HasValue)
            {
                if (!_monitoringTask.Value.CancellationTokenSource.IsCancellationRequested)
                {
                    _monitoringTask.Value.CancellationTokenSource.Cancel();
                }

                await _monitoringTask.Value.Task;
                _monitoringTask.Value.CancellationTokenSource.Dispose();
                _monitoringTask = null;
            }
        }

        public static BackgroundQueuedProcessorMonitor CreateAndStartMonitoring(SemaphoreSlim processorsLock, Dictionary<string, KeySpecificQueuedProcessor> dataProcessors, ILogger<BackgroundQueuedProcessorMonitor> logger, CancellationToken monitoringCancellationToken = default)
        {
            var monitor = new BackgroundQueuedProcessorMonitor(processorsLock, dataProcessors, logger);
            monitor.StartMonitoring(monitoringCancellationToken);
            return monitor;
        }

        private readonly record struct MonitoringTask(Task Task, CancellationTokenSource CancellationTokenSource);
    }
}
