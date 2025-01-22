using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using QueueServicesPoc.Implementation;
using QueueServicesPoc.Interfaces;
using System.Net.Http;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
builder.Services.AddSingleton<IFunctionQueuedProcessor, BackgroundQueuedProcessor>();
builder.Services.AddHostedService(provider => (provider.GetRequiredService<IFunctionQueuedProcessor>() as BackgroundQueuedProcessor)!);
builder.Services.AddScoped<IDependency, Dependency>();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

app.MapGet("/processar", async ([FromServices] IFunctionQueuedProcessor processor,
                                [FromServices] ILogger<Program> logger,
                                [FromServices] IDependency dependency,
                                [FromQuery] string key) =>
{
    var taskCompletionSource = new TaskCompletionSource<bool>();

    await processor.ScheduleProcessing(new QueueServicesPoc.Data.FunctionWithKey(key, async (CancellationToken token) =>
    {
        int delayLoop = 0;
        var guid = Guid.NewGuid();

        dependency.SetKey(key);

        logger.LogInformation("Queued work item {Guid} is starting.", guid);
        logger.LogInformation("Dependency used is {Guid}.", dependency.GetGuid());
        logger.LogInformation("Dependency used has key {Key}.", dependency.GetKey());

        while (!token.IsCancellationRequested && delayLoop < 3)
        {
            try
            {
                await Task.Delay(TimeSpan.FromSeconds(5), token);
            }
            catch (OperationCanceledException)
            {
                // Prevent throwing if the Delay is cancelled
            }
            ++delayLoop;

            logger.LogInformation("Queued work item {Guid} is running. {DelayLoop}/3", guid, delayLoop);
        }

        if (delayLoop is 3)
        {
            logger.LogInformation("Queued Background Task {Guid} is complete.", guid);
        }
        else
        {
            logger.LogInformation("Queued Background Task {Guid} was cancelled.", guid);
        }

        taskCompletionSource.SetResult(true);
    }));

    var result = await taskCompletionSource.Task;

    logger.LogInformation("Result is {result}", result);
})
.WithName("Processar")
.WithOpenApi();

app.Run();