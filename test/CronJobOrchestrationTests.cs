using aws_backup;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;

namespace test;

internal class FakeClock : IClock
{
    public DateTimeOffset UtcNow { get; set; }
}

internal class StubScheduler : ICronScheduler
{
    private readonly Queue<Func<DateTimeOffset?>> _times = new();

    public DateTimeOffset? GetNext(DateTimeOffset afterUtc)
    {
        if (_times.Count == 0) return null;
        return _times.Dequeue()();
    }

    public void Enqueue(Func<DateTimeOffset?> next)
    {
        _times.Enqueue(next);
    }
}

public class CronJobOrchestrationTests
{
    [Fact]
    public async Task ExecutesJob_OnSchedule()
    {
        // Arrange
        var runTimes = new List<DateTimeOffset>();

        var cfgMon = new Mock<IOptionsMonitor<Configuration>>();
        cfgMon
            .Setup(m => m.CurrentValue)
            .Returns(new Configuration { CronSchedule = "irrelevant" });

        var clock = new FakeClock { UtcNow = new DateTimeOffset(2025, 6, 25, 10, 0, 0, TimeSpan.Zero) };
        var scheduler = new StubScheduler();
        var resolverMock = new Mock<IContextResolver>();
        resolverMock
            .Setup(r => r.ArchiveRunId(It.IsAny<DateTimeOffset>()))
            .Callback((DateTimeOffset time) =>
            {
                runTimes.Add(time);
                clock.UtcNow = time.AddSeconds(10);
            })
            .Returns("resolved-context");

        var mediatorMock = new Mock<IMediator>();
        mediatorMock
            .Setup(m => m.ScheduleRunRequest(It.IsAny<RunRequest>(), It.IsAny<CancellationToken>()))
            .Returns(ValueTask.CompletedTask);

        var mockFactory = new Mock<ICronSchedulerFactory>();
        mockFactory
            .Setup(f => f.Create(It.IsAny<string>()))
            .Returns((string _) => scheduler);

        var now = clock.UtcNow;
        // job just records invocation times

        // next occurrences at +10s, +20s, then stop
        scheduler.Enqueue(() => now.AddSeconds(11));
        scheduler.Enqueue(() => now.AddSeconds(21));
        scheduler.Enqueue(() => null);

        var orchestrator = new CronJobOrchestration(
            cfgMon.Object,
            mediatorMock.Object,
            resolverMock.Object,
            clock,
            mockFactory.Object,
            NullLogger<CronJobOrchestration>.Instance);

        // Act: run in background
        var cts = new CancellationTokenSource();
        clock.UtcNow = clock.UtcNow.AddSeconds(10);
        var task = orchestrator.StartAsync(cts.Token);

        // Advance clock to trigger first run
        await Task.Delay(100);

        // Stop
        await task;
        await orchestrator.ExecuteTask;

        // Assert
        mediatorMock.Verify(m => m.ScheduleRunRequest(It.IsAny<RunRequest>(), It.IsAny<CancellationToken>()),
            Times.Exactly(2));

        Assert.Equal(2, runTimes.Count);
        Assert.Equal(new DateTimeOffset(2025, 6, 25, 10, 0, 10, TimeSpan.Zero), runTimes[0]);
        Assert.Equal(new DateTimeOffset(2025, 6, 25, 10, 0, 20, TimeSpan.Zero), runTimes[1]);
    }

    [Fact]
    public async Task CancelsPendingDelay_WhenCronScheduleChanges()
    {
        // Arrange
        var runCount = 0;

        var cfgMon = new Mock<IOptionsMonitor<Configuration>>();
        var listeners = new List<Action<Configuration, string>>();
        cfgMon.Setup(m => m.CurrentValue).Returns(new Configuration { CronSchedule = "* * * * *" });
        cfgMon
            .Setup(m => m.OnChange(It.IsAny<Action<Configuration, string>>()))
            .Callback<Action<Configuration, string>>(h => listeners.Add((cfg, h2) => h(cfg, h2)));

        var resolverMock = new Mock<IContextResolver>();
        resolverMock
            .Setup(r => r.ArchiveRunId(It.IsAny<DateTimeOffset>()))
            .Callback((DateTimeOffset time) => runCount++)
            .Returns("resolved-context");

        var mediatorMock = new Mock<IMediator>();
        mediatorMock
            .Setup(m => m.ScheduleRunRequest(It.IsAny<RunRequest>(), It.IsAny<CancellationToken>()))
            .Returns(ValueTask.CompletedTask);

        var now = DateTimeOffset.UtcNow;
        var clock = new FakeClock { UtcNow = now };
        var sched = new StubScheduler();
        var mockFactory = new Mock<ICronSchedulerFactory>();
        mockFactory
            .Setup(f => f.Create(It.IsAny<string>()))
            .Returns((string _) => sched);
        // first schedule far in the future so Delay would be long
        sched.Enqueue(() => clock.UtcNow.AddMinutes(10));
        // after change, schedule soon
        sched.Enqueue(() => clock.UtcNow.AddSeconds(1));
        sched.Enqueue(() => null);

        var cts = new CancellationTokenSource();
        Func<CancellationToken, Task> job = ct =>
        {
            runCount++;
            return Task.CompletedTask;
        };

        var orchestrator = new CronJobOrchestration(
            cfgMon.Object,
            mediatorMock.Object,
            resolverMock.Object,
            clock,
            mockFactory.Object,
            NullLogger<CronJobOrchestration>.Instance);
        
        // Act
        var exec = orchestrator.StartAsync(cts.Token);

        // Immediately "change" the config
        await Task.Delay(100);
        listeners[0].Invoke(new Configuration { CronSchedule = "*/1 * * * *" }, "");

        // Stop
        await exec;
        await orchestrator.ExecuteTask;

        // Assert that job ran exactly once under the new schedule
        mediatorMock.Verify(m => m.ScheduleRunRequest(It.IsAny<RunRequest>(), It.IsAny<CancellationToken>()),
            Times.Once);
        
        Assert.Equal(1, runCount);
    }

    [Fact]
    public async Task StopsWhenNoFutureOccurrences()
    {
        // Arrange
        var cfgMon = new Mock<IOptionsMonitor<Configuration>>();
        cfgMon.Setup(m => m.CurrentValue).Returns(new Configuration { CronSchedule = "irrelevant" });
        var clock = new FakeClock { UtcNow = DateTimeOffset.UtcNow };
        var sched = new StubScheduler();
        sched.Enqueue(() => null); // will never occur
        var mockFactory = new Mock<ICronSchedulerFactory>();
        mockFactory
            .Setup(f => f.Create(It.IsAny<string>()))
            .Returns((string _) => sched);

        var resolverMock = new Mock<IContextResolver>();
        resolverMock
            .Setup(r => r.ArchiveRunId(It.IsAny<DateTimeOffset>()))
            .Returns("resolved-context");

        var mediatorMock = new Mock<IMediator>();
        mediatorMock
            .Setup(m => m.ScheduleRunRequest(It.IsAny<RunRequest>(), It.IsAny<CancellationToken>()))
            .Returns(ValueTask.CompletedTask);
        
        var jobCalled = false;
        Func<CancellationToken, Task> job = ct =>
        {
            jobCalled = true;
            return Task.CompletedTask;
        };

        var orchestrator = new CronJobOrchestration(
            cfgMon.Object,
            mediatorMock.Object,
            resolverMock.Object,
            clock,
            mockFactory.Object,
            NullLogger<CronJobOrchestration>.Instance);
        
        // Act
        await orchestrator.StartAsync(CancellationToken.None);
        await orchestrator.ExecuteTask;

        // Assert
        Assert.False(jobCalled, "Job must never be called if no future occurrences");
    }
}