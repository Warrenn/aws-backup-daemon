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
        var cfgMon = new Mock<IOptionsMonitor<Configuration>>();
        cfgMon.Setup(m => m.CurrentValue).Returns(new Configuration { CronSchedule = "irrelevant" });
        var clock = new FakeClock { UtcNow = new DateTimeOffset(2025, 6, 25, 10, 0, 0, TimeSpan.Zero) };
        var sched = new StubScheduler();
        var mockFactory = new Mock<ICronSchedulerFactory>();
        mockFactory
            .Setup(f => f.Create(It.IsAny<string>()))
            .Returns((string _) => sched);
        var runTimes = new List<DateTimeOffset>();
        var now = clock.UtcNow;
        // job just records invocation times
        Func<CancellationToken, Task> job = ct =>
        {
            runTimes.Add(clock.UtcNow);
            clock.UtcNow = clock.UtcNow.AddSeconds(10);
            return Task.CompletedTask;
        };

        // next occurrences at +10s, +20s, then stop
        sched.Enqueue(() => now.AddSeconds(11));
        sched.Enqueue(() => now.AddSeconds(21));
        sched.Enqueue(() => null);

        var orchestrator = new CronJobOrchestration(
            cfgMon.Object, job,
            clock, mockFactory.Object, NullLogger<CronJobOrchestration>.Instance);

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
        Assert.Equal(2, runTimes.Count);
        Assert.Equal(new DateTimeOffset(2025, 6, 25, 10, 0, 10, TimeSpan.Zero), runTimes[0]);
        Assert.Equal(new DateTimeOffset(2025, 6, 25, 10, 0, 20, TimeSpan.Zero), runTimes[1]);
    }

    [Fact]
    public async Task CancelsPendingDelay_WhenCronScheduleChanges()
    {
        // Arrange
        var cfgMon = new Mock<IOptionsMonitor<Configuration>>();
        var listeners = new List<Action<Configuration, string>>();
        cfgMon.Setup(m => m.CurrentValue).Returns(new Configuration { CronSchedule = "* * * * *" });
        cfgMon
            .Setup(m => m.OnChange(It.IsAny<Action<Configuration, string>>()))
            .Callback<Action<Configuration, string>>(h => listeners.Add((cfg, h2) => h(cfg, h2)));

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

        var runCount = 0;
        var cts = new CancellationTokenSource();
        Func<CancellationToken, Task> job = ct =>
        {
            runCount++;
            return Task.CompletedTask;
        };

        var orchestrator = new CronJobOrchestration(
            cfgMon.Object, job,
            clock, mockFactory.Object, NullLogger<CronJobOrchestration>.Instance);

        // Act
        var exec = orchestrator.StartAsync(cts.Token);

        // Immediately "change" the config
        await Task.Delay(100);
        listeners[0].Invoke(new Configuration { CronSchedule = "*/1 * * * *" }, "");

        // Stop
        await exec;
        await orchestrator.ExecuteTask;

        // Assert that job ran exactly once under the new schedule
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

        var jobCalled = false;
        Func<CancellationToken, Task> job = ct =>
        {
            jobCalled = true;
            return Task.CompletedTask;
        };

        var orchestrator = new CronJobOrchestration(
            cfgMon.Object, job,
            clock, mockFactory.Object, NullLogger<CronJobOrchestration>.Instance);

        // Act
        await orchestrator.StartAsync(CancellationToken.None);
        await orchestrator.ExecuteTask;

        // Assert
        Assert.False(jobCalled, "Job must never be called if no future occurrences");
    }
}