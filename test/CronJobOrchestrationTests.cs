using aws_backup;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Moq;

namespace test;

internal class StubScheduler : ICronScheduler
{
    private readonly Queue<Func<DateTimeOffset?>> _times = new();

    public DateTimeOffset? GetNext(DateTimeOffset afterUtc)
    {
        return _times.Count == 0 ? null : _times.Dequeue()();
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
        var startTime = new DateTimeOffset(2025, 6, 25, 10, 0, 0, TimeSpan.Zero);
        var timeProvider = new FakeTimeProvider(startTime);

        var cfgMon = new Mock<IOptionsMonitor<Configuration>>();
        cfgMon
            .Setup(m => m.CurrentValue)
            .Returns(new Configuration(
                PathsToArchive: "",
                ClientId: "test-client",
                CronSchedule: "* * * * *"));

        var scheduler = new StubScheduler();
        var resolverMock = new Mock<IContextResolver>();
        resolverMock
            .Setup(r => r.ArchiveRunId(It.IsAny<DateTimeOffset>()))
            .Returns("resolved-context");

        var mediatorMock = new Mock<IRunRequestMediator>();
        mediatorMock
            .Setup(m => m.ScheduleRunRequest(It.IsAny<RunRequest>(), It.IsAny<CancellationToken>()))
            .Callback((RunRequest _, CancellationToken _) =>
            {
                runTimes.Add(timeProvider.GetUtcNow());
                timeProvider.Advance(TimeSpan.FromSeconds(10));
            })
            .Returns(Task.CompletedTask);

        var mockFactory = new Mock<ICronSchedulerFactory>();
        mockFactory
            .Setup(f => f.Create(It.IsAny<string>()))
            .Returns((string _) => scheduler);

        // job just records invocation times

        // next occurrences at +10s, +20s, then stop
        scheduler.Enqueue(() => startTime.AddSeconds(1));
        scheduler.Enqueue(() => startTime.AddSeconds(11));
        scheduler.Enqueue(() => null);

        var orchestrator = new CronJobOrchestration(
            cfgMon.Object,
            mediatorMock.Object,
            resolverMock.Object,
            mockFactory.Object,
            NullLogger<CronJobOrchestration>.Instance,
            timeProvider,
            Mock.Of<ISnsMessageMediator>());

        // Act: run in background
        var cts = new CancellationTokenSource();
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
        Assert.Equal(new DateTimeOffset(2025, 6, 25, 10, 0, 0, TimeSpan.Zero), runTimes[0]);
        Assert.Equal(new DateTimeOffset(2025, 6, 25, 10, 0, 10, TimeSpan.Zero), runTimes[1]);
    }

    [Fact]
    public async Task CancelsPendingDelay_WhenCronScheduleChanges()
    {
        // Arrange
        var runCount = 0;

        var timeProvider = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var cfgMon = new Mock<IOptionsMonitor<Configuration>>();
        var listeners = new List<Action<Configuration, string>>();
        cfgMon.Setup(m => m.CurrentValue).Returns(new Configuration(
            PathsToArchive: "",
            ClientId: "test-client",
            CronSchedule: "* * * * *"));
        cfgMon
            .Setup(m => m.OnChange(It.IsAny<Action<Configuration, string>>()!))
            .Callback<Action<Configuration, string>>(h => listeners.Add(h));

        var resolverMock = new Mock<IContextResolver>();
        resolverMock
            .Setup(r => r.ArchiveRunId(It.IsAny<DateTimeOffset>()))
            .Callback((DateTimeOffset time) => runCount++)
            .Returns("resolved-context");

        var mediatorMock = new Mock<IRunRequestMediator>();
        mediatorMock
            .Setup(m => m.ScheduleRunRequest(It.IsAny<RunRequest>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        var sched = new StubScheduler();
        var mockFactory = new Mock<ICronSchedulerFactory>();
        mockFactory
            .Setup(f => f.Create(It.IsAny<string>()))
            .Returns((string _) => sched);
        // first schedule far in the future so Delay would be long
        sched.Enqueue(() => timeProvider.GetUtcNow().AddMinutes(10));
        // after change, schedule soon
        sched.Enqueue(() => timeProvider.GetUtcNow().AddSeconds(1));
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
            mockFactory.Object,
            NullLogger<CronJobOrchestration>.Instance,
            timeProvider,
            Mock.Of<ISnsMessageMediator>());

        // Act
        var exec = orchestrator.StartAsync(cts.Token);

        // Immediately "change" the config
        await Task.Delay(100);
        listeners[0].Invoke(new Configuration(
            PathsToArchive: "",
            ClientId: "test-client",
            CronSchedule: "*/1 * * * *"), "");

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
        cfgMon.Setup(m => m.CurrentValue).Returns(new Configuration(
            PathsToArchive: "",
            ClientId: "test-client",
            CronSchedule: "irrelevant"));
        var clock = new FakeTimeProvider();
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

        var mediatorMock = new Mock<IRunRequestMediator>();
        mediatorMock
            .Setup(m => m.ScheduleRunRequest(It.IsAny<RunRequest>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

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
            mockFactory.Object,
            NullLogger<CronJobOrchestration>.Instance,
            clock,
            Mock.Of<ISnsMessageMediator>());

        // Act
        await orchestrator.StartAsync(CancellationToken.None);
        await orchestrator.ExecuteTask;

        // Assert
        Assert.False(jobCalled, "Job must never be called if no future occurrences");
    }
}