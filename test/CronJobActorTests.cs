using System.Threading.Channels;
using aws_backup;
using Microsoft.Extensions.Logging.Abstractions;
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

public class CronJobActorTests
{
    [Fact]
    public async Task ExecutesJob_OnSchedule()
    {
        // Arrange
        var runTimes = new List<DateTimeOffset>();
        var startTime = new DateTimeOffset(2025, 6, 25, 10, 0, 0, TimeSpan.Zero);
        var timeProvider = new FakeTimeProvider(startTime);
        var channel = Channel.CreateUnbounded<string>();

        var cfgMon = new Configuration(
            PathsToArchive: "",
            ClientId: "test-client",
            CronSchedule: "* * * * *");

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

        var signalHubMock = new Mock<ISignalHub<string>>();
        signalHubMock
            .Setup(h => h.WaitAsync(It.IsAny<CancellationToken>()))
            .Returns(() => channel.Reader.ReadAsync().AsTask());

        // job just records invocation times

        // next occurrences at +10s, +20s, then stop
        scheduler.Enqueue(() => startTime.AddSeconds(1));
        scheduler.Enqueue(() => startTime.AddSeconds(11));
        scheduler.Enqueue(() => null);

        var orchestrator = new CronJobActor(
            cfgMon,
            mediatorMock.Object,
            resolverMock.Object,
            mockFactory.Object,
            NullLogger<CronJobActor>.Instance,
            timeProvider,
            Mock.Of<ISnsMessageMediator>(),
            signalHubMock.Object);

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
        var timeProvider = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var channel = Channel.CreateUnbounded<string>();
        var signalHubMock = new Mock<ISignalHub<string>>();
        signalHubMock
            .Setup(h => h.WaitAsync(It.IsAny<CancellationToken>()))
            .Returns(() => channel.Reader.ReadAsync().AsTask());

        var cfgMon = new Configuration(
            PathsToArchive: "",
            ClientId: "test-client",
            CronSchedule: "* * * * *");

        var resolverMock = new Mock<IContextResolver>();

        var mediatorMock = new Mock<IRunRequestMediator>();
        mediatorMock
            .Setup(m => m.ScheduleRunRequest(It.IsAny<RunRequest>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        var scheduler = new StubScheduler();
        var mockFactory = new Mock<ICronSchedulerFactory>();
        mockFactory
            .Setup(f => f.Create(It.IsAny<string>()))
            .Returns((string _) => scheduler);
        // first schedule far in the future so Delay would be long
        scheduler.Enqueue(() => timeProvider.GetUtcNow().AddMinutes(10));
        // after change, schedule soon
        scheduler.Enqueue(() => null);

        var cts = new CancellationTokenSource();

        var orchestrator = new CronJobActor(
            cfgMon,
            mediatorMock.Object,
            resolverMock.Object,
            mockFactory.Object,
            NullLogger<CronJobActor>.Instance,
            timeProvider,
            Mock.Of<ISnsMessageMediator>(),
            signalHubMock.Object);

        // Act
        var exec = orchestrator.StartAsync(cts.Token);

        // Immediately "change" the config
        await Task.Delay(100);
        channel.Writer.TryWrite("*/1 * * * *");

        // Stop
        await exec;
        await orchestrator.ExecuteTask;

        // Assert that job ran exactly once under the new schedule
        mediatorMock.Verify(m => m.ScheduleRunRequest(It.IsAny<RunRequest>(), It.IsAny<CancellationToken>()),
            Times.Never);
        mockFactory.Verify<ICronScheduler>(
            m => m.Create(It.Is<string>("* * * * *", StringComparer.InvariantCultureIgnoreCase)),
            Times.Once);
        mockFactory.Verify<ICronScheduler>(
            m => m.Create(It.Is<string>("*/1 * * * *", StringComparer.InvariantCultureIgnoreCase)),
            Times.Once);
        resolverMock
            .Verify(r => r.ArchiveRunId(It.IsAny<DateTimeOffset>()), Times.Never);
    }

    [Fact]
    public async Task StopsWhenNoFutureOccurrences()
    {
        // Arrange
        var cfgMon = new Configuration(
            PathsToArchive: "",
            ClientId: "test-client",
            CronSchedule: "* * * * *");

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

        var orchestrator = new CronJobActor(
            cfgMon,
            mediatorMock.Object,
            resolverMock.Object,
            mockFactory.Object,
            NullLogger<CronJobActor>.Instance,
            clock,
            Mock.Of<ISnsMessageMediator>(),
            Mock.Of<ISignalHub<string>>());

        // Act
        await orchestrator.StartAsync(CancellationToken.None);
        await orchestrator.ExecuteTask;

        // Assert
        mediatorMock.Verify(m => m.ScheduleRunRequest(It.IsAny<RunRequest>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }
}