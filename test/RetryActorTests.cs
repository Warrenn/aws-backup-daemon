using System.Threading.Channels;
using aws_backup;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Time.Testing;
using Moq;

namespace test;

internal record TestRetryState : RetryState;

public class RetryActorTests
{
    private readonly Mock<IContextResolver> _ctx = new();
    private readonly Mock<ILogger<RetryActor>> _logger = new();
    private readonly Mock<IRetryMediator> _mediator = new();

    private RetryActor CreateOrch(Channel<RetryState> chan, TimeProvider tp)
    {
        _mediator
            .Setup(m => m.GetRetries(It.IsAny<CancellationToken>()))
            .Returns(chan.Reader.ReadAllAsync());
        return new RetryActor(
            _mediator.Object,
            _ctx.Object,
            _logger.Object,
            tp
        );
    }

    [Fact]
    public async Task LimitExceeded_InvokesLimitExceededOnly()
    {
        // Arrange: state with AttemptCount 5, explicit RetryLimit 3
        var state = new TestRetryState
        {
            AttemptCount = 5,
            RetryLimit = 3,
            // we'll set these delegates to mark flags
            Retry = (_, _) => throw new InvalidOperationException("should-not-call")
        };
        var limitCalled = false;
        state.LimitExceeded = async (st, ct) =>
        {
            limitCalled = true;
            await Task.CompletedTask;
        };

        // feed one state, then complete
        var chan = Channel.CreateUnbounded<RetryState>();
        await chan.Writer.WriteAsync(state);
        chan.Writer.Complete();

        // context: NextRetryTime shouldn't matter
        _ctx.Setup(c => c.NextRetryTime(It.IsAny<int>()))
            .Returns(DateTimeOffset.UtcNow.AddSeconds(1));

        // Act
        var tp = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var orch = CreateOrch(chan, tp);
        await orch.StartAsync(CancellationToken.None);
        await orch.ExecuteTask;

        // Assert
        Assert.True(limitCalled, "LimitExceeded delegate must be invoked");
        _mediator.Verify(m => m.RetryAttempt(It.IsAny<RetryState>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task BeforeNextAttempt_CallsRetryAttemptOnly()
    {
        // Arrange: state under limit, NextAttemptAt in future
        var state = new TestRetryState
        {
            AttemptCount = 1,
            RetryLimit = 5,
            NextAttemptAt = DateTimeOffset.UtcNow.AddSeconds(30)
        };
        var retryCalled = false;
        state.Retry = async (_, _) =>
        {
            retryCalled = true;
            await Task.CompletedTask;
        };
        var limitCalled = false;
        state.LimitExceeded = async (_, _) =>
        {
            limitCalled = true;
            await Task.CompletedTask;
        };

        var chan = Channel.CreateUnbounded<RetryState>();
        await chan.Writer.WriteAsync(state);
        chan.Writer.Complete();

        _ctx.Setup(c => c.NextRetryTime(It.IsAny<int>()))
            .Returns(DateTimeOffset.UtcNow);
        _ctx.Setup(c => c.RetryCheckIntervalMs())
            .Returns(1);

        // Act
        var tp = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var orch = CreateOrch(chan, tp);
        await orch.StartAsync(CancellationToken.None);
        await orch.ExecuteTask;

        // Assert: we should have enqueued it once for retry
        _mediator.Verify(m => m.RetryAttempt(state, It.IsAny<CancellationToken>()), Times.Once);
        Assert.False(limitCalled, "LimitExceeded should not fire");
        Assert.False(retryCalled, "Retry should not fire before NextAttemptAt");
    }

    [Fact]
    public async Task OnOrAfterNextAttempt_UnderLimit_InvokesRetryAndUpdatesState()
    {
        // Arrange: state under limit, NextAttemptAt in past
        var now = DateTimeOffset.UtcNow;
        var state = new TestRetryState
        {
            AttemptCount = 0,
            RetryLimit = 3,
            NextAttemptAt = now.AddSeconds(-1)
        };
        var retryCalled = false;
        state.Retry = async (st, ct) =>
        {
            retryCalled = true;
            // check that AttemptCount was incremented
            Assert.Equal(1, st.AttemptCount);
            await Task.CompletedTask;
        };
        state.LimitExceeded = (_, _) => Task.CompletedTask;

        var chan = Channel.CreateUnbounded<RetryState>();
        await chan.Writer.WriteAsync(state);
        chan.Writer.Complete();

        // NextRetryTime should be called with new attempt count
        _ctx.Setup(c => c.NextRetryTime(1))
            .Returns(now.AddMinutes(5));

        var tp = new FakeTimeProvider(now);
        var orch = CreateOrch(chan, tp);

        // Act
        await orch.StartAsync(CancellationToken.None);
        await orch.ExecuteTask;

        // Assert
        Assert.True(retryCalled, "Retry delegate should fire");
        Assert.True(state.NextAttemptAt > now, "NextAttemptAt should be updated");
        _mediator.Verify(m => m.RetryAttempt(It.IsAny<RetryState>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task NoLimitAndZeroRetryLimit_UsesGeneralRetryLimit()
    {
        // Arrange: state with RetryLimit = 0 forces general limit
        var state = new TestRetryState
        {
            AttemptCount = 10,
            RetryLimit = 0,
            NextAttemptAt = DateTimeOffset.UtcNow.AddSeconds(-1)
        };
        var exceeded = false;
        state.LimitExceeded = async (_, _) =>
        {
            exceeded = true;
            await Task.CompletedTask;
        };
        var chan = Channel.CreateUnbounded<RetryState>();
        await chan.Writer.WriteAsync(state);
        chan.Writer.Complete();

        // general limit set to 5
        _ctx.Setup(c => c.GeneralRetryLimit()).Returns(5);
        _ctx.Setup(c => c.NextRetryTime(It.IsAny<int>()))
            .Returns(DateTimeOffset.UtcNow.AddSeconds(1));

        var tp = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var orch = CreateOrch(chan, tp);

        // Act
        await orch.StartAsync(CancellationToken.None);
        await orch.ExecuteTask;

        // Assert: since AttemptCount(10) > general limit(5)
        Assert.True(exceeded, "LimitExceeded should fire when general limit is exceeded");
    }
}