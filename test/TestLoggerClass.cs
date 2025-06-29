using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace test;

public class TestLoggerClass<T> : ILogger<T>, IDisposable
{
    public ConcurrentBag<LogRecord> LogRecords { get; } = [];

    public void Dispose()
    {
    }

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
        Func<TState, Exception?, string> formatter)
    {
        var message = formatter(state, exception);
        LogRecords.Add(new LogRecord(logLevel, eventId, exception, message));
    }

    public bool IsEnabled(LogLevel logLevel)
    {
        return true;
    }

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull
    {
        return this;
    }

    public record LogRecord(
        LogLevel LogLevel,
        EventId EventId,
        Exception? Exception,
        string Message);
}