// ConsecutiveFailureCircuit.cs -- simple open/half-open breaker for journal and bloom I/O degradation.

using System.Diagnostics;

namespace HistoryDB.Utilities;

internal sealed class ConsecutiveFailureCircuit
{
    private readonly int _openAfterFailures;
    private readonly TimeSpan _cooldown;
    private int _consecutiveFailures;
    private long _openedUntilTicks;
    private bool _halfOpen;

    public ConsecutiveFailureCircuit(int openAfterFailures, TimeSpan cooldownUnbounded)
    {
        if (openAfterFailures < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(openAfterFailures), "Must be >= 1.");
        }

        _openAfterFailures = openAfterFailures;
        _cooldown = cooldownUnbounded;
    }

    public bool IsOpen => !_halfOpen && Stopwatch.GetTimestamp() < _openedUntilTicks;

    public void RecordSuccess()
    {
        Interlocked.Exchange(ref _consecutiveFailures, 0);
        _halfOpen = false;
        _openedUntilTicks = 0;
    }

    public void RecordFailure()
    {
        int n = Interlocked.Increment(ref _consecutiveFailures);
        if (n >= _openAfterFailures)
        {
            long until = Stopwatch.GetTimestamp() + (long)(_cooldown.TotalSeconds * Stopwatch.Frequency);
            Volatile.Write(ref _openedUntilTicks, until);
            Interlocked.Exchange(ref _consecutiveFailures, 0);
            _halfOpen = false;
        }
    }

    /// <summary>Call before a retry after open period elapses; returns false if still in cooldown.</summary>
    public bool TryEnterRetry()
    {
        if (Stopwatch.GetTimestamp() < Volatile.Read(ref _openedUntilTicks))
        {
            return false;
        }

        _halfOpen = true;
        return true;
    }
}
