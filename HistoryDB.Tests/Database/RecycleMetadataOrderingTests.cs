using HistoryDB;
using HistoryDB.Contracts;

namespace HistoryDB.Tests.Database;

/// <summary>
/// Mirrors HisCTL recycle ordering: date_accessed_ticks asc, accessed_counter asc, date_obtained_ticks asc, hash tie-break.
/// </summary>
public sealed class RecycleMetadataOrderingTests
{
    [Fact]
    public void Sort_PrefersOlderAccessTimeOverCounter()
    {
        // B was accessed earlier (400) than A (500); B should sort first for eviction even though A has lower counter.
        StoredArticleMetadata a = new(0, 0, 0, new Hash128(1, 1), DateObtainedTicks: 100, DateAccessedTicks: 500, AccessedCounter: 1);
        StoredArticleMetadata b = new(0, 0, 0, new Hash128(1, 2), DateObtainedTicks: 200, DateAccessedTicks: 400, AccessedCounter: 99);

        List<StoredArticleMetadata> list = [a, b];
        list.Sort(static (x, y) => CompareRecyclePriority(x, y));

        Assert.Equal(b, list[0]);
        Assert.Equal(a, list[1]);
    }

    [Fact]
    public void Sort_BreaksTiesByAccessCounterThenObtainedThenHash()
    {
        StoredArticleMetadata a = new(0, 0, 0, new Hash128(2, 0), 300, 100, 5);
        StoredArticleMetadata b = new(0, 0, 0, new Hash128(1, 0), 200, 100, 3);
        StoredArticleMetadata c = new(0, 0, 0, new Hash128(1, 1), 100, 100, 3);

        List<StoredArticleMetadata> list = [a, b, c];
        list.Sort(static (x, y) => CompareRecyclePriority(x, y));

        Assert.Equal(c, list[0]);
        Assert.Equal(b, list[1]);
        Assert.Equal(a, list[2]);
    }

    private static int CompareRecyclePriority(StoredArticleMetadata a, StoredArticleMetadata b)
    {
        int c = a.DateAccessedTicks.CompareTo(b.DateAccessedTicks);
        if (c != 0)
        {
            return c;
        }

        c = a.AccessedCounter.CompareTo(b.AccessedCounter);
        if (c != 0)
        {
            return c;
        }

        c = a.DateObtainedTicks.CompareTo(b.DateObtainedTicks);
        if (c != 0)
        {
            return c;
        }

        c = a.MessageHash.Hi.CompareTo(b.MessageHash.Hi);
        return c != 0 ? c : a.MessageHash.Lo.CompareTo(b.MessageHash.Lo);
    }
}
