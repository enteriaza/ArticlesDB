namespace HistoryDB.Tests.Core.Policies;

public static class BloomSizingPolicyTheoryData
{
    public static TheoryData<ulong, ulong, int, ulong> SelectDesiredBitsCases => new TheoryData<ulong, ulong, int, ulong>
    {
        { 100UL, 50UL, 10, 50UL },
        { 100UL, 200UL, 10, 100UL },
        { 5UL, 100UL, 10, 10UL },
    };
}
