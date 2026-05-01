namespace HistoryDB.Tests.Core.Utilities;

/// <summary>
/// Strongly typed theory matrices for <see cref="HistoryDB.Utilities.BitOperationsUtil"/> (single place for row definitions).
/// </summary>
public static class BitOperationsUtilTheoryData
{
    public static TheoryData<ulong, bool> IsPowerOfTwoUInt64Cases => new TheoryData<ulong, bool>
    {
        { 0UL, false },
        { 1UL, true },
        { 2UL, true },
        { 3UL, false },
        { 1024UL, true },
        { 1023UL, false },
    };

    public static TheoryData<int, bool> IsPowerOfTwoInt32Cases => new TheoryData<int, bool>
    {
        { 0, false },
        { -1, false },
        { 1, true },
        { 256, true },
        { 255, false },
    };

    public static TheoryData<ulong, ulong> RoundDownToPowerOfTwoCases => new TheoryData<ulong, ulong>
    {
        { 0UL, 1UL },
        { 1UL, 1UL },
        { 2UL, 2UL },
        { 3UL, 2UL },
        { 1023UL, 512UL },
        { 1024UL, 1024UL },
    };
}
