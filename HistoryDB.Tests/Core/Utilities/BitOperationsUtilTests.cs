using HistoryDB.Utilities;

namespace HistoryDB.Tests.Core.Utilities;

public sealed class BitOperationsUtilTests
{
    [Theory]
    [MemberData(nameof(BitOperationsUtilTheoryData.IsPowerOfTwoUInt64Cases), MemberType = typeof(BitOperationsUtilTheoryData))]
    public void IsPowerOfTwo_UInt64_ClassifiesCorrectly(ulong value, bool expected)
    {
        Assert.Equal(expected, BitOperationsUtil.IsPowerOfTwo(value));
    }

    [Theory]
    [MemberData(nameof(BitOperationsUtilTheoryData.IsPowerOfTwoInt32Cases), MemberType = typeof(BitOperationsUtilTheoryData))]
    public void IsPowerOfTwo_Int32_ClassifiesCorrectly(int value, bool expected)
    {
        Assert.Equal(expected, BitOperationsUtil.IsPowerOfTwo(value));
    }

    [Theory]
    [MemberData(nameof(BitOperationsUtilTheoryData.RoundDownToPowerOfTwoCases), MemberType = typeof(BitOperationsUtilTheoryData))]
    public void RoundDownToPowerOfTwo_ReturnsExpected(ulong value, ulong expected)
    {
        Assert.Equal(expected, BitOperationsUtil.RoundDownToPowerOfTwo(value));
    }
}
