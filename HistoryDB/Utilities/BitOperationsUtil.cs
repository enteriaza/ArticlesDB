// BitOperationsUtil.cs -- shared power-of-two and rounding helpers for shard sizing, Bloom bit budgets, and channel bounds.
// Used by HistoryShard table sizing and ShardedHistoryWriter shard/Bloom configuration. Pure functions; thread-safe.

using System.Numerics;

namespace HistoryDB.Utilities;

/// <summary>
/// Bit-oriented helpers for sizes that must be powers of two (hash table capacity, Bloom filter bit counts, shard counts).
/// </summary>
/// <remarks>
/// <para>
/// <b>Thread safety:</b> All members are static and side-effect free; safe to call from any thread.
/// </para>
/// </remarks>
internal static class BitOperationsUtil
{
    /// <summary>
    /// Returns whether <paramref name="value"/> is a positive integral power of two.
    /// </summary>
    /// <param name="value">Candidate value; zero is not a power of two.</param>
    /// <returns><see langword="true"/> if <paramref name="value"/> is 2^n for some n &gt;= 0.</returns>
    /// <remarks>
    /// <para>
    /// Used when validating mmap shard table sizes and Bloom filter dimensions so masking with <c>size - 1</c> is valid.
    /// </para>
    /// </remarks>
    internal static bool IsPowerOfTwo(ulong value) => value != 0 && (value & (value - 1)) == 0;

    /// <summary>
    /// Returns whether <paramref name="value"/> is a positive integral power of two.
    /// </summary>
    /// <param name="value">Candidate value; zero and negative values are not powers of two.</param>
    /// <returns><see langword="true"/> if <paramref name="value"/> is 2^n for some n &gt;= 0.</returns>
    internal static bool IsPowerOfTwo(int value) => value > 0 && (value & (value - 1)) == 0;

    /// <summary>
    /// Rounds <paramref name="value"/> down to the largest power of two not exceeding <paramref name="value"/>, or 1 if <paramref name="value"/> &lt;= 1.
    /// </summary>
    /// <param name="value">Non-negative desired magnitude.</param>
    /// <returns>The floored power of two.</returns>
    /// <remarks>
    /// <para>
    /// Used when capping Bloom bits per shard to a power of two so bit indexing uses a single mask operation.
    /// </para>
    /// </remarks>
    internal static ulong RoundDownToPowerOfTwo(ulong value)
    {
        if (value <= 1)
        {
            return 1;
        }

        int shift = BitOperations.Log2(value);
        return 1UL << shift;
    }
}
