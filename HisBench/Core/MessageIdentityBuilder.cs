namespace HisBench.Core;

internal static class MessageIdentityBuilder
{
    internal static string MessageId(long index, bool random)
    {
        if (random)
        {
            return $"<{Guid.NewGuid():N}.{Random.Shared.NextInt64():x16}@hisbench.local>";
        }

        ulong mixed = unchecked((ulong)index * 11400714819323198485UL);
        return $"<{index:x16}.{mixed:x16}@hisbench.local>";
    }

    internal static string ServerId(long index)
    {
        Span<byte> bytes = stackalloc byte[16];
        BitConverter.TryWriteBytes(bytes[..8], index);
        BitConverter.TryWriteBytes(bytes[8..], index ^ unchecked((long)0x9E3779B97F4A7C15UL));
        return new Guid(bytes).ToString();
    }
}
