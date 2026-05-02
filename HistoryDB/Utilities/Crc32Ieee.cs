// Crc32Ieee.cs -- IEEE CRC-32 (same polynomial as PNG / ZIP) for on-disk integrity tags.

using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace HistoryDB.Utilities;

/// <summary>
/// CRC-32 with polynomial 0xEDB88320 and refin/refout (IEEE / Ethernet / zlib style).
/// </summary>
internal static class Crc32Ieee
{
    private static readonly uint[] Table = CreateTable();

    public static uint Compute(ReadOnlySpan<byte> data)
    {
        uint crc = 0xFFFFFFFF;
        for (int i = 0; i < data.Length; i++)
        {
            crc = Table[(crc ^ data[i]) & 0xFF] ^ (crc >> 8);
        }

        return crc ^ 0xFFFFFFFF;
    }

    public static unsafe uint ComputeUnmanaged(nint ptr, int length)
    {
        ReadOnlySpan<byte> span = MemoryMarshal.CreateReadOnlySpan(ref Unsafe.AsRef<byte>((void*)ptr), length);
        return Compute(span);
    }

    /// <summary>Chunked CRC for slot regions larger than <see cref="int.MaxValue"/> bytes.</summary>
    public static unsafe uint ComputeUnmanaged(nint ptr, long length)
    {
        if (length <= int.MaxValue)
        {
            return ComputeUnmanaged(ptr, (int)length);
        }

        uint crc = 0xFFFFFFFF;
        byte* p = (byte*)ptr;
        long offset = 0;
        while (offset < length)
        {
            long rem = length - offset;
            int chunk = rem > int.MaxValue ? int.MaxValue : (int)rem;
            ReadOnlySpan<byte> span = MemoryMarshal.CreateReadOnlySpan(ref Unsafe.AsRef<byte>(p + offset), chunk);
            for (int i = 0; i < span.Length; i++)
            {
                crc = Table[(crc ^ span[i]) & 0xFF] ^ (crc >> 8);
            }

            offset += chunk;
        }

        return crc ^ 0xFFFFFFFF;
    }

    public static void WriteUInt32LittleEndian(Span<byte> destination, uint value) =>
        BinaryPrimitives.WriteUInt32LittleEndian(destination, value);

    public static uint ReadUInt32LittleEndian(ReadOnlySpan<byte> source) =>
        BinaryPrimitives.ReadUInt32LittleEndian(source);

    private static uint[] CreateTable()
    {
        uint[] table = new uint[256];
        for (uint i = 0; i < 256; i++)
        {
            uint c = i;
            for (int k = 0; k < 8; k++)
            {
                c = (c & 1) != 0 ? (0xEDB88320 ^ (c >> 1)) : (c >> 1);
            }

            table[i] = c;
        }

        return table;
    }
}
