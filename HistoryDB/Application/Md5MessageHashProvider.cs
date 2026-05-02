// Md5MessageHashProvider.cs -- MD5-based IMessageHashProvider for HistoryDB metadata and shard routing.

using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;

using HistoryDB.Contracts;

namespace HistoryDB.Application;

internal sealed class Md5MessageHashProvider : IMessageHashProvider
{
    public Hash128 ComputeHash(string text)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(text);
        byte[] data = Encoding.UTF8.GetBytes(text);
        Span<byte> hash = stackalloc byte[16];
        MD5.HashData(data, hash);
        ulong hi = BinaryPrimitives.ReadUInt64LittleEndian(hash[..8]);
        ulong lo = BinaryPrimitives.ReadUInt64LittleEndian(hash[8..]);
        return new Hash128(hi, lo);
    }
}
