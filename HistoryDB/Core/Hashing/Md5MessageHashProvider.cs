// Md5MessageHashProvider.cs -- stable 128-bit UTF-8 MD5 hash provider for HistoryDB message/server identifiers.

using System.Security.Cryptography;
using System.Text;

using HistoryDB.Contracts;

namespace HistoryDB.Core.Hashing;

internal sealed class Md5MessageHashProvider : IMessageHashProvider
{
    public Hash128 ComputeHash(string text)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(text);

        int byteCount = Encoding.UTF8.GetByteCount(text);
        byte[]? rented = null;
        Span<byte> utf8 = byteCount <= 512
            ? stackalloc byte[byteCount]
            : (rented = System.Buffers.ArrayPool<byte>.Shared.Rent(byteCount)).AsSpan(0, byteCount);

        try
        {
            int written = Encoding.UTF8.GetBytes(text, utf8);
            Span<byte> hash = stackalloc byte[16];
            MD5.HashData(utf8[..written], hash);
            return new Hash128(
                BitConverter.ToUInt64(hash[..8]),
                BitConverter.ToUInt64(hash[8..]));
        }
        finally
        {
            if (rented is not null)
            {
                System.Buffers.ArrayPool<byte>.Shared.Return(rented);
            }
        }
    }
}
