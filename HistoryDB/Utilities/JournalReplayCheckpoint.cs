// JournalReplayCheckpoint.cs -- byte offset checkpoint for incremental journal replay.

using System.Buffers.Binary;

namespace HistoryDB.Utilities;

internal static class JournalReplayCheckpoint
{
    internal const int FileByteLength = 16;
    private const uint Magic = 0x5043524A; // "JRCP" LE

    internal static void Write(string checkpointPath, long nextReadOffset)
    {
        string? dir = Path.GetDirectoryName(checkpointPath);
        if (!string.IsNullOrEmpty(dir))
        {
            Directory.CreateDirectory(dir);
        }

        byte[] bytes = new byte[FileByteLength];
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(0, 4), Magic);
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(4, 4), 1);
        BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(8, 8), nextReadOffset);
        File.WriteAllBytes(checkpointPath, bytes);
    }

    /// <summary>Returns zero if missing or invalid.</summary>
    internal static long TryReadOffset(string checkpointPath)
    {
        if (!File.Exists(checkpointPath))
        {
            return 0;
        }

        try
        {
            byte[] bytes = File.ReadAllBytes(checkpointPath);
            if (bytes.Length < FileByteLength)
            {
                return 0;
            }

            uint magic = BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(0, 4));
            if (magic != Magic)
            {
                return 0;
            }

            return BinaryPrimitives.ReadInt64LittleEndian(bytes.AsSpan(8));
        }
        catch
        {
            return 0;
        }
    }
}
