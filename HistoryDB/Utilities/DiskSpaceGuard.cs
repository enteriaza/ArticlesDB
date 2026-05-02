// DiskSpaceGuard.cs -- best-effort free-space probe for compaction and bloom checkpoints.

namespace HistoryDB.Utilities;

internal static class DiskSpaceGuard
{
    internal static bool TryGetAvailableBytes(string pathRootOrFile, out long availableBytes)
    {
        availableBytes = 0;
        try
        {
            string? root = Path.GetPathRoot(Path.GetFullPath(pathRootOrFile));
            if (string.IsNullOrEmpty(root))
            {
                return false;
            }

            DriveInfo drive = new(root);
            if (!drive.IsReady)
            {
                return false;
            }

            availableBytes = drive.AvailableFreeSpace;
            return true;
        }
        catch
        {
            return false;
        }
    }
}
