namespace HisBench.Core;

internal static class DirectorySizeUtil
{
    internal static long GetTotalBytes(string rootPath)
    {
        if (!Directory.Exists(rootPath))
        {
            return 0;
        }

        long total = 0;
        try
        {
            foreach (string file in Directory.EnumerateFiles(rootPath, "*", SearchOption.AllDirectories))
            {
                try
                {
                    total += new FileInfo(file).Length;
                }
                catch
                {
                    // Ignore transient file race/access errors in footprint scan.
                }
            }
        }
        catch
        {
            return total;
        }

        return total;
    }
}
