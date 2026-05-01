using HisBench.Contracts;
using System.Globalization;
using System.Diagnostics;
using System.Reflection;
using System.Runtime.InteropServices;

namespace HisBench.Infrastructure;

internal sealed class ProcessTelemetryReader : IProcessTelemetryReader
{
    private static readonly object IoSync = new();
    private static bool _initialized;
    private static PropertyInfo? _readBytesProp;
    private static PropertyInfo? _writeBytesProp;

    public ProcessResourceSnapshot ReadCurrent()
    {
        // Process.GetCurrentProcess() opens a new OS handle every call; without disposing, repeated sampling under
        // long-running benchmarks (auto-tune sweeps, sustained correlation samplers) leaks handles on both Windows and Linux.
        using Process process = Process.GetCurrentProcess();
        process.Refresh();

        bool hasIo = TryGetIoBytes(process, out long readBytes, out long writeBytes);
        return new ProcessResourceSnapshot(
            process.WorkingSet64,
            process.TotalProcessorTime,
            readBytes,
            writeBytes,
            hasIo,
            GC.CollectionCount(0),
            GC.CollectionCount(1),
            GC.CollectionCount(2),
            GC.GetTotalMemory(false));
    }

    private static bool TryGetIoBytes(Process process, out long readBytes, out long writeBytes)
    {
        if (OperatingSystem.IsWindows() && TryReadWindowsIoCounters(process, out readBytes, out writeBytes))
        {
            return true;
        }

        if (OperatingSystem.IsLinux())
        {
            return TryReadLinuxProcIo(out readBytes, out writeBytes);
        }

        EnsureReflectionInitialized();
        if (_readBytesProp is null || _writeBytesProp is null)
        {
            readBytes = -1;
            writeBytes = -1;
            return false;
        }

        object? read = _readBytesProp.GetValue(process);
        object? write = _writeBytesProp.GetValue(process);
        if (read is null || write is null)
        {
            readBytes = -1;
            writeBytes = -1;
            return false;
        }

        readBytes = Convert.ToInt64(read, CultureInfo.InvariantCulture);
        writeBytes = Convert.ToInt64(write, CultureInfo.InvariantCulture);
        return true;
    }

    private static bool TryReadWindowsIoCounters(Process process, out long readBytes, out long writeBytes)
    {
        readBytes = -1;
        writeBytes = -1;
        try
        {
            if (!GetProcessIoCounters(process.Handle, out IoCounters counters))
            {
                return false;
            }

            readBytes = unchecked((long)counters.ReadTransferCount);
            writeBytes = unchecked((long)counters.WriteTransferCount);
            return true;
        }
        catch
        {
            return false;
        }
    }

    private static bool TryReadLinuxProcIo(out long readBytes, out long writeBytes)
    {
        readBytes = 0;
        writeBytes = 0;
        try
        {
            foreach (string line in File.ReadLines("/proc/self/io"))
            {
                if (line.StartsWith("read_bytes: ", StringComparison.Ordinal))
                {
                    readBytes = long.Parse(line.AsSpan("read_bytes: ".Length), NumberStyles.Integer, CultureInfo.InvariantCulture);
                }
                else if (line.StartsWith("write_bytes: ", StringComparison.Ordinal))
                {
                    writeBytes = long.Parse(line.AsSpan("write_bytes: ".Length), NumberStyles.Integer, CultureInfo.InvariantCulture);
                }
            }

            return true;
        }
        catch
        {
            readBytes = -1;
            writeBytes = -1;
            return false;
        }
    }

    private static void EnsureReflectionInitialized()
    {
        lock (IoSync)
        {
            if (_initialized)
            {
                return;
            }

            _readBytesProp = typeof(Process).GetProperty("ReadBytes", BindingFlags.Instance | BindingFlags.Public);
            _writeBytesProp = typeof(Process).GetProperty("WriteBytes", BindingFlags.Instance | BindingFlags.Public);
            _initialized = true;
        }
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct IoCounters
    {
        public ulong ReadOperationCount;
        public ulong WriteOperationCount;
        public ulong OtherOperationCount;
        public ulong ReadTransferCount;
        public ulong WriteTransferCount;
        public ulong OtherTransferCount;
    }

    [DllImport("kernel32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool GetProcessIoCounters(IntPtr hProcess, out IoCounters lpIoCounters);
}
