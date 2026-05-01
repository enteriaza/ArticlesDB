namespace HisBench.Core;

internal static class BurstProfileEvaluator
{
    internal static double GetRampDuty(int baseDutyPercent)
    {
        const long cycleMs = 10_000;
        long phase = Environment.TickCount64 % cycleMs;
        return Math.Clamp(baseDutyPercent * (phase / (double)cycleMs) * 2.0, 1.0, 99.0);
    }

    internal static double GetWaveDuty(int baseDutyPercent)
    {
        const double periodMs = 8_000.0;
        double angle = (Environment.TickCount64 % periodMs) / periodMs * Math.PI * 2.0;
        double normalized = (Math.Sin(angle) + 1.0) * 0.5;
        double centered = 0.2 + (0.8 * normalized);
        return Math.Clamp(baseDutyPercent * centered * 2.0, 1.0, 99.0);
    }
}
