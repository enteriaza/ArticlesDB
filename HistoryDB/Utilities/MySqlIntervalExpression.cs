using System.Globalization;
using System.Text.RegularExpressions;

namespace HistoryDB.Utilities;

/// <summary>
/// Parses MySQL-style <c>INTERVAL n unit</c> into a UTC cutoff: values with tick fields strictly less than
/// <see cref="DateTime.UtcNow"/> minus the interval are considered older than the interval.
/// </summary>
public static partial class MySqlIntervalExpression
{
    [GeneratedRegex(@"^\s*INTERVAL\s+(-?\d+)\s+(\w+)\s*$", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant)]
    private static partial Regex IntervalRegex();

    /// <summary>
    /// Computes the cutoff instant <c>anchorUtc - interval</c> using calendar rules for MONTH and YEAR.
    /// </summary>
    public static bool TryParseCutoffUtc(string text, DateTime anchorUtc, out DateTime cutoffUtc, out string? error)
    {
        cutoffUtc = default;
        error = null;
        if (string.IsNullOrWhiteSpace(text))
        {
            error = "Interval text is empty.";
            return false;
        }

        Match m = IntervalRegex().Match(text.Trim());
        if (!m.Success)
        {
            error = "Expected INTERVAL <integer> <UNIT>, e.g. INTERVAL 1 DAY or INTERVAL 2 YEAR.";
            return false;
        }

        if (!int.TryParse(m.Groups[1].Value, NumberStyles.Integer, CultureInfo.InvariantCulture, out int qty))
        {
            error = "Invalid INTERVAL quantity.";
            return false;
        }

        string unitRaw = m.Groups[2].Value;
        string unit = unitRaw.ToUpperInvariant();
        unit = unit switch
        {
            "MICROSECONDS" => "MICROSECOND",
            "SECONDS" => "SECOND",
            "MINUTES" => "MINUTE",
            "HOURS" => "HOUR",
            "DAYS" => "DAY",
            "WEEKS" => "WEEK",
            "MONTHS" => "MONTH",
            "YEARS" => "YEAR",
            _ => unit,
        };

        try
        {
            cutoffUtc = unit switch
            {
                "MICROSECOND" => anchorUtc.AddTicks(-(long)qty * 10L),
                "SECOND" => anchorUtc.AddSeconds(-qty),
                "MINUTE" => anchorUtc.AddMinutes(-qty),
                "HOUR" => anchorUtc.AddHours(-qty),
                "DAY" => anchorUtc.AddDays(-qty),
                "WEEK" => anchorUtc.AddDays(-7L * qty),
                "MONTH" => anchorUtc.AddMonths(-qty),
                "YEAR" => anchorUtc.AddYears(-qty),
                _ => throw new FormatException($"Unknown INTERVAL unit '{unitRaw}'."),
            };
        }
        catch (Exception ex)
        {
            error = ex.Message;
            return false;
        }

        return true;
    }
}
