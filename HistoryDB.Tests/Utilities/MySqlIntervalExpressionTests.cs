using HistoryDB.Utilities;

namespace HistoryDB.Tests.Utilities;

public sealed class MySqlIntervalExpressionTests
{
    [Theory]
    [InlineData("INTERVAL 1 DAY")]
    [InlineData("  interval  2  hour  ")]
    [InlineData("INTERVAL 3 WEEK")]
    public void TryParseCutoffUtc_AcceptsCommonForms(string text)
    {
        DateTime anchor = new(2026, 5, 1, 12, 0, 0, DateTimeKind.Utc);
        Assert.True(MySqlIntervalExpression.TryParseCutoffUtc(text, anchor, out DateTime cutoff, out string? err), err);
        Assert.Null(err);
        Assert.True(cutoff < anchor);
    }

    [Fact]
    public void TryParseCutoffUtc_DaySubtractsTwentyFourHours()
    {
        DateTime anchor = new(2026, 5, 1, 12, 0, 0, DateTimeKind.Utc);
        Assert.True(MySqlIntervalExpression.TryParseCutoffUtc("INTERVAL 1 DAY", anchor, out DateTime cutoff, out _));
        Assert.Equal(anchor.AddDays(-1), cutoff);
    }

    [Fact]
    public void TryParseCutoffUtc_MonthUsesCalendarSemantics()
    {
        DateTime anchor = new(2026, 3, 31, 0, 0, 0, DateTimeKind.Utc);
        Assert.True(MySqlIntervalExpression.TryParseCutoffUtc("INTERVAL 1 MONTH", anchor, out DateTime cutoff, out _));
        Assert.Equal(anchor.AddMonths(-1), cutoff);
    }

    [Fact]
    public void TryParseCutoffUtc_UnknownUnit_Fails()
    {
        Assert.False(MySqlIntervalExpression.TryParseCutoffUtc("INTERVAL 1 FORTNIGHT", DateTime.UtcNow, out _, out string? err));
        Assert.NotNull(err);
    }

    [Fact]
    public void TryParseCutoffUtc_InvalidGrammar_Fails()
    {
        Assert.False(MySqlIntervalExpression.TryParseCutoffUtc("1 DAY", DateTime.UtcNow, out _, out _));
    }
}
