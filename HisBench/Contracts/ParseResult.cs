namespace HisBench.Contracts;

internal readonly record struct ParseResult(bool Success, BenchmarkOptions? Options, string? ErrorMessage)
{
    internal static ParseResult Ok(BenchmarkOptions options) => new(true, options, null);

    internal static ParseResult Fail(string message) => new(false, null, message);
}
