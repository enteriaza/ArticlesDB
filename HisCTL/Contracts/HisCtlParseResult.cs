namespace HisCTL.Contracts;

internal readonly record struct HisCtlParseResult(bool Success, string? ErrorMessage, HisCtlOptions? Options);
