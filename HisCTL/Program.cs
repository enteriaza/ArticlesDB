using System.Globalization;
using HisCTL.Application;
using HisCTL.Contracts;
using Microsoft.Extensions.Logging;

internal static class Program
{
    private static async Task<int> Main(string[] args)
    {
        CultureInfo.DefaultThreadCurrentCulture = CultureInfo.InvariantCulture;
        CultureInfo.DefaultThreadCurrentUICulture = CultureInfo.InvariantCulture;
        CultureInfo.CurrentCulture = CultureInfo.InvariantCulture;
        CultureInfo.CurrentUICulture = CultureInfo.InvariantCulture;

        using ILoggerFactory loggerFactory = LoggerFactory.Create(static builder =>
        {
            builder
                .SetMinimumLevel(LogLevel.Information)
                .AddSimpleConsole(static options =>
                {
                    options.TimestampFormat = "HH:mm:ss ";
                    options.SingleLine = true;
                });
        });

        ILogger logger = loggerFactory.CreateLogger("HisCTL");

        if (args.Length == 0 || string.Equals(args[0], "--help", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(args[0], "-h", StringComparison.OrdinalIgnoreCase))
        {
            Console.WriteLine(HisCtlUsageFormatter.Format());
            return 0;
        }

        string verb = args[0];
        string[] tail = args.Length > 1 ? args.AsSpan(1).ToArray() : Array.Empty<string>();
        HisCtlOptionsParser parser = new();
        HisCtlParseResult parse = parser.Parse(verb, tail);
        if (!parse.Success)
        {
            Console.Error.WriteLine(parse.ErrorMessage);
            Console.Error.WriteLine(HisCtlUsageFormatter.Format());
            return 1;
        }

        HisCtlOptions options = parse.Options!;
        if (options.ShowHelp)
        {
            Console.WriteLine(HisCtlUsageFormatter.Format());
            return 0;
        }

        return await HisCtlRunner.RunAsync(options, logger, CancellationToken.None).ConfigureAwait(false);
    }
}
