# Contributing Guidelines

## Code Style

This project targets modern development environments with wide displays. The traditional 80-character line limit is unnecessarily restrictive and
results in excessive line wrapping and vertical scrolling. To balance readability with modern screen real estate, we adopt a **maximum line
length of 160 characters**. This allows for more natural formatting of fluent APIs, LINQ queries, and structured logging statements without
sacrificing clarity. The key is to prioritize readability and logical grouping of code elements over strict adherence to a character count.
If a line exceeds 160 characters but is more readable than the alternatives, it is acceptable. However, lines should not be excessively
long or dense; use discretion to break lines at logical points (e.g., after method calls, between parameters) to enhance readability. 

This applies to all source code, including XML documentation comments, where longer lines may be necessary for clarity. The goal is to write code
that is easy to read and understand, not to conform to an arbitrary line length limit.

### Line Length

Source code should aim for a **maximum line length of 160 characters**.

Guidelines:

- Prefer readability over strict wrapping.
- Avoid wrapping fluent APIs or method chains unnecessarily.
- Long log messages, LINQ queries, and JSON payload builders may exceed this when it improves clarity.
- XML documentation comments may exceed this limit when needed.

Example (preferred):

```csharp
logger.LogInformation("Published certificate state: StateQueue={StateQueue}, BroadcastExchange={Exchange}, NotAfter={NotAfter}",
    StateQueueName, BroadcastExchangeName, current?.NotAfter);
```

### Section Organization

- Use `#region` / `#endregion` directives to organize code sections within classes.
- Standard region order: Constants, Fields, Properties, Constructors, Public Methods, Private Methods (grouped by responsibility), Nested Types.
- In `Program.cs`, local functions are organized by region: Configuration, Process Tuning, Exception Handlers, Logging, Infrastructure, HTTP Clients.
- Do **not** use decorative comment-block separators (e.g., `// ═══... or // ───...`) as section dividers.

### Naming Conventions

| Element              | Convention       | Example                                          |
|----------------------|------------------|--------------------------------------------------|
| Private fields       | `_camelCase`     | `_reconcileLock`, `_poolSnapshots`                |
| Constants            | `PascalCase`     | `MaxBackOffMS`, `BaseRestartDelayMS`              |
| Static readonly      | `PascalCase`     | `PollInterval`, `JsonOptions`                     |
| Public properties    | `PascalCase`     | `GrabberConfig`, `Hostname`                       |
| Local variables      | `camelCase`      | `earlyConfig`, `minWorkerThreads`                 |
| Parameters           | `camelCase`      | `stoppingToken`, `workerCount`                    |
| Enum values          | `PascalCase`     | `Found`, `WaitingToRestart`                       |
| Config section names | `const string`   | `public const string SectionName = "..."`         |

### RADIUS Protocol Enum Naming Exception (SCREAMING_CASE)

All enums in the `NNRPD.Radius` namespace that represent **RADIUS Attribute Names**, **Attribute Values**, or **Packet Codes** are an intentional exception to the PascalCase enum convention above. These identifiers use **SCREAMING_CASE** (`UPPER_SNAKE_CASE`) to maintain a 1:1 correspondence with the canonical IANA RADIUS dictionary names and RFC definitions.

**This applies to:**

- `RadiusCode` — packet type codes (e.g., `ACCESS_REQUEST`, `ACCOUNTING_RESPONSE`).
- `RadiusAttributeType` — attribute type identifiers (e.g., `USER_NAME`, `NAS_IP_ADDRESS`, `SERVICE_TYPE`).
- Attribute value enums — `SERVICE_TYPE`, `FRAMED_PROTOCOL`, `FRAMED_ROUTING`, `FRAMED_COMPRESSION`, `LOGIN_SERVICE`, `TERMINATION_ACTION`, `ACCT_STATUS_TYPE`, `ACCT_AUTHENTIC`, `ACCT_TERMINATE_CAUSE`, `NAS_PORT_TYPE`, `TUNNEL_TYPE`, `TUNNEL_MEDIUM_TYPE`, `ARAP_ZONE_ACCESS`, `PROMPT`, `INGRESS_FILTERS_VALUE`, `ERROR_CAUSE`, `NAMESPACE_IDENTIFIER`, `LOCATION_PROFILES`, `LOCATION_INFORMATION_SOURCE`, `BASIC_LOCATION_POLICY_RULES`, `LOCATION_CAPABLE`, `REQUESTED_LOCATION_INFO`, `FRAMED_MANAGEMENT_PROTOCOL`, `MANAGEMENT_TRANSPORT_PROTECTION`, `EAP_LOWER_LAYER`, `FRAG_STATUS`.

**Do not:**

- Rename these identifiers to PascalCase or any other convention.
- Apply IDE naming suggestions, code-style fixers, or bulk renames to these identifiers.
- Suppress or override the SCREAMING_CASE convention for any new RADIUS enum added to `RadiusEnums.cs`.

**Rationale:** Renaming would break the direct mapping to the RADIUS protocol specification, make wire-level debugging harder, and invalidate `.ToString()` output used in structured logging and diagnostics.

### Suffix Conventions for Constants

- Time-related constants use a unit suffix: `ms` for milliseconds, `Seconds` for seconds.
  - Example: `MaxBackOffms = 60_000`, `DefaultKeepAliveSeconds = 120`.
- Use digit separators (`_`) in large numeric literals for readability: `60_000`, `1_099_511_627_776`.

### Access Modifiers

- Always specify explicit access modifiers on all types, members, and nested types.
- Prefer `sealed` on classes that are not designed for inheritance.
- Use `readonly` on `record struct` types and fields wherever possible.

### Nullable Reference Types

- The project has nullable reference types enabled globally.
- Use `?` on types that are legitimately nullable; never suppress warnings with `!` unless a DI guarantee is documented.

## XML Documentation Standards

### Required Documentation

Every **public** type, method, property, constant, and field must have a `<summary>` tag. No exceptions.

### Documentation Depth

- **Classes**: Include `<summary>`, `<remarks>` with lifecycle, thread-safety notes, and relationship to other types.
- **Methods**: Include `<summary>`, `<remarks>` (with numbered `<list type="number">` for multi-step flows), `<param>`, `<returns>`, and `<exception>` where applicable.
- **Properties**: Include `<summary>`, `<remarks>` explaining validation, default values, and how/where the property is consumed.
- **Constants / Fields**: Include `<summary>` and `<remarks>` explaining the value's derivation and where it is used.

### Remarks Style

- Use `<para>` blocks inside `<remarks>` for paragraph separation.
- Use **bold lead-ins** with `<b>` tags: `<b>Lifecycle:</b>`, `<b>Thread safety:</b>`, `<b>Failure handling:</b>`.
- Use `<list type="number">` for sequential steps, `<list type="bullet">` for unordered items.
- Use `<see cref="..." />` for all cross-references to types, methods, properties, and parameters.
- Use `<c>code</c>` for inline code references (values, config keys, NNTP commands).
- Use `<code>` blocks for multi-line examples (e.g., JSON configuration snippets).

### Primary Constructor Parameters

- Document each primary constructor parameter in the class-level `<summary>` or `<remarks>` block, not separately above the parameter list.

### XML Documentation Example

    /// <summary>
    /// Sends a best-effort NNTP <c>QUIT</c> command and disposes all connection resources.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Best-effort:</b> The QUIT command is bounded by <see cref="QuitTimeout"/> (2 s). If the server is
    /// unresponsive, the timeout fires and disposal proceeds immediately.
    /// </para>
    /// <para>
    /// <b>Disposal order:</b>
    /// </para>
    /// <list type="number">
    ///   <item><description>Send QUIT command with timeout.</description></item>
    ///   <item><description>Dispose <see cref="PipeReader"/>.</description></item>
    ///   <item><description>Dispose <see cref="SslStream"/> (if TLS).</description></item>
    ///   <item><description>Dispose <see cref="Socket"/>.</description></item>
    /// </list>
    /// </remarks>
    private async Task QuitAndDisposeAsync()

### File Header Comments

### File Header Comments

Each `.cs` file begins with a block comment summarizing the file's purpose, lifecycle, I/O architecture, and key call sequences. This is a plain `//` comment block (not XML doc), separated from the `using` directives by a blank line.

The first line **must** begin with the filename (with or without the `.cs` extension) followed by an em-dash (`—`) or double-hyphen (`--`) and a brief one-line summary of the file's responsibility:

// ConnectionHolder.cs — thread-safe singleton that holds the shared RabbitMQ IConnection.


For partial class files, include the partial-file suffix in the filename:

// NodeIdentity.Logging.cs — Source-generated [LoggerMessage] partial methods for NodeIdentity.

Subsequent lines provide additional detail: lifecycle, threading model, caller/callee relationships, cross-platform notes, and SIMD applicability.


## Logging Standards

### Structured Logging

- Use **Serilog's structured logging** with named placeholders (`{PropertyName}`) instead of string interpolation.
- Log property names use `PascalCase`: `{WorkerId}`, `{Backbone}`, `{ConfigEndpoint}`.

### Source-Generated Logging with [LoggerMessage]

All new logging should use the **`[LoggerMessage]` source generator** pattern from `Microsoft.Extensions.Logging` for compile-time validation, zero-allocation logging, and consistent structure:

- Classes receive `ILogger<T>` via DI constructor injection.
- Log methods are declared as `partial` methods with the `[LoggerMessage]` attribute.
- Log methods are placed in a dedicated `*.Logging.cs` partial file (e.g., `ConnectionFactory.Logging.cs`).
- Each `[LoggerMessage]` method specifies `Level` and `Message` explicitly.
- Event IDs should be unique within a class and assigned sequentially.
- Message strings must contain **only ASCII characters** (see ASCII-Only Log Messages below).

Example:

```csharp
// ConnectionFactory.Logging.cs
internal sealed partial class ConnectionFactory
{
    [LoggerMessage(Level = LogLevel.Information,
        Message = "RabbitMQ connecting -- Endpoints=[{Endpoints}], VHost={VirtualHost}, SSL={EnableSsl}, " +
                  "Heartbeat={Heartbeat}s, Recovery={Recovery}s, FrameMax={FrameMax}")]
    private partial void LogConnectionAttempt(string endpoints, string virtualHost, bool enableSsl,
        double heartbeat, double recovery, uint frameMax);
}
```

**Exception note:** Serilog's static `Log.ForContext(...)` pattern (`Serilog.ILogger`) should only be used in POCO types that cannot receive DI-injected loggers (e.g., `RabbitMQOptions` where `IValidatableObject.Validate` runs during options binding before DI is fully available).

### ASCII-Only Log Messages

All `[LoggerMessage]` `Message` strings must contain **only ASCII characters** (U+0020–U+007E). This ensures log
output is readable regardless of the terminal or log-viewer encoding (Windows-1252, Latin-1, UTF-8 without BOM).

Substitution rules for common Unicode characters:

| Unicode Character | Replacement | Example |
|---|---|---|
| `—` (em-dash, U+2014) | `--` | `"Connection stalled -- reconnecting..."` |
| `–` (en-dash, U+2013) | `-` | `"Range: [100-200]"` |
| `→` (right arrow, U+2192) | `->` | `"{Source} -> {Target}"` |
| `←` (left arrow, U+2190) | `<-` | |
| `≥` (greater-or-equal, U+2265) | `>=` | |
| `≤` (less-or-equal, U+2264) | `<=` | |

**Scope:** This rule applies only to the `Message` string in `[LoggerMessage]` attributes. XML documentation
comments, file header comments, and C# source identifiers may continue to use Unicode characters since they
are not emitted to log output.

### Log Levels

| Level         | Usage                                                                        |
|---------------|------------------------------------------------------------------------------|
| `Trace`       | NNTP command/response wire-level detail (redacted credentials).              |
| `Debug`       | Per-article request lifecycle, throughput stats, client properties.           |
| `Information` | Startup banners, connection established, pool created/scaled, periodic status. |
| `Warning`     | Transient failures, config poll failures (< escalation threshold), non-standard configs. |
| `Error`       | Sustained control-plane unavailability, unobserved task exceptions, callback exceptions. |
| `Fatal`       | Configuration validation failure (hard-terminate), unhandled exceptions.      |

### Guard Clauses for Expensive Logging

- Always guard `Trace` and `Debug` log calls with `logger.IsEnabled(LogLevel.Xxx)` when the message requires formatting or computation (e.g., throughput stats, client property enumeration).

## Configuration Conventions

### Options Pattern

- All configuration sections use the strongly-typed **Options pattern** with `IOptions<T>`.
- Options classes include a `public const string SectionName` for the configuration key.
- Validation uses `[Required]`, `[Range]`, `[Url]`, and `IValidatableObject` for cross-property checks.
- Register with `.ValidateDataAnnotations().ValidateOnStart()` in the DI pipeline.


## Dependency Injection Patterns

## Error Handling Patterns

### Resource Disposal

- Dispose resources in a fixed order, each in its own `try`/`catch` to prevent one failure from blocking subsequent disposals.
- Use `Interlocked.Exchange` for double-dispose guards.
- Use `finally` blocks for metric gauge decrements and resource cleanup to guarantee execution on all exit paths.

### Cancellation

- Distinguish between global shutdown cancellation and individual worker cancellation using linked `CancellationTokenSource`.
- Always check `stoppingToken.IsCancellationRequested` in `catch (OperationCanceledException)` to differentiate timeout from shutdown.
