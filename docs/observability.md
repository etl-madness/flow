# Run-Level Observability Implementation Plan

## Status

Core implementation delivered: `ExecuteRun`, structured run/node/attempt results, lifecycle events, sink registration, and `JSONLineSink` are available. OpenTelemetry and metrics adapters remain separate integration work; they can consume the stable `EventSink` contract without changing the executor.

## Goals

The observability model must make one pipeline run diagnosable without parsing console output. It must provide:

* A unique run identifier and a definitive run status.
* UTC start and finish times plus run duration.
* A structured record for every executed node and every attempt.
* Parent/child execution relationships for groups, branches, loops, and assertion failure handlers.
* ETL row counts where the operation can determine them.
* Stable error categories suitable for metrics and alert routing.
* Pluggable sinks for JSON logging, OpenTelemetry, and metrics.
* Safe behavior under parallel execution, sink failures, cancellation, and sensitive data.

The existing `Execute` method and `ScriptResult` must remain supported. Existing users should be able to adopt observability without changing their XML definitions.

## Non-Goals

The first release will not persist execution history, implement retries, or expose arbitrary pipeline variables and payloads in events. Persistence, retry policies, and checkpointing can build on these contracts later.

## Proposed Public Contracts

The `observability.go` file in package `flow` provides the following concepts.

```go
type RunStatus string

const (
    RunStatusSucceeded RunStatus = "succeeded"
    RunStatusFailed    RunStatus = "failed"
    RunStatusCanceled  RunStatus = "canceled"
)

type ErrorClass string

const (
    ErrorClassCanceled       ErrorClass = "canceled"
    ErrorClassConfiguration  ErrorClass = "configuration"
    ErrorClassValidation     ErrorClass = "validation"
    ErrorClassDatabase       ErrorClass = "database"
    ErrorClassHTTP           ErrorClass = "http"
    ErrorClassFileSystem     ErrorClass = "filesystem"
    ErrorClassScript         ErrorClass = "script"
    ErrorClassTemplate       ErrorClass = "template"
    ErrorClassDataFormat     ErrorClass = "data_format"
    ErrorClassInternal       ErrorClass = "internal"
    ErrorClassUnknown        ErrorClass = "unknown"
)

type RowCounts struct {
    Read    int64 `json:"read,omitempty"`
    Written int64 `json:"written,omitempty"`
    Affected int64 `json:"affected,omitempty"`
}

type AttemptResult struct {
    Attempt       int       `json:"attempt"`
    StartedAt     time.Time `json:"started_at"`
    FinishedAt    time.Time `json:"finished_at"`
    Status        RunStatus `json:"status"`
    ErrorClass    ErrorClass `json:"error_class,omitempty"`
    ErrorMessage  string    `json:"error_message,omitempty"`
    RowCounts     RowCounts `json:"row_counts,omitempty"`
}

type NodeResult struct {
    ExecutionID       string          `json:"execution_id"`
    ParentExecutionID string          `json:"parent_execution_id,omitempty"`
    NodeID            string          `json:"node_id,omitempty"`
    NodeKind          string          `json:"node_kind"`
    NodePath          string          `json:"node_path"`
    StartedAt         time.Time       `json:"started_at"`
    FinishedAt        time.Time       `json:"finished_at"`
    Status            RunStatus       `json:"status"`
    Attempts          []AttemptResult `json:"attempts"`
    RowCounts         RowCounts       `json:"row_counts,omitempty"`
    ErrorClass        ErrorClass      `json:"error_class,omitempty"`
    ErrorMessage      string          `json:"error_message,omitempty"`
}

type RunResult struct {
    RunID       string       `json:"run_id"`
    StartedAt   time.Time    `json:"started_at"`
    FinishedAt  time.Time    `json:"finished_at"`
    Status      RunStatus    `json:"status"`
    ErrorClass  ErrorClass   `json:"error_class,omitempty"`
    ErrorMessage string      `json:"error_message,omitempty"`
    Nodes       []NodeResult `json:"nodes"`
}
```

`RunStatus` is deliberately used for both runs and nodes so dashboards can aggregate status uniformly. A skipped state should be introduced only when conditional and dependency reporting needs it; the initial release records only nodes that actually start.

`RowCounts` is additive. A node reports only quantities it knows: SQL DML reports `Affected`, query-to-Excel reports `Read` and `Written`, and bulk ETL reports source rows read plus destination rows written. Unknown values remain zero and must not be interpreted as an observed zero-row result unless the corresponding operation ran successfully.

## Execution Identity and Relationships

Generate a UUID run ID at the beginning of each observed execution. Generate an execution ID whenever the executor begins a node. The node result must include:

* `ParentExecutionID`: the execution ID of the enclosing group, branch, loop iteration, or assertion failure handler.
* `NodePath`: a stable structural address, such as `flow[0]/parallel[1]/script[0]`. Loop iterations append an index, for example `foreach[0]/iteration[42]/script[0]`.
* `NodeID`: the XML `id` when supplied. It is not globally sufficient because nodes may omit IDs and loops execute the same node many times.

Build an internal execution context that carries the run pointer, parent execution ID, and node path. Pass it through `executeNodes` and every `execute*Node` method. This replaces the current result-slice-only propagation with a richer internal state while retaining `ScriptResult` generation for compatibility.

Parallel branches must receive a shared, concurrency-safe run collector and their own derived parent/path context. Assign each event a monotonically increasing sequence number at collection time. Consumers can use that sequence for deterministic presentation while timestamps retain actual execution timing.

## Event Sink Contract

Add a small synchronous interface:

```go
type EventType string

const (
    EventRunStarted     EventType = "run.started"
    EventRunFinished    EventType = "run.finished"
    EventNodeStarted    EventType = "node.started"
    EventNodeFinished   EventType = "node.finished"
    EventAttemptStarted EventType = "attempt.started"
    EventAttemptFinished EventType = "attempt.finished"
)

type ExecutionEvent struct {
    Sequence           uint64    `json:"sequence"`
    Type               EventType `json:"type"`
    OccurredAt         time.Time `json:"occurred_at"`
    RunID              string    `json:"run_id"`
    ExecutionID        string    `json:"execution_id,omitempty"`
    ParentExecutionID  string    `json:"parent_execution_id,omitempty"`
    NodeID             string    `json:"node_id,omitempty"`
    NodeKind           string    `json:"node_kind,omitempty"`
    NodePath           string    `json:"node_path,omitempty"`
    Attempt            int       `json:"attempt,omitempty"`
    Status             RunStatus `json:"status,omitempty"`
    RowCounts          RowCounts `json:"row_counts,omitempty"`
    ErrorClass         ErrorClass `json:"error_class,omitempty"`
    ErrorMessage       string    `json:"error_message,omitempty"`
}

type EventSink interface {
    Emit(context.Context, ExecutionEvent) error
}
```

Add `SetEventSink(EventSink)` and `SetEventSinks(...EventSink)` to `Executor`. A fan-out sink should call registered sinks in order. By default, sink failures must not change pipeline success or failure; record them through an optional diagnostic callback and continue. Add `FailOnSinkError` only after a concrete delivery requirement exists.

Do not emit raw SQL, connection strings, HTTP authorization headers, request bodies, response bodies, or variable values. Events carry metadata and sanitized error messages only. Implement a redaction helper for errors before events or results are stored.

## Backward-Compatible Execution API

Keep the current API unchanged:

```go
func (e *Executor) Execute(ctx context.Context, nodes []PipelineNode) ([]ScriptResult, error)
```

Add an opt-in API:

```go
func (e *Executor) ExecuteRun(ctx context.Context, nodes []PipelineNode) (RunResult, error)
```

Implement `Execute` as a compatibility wrapper around the shared execution core, returning the legacy flattened results. `ExecuteRun` returns a full `RunResult` even when a pipeline fails or its context is canceled. The returned Go error remains the control-flow signal; `RunResult.Status`, `ErrorClass`, and `ErrorMessage` explain the outcome.

Use `ExecuteRun` as the only code path that initializes run state, emits `run.started`, runs nodes, finalizes unclosed state, emits `run.finished`, and computes final status. This prevents the two public methods from developing divergent behavior.

## Error Classification

Implement `ClassifyError(error) ErrorClass` using `errors.Is` and narrow type checks. Classification order should be:

1. Context cancellation and deadline errors map to `canceled`.
2. Parsing and semantic validation errors map to `validation` or `configuration`.
3. `*url.Error`, HTTP request construction, and non-success HTTP response policies map to `http`.
4. `*os.PathError` maps to `filesystem`.
5. Driver and `database/sql` errors map to `database`.
6. Yaegi, shell, and dotnet-script failures map to `script`.
7. XML, JSON, YAML, Excel, and template parsing failures map to `data_format` or `template`.
8. Unmatched errors map to `unknown`; internal invariants map to `internal`.

Preserve the original error as the Go return error. Classification is for observability and must never discard driver-specific diagnostic detail.

## Node Instrumentation Plan

1. Create a `runCollector` with mutex-protected node storage, sequence allocation, and event emission. It owns node start/finish bookkeeping and prevents duplicate finishes through `sync.Once` or explicit state checks.
2. Refactor `executeNodes` to receive an internal execution context. Start and finish a `NodeResult` around each actual leaf or structural node, including groups, parallel blocks, conditionals, loops, and assertions.
3. Refactor leaf node methods to return a typed internal outcome containing `ScriptResult`, `RowCounts`, and `error`. Continue appending the legacy result exactly once.
4. Record attempt one for every current execution. Keep `Attempts` as a slice now so the later retry feature adds attempts without changing the result schema.
5. Instrument SQL DML using `RowsAffected`; SQL reads using counted rows; `StreamETL` using its returned inserted count and a new source-read count result; file operations using bytes only in event attributes if that is later needed; Excel operations using records read/written; HTTP using status code as a future optional HTTP-specific field.
6. Ensure assertions that warn or continue end as successful node executions with assertion metadata in a future extension. Assertions that halt end as failed validation results. The nested `<on_failure>` nodes use the assertion execution ID as their parent.
7. Make transaction groups report their own status after commit or rollback. Child SQL nodes retain individual results so both the local failure and transaction outcome are visible.

## Built-In Adapters

Deliver adapters as independent Go types with no required XML configuration.

* `JSONLineSink`: writes one `ExecutionEvent` per line to an `io.Writer` using `encoding/json`; it is the default example and easiest integration for existing log collectors.
* `OpenTelemetrySink`: in a separate `otel` package or optional module, creates one run span and nested node/attempt spans. Keep OpenTelemetry dependencies out of the base package unless users explicitly install the integration package.
* `MetricsSink`: expose a small recorder interface rather than binding the core to Prometheus. Provide a Prometheus adapter later with counters for runs/nodes by status and error class, histograms for durations, and counters for ETL rows.

Map event attributes to low-cardinality values. Do not use run IDs, node paths containing loop indices, or raw error strings as metric labels.

## Test Plan

Add focused tests alongside the executor tests:

1. Successful sequential execution returns a populated `RunResult`, correct final status, run timestamps, and parent-child node links.
2. A failing SQL, file, HTTP, and script node produces the expected error class and exactly one node-finished event.
3. Context cancellation marks both the run and active node as canceled.
4. A parallel block records unique execution IDs, correct structural paths, race-free collection, and monotonically ordered event sequences.
5. A foreach loop distinguishes multiple executions of the same XML node by execution ID and iteration-specific path.
6. Transaction commit and rollback produce correct group and child statuses.
7. SQL DML and bulk ETL populate their applicable row counts.
8. Legacy `Execute` returns the same `ScriptResult` values as before for successful and failing flows.
9. A failing sink is isolated from execution and records a diagnostic without changing the run status.
10. JSON event output is valid, excludes configured secret values, and remains stable under concurrent nodes. Run the race detector for the parallel cases with `go test -race ./...`.

## Delivery Sequence

### Phase 1: Core Model

Add result/event types, a run collector, error classification, redaction, `ExecuteRun`, and the legacy `Execute` wrapper. Instrument scripts, SQL, HTTP, files, templates, and assertions. Add unit tests for status, errors, and compatibility.

### Phase 2: Structural Coverage and Counters

Instrument groups, parallel branches, conditionals, loops, transactions, Excel, XML, JSON, and YAML nodes. Add parent/path tests and row-count reporting for SQL and ETL. Verify with `go test -race ./...`.

### Phase 3: Sinks

Add `JSONLineSink`, sink fan-out, sink-isolation diagnostics, and documentation examples. Release the OpenTelemetry and metrics adapters as optional packages so the core library remains lightweight.

### Phase 4: Adoption and Stability

Document the JSON event schema as a compatibility contract, add an example consumer, and publish a migration note. Once the contract is stable, build retry telemetry, checkpoint events, and persistent run storage on top of the same run and node identities.

## Documentation and Examples

Update the README API reference when implementation begins. Include examples showing an `ExecuteRun` call, JSON Lines logging to a file or standard output, and how to inspect failed node results. Keep verbose console output available during the transition, but reimplement it as an event sink so console messages and structured telemetry describe the same lifecycle.
