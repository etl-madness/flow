# Executing Flow Pipelines

Flow provides two public execution methods on `Executor`:

```go
func (e *Executor) Execute(ctx context.Context, nodes []PipelineNode) ([]ScriptResult, error)
func (e *Executor) ExecuteRun(ctx context.Context, nodes []PipelineNode) (RunResult, error)
```

Both methods execute the same nodes through the same executor implementation. They honor the same `context.Context`, execute the same sequential, parallel, conditional, loop, transaction, and leaf-node behavior, and return a non-nil Go error when the pipeline fails or is canceled.

The difference is the returned result model.

## Choose an API

| Need | Use |
| --- | --- |
| Existing caller that reads script output or return codes | `Execute` |
| Flat results for a simple sequential pipeline | `Execute` |
| Run ID, overall run status, or start/finish timestamps | `ExecuteRun` |
| Structured failure diagnosis for nested, parallel, or looped nodes | `ExecuteRun` |
| Row-count reporting for SQL and bulk ETL | `ExecuteRun` |
| JSON events, tracing, metrics, or an audit trail | `ExecuteRun` with an `EventSink` |

## `Execute`: Flat Results

`Execute` returns a `[]ScriptResult`. Each result describes a script or leaf operation in a flat list:

```go
type ScriptResult struct {
    ScriptID      string
    ReturnCode    any
    ResultsString string
    Duration      string
}
```

Use it when application code needs the output from a step, such as a SQL query result saved to `ResultsString`, and does not need a complete execution graph.

```go
executor := flow.NewExecutor(registry)

results, err := executor.Execute(context.Background(), config.FlowNodes)
if err != nil {
    return fmt.Errorf("pipeline failed: %w", err)
}

for _, result := range results {
    fmt.Printf(
        "step=%s code=%v duration=%s\n%s\n",
        result.ScriptID,
        result.ReturnCode,
        result.Duration,
        result.ResultsString,
    )
}
```

`Execute` is fully compatible with callers written before run-level observability was introduced. It still collects the same flat `ScriptResult` values and returns the same generic execution error on failure.

### Handling a Failure with `Execute`

When a pipeline fails, `Execute` returns the results accumulated before execution stopped plus a non-nil error:

```go
results, err := executor.Execute(ctx, config.FlowNodes)
if err != nil {
    for _, result := range results {
        if result.ReturnCode != 0 {
            log.Printf("step %s failed: %v", result.ScriptID, result.ReturnCode)
        }
    }
    return err
}
```

This works well for a linear workflow, but it does not provide a run ID, parent/child links, typed error categories, or a stable way to distinguish repeated executions of the same node inside a loop.

## `ExecuteRun`: Structured Run Result

`ExecuteRun` returns one `RunResult`, including details for the entire execution and a `NodeResult` for every node that actually starts:

```go
type RunResult struct {
    RunID        string
    StartedAt    time.Time
    FinishedAt   time.Time
    Status       flow.RunStatus
    ErrorClass   flow.ErrorClass
    ErrorMessage string
    Nodes        []flow.NodeResult
}
```

Use it for production execution hosts, job runners, monitoring services, and operational tools. The method returns a populated `RunResult` even when execution fails or the context is canceled.

```go
executor := flow.NewExecutor(registry)

run, err := executor.ExecuteRun(context.Background(), config.FlowNodes)
log.Printf(
    "run=%s status=%s nodes=%d duration=%s",
    run.RunID,
    run.Status,
    len(run.Nodes),
    run.FinishedAt.Sub(run.StartedAt),
)

if err != nil {
    log.Printf(
        "run=%s class=%s error=%s",
        run.RunID,
        run.ErrorClass,
        run.ErrorMessage,
    )
    return err
}
```

### Inspect Failed Nodes

Each `NodeResult` has a unique `ExecutionID`, its configured XML `NodeID` when present, a readable `NodePath`, optional `ParentExecutionID`, status, attempts, error classification, and row counts.

```go
run, err := executor.ExecuteRun(ctx, config.FlowNodes)
if err != nil {
    for _, node := range run.Nodes {
        if node.Status != flow.RunStatusFailed && node.Status != flow.RunStatusCanceled {
            continue
        }

        log.Printf(
            "run=%s execution=%s node=%s path=%s class=%s error=%s",
            run.RunID,
            node.ExecutionID,
            node.NodeID,
            node.NodePath,
            node.ErrorClass,
            node.ErrorMessage,
        )
    }
    return err
}
```

For a nested XML structure, `NodePath` gives a stable structural location and `ParentExecutionID` points to the enclosing group, branch, loop, or assertion handler. Use `ExecutionID`, not `NodeID`, as the identity in external systems: a loop can execute one XML node multiple times.

### Read ETL Counts

Where the operation can measure them, `NodeResult.RowCounts` reports:

- `Affected` for SQL DML when the driver provides rows affected.
- `Read` and `Written` for bulk SQL ETL.

```go
run, err := executor.ExecuteRun(ctx, config.FlowNodes)
if err != nil {
    return err
}

for _, node := range run.Nodes {
    if node.NodeKind != "sql_bulk" {
        continue
    }
    log.Printf(
        "bulk node %s streamed read=%d written=%d",
        node.NodeID,
        node.RowCounts.Read,
        node.RowCounts.Written,
    )
}
```

An omitted count is not a confirmed zero; it means that operation does not currently expose that measurement.

## The Go Error Is Still Required

Do not replace the returned Go error with `RunResult.Status`. Use both:

```go
run, err := executor.ExecuteRun(ctx, config.FlowNodes)
if err != nil {
    switch run.Status {
    case flow.RunStatusCanceled:
        return fmt.Errorf("run %s canceled: %w", run.RunID, err)
    case flow.RunStatusFailed:
        return fmt.Errorf("run %s failed (%s): %w", run.RunID, run.ErrorClass, err)
    default:
        return err
    }
}
```

The error controls the caller's success path. `RunResult` supplies the structured context needed for logs, retries, alerting, or incident investigation.

## Add Events Without Changing Execution Semantics

Both APIs emit events when an event sink is configured, because both delegate to the same execution core. The sink does not change which method should be chosen; choose the API based on the return value your caller needs.

```go
executor := flow.NewExecutor(registry)
executor.SetEventSink(&flow.JSONLineSink{Writer: os.Stdout})

// Legacy caller: events are emitted, but the returned value is still []ScriptResult.
results, err := executor.Execute(ctx, config.FlowNodes)

// Observability-aware caller: the same events are emitted and the returned value is RunResult.
run, err := executor.ExecuteRun(ctx, config.FlowNodes)
```

Do not call both methods for the same work. Each call starts a new pipeline run with a new run ID and executes the nodes again. Use `ExecuteRun` as the single call when a caller needs both execution and run-level observability.

See [observability_usage.md](observability_usage.md) for JSON Lines, multiple sinks, and custom sink examples.
