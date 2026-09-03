# Observability Usage Guide

Flow can return a structured record for each pipeline run and emit lifecycle events while work is executing. Use `ExecuteRun` when your application needs run status, node relationships, failures, or row counts. Continue using `Execute` when the existing `[]ScriptResult` result is sufficient.

## Run a Pipeline and Inspect Its Result

`ExecuteRun` returns a `RunResult` whether the run succeeds, fails, or is canceled. The Go error remains the signal for control flow; inspect the result for the run ID, classification, and individual node outcomes.

```go
ctx := context.Background()
executor := flow.NewExecutor(registry)

run, err := executor.ExecuteRun(ctx, config.FlowNodes)
if err != nil {
    log.Printf("run %s finished with %s: %s", run.RunID, run.ErrorClass, run.ErrorMessage)
}

log.Printf(
    "run=%s status=%s started=%s finished=%s nodes=%d",
    run.RunID,
    run.Status,
    run.StartedAt.Format(time.RFC3339),
    run.FinishedAt.Format(time.RFC3339),
    len(run.Nodes),
)

for _, node := range run.Nodes {
    log.Printf(
        "node=%s id=%s status=%s path=%s parent=%s error_class=%s error=%s",
        node.ExecutionID,
        node.NodeID,
        node.Status,
        node.NodePath,
        node.ParentExecutionID,
        node.ErrorClass,
        node.ErrorMessage,
    )
}
```

The run status is one of:

- `flow.RunStatusSucceeded`: every executed node completed successfully.
- `flow.RunStatusFailed`: a node failed and the executor stopped the pipeline.
- `flow.RunStatusCanceled`: the context was canceled or reached its deadline.

Each `NodeResult` currently has exactly one `AttemptResult`. The attempts collection is present so retry support can add future attempts without changing the result format.

## Configure `SetEventSink`

Call `SetEventSink` before `Execute` or `ExecuteRun` to register one destination for structured lifecycle events:

```go
executor := flow.NewExecutor(registry)
executor.SetEventSink(sink)

run, err := executor.ExecuteRun(ctx, config.FlowNodes)
```

The sink receives `run.started` before node work begins and `run.finished` after the executor has finalized the `RunResult`. Each executed node also emits `node.started`, `attempt.started`, `attempt.finished`, and `node.finished` events. Sinks do not require any XML configuration.

`SetEventSink` replaces every previously registered sink. Pass `nil` to disable event emission for later runs:

```go
executor.SetEventSink(&flow.JSONLineSink{Writer: os.Stdout})
_, _ = executor.ExecuteRun(ctx, config.FlowNodes)

executor.SetEventSink(nil)
_, _ = executor.ExecuteRun(ctx, config.FlowNodes) // Does not emit events.
```

For more than one destination, use `SetEventSinks` rather than calling `SetEventSink` repeatedly:

```go
executor.SetEventSinks(
    &flow.JSONLineSink{Writer: os.Stdout},
    auditSink,
)
```

### Print Failed Nodes Only

This sink sends a compact message to the standard logger only when an executed node fails or is canceled:

```go
type FailureLogSink struct{}

func (FailureLogSink) Emit(_ context.Context, event flow.ExecutionEvent) error {
    if event.Type != flow.EventNodeFinished || event.Status == flow.RunStatusSucceeded {
        return nil
    }

    log.Printf(
        "run=%s node=%s id=%s status=%s class=%s error=%s",
        event.RunID,
        event.ExecutionID,
        event.NodeID,
        event.Status,
        event.ErrorClass,
        event.ErrorMessage,
    )
    return nil
}

executor.SetEventSink(FailureLogSink{})
```

### Capture Events in a Test

Custom sinks may be invoked by concurrent pipeline branches. Protect mutable in-memory state with a mutex when collecting events:

```go
type MemorySink struct {
    mu     sync.Mutex
    events []flow.ExecutionEvent
}

func (s *MemorySink) Emit(_ context.Context, event flow.ExecutionEvent) error {
    s.mu.Lock()
    defer s.mu.Unlock()
    s.events = append(s.events, event)
    return nil
}

func (s *MemorySink) Events() []flow.ExecutionEvent {
    s.mu.Lock()
    defer s.mu.Unlock()
    return append([]flow.ExecutionEvent(nil), s.events...)
}

sink := &MemorySink{}
executor.SetEventSink(sink)

_, err := executor.ExecuteRun(context.Background(), config.FlowNodes)
if err != nil {
    t.Fatal(err)
}

events := sink.Events()
if events[0].Type != flow.EventRunStarted {
    t.Fatalf("first event = %s, want %s", events[0].Type, flow.EventRunStarted)
}
```

An error returned from `Emit` is intentionally ignored by the executor. This prevents a logging, tracing, or metrics outage from changing pipeline execution. A sink that requires reliable delivery should buffer or persist work internally and report its own health separately.

## Write JSON Lines Events

`JSONLineSink` writes one `ExecutionEvent` as a JSON object followed by a newline. This format works with common log collectors and makes it easy to filter by `run_id`, `node_kind`, `status`, or `error_class`.

```go
logFile, err := os.OpenFile("flow-events.jsonl", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
if err != nil {
    return err
}
defer logFile.Close()

executor := flow.NewExecutor(registry)
executor.SetEventSink(&flow.JSONLineSink{Writer: logFile})

run, err := executor.ExecuteRun(ctx, config.FlowNodes)
if err != nil {
    return fmt.Errorf("flow run %s failed: %w", run.RunID, err)
}
```

To write events to standard output, use `os.Stdout` as the writer:

```go
executor.SetEventSink(&flow.JSONLineSink{Writer: os.Stdout})
```

A typical event resembles this:

```json
{
  "sequence": 6,
  "type": "node.finished",
  "occurred_at": "2026-09-03T12:00:01Z",
  "run_id": "4b7e3b2a5d4f...",
  "execution_id": "c0a8c3f6452e...",
  "parent_execution_id": "7683f0a2a9bd...",
  "node_id": "load_customers",
  "node_kind": "sql_bulk",
  "node_path": "group[0]/sql_bulk[0]",
  "attempt": 1,
  "status": "succeeded",
  "row_counts": {"read": 50000, "written": 50000}
}
```

Events are emitted in this lifecycle order for a run and each node:

1. `run.started`
2. `node.started`
3. `attempt.started`
4. `attempt.finished`
5. `node.finished`
6. `run.finished`

The `sequence` field increases for each event within a run. Parallel branches may complete in different wall-clock order, but the sequence provides a single ordering for a consumer.

## Use Multiple Sinks

Register more than one destination with `SetEventSinks`:

```go
executor.SetEventSinks(
    &flow.JSONLineSink{Writer: os.Stdout},
    auditSink,
)
```

Calling `SetEventSink` or `SetEventSinks` replaces the previous sink configuration. Sinks are called for every event in registration order. A sink error is ignored so an unavailable telemetry destination cannot change pipeline execution; applications that need delivery guarantees should make the sink durable and monitor the sink independently.

## Implement a Custom Sink

Implement `EventSink` to forward events to a logging, tracing, or metrics system. An event sink can be called concurrently by parallel pipeline branches, so protect mutable state with a mutex, channel, or a concurrency-safe client.

```go
type MetricsSink struct {
    mu      sync.Mutex
    finished map[flow.RunStatus]int64
}

func (s *MetricsSink) Emit(_ context.Context, event flow.ExecutionEvent) error {
    if event.Type != flow.EventNodeFinished {
        return nil
    }

    s.mu.Lock()
    defer s.mu.Unlock()
    s.finished[event.Status]++
    return nil
}
```

Use the same pattern to create an OpenTelemetry bridge: start a span on `node.started`, key it by `ExecutionID`, use `ParentExecutionID` to find its parent span, attach non-sensitive fields such as `NodeKind`, `Status`, and `ErrorClass`, then end it on `node.finished`. The base `flow` package deliberately has no OpenTelemetry or metrics backend dependency.

## Navigate Nested Results

`RunResult.Nodes` is a flat collection because it remains easy to serialize and query. Reconstruct the execution tree by matching `ParentExecutionID` to `ExecutionID`.

`NodePath` is a readable structural address. For example, `group[0]/parallel[1]/script[0]` describes a script beneath a parallel block inside a group. `NodeID` is the XML `id` when configured, while `ExecutionID` uniquely identifies a specific execution. Use `ExecutionID` rather than `NodeID` for loops and parallel work, where the same configured node can execute more than once.

## Interpret Row Counts and Errors

`RowCounts` contains only measurements known to the executed node:

- SQL DML exposes `Affected` when the driver returns rows affected.
- Bulk SQL ETL exposes `Read` and `Written` from the number of streamed rows.
- Other nodes currently leave row counts empty.

An omitted count means the executor did not measure that value. Do not treat it as a confirmed zero.

`ErrorClass` is designed for filtering and alert routing. Current values include `canceled`, `validation`, `database`, `http`, `filesystem`, `script`, `template`, `data_format`, and `unknown`. Preserve the Go error returned by `ExecuteRun` for detailed handling; the classified fields are a summary rather than a replacement.

Events do not include SQL text, request/response bodies, headers, or pipeline variables. Common credential fragments in recorded error messages, such as `password=...` and `token=...`, are redacted. Applications should still avoid placing sensitive values in custom event fields or logs.

## Keep Existing Callers Unchanged

The original API remains available and keeps its existing behavior:

```go
results, err := executor.Execute(ctx, config.FlowNodes)
```

Adopt `ExecuteRun` where structured observability is needed. No XML changes are required for either API.
