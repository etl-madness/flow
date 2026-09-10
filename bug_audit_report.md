# Bug Audit Report

## Summary
The bug audit revealed several logical issues, primarily regarding error handling and resource management. While concurrency primitives are used, there are potential leaks and ignored errors that could lead to instability.

## Findings

### 1. Resource Leaks
- **Medium**: In `executor.go:1540-1543`, `f.GetSheetIndex(sheet)` and `f.NewSheet(sheet)` errors are ignored (`_`). If these fail, subsequent operations on the sheet will fail or behave unexpectedly.
- **Medium**: In `executor.go:1567`, `rows.Columns()` error is ignored.
- **Medium**: In `executor.go:1569`, `excelize.CoordinatesToCellName` errors are ignored.
- **Medium**: In `executor.go:1588`, `excelize.CoordinatesToCellName` errors are ignored.

### 2. Error Handling (Ignored Errors)
- **Low**: Extensive use of `_ = ...` throughout the codebase:
    - `executor.go:106`: Interp cache check.
    - `executor.go:1542`: Sheet creation.
    - `executor.go:1757, 1895, 1914`: JSON unmarshalling.
    - `registry.go:248`: Directory creation.
    - `registry.go:311, 351, 355`: Client closing.
- This pattern hides failures and makes debugging difficult.

### 3. Race Conditions & Concurrency
- **Low**: `executor.go:881-948` implements a complex parallel execution and merge logic. While `resultsMu` and `varMu` are used, the merging of `dirtyVars` from worker registries back to the main registry is a critical section that could be prone to subtle race conditions if not perfectly synchronized.
- **Low**: `executor.go:50` (`resultsMu`) and `executor.go:57` (`interpMu`) are used correctly for basic protection, but the `activeTxs` map (`executor.go:55`) is accessed without a mutex in `getActiveTx` (`executor.go:144`), which will cause a panic if `Execute` is called in parallel or if `executeParallelNode` uses transactions.

### 4. Context Mismanagement
- **Low**: Most nodes propagate `ctx` correctly. However, in `registry.go:306`, a `context.Background()` is used for the Redis ping instead of a passed-in context, making the initialization phase non-cancelable.

## Recommendations
- Replace `_ =` with proper error checking and logging.
- Add a mutex to `activeTxs` in the `Executor` struct to prevent concurrent map access panics.
- Ensure all `excelize` and `sql` operation errors are handled, especially during loop iterations.
- Pass `ctx` into `InitDatabases` to allow cancellation of the startup sequence.
