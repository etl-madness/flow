# Extended Session and Resource Analysis Report

## 1. HTTP Client Sessions
*   **`executor.go:1183`**: The `HTTPClientNode` uses `defer resp.Body.Close()`. If `io.ReadAll` (line 1185) hangs due to a slow server, the connection remains open. Without an explicit `http.Client` timeout, it relies entirely on the global context. If the global context has no deadline, the session can hang indefinitely.

## 2. File System Handles
*   **`executor.go:1382` & `1463`**: `FileSave` and `ExcelRead` nodes use `defer file.Close()`. During high-throughput parallel execution (`executeParallelNode`), the number of concurrent open file handles may spike, potentially leading to OS-level "too many open files" errors.

## 3. ETL Streaming Sessions (`StreamETL`)
*   **`etl.go:108`**: `StreamETL` defers `rows.Close()`. While it includes cooperative cancellation checks (`etl.go:172-177`), a failure in the underlying driver to respect the context could leave the source cursor open.
*   **`etl.go:43`**: `flushMSSQLBatch` initiates a transaction (`dstDB.BeginTx`). If the bulk insert operation hangs, the transaction remains open on the SQL Server, potentially holding exclusive locks on the target table.

## 4. Subprocess / Shell Sessions
*   **`executor.go:619-690`**: External scripts are spawned via `exec.CommandContext(ctx, ...)`. While this terminates the primary shell process, it may not propagate signals to grandchild processes (orphaned processes), depending on the shell and OS.

## 5. Redis and Etcd Sessions
*   **`registry.go:348-357`**: Redis and Etcd clients are managed in the central registry. While shared across threads, these require an explicit call to `CloseDatabases` at the end of the pipeline lifecycle to prevent leaking cluster connections.

## Summary Table

| Component | Location | Potential Issue | Impact |
| :--- | :--- | :--- | :--- |
| **HTTP Client** | `executor.go:1183` | Missing Client Timeout | Hung TCP connection on slow responses |
| **ETL Bulk** | `etl.go:43` | Long-lived `BeginTx` | Locked target tables in MSSQL |
| **Shell** | `executor.go:619` | Signal propagation | Orphaned child OS processes |
| **File I/O** | `executor.go:1382` | Concurrent handle limit | OS File Descriptor exhaustion |
| **Registry** | `registry.go:338` | Lifecycle dependency | Leaked Redis/Etcd connections |
