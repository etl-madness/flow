# Database Connection and Session Analysis Report

## 1. Potential Connection Leaks & Hung Sessions
*   **`executor.go:295`**: In `executeSQLQuery`, `rows.Close()` is called via `defer`. If a query returns multiple result sets (`rows.NextResultSet()` at line 342), the connection remains active throughout the processing loop.
*   **`executor.go:755`**: In `executeForEachNode`, `rows.Close()` is deferred. If the inner `e.executeNodes` (line 801) takes a significant amount of time or hangs, the database cursor and connection remain open, potentially leading to hung sessions or blocking locks.
*   **`executor.go:1565`**: In `executeExcelWriteNode`, `rows.Close()` is deferred. The loop (line 1574) processes rows and writes to Excel; slow file I/O will hold the DB session open.

## 2. SQL Bulk Issues
*   **`executor.go:476`**: `executeSQLBulkNode` calls `StreamETL`. If `StreamETL` does not strictly honor the `context.Context` passed from the executor, bulk operations could become hung sessions on the SQL Server.

## 3. Long Running Tasks & Timeouts
*   **`registry.go:202`**: Connection pool settings are configurable. If `ConnMaxLifetime` is set too high, the application may maintain stale sessions.
*   **`executor.go:1043`**: Transactions are started via `dbConn.BeginTx(ctx, nil)`. If the `ctx` lacks a deadline, a hanging node within a transaction group (line 1059) will keep a database transaction open indefinitely, causing severe blocking.

## Summary Table

| Location | Issue | Risk |
| :--- | :--- | :--- |
| `executor.go:295` | Deferred `rows.Close()` | Connection held during result set processing |
| `executor.go:755` | Deferred `rows.Close()` | Session held during recursive node execution |
| `executor.go:1565` | Deferred `rows.Close()` | Session held during file I/O (Excel) |
| `executor.go:1043` | Tx without explicit timeout | Long-lived locks / Hung transactions |
