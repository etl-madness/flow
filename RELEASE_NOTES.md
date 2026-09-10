# Release Notes - Flow Pipeline & ETL Engine (v1.2.31)

## Overview

This release delivers major architectural hardening, critical bug fixes, database/session lifecycle improvements, security enhancements, and high-scale performance optimizations across the **Flow** data pipeline orchestration and ETL engine.

---

## 🌟 Key Highlights

- **60M+ Row Hybrid ForEach Processing**: `<foreach>` loops now default to $O(1)$ constant-memory streaming (`rows.Next()`) capable of processing 60 million+ records with zero memory pressure. An opt-in in-memory buffer mode (`buffer="true"` or `mode="buffer"`, safety capped at 100,000 rows) is available to release database locks and cursors early for smaller iteration sets.
- **Thread-Safe Transaction Stack & Dialect-Aware Savepoints**: Replaced raw transaction pointers with a thread-safe, mutex-protected transaction stack (`activeTxs`). Added full support for nested transaction groups on the same database via dialect-aware savepoints: ANSI `SAVEPOINT` / `ROLLBACK TO SAVEPOINT` / `RELEASE SAVEPOINT` for PostgreSQL, SQLite, and MySQL; T-SQL `SAVE TRANSACTION` / `ROLLBACK TRANSACTION` (with automatic release omission) for Microsoft SQL Server; and Oracle `SAVEPOINT`, completely eliminating single-writer deadlocks.
- **Fail-Fast Parallel Worker Cancellation**: Multi-threaded `<parallel>` blocks now derive worker contexts using `context.WithCancel`. If any worker encounters an unrecoverable failure, sibling workers and queued jobs are cancelled immediately, releasing CPU, memory, and database connections.
- **Atomic Database Initialization Rollback**: `InitDatabasesWithContext` guarantees that if any database fails to initialize during batch startup, all connections opened during that batch are closed and evicted immediately, preventing orphaned connection pools and lingering file locks.
- **Full AST Semantic Validation**: Expanded `ValidateAST` to validate required attributes and enforce duplicate ID prevention across all 9 previously unhandled node types: `file_save`, `file_read`, `excel_read`, `excel_write`, `template`, `html_template`, `xml_xpath`, `json_path`, and `yaml_path`.
- **Binary Data Integrity in ETL**: `StreamETL` now verifies UTF-8 validity before casting byte arrays to strings. Arbitrary binary data (BLOBs, compressed payloads, encrypted content) is preserved verbatim without corruption.

---

## 🛡️ Security Fixes & Hardening

### 1. HTTP Client Isolation & Resource Limits
- **Transport State Isolation**: Switched from sharing global `http.DefaultTransport` pointers to `http.DefaultTransport.Clone()`. Custom TLS configs, proxies, and connection timeouts on individual `<http_client>` nodes can no longer leak into or mutate global client state.
- **Enforced Safety Timeouts**: Configured a default 30-second timeout when `<http_client>` omits explicit `timeout` attributes, preventing indefinite hangs on unresponsive external endpoints.
- **Response Read Capping (DoS Prevention)**: Wrapped HTTP response bodies in `io.LimitReader(resp.Body, 50*1024*1024)`, enforcing a 50 MB response size cap to defend against out-of-memory denial-of-service attacks from malicious or oversized upstream responses.
- **Socket & File Descriptor Leaks**: Explicitly closed response bodies immediately after buffered reads rather than relying on delayed GC finalization.

### 2. SQL DML Query Classification
- **Token-Aware Query Parser (`isDMLQuery`)**: Replaced fragile substring matching with a robust, token-aware classifier. Strips line comments (`--`), block comments (`/* ... */`), and XML `<![CDATA[...]]>` tags.
- **Elimination of False DML Matches**: Queries selecting columns such as `deleted_at`, `is_deleted`, `update_count`, or `insert_ts` are no longer misclassified as DML statements.
- **Row-Returning Statement Preservation**: Statements containing `RETURNING` or `OUTPUT` clauses are correctly dispatched to `QueryContext` to ensure generated identifiers and row counts are captured.

### 3. Registry & Sensitive State Protection
- **Bounded Redis Ping**: Health checks against Redis during registration now execute under scoped context timeouts rather than un-cancelable background contexts.
- **Directory Path Validation**: Explicitly verifies errors during directory creation in BoltDB (`os.MkdirAll`) to avoid silent write failures.

---

## 🐛 Bug Fixes & Stability Improvements

### Core Execution Engine
- **Shadowed C# Script Runner**: Removed a duplicate/shadowed execution branch in `executor.go` where `.csx` and `dotnet-script` were mistakenly routed to the Yaegi Go interpreter instead of the host .NET script engine.
- **Excel Reading Without Headers**: Fixed an issue in `executeExcelReadNode` where setting `header="false"` resulted in missing or malformed JSON keys. The engine now automatically generates normalized column keys (`col1`, `col2`, ...).
- **Excel Writing Transaction Resolution**: Fixed `executeExcelWriteNode` to look up active transaction handles from `activeTxs` before falling back to `registry.GetDB`, ensuring uncommitted rows inside a transaction group can be exported.
- **Excel Cursor Resource Cleanup**: Explicitly closed SQL row cursors prior to workbook serialization to avoid connection hogging during large file writes.
- **Key-Value Store Write Batching**:
  - `executeBadgerBulk`: Added periodic batch flushing (`wb.Flush()`) every `batchSize` records (default: 1000) to prevent unbounded RAM consumption.
  - `executeEtcdBulk`: Batched mutations into transactional commits (`etcdClient.Txn`) with graceful fallback to individual puts if transactions are unsupported on target proxy gateways.

### Concurrency & Memory
- **Concurrent Map Access Panic**: Updated `Registry.Snapshot()` to clone underlying database reference map containers, preventing concurrent read/write panics when parallel workers query the registry.
- **Semaphore Dispatch Deadlock**: Fixed a subtle Go channel trap in `executeParallelNode` where context cancellation could deadlock workers waiting on channel receive operations. Applied labeled loop breaking (`break Loop`) on context cancellation.

---

## 📋 Node & Component Change Matrix

| Component | Node Type / Function | Change Description | File |
| :--- | :--- | :--- | :--- |
| **AST Validator** | `ValidateAST` | Added semantic validation & duplicate ID checks for 9 omitted node kinds | `validator.go` |
| **AST Validator** | `<group>` / `<excel_write>` | Enforced validation checks for required database name and registration | `validator.go` |
| **HTTP Client** | `<http_client>` | Cloned transport, 30s default timeout, 50MB response cap | `http_client.go`, `executor.go` |
| **Registry** | `InitDatabasesWithContext` | Context-aware initialization with atomic rollback on partial failure | `registry.go` |
| **Registry** | `Snapshot` | Deep copy of database map containers for thread isolation | `registry.go` |
| **ETL Engine** | `StreamETL` | Validated UTF-8 before string conversion; preserved raw byte arrays | `etl.go` |
| **Core Engine** | `<foreach>` | Hybrid streaming ($O(1)$ memory, 60M+ scale) and buffer mode (`buffer="true"`) | `config.go`, `executor.go` |
| **Core Engine** | `<group>` | Thread-safe transaction stack with dialect-aware savepoint support (MSSQL `SAVE TRANSACTION`, ANSI `SAVEPOINT`) | `executor.go` |
| **Core Engine** | `<parallel>` | Fail-fast sibling cancellation and deadlock-free semaphore acquisition | `executor.go` |
| **Core Engine** | `<sql>` | Token-based `isDMLQuery` classifier; fixed `deleted_at` column handling | `executor.go` |
| **Core Engine** | `<excel_read>` | Added synthetic column key generation for headerless sheets | `executor.go` |
| **Core Engine** | `<excel_write>` | Transaction handle reuse and prompt cursor closure | `executor.go` |
| **Core Engine** | `<kv_bulk>` | Batch flushes for Badger and transactional batching for Etcd | `executor.go` |
| **XSD Schema** | `xsd/pipeline.xsd` | Added `timeout` and `tx` to `<group>`; `buffer`, `mode`, `stream` to `<foreach>`; `timeout` to `<sql>` & `<sql_bulk>`; `database` to `<excel_write>`; and `<html_template>` element alias | `xsd/pipeline.xsd`, `schema.go` |

---

## 🧪 Verification & Regression Testing

All changes have been validated through targeted test suites and full repository test runs:

- **Targeted Suite (`executor_test.go`)**:
  - `TestDMLClassification`: Verified comment stripping, column name isolation (`deleted_at`), and DML token checks.
  - `TestSQLExecutionDoesNotDropDeletedAtColumns`: Verified query result set parsing with `deleted_at` columns.
  - `TestForEachHybridStreamingAndBuffering`: Verified both streaming and buffered modes across datasets.
  - `TestNestedTransactions`: Verified nested transaction groups with dialect-aware savepoint commits and rollbacks.
  - `TestSavepointDialectSQL`: Verified dialect-accurate SQL generation across SQL Server (`SAVE/ROLLBACK TRANSACTION`), PostgreSQL, SQLite, MySQL, and Oracle.
  - `TestExcelReadWithoutHeader`: Verified headerless spreadsheet reading with synthetic column keys.
  - `TestASTValidationOmittedNodes`: Verified validation catches missing attributes across all newly covered node kinds.
  - `TestParallelFailFastCancellation`: Verified that worker errors cancel sibling goroutines immediately.
  - `TestDatabaseInitWithContextRollback`: Verified connection handle cleanup on partial startup failure.
  - `TestXSDAlignedNodeAttributes`: Verified parsing and configuration of newly aligned attributes and tags.
- **Race Detector**:
  - Executed `go test -race ./...` with 0 data races, 0 deadlocks, and clean passes across all packages.
