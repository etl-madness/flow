# Release Notes - Flow Pipeline & ETL Engine (v1.4.1)

## Overview

Version **v1.4.1** delivers a comprehensive set of SQL connection pool, streaming ETL (`<sql_bulk>`), row-scanning (`<foreach>`, `<sql>`, `<excel_write>`, `<kv_bulk>`), HTTP transport pooling (`<http_client>`), variable interpolation (`{{VarName}}`), template compilation (`<template>`, `<html_template>`), and observability (`runtimeIdentity()`) performance optimizations. These improvements eliminate hot-loop memory allocations, mutex contention, redundant template parsing, per-request TCP/TLS handshakes, and synchronous DNS latency while maintaining full backwards compatibility with existing XML pipelines and Go API callers.

---

## 🌟 What's New in v1.4.1

### 1. Immediate High-Impact Fixes (Zero-Allocation Hot Paths, HTTP Pooling & Observability)
- **Row-Scan Buffer Hoisting & 1D Batch Arena (`StreamETL`, `<foreach>`, `<sql>`, `<excel_write>`, `<kv_bulk>`)**:
  - Hoisted `vals` (`[]interface{}`) and `scans`/`valPtrs` (`[]interface{}`) outside `for rows.Next()` loops across `StreamETL`, `executeSQLQuery`, `executeForEachNode` (streaming and buffered modes), `executeExcelWriteNode`, `executeBadgerBulk`, `executeBoltBulk`, and `executeEtcdBulk` to eliminate per-row slice allocations and GC pressure.
  - Added a pre-allocated 1D contiguous batch backing slice (`rowArena`) in `StreamETL` with UTF-8 string conversion (`utf8.Valid`) and explicit binary `[]byte` cloning so reuse of `scans`/`vals` never aliases driver-owned byte buffers across rows.
- **Process-Level Caching & Bounded DNS Timeout in `runtimeIdentity()` (`observability.go`)**:
  - Cached the OS username and FQDN hostname once per process using `sync.Once` (`identityOnce`, `cachedUserName`, `cachedHostname`).
  - Bounded reverse/forward DNS resolution in `fqdnHostname()` with a `250ms` `context.WithTimeout` on `net.DefaultResolver`, eliminating multi-second synchronous DNS stalls at the start of every pipeline execution in restricted or offline environments.
- **Concurrency-Safe `http.Transport` Pooling & Keep-Alive Reuse (`http_client.go`)**:
  - Added a package-level `*http.Transport` cache (`httpTransportCache`, bounded at `maxCachedHTTPTransports = 64`) keyed by a comparable `httpTransportKey` covering all 18 transport, TLS, and proxy attributes.
  - Defaulted `MaxIdleConnsPerHost` to `10` when unset so `<http_client>` requests inside `<foreach>` or `<retry>` loops reuse persistent TCP/TLS keep-alive connections instead of performing a fresh handshake per iteration.
  - Added `CloseIdleHTTPConnections()` to explicitly close idle keep-alive sockets across all cached HTTP transports when needed.

### 2. Intermediate Engine Optimizations (Single-Pass Interpolation, Batch Registry Updates & Template Caching)
- **Single-Pass $O(N)$ Variable Interpolation (`executor.go`, `registry.go`)**:
  - Replaced $O(V \times N)$ `strings.ReplaceAll` loops over the variable map with a single-pass $O(N)$ `{{...}}` scanner in `Executor.interpolateVars` and `Registry.InterpolateVars` (~12.4x faster at 100 registered variables, 1 allocation per call).
  - Includes a zero-allocation fast path when no `{{` token is present, preserves unresolved placeholders (such as Go template `{{.Field}}` expressions), and resolves the innermost `{{VarName}}` when nested inside triple braces (`{{{VarName}}}`).
- **Batch Variable Mutation (`Registry.SetVars` & `Registry.SetRowVars`) (`registry.go`, `executor.go`)**:
  - Added `Registry.SetVars(map[string]interface{})` for atomic multi-variable updates under a single write lock.
  - Added `Registry.SetRowVars(cols, lowerCols, upperCols []string, vals []interface{}, loopIndex int)` and pre-computed lowercase/uppercase column names once before `<foreach>` row loops, reducing `3 * N_cols + 1` mutex lock/unlock cycles per row down to `1`.
- **Bounded Compiled Template Caching (`executor.go`)**:
  - Added concurrency-safe, content-keyed caches (`templateCacheKey{name, content}`, bounded at `maxCachedTemplates = 256`) for `text/template` (`getOrParseTextTemplate`) and `html/template` (`getOrParseHTMLTemplate`) on `Executor`, eliminating repeated `template.New().Parse()` overhead in `<template>`, `<html_template>`, and `<http_client>` nodes.

### 3. SQL Connection Pool Tuning, `<sql_bulk>` Batching & Read-Lock Dispatch
- **Workload-Aware Database Pool Profiles & `conn_max_idle_time_seconds` (`config.go`, `registry.go`, `xsd/pipeline.xsd`)**:
  - Added `ConnMaxIdleTime` (`conn_max_idle_time_seconds` / `conn_max_idle_time`) to `DatabaseConfig`, `ParseXMLConfig`, and `DatabaseType` in `xsd/pipeline.xsd`.
  - Implemented `resolveDatabasePoolSettings` and `applyDatabasePoolSettings` in `registry.go`, automatically applying sensible defaults for `workload="oltp"`, `workload="bulk"`, `workload="analytics"`, and `workload="batch"` whenever individual pool attributes are not explicitly specified in XML.
- **Dialect-Aware `<sql_bulk>` Parameter Limits, `strings.Builder` & Prepared Statement Reuse (`etl.go`)**:
  - Raised dialect-specific parameter limits (`maxParametersForDriver`) from the flat `2,100` cap to `65,535` for PostgreSQL/MySQL, `32,767` for Oracle, and `32,766` for SQLite (`2,100` fallback for other drivers; native `mssql.CopyIn` TDS bulk protocol remains used for SQL Server).
  - Replaced `strings.Join` slice allocations with pre-sized `strings.Builder` in `buildMultiRowInsertSQL` (`80.6%` memory reduction and `61.4%` fewer allocations in `BenchmarkStreamETL`).
  - Added lazy prepared statement reuse (`fullBatchStmt`) across full-sized batches in `StreamETL`, preparing the multi-row `INSERT` once and reusing it for all subsequent full batches while falling back to direct `ExecContext` for a smaller trailing remainder batch.
- **Single Read-Lock Active Target Resolution (`executor.go`)**:
  - Converted `Executor.txMu` from `sync.Mutex` to `sync.RWMutex` and consolidated active transaction and pinned session connection lookups into `getActiveTarget(dbName string) (*sql.Tx, *sql.Conn)`, acquiring a single `RLock` across concurrent `<parallel>` branches.

---

## 🔄 Functional Behavior & Backwards Compatibility

Version **v1.4.1** is **100% backwards-compatible** with existing XML pipeline definitions and Go APIs. No existing XML attributes or exported Go symbols were removed or had their signatures broken.

Callers should be aware of the following minor behavioral nuances introduced by these optimizations:

1. **Single-Pass (Non-Recursive) Variable Interpolation (`{{VarName}}`)**:
   - `interpolateVars` and `Registry.InterpolateVars` now scan the input string once from left to right rather than iterating over the variable map with `strings.ReplaceAll` in non-deterministic Go map iteration order.
   - **Impact**: Interpolation is now deterministic and strictly single-pass. If variable `A`'s value literally contains the text `{{B}}`, it is emitted verbatim as `{{B}}` rather than sometimes expanding `{{B}}` depending on map iteration order. Unresolved tokens (such as Go template `{{.Field}}` syntax) and triple-brace wrappers (`{{{VarName}}}`) continue to work identically.
2. **Pooled HTTP Transports & Persistent Keep-Alive Connections**:
   - `<http_client>` nodes with identical transport, TLS, and proxy settings now share a pooled `*http.Transport` (with `MaxIdleConnsPerHost` defaulting to `10` when omitted) rather than creating an unpooled transport per call.
   - **Impact**: Idle TCP/TLS sockets remain open in the standard library's connection pool across requests for reuse. Long-lived processes that dynamically connect to dozens of transient hosts and wish to forcibly release idle sockets can call `flow.CloseIdleHTTPConnections()`.
3. **Process-Cached Runtime Identity (`runtimeIdentity()`)**:
   - `runtimeIdentity()` now resolves the OS user and FQDN hostname once per process (`sync.Once`) with a `250ms` DNS lookup timeout instead of querying the OS and DNS resolver on every `ExecutePipeline` invocation.
   - **Impact**: If the machine's hostname or DNS PTR record changes while a long-lived daemon process is already running, telemetry will continue reporting the hostname resolved at startup.
4. **Workload-Aware Pool Defaults & Larger Multi-Row `<sql_bulk>` Batches**:
   - Specifying `workload="bulk"`, `"analytics"`, `"batch"`, or `"oltp"` on `<database>` now populates unconfigured pool parameters (`max_open_conns`, `max_idle_conns`, `conn_max_lifetime_seconds`, `conn_max_idle_time_seconds`) with workload-specific defaults. Explicit XML attributes always take precedence over workload defaults.
   - Non-MSSQL `<sql_bulk>` targets (`postgres`, `mysql`, `oracle`, `sqlite`) now allow larger multi-row `INSERT` batches up to their respective driver parameter limits (`65,535`, `32,767`, `32,766`) when `batch_size` permits, instead of splitting every batch at `2,100` parameters.

---

## 📄 Updated Markdown Documents in v1.4.1

The following markdown documentation files were updated in **v1.4.1**:

1. [`README.md`](README.md) — Updated Key Features overview, `<database>` pool configuration examples, and added the v1.4.1 engine performance highlights (zero-allocation row scanning, dialect-aware `<sql_bulk>`, HTTP keep-alive pooling, $O(N)$ interpolation, and template caching).
2. [`docs/database.md`](docs/database.md) — Documented `conn_max_idle_time_seconds` (and alias `conn_max_idle_time`), workload-aware pool tuning profiles (`oltp`, `bulk`, `analytics`, `batch`), and pool sizing best practices.
3. [`docs/sql_sql_bulk.md`](docs/sql_sql_bulk.md) — Documented dialect-aware `<sql_bulk>` parameter limits (`65,535` for PostgreSQL/MySQL, `32,767` for Oracle, `32,766` for SQLite, native `CopyIn` for MSSQL), prepared statement reuse across full batches, and 1D batch arena memory behavior.
4. [`docs/variables.md`](docs/variables.md) — Documented single-pass $O(N)$ `{{VarName}}` variable interpolation semantics, `Registry.SetVars` / `Registry.SetRowVars` batch updates in `<foreach>`, and compiled template caching in `<template>` and `<html_template>`.
5. [`docs/http_client.md`](docs/http_client.md) — Documented automatic `*http.Transport` pooling (`httpTransportKey`), the default `max_idle_conns_per_host="10"` keep-alive behavior, and `flow.CloseIdleHTTPConnections()`.
6. [`release_notes.md`](release_notes.md) — Added the **v1.4.1** release notes, compatibility guide, updated documentation list, and component change matrix.

---

### 📋 v1.4.1 Component Change Matrix

| Component | Node Type / Function | Change Description | File |
| :--- | :--- | :--- | :--- |
| **ETL Engine** | `StreamETL` / `buildMultiRowInsertSQL` / `maxParametersForDriver` | Hoisted `scans`/`vals` buffers, 1D `rowArena` slice, dialect-aware parameter limits (`65535`/`32767`/`32766`/`2100`), `strings.Builder` SQL generation, and `fullBatchStmt` prepared statement reuse | `etl.go` |
| **Core Engine** | `executeSQLQuery` / `executeForEachNode` / `executeExcelWriteNode` / `execute*Bulk` | Hoisted `vals` and `valPtrs` outside `rows.Next()` loops; pre-computed column casing and single-lock `SetRowVars` in `<foreach>` | `executor.go` |
| **Core Engine** | `interpolateVars` / `Registry.InterpolateVars` | Single-pass $O(N)$ `{{VarName}}` scanner with fast-path return when no `{{` is present | `executor.go`, `registry.go` |
| **Core Engine** | `getOrParseTextTemplate` / `getOrParseHTMLTemplate` | Bounded (`256` max) content-keyed compiled `text/template` and `html/template` caches | `executor.go` |
| **Core Engine** | `getActiveTarget` (`txMu` `sync.RWMutex`) | Single `RLock` lookup for active `*sql.Tx` and pinned `*sql.Conn` across concurrent branches | `executor.go` |
| **Registry** | `SetVars` / `SetRowVars` / `resolveDatabasePoolSettings` | Batch variable mutation under a single lock and workload-aware SQL connection pool tuning (`oltp`, `bulk`, `analytics`, `batch`) | `registry.go` |
| **Configuration & XSD** | `DatabaseConfig` / `ParseXMLConfig` / `DatabaseType` | Added `ConnMaxIdleTime` (`conn_max_idle_time_seconds` / `conn_max_idle_time`) attribute support | `config.go`, `xsd/pipeline.xsd` |
| **HTTP Client** | `buildHTTPClient` / `getOrCreateHTTPTransport` / `CloseIdleHTTPConnections` | Concurrency-safe `*http.Transport` cache (`64` max) keyed by `httpTransportKey` with `MaxIdleConnsPerHost` defaulting to `10` | `http_client.go` |
| **Observability** | `runtimeIdentity` / `fqdnHostname` | `sync.Once` process-level identity caching and `250ms` bounded DNS lookup timeout | `observability.go` |
| **Tests & Benchmarks** | `BenchmarkInterpolateVars` / `BenchmarkStreamETL` / v1.4.1 unit tests | Added unit tests and benchmarks verifying all v1.4.1 performance and functional guarantees | `etl_test.go` |

---

# Release Notes - Flow Pipeline & ETL Engine (v1.3.6)

## Overview

Version **v1.3.6** introduces **Session Connection Pinning** on `<group>` (`session="true"` with aliases `pin_connection`, `pin_conn`, and `sticky`), enabling separate `<sql>`, `<sql_bulk>`, `<foreach>`, `<excel_write>`, `<kv_bulk>`, and nested `<group transaction="true">` nodes to share a dedicated physical database connection (`*sql.Conn`) and persist session-scoped temporary tables (such as `#Temp` in SQL Server, `CREATE TEMP TABLE` in PostgreSQL/SQLite, and `CREATE TEMPORARY TABLE` in MySQL) without holding long-lived transaction locks across the entire workflow. It also expands `<group>` attribute support (`name` and `val` aliases), updates `xsd/pipeline.xsd` and `ValidateAST`, and adds comprehensive documentation for all supported `<group>` options.

---

## 🌟 What's New in v1.3.6

- **Session-Pinned `<group>` (`session="true"`) for Temporary Table Persistence**:
  - Acquires and pins a dedicated `*sql.Conn` from the database pool (`db.Conn(ctx)`) for the duration of the `<group>` block (or reuses an ancestor's pinned connection when nested on the same database) and automatically returns it to the pool on group exit.
  - Supports attribute aliases `session="true"`, `pin_connection="true"`, `pin_conn="true"`, and `sticky="true"`.
  - Routes all child `<sql>`, `<sql_bulk>`, `<foreach>`, `<excel_write>`, and `<kv_bulk>` queries/DML for the target database through the pinned `*sql.Conn`, allowing individual steps to auto-commit immediately while preserving session-scoped temporary tables (`#Temp`, `TEMP TABLE`) and session settings (`SET NOCOUNT ON`, `SET LOCAL`, etc.).
- **Nested Atomic Transactions Inside Pinned Sessions**:
  - When a `<group transaction="true" db="...">` is nested inside (or combined on the same node with) a `<group session="true" db="...">`, the transaction is started directly on the pinned `*sql.Conn` (`conn.BeginTx(ctx, nil)`) rather than checking out a separate connection from `*sql.DB`.
  - Enables multi-step ETL workflows to create and bulk-load `#Temp` tables outside a transaction, and then wrap only the final publish/merge steps in an atomic transaction that still has full visibility into the session's `#Temp` tables.
- **Single-Connection Cursor Buffering in `StreamETL` & `<foreach>`**:
  - Extended `ETLOptions` and `StreamETL` in `etl.go` (including `flushMSSQLBatch`) to accept pinned connections (`SrcConn`, `DstConn`) and active transactions (`SrcTx`, `DstTx`).
  - Automatically buffers source rows and closes the source `*sql.Rows` cursor before executing destination batch writes when `src_db` and `dst_db` share the same pinned `*sql.Conn` or `*sql.Tx`, preventing single-connection mutex contention in `database/sql`.
  - Updated `<foreach>` (`executeForEachNode`) to automatically buffer driver query rows when an active transaction or pinned session connection is present on the driver database, releasing the connection before child nodes execute queries on that same session.
- **Expanded `<group>` Attribute Aliases, XSD Schema & AST Validation**:
  - Added `name` (alias for `id`) and `val` (alias for `equals`) attribute support on `<group>` in `config.go`.
  - Updated `GroupType` in `xsd/pipeline.xsd` with `session`, `pin_connection`, `pin_conn`, `sticky`, and `name` attributes.
  - Updated `ValidateAST` in `validator.go` to enforce `db` / `database` presence and database registration for `session="true"` groups.
- **Documentation for Supported `<group>` Options**:
  - Added comprehensive `<group>` documentation covering all attributes, aliases, and execution modes in `docs/groups.md`, `docs/summary_of_available_components.md`, and `docs/transactions.md`.

### 📋 v1.3.6 Component Change Matrix

| Component | Node Type / Function | Change Description | File |
| :--- | :--- | :--- | :--- |
| **Core Engine** | `<group session="true">` | Pinned `*sql.Conn` stack (`activeConns`), connection lifecycle, and nested transaction binding (`activeConn.BeginTx`) | `executor.go` |
| **Core Engine** | `querySQLContext` / `execSQLContext` | Unified SQL routing prioritizing `activeTxs` → `activeConns` → `*sql.DB` pool across `<sql>`, `<foreach>`, `<excel_write>`, and `<kv_bulk>` | `executor.go` |
| **ETL Engine** | `StreamETL` / `flushMSSQLBatch` | Support for `SrcConn`, `DstConn`, `SrcTx`, `DstTx` and automatic cursor buffering on shared single-connection ETL | `etl.go`, `executor.go` |
| **XML Parser** | `parseNodeElement` (`group`) | Parsed `session`, `pin_connection`, `pin_conn`, `sticky`, `name`, and `val` attributes | `config.go` |
| **AST Validator** | `ValidateAST` (`NodeGroup`) | Validated required `db` attribute and registered database lookup when `session="true"` | `validator.go` |
| **XSD Schema** | `GroupType` | Added `session`, `pin_connection`, `pin_conn`, `sticky`, and `name` attributes | `xsd/pipeline.xsd` |
| **Documentation** | Guides & Reference | Added `<group>` reference guide and updated component and transaction docs | `docs/groups.md`, `docs/summary_of_available_components.md`, `docs/transactions.md` |

---

# Release Notes - Flow Pipeline & ETL Engine (v1.3.5)

## Overview

Version **v1.3.5** introduces **flowcrypto**, bringing built-in cryptographic support for Flow pipelines and ETL workflows.

---

## 🌟 What's New in v1.3.5

- **flowcrypto Support**:
  - Added built-in flowcrypto capabilities for secure hashing, encryption, and related cryptographic operations in Flow pipelines.

---

# Release Notes - Flow Pipeline & ETL Engine (v1.3.4)

## Overview

Version **v1.3.4** introduces embedded **Lua** scripting (`<script language="lua">`) powered by a pure-Go Lua VM (`gopher-lua`), external script file sourcing via `file_path` (with `file` and `path` aliases), first-class `<![CDATA[...]]>` block support across all script sources, and aligned XSD schema (`xsd/pipeline.xsd`) and AST validation rules.

---

## 🌟 What's New in v1.3.4

- **Embedded Lua Scripting (`<script language="lua">`)**:
  - Executes Lua scripts in-process using the pure-Go `github.com/yuin/gopher-lua` VM with full `context.Context` cancellation and timeout propagation.
  - Exposes a global `vars` bridge table (`vars.Get`, `vars.GetString`, `vars.GetInt`, `vars.GetNumber`, `vars.GetFloat`, `vars.GetBool`, `vars.GetAll`, and `vars.Set`) for reading and mutating pipeline variables directly from Lua (supporting both `vars.Func(...)` and `vars:Func(...)` call styles).
  - Captures Lua `print(...)` output directly into `ScriptResult.ResultsString`, `LAST_OUTPUT`, and `output_var`, while also supporting `{{Variable}}` placeholder interpolation.
- **External Script File Sourcing (`file_path`, `file`, `path`)**:
  - `<script>` nodes can now load their executable code from external files via `file_path` (or aliases `file` and `path`).
  - File paths support dynamic `{{Variable}}` interpolation and automatic character encoding detection (UTF-8, UTF-8 BOM, UTF-16 LE, and UTF-16 BE via `DecodeTextBytes`).
- **First-Class `<![CDATA[...]]>` Support**:
  - Inline `<script>` bodies wrapped in `<![CDATA[ ... ]]>` are cleanly unwrapped so scripts containing `<`, `>`, and `&` require no XML escaping, with defensive stripping of literal `<![CDATA[...]]>` wrappers across inline XML, `file_path` files, and `var`-sourced scripts.
- **XSD Schema & AST Validator Updates**:
  - Updated `xsd/pipeline.xsd` (`ScriptType`) to include `<xs:enumeration value="lua" />` and optional `file_path`, `file`, and `path` attributes.
  - Updated `ValidateAST` in `validator.go` to allow `<script>` nodes with an empty inline body when `file_path` is provided.
- **Numeric Variable Conversion Hardening**:
  - Enhanced `Registry.GetVarInt` and `Registry.GetVarFloat` in `registry.go` to convert seamlessly across `int`, `int64`, `float32`, `float64`, and `string` values.

### 📋 v1.3.4 Component Change Matrix

| Component | Node Type / Function | Change Description | File |
| :--- | :--- | :--- | :--- |
| **Core Engine** | `<script language="lua">` | Embedded Gopher-Lua VM with `vars` host table and `print` capture | `executor.go` |
| **Core Engine** | `<script file_path="...">` | External script loading with `{{Var}}` interpolation and UTF-8/UTF-16 decoding | `config.go`, `executor.go` |
| **XML Parser** | `parseNodeElement` / `stripCDATAWrapper` | Added `lua`, `file_path`/`file`/`path` attributes, and CDATA unwrapping | `config.go` |
| **AST Validator** | `ValidateAST` (`NodeScript`) | Validated `<script>` nodes sourced via `file_path` | `validator.go` |
| **Registry** | `GetVarInt` / `GetVarFloat` | Cross-numeric type conversion (`int`, `int64`, `float32`, `float64`, `string`) | `registry.go` |
| **XSD Schema** | `ScriptType` | Added `lua` enumeration and `file_path`, `file`, `path` attributes | `xsd/pipeline.xsd` |
| **Documentation** | Guides & Reference | Added Lua, CDATA, and `file_path` examples and attribute tables | `README.md`, `docs/execution.md`, `docs/summary_of_available_components.md` |

---

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
