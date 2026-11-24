# Timeout & Duration Reference Guide

This guide explains how timeouts, deadlines, and connection lifetimes are configured and enforced across every node type, database connection pool, and execution scope in the `flow` pipeline engine.

---

## 1. Allowed Duration Formats

### Standard Go Duration Format (All Node `timeout` Attributes)

All node-level timeout attributes (`<sql>`, `<sql_bulk>`, `<group>`, and `<http_client>`) are parsed using Go's standard [`time.ParseDuration`](https://pkg.go.dev/time#ParseDuration) function.

A duration string is a sequence of decimal numbers, each with an optional fractional part and a required **unit suffix**:

| Unit Suffix | Meaning | Examples |
| :---: | :--- | :--- |
| `ns` | Nanoseconds | `500000ns` |
| `us` or `µs` | Microseconds | `250us` |
| `ms` | Milliseconds | `100ms`, `250ms`, `1500ms` |
| `s` | Seconds | `5s`, `30s`, `90s` |
| `m` | Minutes | `2m`, `10m`, `45m` |
| `h` | Hours | `1h`, `2h`, `24h` |

Compound durations and decimals are also valid:
- `"1m30s"` (1 minute, 30 seconds)
- `"1h15m"` (1 hour, 15 minutes)
- `"2.5m"` (2 minutes, 30 seconds)

> [!IMPORTANT]
> - **Unit Suffix Required on Nodes**: On `<sql>`, `<sql_bulk>`, `<group>`, and `<http_client>`, plain numbers without a unit suffix (e.g., `timeout="30"`) are **invalid** and will be ignored (falling back to the default or parent context deadline). Always include a unit such as `"30s"` or `"5m"`.
> - **No `"d"` (Days) Unit**: Go's `time.ParseDuration` does not support `"d"` for days. Use hours instead (e.g., `"24h"` or `"48h"`).
> - **Static Parsing on `<sql>`, `<sql_bulk>`, `<group>`, and `<http_client>`**: Timeout attributes are parsed directly as duration literals; they do not undergo `{{VarName}}` variable interpolation prior to `time.ParseDuration`.

### Dual Format on `<database>` Pool Attributes

Unlike node-level `timeout` attributes, `<database>` connection pool duration attributes (`conn_max_lifetime_seconds` and `conn_max_idle_time_seconds` / `conn_max_idle_time`) accept **either**:
1. A standard Go duration string (e.g., `"5m"`, `"30m"`, `"2m"`), **or**
2. A positive integer representing **seconds** (e.g., `"300"`, `"1800"`, `"120"`).

---

## 2. Node-by-Node Timeout Summary Matrix

| XML Element / Component | Timeout Attribute(s) | Default Value | Accepted Format | Scope & Enforcement Mechanism |
| :--- | :--- | :--- | :--- | :--- |
| **`<sql>`** | `timeout` | **Unlimited** *(inherits parent `ctx`)* | Go `time.Duration` (`> 0`) | Wraps query/DML execution in `context.WithTimeout` (`ExecContext` / `QueryContext`). |
| **`<sql_bulk>`** | `timeout` | **Unlimited** *(inherits parent `ctx`)* | Go `time.Duration` (`> 0`) | Wraps the entire `StreamETL` run (source query, row scan, and all destination batch flushes) in `context.WithTimeout`. |
| **`<group>`** | `timeout` | **Unlimited** *(inherits parent `ctx`)* | Go `time.Duration` (`> 0`) | Active **only** when `transaction="true"` (alias `tx="true"`) or `session="true"` (aliases `pin_connection`, `pin_conn`, `sticky`) is set. Applies a shared deadline across all child nodes in the group and automatically rolls back uncommitted transactions on timeout. |
| **`<http_client>`** | `timeout` | **`30s`** | Go `time.Duration` (`> 0`) | End-to-end `http.Client.Timeout` (dial + TLS + request + response headers + body read). Defaults to `30s` if omitted, `<= 0`, or invalid. Also honors parent `ctx` via `req.WithContext(ctx)`. |
| **`<http_client>`** *(Transport)* | `tls_handshake_timeout`<br>`idle_conn_timeout`<br>`response_header_timeout`<br>`expect_continue_timeout` | **`10s`**<br>**`90s`**<br>**`0` (unlimited)**<br>**`1s`** | Go `time.Duration` | Configures the pooled `*http.Transport` cloned from `http.DefaultTransport` (which also includes a `30s` TCP dial timeout and `30s` TCP keep-alive interval). |
| **`<script>`** (`shell`, `pwsh`, `bash`, `cmd`, `csx`, etc.) | *(None — use enclosing `<group>` or pipeline `ctx`)* | **Unlimited** *(inherits parent `ctx`)* | N/A | Uses `exec.CommandContext(ctx, ...)`. OS child process is killed when `ctx` expires. |
| **`<script language="lua">`** | *(None — use enclosing `<group>` or pipeline `ctx`)* | **Unlimited** *(inherits parent `ctx`)* | N/A | Binds `L.SetContext(ctx)` on the `gopher-lua` VM; halts execution immediately when `ctx` expires. |
| **`<script language="go">`** | *(None — use enclosing `<group>` or pipeline `ctx`)* | **Unlimited** *(inherits parent `ctx`)* | N/A | Exports `host/db.StreamETL` bound to `ctx`. Synchronous Go evaluation runs until completion or `StreamETL` context cancellation. |
| **`<foreach>`** / **`<loop>`** | *(None — use enclosing `<group>` or pipeline `ctx`)* | **Unlimited** *(inherits parent `ctx`)*<br>*(Buffer cap: `100,000` rows)* | N/A | Driver SQL query uses `querySQLContext(ctx, ...)`. Checks `ctx.Done()` before scanning every row in both streaming and buffered modes. |
| **`<while>`** | `max_iterations` (alias `max_loops`) *(iteration cap, not wall-clock)* | **`1000` iterations** *(inherits parent `ctx`)* | Positive integer | Checks `ctx.Done()` and `iterations < max_iterations` at the start of each loop pass. |
| **`<parallel>`** | *(None — use enclosing `<group>` or pipeline `ctx`)* | **Unlimited** *(inherits parent `ctx`)* | N/A | Derives a cancellable child context (`context.WithCancel(ctx)`). If **any** parallel branch fails or times out, all sibling branches are canceled immediately. |
| **`<kv>`** | *(None — use enclosing `<group>` or pipeline `ctx`)* | **Unlimited** *(inherits parent `ctx`)* | N/A | Passes `ctx` to `etcd` and `redis`/`valkey` client calls; checks `ctx.Done()` on every key during `bbolt` and `badger` prefix scans. |
| **`<kv_bulk>`** | *(None — use enclosing `<group>` or pipeline `ctx`)* | **Unlimited** *(inherits parent `ctx`)* | N/A | Source query uses `querySQLContext(ctx, ...)`, checks `ctx.Done()` on every scanned row, and passes `ctx` to `redis` pipeline and `etcd` batch commits. |
| **`<excel_write>`** | *(None — use enclosing `<group>` or pipeline `ctx`)* | **Unlimited** *(inherits parent `ctx`)* | N/A | Passes `ctx` to `querySQLContext(ctx, ...)` when streaming SQL query results into an Excel worksheet. |
| **`<if>`, `<assert>`, `<template>`, `<html_template>`, `<file_read>`, `<file_save>`, `<excel_read>`, `<xml_xpath>`, `<json_path>`, `<yaml_path>`** | *(None)* | **Unlimited** *(inherits parent `ctx`)* | N/A | `executeNodes` checks `ctx.Done()` before dispatching every node in a sequence; `<if>` and `<assert>` propagate `ctx` to child/fallback nodes. |

---

## 3. Detailed Node Timeout Behavior & Examples

### 3.1 `<sql>` Statement Timeout

The `<sql>` node accepts an optional `timeout` attribute. When specified with a valid duration `> 0`, `executeSQLNode` creates a child `context.WithTimeout(ctx, duration)` for that SQL statement (`QueryContext` for `SELECT`/`RETURNING`/`OUTPUT` queries, or `ExecContext` for DML/DDL statements).

- **Default**: None (inherits parent `ctx`).
- **Behavior on Timeout**: The database driver cancels the in-flight statement, `executeSQLNode` records `context deadline exceeded` as the node error, and the pipeline halts (or rolls back if inside a transaction).

```xml
<!-- Cancel query if it runs longer than 30 seconds -->
<sql id="fetch_recent_orders" db="AnalyticsDB" output_var="OrderCount" timeout="30s">
    SELECT COUNT(*) FROM dbo.Orders WHERE OrderDate >= '2026-01-01';
</sql>

<!-- Allow up to 15 minutes for an index rebuild -->
<sql id="rebuild_indexes" db="WarehouseDB" timeout="15m">
    ALTER INDEX ALL ON dbo.FactSales REBUILD;
</sql>
```

---

### 3.2 `<sql_bulk>` Streaming ETL Timeout

The `<sql_bulk>` node accepts an optional `timeout` attribute that bounds the **entire** streaming bulk transfer (`StreamETL`), including:
1. Executing the source `SELECT` query (`QueryContext`).
2. Streaming and scanning rows (`select { case <-ctx.Done(): ... }`).
3. Preparing and executing all destination multi-row `INSERT` batches or SQL Server TDS `CopyIn` batches.

- **Default**: None (inherits parent `ctx`).
- **Behavior on Timeout**: `StreamETL` stops reading/flushing immediately and returns the number of rows committed prior to `context.DeadlineExceeded`. Wrap `<sql_bulk>` in `<group transaction="true" db="...">` if you require all batches to roll back atomically on timeout.

```xml
<!-- Stream 10M rows with a 20-minute overall deadline -->
<sql_bulk id="archive_events"
          db="ProdDB"
          target_db="ArchiveDB"
          target_table="dbo.EventsArchive"
          batch_size="10000"
          timeout="20m">
    SELECT EventID, UserID, EventType, Payload, CreatedAt
    FROM dbo.Events
    WHERE CreatedAt &lt; '2026-01-01';
</sql_bulk>
```

---

### 3.3 `<group>` Transaction & Session Timeout

The `<group>` node supports a `timeout` attribute that establishes a shared execution deadline across **all child nodes** inside the group.

> [!WARNING]
> On `<group>`, the `timeout` attribute is **only applied when `transaction="true"` (or `tx="true"`) or `session="true"` (or `pin_connection="true"`, `pin_conn="true"`, `sticky="true"`) is enabled**. On a plain `<group>` without `transaction` or `session`, `timeout` has no effect.

- **Default**: None (inherits parent `ctx`).
- **Behavior on Timeout**:
  - The group context (`groupCtx`) expires with `context.DeadlineExceeded`.
  - Any child node (`<sql>`, `<sql_bulk>`, `<foreach>`, `<http_client>`, `<script>`, etc.) currently executing inside the group receives the cancellation signal immediately.
  - If `transaction="true"`, the active `*sql.Tx` is **automatically rolled back** (`tx.Rollback()`), or for a nested transaction on the same database, rolled back to its savepoint (`ROLLBACK TO SAVEPOINT` / `ROLLBACK TRANSACTION <sp>`) using an uncanceled background context.
  - If `session="true"`, the pinned `*sql.Conn` is released and closed/returned to the pool.

```xml
<!-- Atomic transaction group with a strict 2-minute deadline across all steps -->
<group id="settlement_tx" transaction="true" db="LedgerDB" timeout="2m">
    <sql id="debit_source" db="LedgerDB">
        UPDATE Accounts SET Balance = Balance - {{Amount}} WHERE AccountID = '{{SourceID}}';
    </sql>

    <!-- Child nodes can still specify a tighter individual timeout (e.g. 15s) -->
    <http_client id="notify_clearinghouse"
                 url="https://clearing.example.com/v1/settle"
                 method="POST"
                 timeout="15s">
        {"source": "{{SourceID}}", "amount": {{Amount}}}
    </http_client>

    <sql id="credit_destination" db="LedgerDB">
        UPDATE Accounts SET Balance = Balance + {{Amount}} WHERE AccountID = '{{DestID}}';
    </sql>
</group>

<!-- Session-pinned group (without a long-lived transaction) bounded to 10 minutes -->
<group id="temp_table_etl" session="true" db="AnalyticsDB" timeout="10m">
    <sql id="create_stage" db="AnalyticsDB">
        CREATE TEMP TABLE tmp_active_users (user_id INT PRIMARY KEY, tier TEXT);
    </sql>
    <sql_bulk id="load_stage" db="SourceDB" target_db="AnalyticsDB" target_table="tmp_active_users">
        SELECT id, tier FROM users WHERE active = 1;
    </sql_bulk>
    <sql id="merge_summary" db="AnalyticsDB">
        INSERT INTO user_tier_summary (tier, total_users)
        SELECT tier, COUNT(*) FROM tmp_active_users GROUP BY tier;
    </sql>
</group>
```

---

### 3.4 `<http_client>` Request & Transport Timeouts

`<http_client>` is the **only** pipeline node that enforces a non-zero timeout by default (`30s`), and it exposes both client-level and transport-level timeout attributes:

| Attribute | Underlying Go Field | Default Value | Description |
| :--- | :--- | :---: | :--- |
| `timeout` | `http.Client.Timeout` | **`30s`** | Total end-to-end request deadline: covers DNS lookup, TCP connect, TLS handshake, sending request body, waiting for response headers, and reading the full response body (up to 50 MB). If omitted, `<= 0`, or invalid, defaults to `30s`. |
| `tls_handshake_timeout` | `http.Transport.TLSHandshakeTimeout` | **`10s`** | Maximum time to wait for a TLS handshake to complete. |
| `idle_conn_timeout` | `http.Transport.IdleConnTimeout` | **`90s`** | Maximum time an idle keep-alive connection remains open in the pooled `*http.Transport` before being closed. |
| `response_header_timeout` | `http.Transport.ResponseHeaderTimeout` | **`0`** *(unlimited)* | Maximum time to wait for the server's response headers after fully writing the request body. When `0`, bounded by `timeout`. |
| `expect_continue_timeout` | `http.Transport.ExpectContinueTimeout` | **`1s`** | Maximum time to wait for a `100 Continue` response before sending the request body when `Headers` includes `Expect: 100-continue`. |

> [!NOTE]
> `<http_client>` transports are cloned from `http.DefaultTransport` and cached by their transport attributes (`maxCachedHTTPTransports = 64`), inheriting a **`30s` TCP dial timeout** and **`30s` TCP keep-alive probe interval** in addition to the settings above.

```xml
<http_client id="fetch_large_report"
             url="https://api.example.com/v2/reports/export"
             method="POST"
             content_type="application/json"
             timeout="2m"
             tls_handshake_timeout="5s"
             response_header_timeout="30s"
             idle_conn_timeout="60s"
             max_idle_conns_per_host="10"
             output_var="ReportPayload"
             status_code_var="HttpStatus">
    {"report_type": "monthly_ledger", "month": "{{ReportMonth}}"}
</http_client>
```

---

### 3.5 Bounding Nodes Without a Dedicated `timeout` Attribute (`<script>`, `<foreach>`, `<while>`, `<parallel>`, `<kv>`, `<kv_bulk>`)

Nodes such as `<script>`, `<foreach>`, `<while>`, `<parallel>`, `<kv>`, and `<kv_bulk>` do not have a standalone `timeout` attribute in XML, but they fully respect Go's `context.Context` cancellation:

1. **Wrap in a Session or Transaction `<group timeout="...">`** when the block is associated with a registered database.
2. **Pass a `context.WithTimeout` from Go** when invoking `Executor.Execute(ctx, ...)` or `Executor.ExecuteRun(ctx, ...)`.
3. **For `<while>` loops**, configure `max_iterations` (or `max_loops`, default `1000`) to bound loop execution count:

```xml
<while id="poll_job_status" var="JobState" equals="RUNNING" max_iterations="60">
    <http_client id="check_status"
                 url="https://api.example.com/jobs/{{JobID}}"
                 timeout="5s"
                 output_var="JobResponse" />
    <json_path id="extract_state" var="JobResponse" path="$.state" output_var="JobState" />
    <script id="wait_before_next_poll" language="pwsh">
        Start-Sleep -Seconds 2
    </script>
</while>
```

---

## 4. Database Pool & Driver Timeouts (`<database>`)

### 4.1 SQL Connection Pool Lifetimes (`conn_max_lifetime_seconds` & `conn_max_idle_time_seconds`)

Each `<database>` element configures Go's `database/sql` connection pool lifecycle:

- **`conn_max_lifetime_seconds`**: Maximum total age of a connection before `database/sql` closes and replaces it (`db.SetConnMaxLifetime`). Prevents stale connections across firewall/load-balancer resets.
- **`conn_max_idle_time_seconds`** (alias **`conn_max_idle_time`**): Maximum duration a connection can sit unused in the idle pool before being closed (`db.SetConnMaxIdleTime`). Automatically clamped so `ConnMaxIdleTime <= ConnMaxLifetime`.

If not explicitly set on `<database>`, defaults are chosen based on the `workload` profile:

| `workload` Profile | `max_open_conns` | `max_idle_conns` | `conn_max_lifetime_seconds` | `conn_max_idle_time_seconds` |
| :--- | :---: | :---: | :---: | :---: |
| `(omitted)` or `"oltp"` | `25` | `10` | **`5m`** (`300s`) | **`2m`** (`120s`) |
| `"bulk"` | `50` | `25` | **`30m`** (`1800s`) | **`10m`** (`600s`) |
| `"analytics"` | `15` | `8` | **`30m`** (`1800s`) | **`10m`** (`600s`) |
| `"batch"` | `20` | `10` | **`15m`** (`900s`) | **`5m`** (`300s`) |

```xml
<databases>
    <!-- Using Go duration strings -->
    <database name="PrimaryOLTP"
              driver="postgres"
              workload="oltp"
              conn_max_lifetime_seconds="10m"
              conn_max_idle_time_seconds="3m"
              connection_string="postgres://app:{{DBPass}}@db.internal:5432/orders?sslmode=require&amp;connect_timeout=10" />

    <!-- Using integer seconds -->
    <database name="BulkWarehouse"
              driver="sqlserver"
              workload="bulk"
              conn_max_lifetime_seconds="1800"
              conn_max_idle_time_seconds="600"
              connection_string="sqlserver://etl:{{DBPass}}@dw.internal:1433?database=Warehouse&amp;dial+timeout=10&amp;connection+timeout=30" />
</databases>
```

### 4.2 Built-In Key-Value Store Startup Timeouts

When `Registry.InitDatabases` or `Registry.InitDatabasesWithContext` opens non-SQL key-value stores, the engine applies the following built-in connection/lock timeouts:

| Driver (`driver` / `type`) | Built-In Timeout | Behavior |
| :--- | :---: | :--- |
| **`bbolt` / `bolt`** | **`2s`** | File lock acquisition timeout (`bbolt.Options{Timeout: 2 * time.Second}`). Fails fast instead of hanging indefinitely if another process holds an exclusive file lock. |
| **`etcd` / `etcdv3`** | **`5s`** | Initial gRPC dial timeout (`clientv3.Config{DialTimeout: 5 * time.Second}`). |
| **`redis` / `valkey`** | **`3s`** | Initial startup `PING` verification timeout (`context.WithTimeout(ctx, 3*time.Second)`). Additional dial/read/write timeouts can be passed in `connection_string` via URL query parameters (e.g., `redis://localhost:6379/0?dial_timeout=5s&read_timeout=3s&write_timeout=3s`). |
| **`badger` / `badgerdb`** | Immediate | Non-blocking directory lock validation on startup; fails immediately if locked by another process. |

---

## 5. Pipeline-Level Deadlines & Observability in Go

### Setting a Global Pipeline Timeout in Go

You can bound both database initialization and total pipeline execution time by passing a `context.WithTimeout` to `InitDatabasesWithContext` and `ExecuteRun` (or `Execute`):

```go
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"flow"
)

func main() {
	xmlBytes, err := os.ReadFile("pipeline.xml")
	if err != nil {
		log.Fatal(err)
	}

	cfg, err := flow.ParseXMLConfig(xmlBytes)
	if err != nil {
		log.Fatal(err)
	}

	reg := flow.NewRegistry()
	if err := reg.InitVariables(cfg.Variables); err != nil {
		log.Fatal(err)
	}

	// 1. Bound database initialization to 15 seconds
	initCtx, initCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer initCancel()
	if err := reg.InitDatabasesWithContext(initCtx, cfg.Databases); err != nil {
		log.Fatalf("database init timed out or failed: %v", err)
	}
	defer reg.CloseDatabases()

	// 2. Bound total pipeline execution to 30 minutes
	runCtx, runCancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer runCancel()

	exec := flow.NewExecutor(reg)
	runResult, err := exec.ExecuteRun(runCtx, cfg.FlowNodes)
	if err != nil {
		fmt.Printf("Pipeline ended with status=%s error_class=%s message=%s\n",
			runResult.Status, runResult.ErrorClass, runResult.ErrorMessage)
	}
}
```

### How Timeouts Appear in Observability & Telemetry

1. **Error Classification**: When a node or pipeline is terminated by `context.DeadlineExceeded` or `context.Canceled`, `ClassifyError` (`observability.go`) categorizes the failure as:
   - `Status`: `RunStatusCanceled` (`"canceled"`)
   - `ErrorClass`: `ErrorClassCanceled` (`"canceled"`)
2. **Internal Host Identity DNS Timeout**: At the start of the first pipeline run in a process, `runtimeIdentity()` resolves the machine's FQDN using a bounded **`250ms`** timeout (`context.WithTimeout(context.Background(), 250*time.Millisecond)`) and caches the result with `sync.Once` so slow or unreachable DNS servers never delay pipeline execution.
