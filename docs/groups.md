# `<group>` Reference Guide

The `<group>` element organizes child pipeline nodes into a sequential execution block. In addition to logical grouping, `<group>` supports:

1. **Conditional Execution** (`var` / `if_var` / `condition` + `equals` / `if_equals`)
2. **Atomic Database Transactions** (`transaction="true"` / `tx="true"`) with automatic nested savepoint support
3. **Session Connection Pinning** (`session="true"` / `pin_connection="true"` / `pin_conn="true"` / `sticky="true"`) for sharing temporary tables (`#Temp`, `TEMP TABLE`) and session state across steps without holding long-lived transaction locks
4. **Scoped Execution Timeouts** (`timeout="30s"`, `timeout="5m"`, etc.)

---

## 1. Supported Attributes

| Attribute | Aliases | Type | Mandatory | Default | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `id` | `name` | `string` | No | — | Unique identifier for the group node in execution results, observability events, and diagnostics. |
| `var` | `if_var` | `string` | No | — | Name of the pipeline variable evaluated before executing the group. If the condition does not match, the group and all child nodes are skipped. |
| `condition` | `cond` | `string` | No | — | Inline condition expression or variable name evaluated when `var` / `if_var` is not set. Supports `{{Var}} == 'val'` and `{{Var}} != 'val'` expressions. |
| `equals` | `val`, `value`, `if_val`, `if_equals` | `string` | No | `"true"` (when `var` is set without `==`/`!=`) | Expected value required for the group to execute when `var` or `if_var` is specified. Supports `{{Variable}}` interpolation. |
| `transaction` | `tx` | `boolean` | No | `false` | Wraps all child database operations on `db` in an atomic database transaction (`*sql.Tx`). Commits when all children succeed; rolls back automatically on error or context cancellation. |
| `session` | `pin_connection`, `pin_conn`, `sticky` | `boolean` | No | `false` | Pins a single physical database connection (`*sql.Conn`) from the pool for `db` across all child steps so session-scoped temporary tables and session settings persist. |
| `db` | `database` | `string` | Conditional | — | Name of the registered `<database>` connection. **Required** when `transaction="true"` or `session="true"`. |
| `timeout` | — | `string` | No | — | Scoped execution deadline for the group (e.g., `"30s"`, `"5m"`, or integer seconds `"60"`). Derives a child `context.WithTimeout` for the group and all nested steps. |
| `description` | — | `string` | No | — | Human-readable description of the group block. |

---

## 2. Execution Modes

### Mode A: Logical & Conditional Grouping

Use `<group>` to organize related steps and optionally gate their execution on a pipeline variable or inline condition expression.

```xml
<group id="run_if_enabled" var="ENABLE_CLEANUP" equals="true" timeout="2m" description="Optional cleanup steps">
    <sql id="purge_old_logs" db="app_db">
        DELETE FROM audit_logs WHERE created_at &lt; CURRENT_DATE - INTERVAL '90 days';
    </sql>
</group>
```

You can also use inline comparison expressions in `condition` (or `var`):

```xml
<group id="non_prod_seed" condition="{{ENV}} != 'production'">
    <sql id="seed_test_users" db="app_db">
        INSERT INTO users (username, role) VALUES ('test_admin', 'admin');
    </sql>
</group>
```

---

### Mode B: Atomic Database Transactions (`transaction="true"`)

When `transaction="true"` (or `tx="true"`) is enabled with `db="..."`, Flow begins a `*sql.Tx` before executing child nodes:
- If all child nodes succeed, Flow commits the transaction.
- If any child node fails or the context times out/cancels, Flow rolls back the transaction.
- **Nested Transactions (Savepoints)**: If a `<group transaction="true" db="X">` is nested inside an outer `<group transaction="true" db="X">`, Flow automatically reuses the outer transaction and issues dialect-aware savepoint commands (`SAVE TRANSACTION` / `ROLLBACK TRANSACTION` on SQL Server; `SAVEPOINT` / `ROLLBACK TO SAVEPOINT` / `RELEASE SAVEPOINT` on PostgreSQL, SQLite, MySQL, and Oracle).

```xml
<group id="order_settlement_tx" transaction="true" db="orders_db" timeout="30s">
    <sql id="debit_account" db="orders_db">
        UPDATE accounts SET balance = balance - 100 WHERE account_id = {{ACCOUNT_ID}};
    </sql>
    <sql id="create_receipt" db="orders_db">
        INSERT INTO receipts (account_id, amount) VALUES ({{ACCOUNT_ID}}, 100);
    </sql>
</group>
```

---

### Mode C: Session Connection Pinning (`session="true"`) for Temporary Tables

Database engines scope temporary tables (`#Temp` in SQL Server, `CREATE TEMP TABLE` in PostgreSQL/SQLite, `CREATE TEMPORARY TABLE` in MySQL) to the **physical session/connection**. Because `*sql.DB` is a connection pool, separate `<sql>` and `<sql_bulk>` steps normally execute on different pooled connections.

Setting `session="true"` (or `pin_connection="true"`, `pin_conn="true"`, `sticky="true"`) pins a single `*sql.Conn` for the specified `db` across the entire group:
- Every child `<sql>`, `<sql_bulk>`, `<foreach>`, `<excel_write>`, and `<kv_bulk>` targeting `db` executes on that same pinned connection.
- Each statement outside a transaction block **auto-commits immediately**, avoiding long-running transaction locks or transaction log bloat during staging/bulk loads.
- When the group finishes, the pinned connection is automatically closed and returned to the pool.

```xml
<group id="temp_table_etl" session="true" db="dw_db" timeout="15m">
    <!-- Step 1: Create session-scoped temp table -->
    <sql id="create_temp_stage" db="dw_db">
        CREATE TABLE #StagingSales (
            SaleID INT,
            Region VARCHAR(50),
            Amount DECIMAL(18,2),
            IsValid BIT DEFAULT 1
        );
    </sql>

    <!-- Step 2: Bulk stream from source DB into #StagingSales on the pinned connection -->
    <sql_bulk id="bulk_load_stage" db="crm_db" target_db="dw_db" target_table="#StagingSales" batch_size="10000">
        SELECT SaleID, Region, Amount, 1 AS IsValid FROM dbo.RawSales
    </sql_bulk>

    <!-- Step 3: Scrub data in #StagingSales (auto-commits immediately) -->
    <sql id="scrub_stage" db="dw_db">
        UPDATE #StagingSales SET IsValid = 0 WHERE Amount &lt;= 0;
    </sql>

    <!-- Step 4: Final insert from #StagingSales -->
    <sql id="publish_clean_sales" db="dw_db">
        INSERT INTO dbo.FactSales (SaleID, Region, Amount)
        SELECT SaleID, Region, Amount FROM #StagingSales WHERE IsValid = 1;

        DROP TABLE #StagingSales;
    </sql>
</group>
```

---

### Mode D: Combining `session="true"` with Nested `transaction="true"`

You can nest `<group transaction="true" db="...">` inside `<group session="true" db="...">` (or set both `session="true" transaction="true"` on the same group). When a transaction group starts inside a pinned session for the same database, Flow starts the transaction **directly on the pinned `*sql.Conn`** (`conn.BeginTx(ctx, nil)`), preserving full visibility into any `#Temp` tables created earlier in the session:

```xml
<group id="staged_publish_session" session="true" db="dw_db">
    <!-- Outside transaction: fast staging into temp table -->
    <sql id="create_temp" db="dw_db">
        CREATE TEMP TABLE temp_updates (id INT, new_status TEXT);
    </sql>
    <sql_bulk id="load_temp" db="src_db" target_db="dw_db" target_table="temp_updates">
        SELECT id, status FROM incoming_events
    </sql_bulk>

    <!-- Inside transaction on the SAME pinned connection: atomic swap/merge -->
    <group id="atomic_apply" transaction="true" db="dw_db">
        <sql id="update_target" db="dw_db">
            UPDATE customers
            SET status = u.new_status
            FROM temp_updates u
            WHERE customers.id = u.id;
        </sql>
        <sql id="audit_batch" db="dw_db">
            INSERT INTO batch_audit (processed_count)
            SELECT COUNT(*) FROM temp_updates;
        </sql>
    </group>
</group>
```

---

## 3. Interaction with Child Nodes & Concurrency

- **Child Node Routing**: Inside a `session="true"` or `transaction="true"` group, child `<sql>`, `<sql_bulk>` (both as `db` source and `target_db` destination), `<foreach>`, `<excel_write>`, and `<kv_bulk>` nodes targeting the group's `db` automatically use the active transaction or pinned connection.
- **Single-Connection Cursor Buffering**: When `<sql_bulk>` reads and writes on the same database connection (`db == target_db` inside a session or transaction group), or when `<foreach>` iterates over a query on a pinned connection/transaction, Flow automatically buffers the source rows and closes the active `*sql.Rows` cursor before executing child/destination statements on that connection.
- **Parallel Isolation**: Each worker branch inside a `<parallel>` block receives an isolated executor state. Avoid sharing a single `*sql.Conn` or `*sql.Tx` across concurrent goroutines; instead, place `<group session="true">` or `<group transaction="true">` *inside* each `<parallel>` branch so each worker pins its own dedicated connection from the pool.

