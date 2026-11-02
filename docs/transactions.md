# Database Transactions in Flow Pipelines

Flow supports wrapping sequential SQL script execution inside an atomic database transaction. This ensures that a group of SQL scripts either succeeds together or rolls back completely if an error occurs.

---

## 1. Configuration Syntax

Transactions are declared at the `<group>` node level using two attributes:
*   `transaction="true"`: Enables transaction wrapping for all SQL script nodes inside the group.
*   `db="..."` (or `database="..."`): Specifies the target database handle to run the transaction on.

### Basic Example
```xml
<pipeline xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
          xsi:noNamespaceSchemaLocation="https://raw.githubusercontent.com/etl-madness/flow/main/xsd/pipeline.xsd">
    <databases>
        <database name="sales_db" driver="postgres" connection_string="postgresql://..." />
    </databases>
    <flow>
        <group id="update_sales_txn" transaction="true" db="sales_db">
            <sql id="deduct_inventory" db="sales_db">
                UPDATE inventory SET stock = stock - 1 WHERE item_id = 42;
            </sql>
            <sql id="record_sale" db="sales_db">
                INSERT INTO sales (item_id, qty) VALUES (42, 1);
            </sql>
        </group>
    </flow>
</pipeline>
```

---

## 2. Dynamic Loops & Nested Transactions (`<foreach>`)

If a group with `transaction="true"` is placed inside a `<foreach>` loop, Flow begins, executes, and commits/rolls back a new transaction **per loop iteration**. 

```xml
<pipeline xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
          xsi:noNamespaceSchemaLocation="https://raw.githubusercontent.com/etl-madness/flow/main/xsd/pipeline.xsd">
    <flow>
        <!-- Loop driver gets item IDs -->
        <foreach id="process_items" db="sales_db">
            SELECT item_id, price FROM active_promotions;
            
            <!-- A transaction is started and finished for each promotion processed -->
            <group id="apply_promo_txn" transaction="true" db="sales_db">
                <sql id="update_price" db="sales_db">
                    UPDATE products SET price = {{price}} WHERE id = {{item_id}};
                </sql>
                <sql id="log_history" db="sales_db">
                    INSERT INTO pricing_log (product_id, new_price) VALUES ({{item_id}}, {{price}});
                </sql>
            </group>
        </foreach>
    </flow>
</pipeline>
```

---

## 3. Parallel Execution & Isolation (`<parallel>`)

When running concurrent tasks in a `<parallel>` block:
*   Flow isolates each parallel branch inside its own execution context with a unique cloned executor.
*   If a `<group transaction="true">` is placed inside a parallel branch, its active transaction is completely isolated to that branch's worker connection.
*   This prevents race conditions or shared transaction states across parallel threads.

```xml
<pipeline xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
          xsi:noNamespaceSchemaLocation="https://raw.githubusercontent.com/etl-madness/flow/main/xsd/pipeline.xsd">
    <flow>
        <parallel max_threads="2">
            <!-- Branch 1: isolated txn on sales_db -->
            <group transaction="true" db="sales_db">
                <sql db="sales_db">INSERT INTO logs VALUES ('Thread A started');</sql>
                <sql db="sales_db">UPDATE stats SET count = count + 1;</sql>
            </group>

            <!-- Branch 2: isolated txn on sales_db -->
            <group transaction="true" db="sales_db">
                <sql db="sales_db">INSERT INTO logs VALUES ('Thread B started');</sql>
                <sql db="sales_db">UPDATE stats SET count = count + 1;</sql>
            </group>
        </parallel>
    </flow>
</pipeline>
```

---

## 3.5 Context Propagation & Automatic Rollback

All transactions in Flow are fully contextualized using Go's `context.Context` API:
*   Transactions are initiated with `BeginTx(ctx, nil)`.
*   If the pipeline's active context is cancelled or times out, the active transactions are automatically aborted and safely rolled back.
*   This prevents database locks, stale transactions, and uncommitted orphaned states across timeouts and unexpected cancellations.

---

## 4. Interaction with SQL Server Bulk Copy (`StreamETL`)

Flow has high-performance support for Microsoft SQL Server native TDS bulk streaming copy via the `StreamETL` function (`<sql_bulk target_table="...">` tag). 

### How Bulk Copy Works
The MS SQL Server bulk copy utility loads data efficiently by writing data pages directly to the database without the overhead of standard row-by-row transaction logs. It utilizes the `mssql.CopyIn` interface.

### Bulk Copy Batch-Level Transactions
Unlike standard sequential SQL scripts, **SQL Server bulk copy manages its own internal transactions** to flush rows in high-speed chunks (defined by `batch_size`, which defaults to `10000` rows). Each batch flush:
1. Opens a separate, native bulk transaction.
2. Writes the chunk of rows.
3. Commits that batch chunk immediately to disk.

### Impact of Mixing Group Transactions and Bulk Copy
> [!WARNING]
> While a standard `<group transaction="true">` is excellent for atomic transactional integrity on conventional queries, you should avoid placing high-throughput bulk copy stream scripts (`<sql_bulk target_table="...">`) inside a transaction-enabled group.

Here is how configuration options impact bulk copy:

1.  **Atomicity vs. Recovery**: 
    If a bulk copy script is wrapped inside an outer group transaction, standard driver behaviors might force all streamed batches to use the same underlying physical connection. However, because bulk copying relies on native SQL Server TDS fast-load streams, mixing outer group transactions with internal batch commits can cause locking, socket blocks, or driver-level command overlap errors.
2.  **TABLOCK (Minimal Logging)**:
    By default, bulk insert operations use the `tablock` option to lock the target table during inserts. This minimizes transactional logging and maximizes throughput. However, if a table lock is acquired within a long-running standard transaction, concurrent queries targeting that table are fully blocked until the entire transaction group is either committed or rolled back.
3.  **Check Constraints & Triggers**:
    By default, constraints checking (`check_constraints`) and triggers (`fire_triggers`) are turned off to speed up ingestion. Enabling them forces SQL Server to run validations, which increases logging overhead and log-write lock contention. Under an outer group transaction, this logging overhead can lead to rapid database transaction log depletion.

### Summary Best Practice
*   **Use `<group transaction="true">`** for transactional business logic, updates, inserts, and system state steps.
*   **Do NOT use outer group transactions** for high-volume data streams (`StreamETL`). Instead, let the streaming engine handle batch-level transaction commits naturally.

---

## 5. Session Connection Pinning & Temporary Tables (`session="true"`)

Database engines scope temporary tables (such as `#Temp` in SQL Server, `CREATE TEMP TABLE` in PostgreSQL/SQLite, and `CREATE TEMPORARY TABLE` in MySQL) to the **physical database session/connection**. Because Go's `*sql.DB` is a connection pool, consecutive `<sql>` or `<sql_bulk>` tags normally borrow arbitrary connections from the pool, causing session-scoped temp tables to be invisible or dropped between steps.

To share and persist temporary tables (and session settings such as `SET NOCOUNT ON` or `SET LOCAL`) across separate pipeline steps **without** holding an open transaction lock across the entire flow, wrap the steps in `<group session="true" db="...">` (aliases: `pin_connection="true"`, `pin_conn="true"`, `sticky="true"`):

```xml
<group id="staging_session" session="true" db="warehouse" timeout="10m">
    <!-- 1. Create a session-scoped temp table -->
    <sql id="create_temp" db="warehouse">
        CREATE TEMP TABLE temp_staging (id INT, amount INT);
    </sql>

    <!-- 2. Bulk load into the temp table on the same pinned connection -->
    <sql_bulk id="load_temp" db="warehouse" target_table="temp_staging" batch_size="5000">
        SELECT id, amount FROM raw_orders WHERE status = 'PENDING'
    </sql_bulk>

    <!-- 3. Optionally use an atomic transaction on the same pinned session -->
    <group id="enrich_temp" transaction="true" db="warehouse">
        <sql id="dedup_temp" db="warehouse">
            DELETE FROM temp_staging WHERE amount &lt;= 0;
        </sql>
    </group>

    <!-- 4. Query or stream out of the temp table -->
    <sql id="merge_final" db="warehouse">
        INSERT INTO final_orders SELECT id, amount FROM temp_staging;
        DROP TABLE temp_staging;
    </sql>
</group>
```

When `<group session="true" db="...">` starts, Flow pins a dedicated `*sql.Conn` from the pool for that database, routes all child `<sql>`, `<sql_bulk>`, `<foreach>`, `<excel_write>`, `<kv_bulk>`, and nested `<group transaction="true">` operations on that database through the pinned connection, and automatically returns the connection to the pool when the group exits.
