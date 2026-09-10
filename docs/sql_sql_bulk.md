# SQL and SQL Bulk Operations Guide 🚀

The Flow execution engine provides first-class support for relational database workloads ranging from simple single-statement lookups to massive multi-million-row streaming replications and multi-stage enterprise data warehouse transforms.

---

## 🎯 Workload Recommendation Matrix: What Node & Options Should You Use?

Choosing the right node and tuning configuration is critical for achieving optimal throughput, minimizing connection contention, and avoiding memory or transaction bottlenecks.

| Workload Scenario | Recommended Node | Key Options & Attributes | Rationale & Performance Impact |
| :--- | :--- | :--- | :--- |
| **Schema Initialization (DDL) & Basic Queries** | `<sql>` | `db="db_name"`<br>`output_var="MY_VAR"` | Direct execution. Statements returning rows (`SELECT`, `SHOW`, `WITH`) format outputs into `output_var`. |
| **Complex In-Database ETL / Multi-Stage Transformations** | `<sql>` | `db="db_name"`<br>`output_var="ROW_COUNT"` | Ideal for heavy in-database processing (CTEs, `MERGE`, temp tables, indexing). Keeps data inside the database engine; ends with `SELECT @@ROWCOUNT` or summary scalar. |
| **Cross-Database Table Replication (e.g. Postgres → SQL Server)** | `<sql_bulk>` | `db="src_db"`<br>`target_db="dst_db"`<br>`target_table="fact_table"`<br>`batch_size="10000"` | Streams rows across disparate engines with constant $O(1)$ memory. Eliminates intermediate CSV/JSON files and memory bloat. |
| **High-Throughput SQL Server Ingestion** | `<sql_bulk>` | `tablock="true"`<br>`check_constraints="false"`<br>`batch_size="25000"` | Activates SQL Server minimal logging (`TABLOCK`) and disables constraint checking during bulk insertion for maximum MB/sec throughput. |
| **Preserving Identity & Explicit NULLs during Bulk Copies** | `<sql_bulk>` | `keep_nulls="true"`<br>`fire_triggers="false"` | Prevents default column constraints from replacing explicit `NULL` values during bulk ingestion. |
| **Multi-Step Atomic Transactions with Rollback** | `<group>` + `<sql>` | `transaction="true"`<br>`db="db_name"`<br>`timeout="60s"` | Wraps multiple child `<sql>` steps in a single atomic transaction. Automatically leverages dialect-aware savepoints (`SAVE TRANSACTION` for MSSQL, ANSI `SAVEPOINT` for PostgreSQL/SQLite/MySQL) for nested groups on the same database. |
| **Row-by-Row Dynamic Operations (e.g. Webhook / API per record)** | `<foreach>` + `<sql>` | `db="db_name"`<br>`buffer="false"` (default) | Streams rows one-by-one into child tasks with constant memory, safely handling 60M+ rows. Use `buffer="true"` only for small control sets (<100k rows) to release DB locks early. |

### Decision Flowchart

```mermaid
flowchart TD
    Start["Incoming Database Task"] --> Q1{"Are you moving data across tables or different databases?"}
    
    Q1 -- "Yes (Replication / Ingestion)" --> Q2{"Are you transforming rows in Go or calling external APIs per row?"}
    Q2 -- "No (Direct Copy)" --> Bulk["Use <sql_bulk><br>• Set batch_size=10000-50000<br>• Enable tablock for MSSQL<br>• Zero RAM footprint"]
    Q2 -- "Yes (Row-by-Row APIs/Scripts)" --> ForEach["Use <foreach><br>• Default streaming mode: O(1) RAM<br>• Sub-millisecond cursor fetch"]

    Q1 -- "No (In-Database Processing)" --> Q3{"Do multiple steps require atomic commit / rollback?"}
    Q3 -- "Yes" --> GroupTx["Wrap in <group transaction='true' db='...'><br>• Automatic SAVEPOINT nesting<br>• Safe deferred rollbacks"]
    Q3 -- "No" --> SqlNode["Use standard <sql><br>• Supports DDL, DML, CTEs, MERGE<br>• Intelligent query vs DML detection<br>• Captures SELECT @@ROWCOUNT"]
```

---

## Node Reference & Attribute Specifications

### 1. The `<sql>` Node

The `<sql>` node executes native SQL scripts directly on a registered database handle.

#### Attributes

| Attribute | Required | Default | Description |
| :--- | :---: | :---: | :--- |
| `id` | **Yes** | — | Unique step identifier across the pipeline. |
| `db` / `database` | **Yes** | — | Name of the database configured in `<databases>`. |
| `output_var` / `var` | No | — | Name of pipeline variable to capture the returned query result set or affected row count. |
| `timeout` | No | — | Execution deadline duration (e.g., `30s`, `5m`). |

#### Intelligent Query vs. DML Classification (`isDMLQuery`)
Flow features a built-in token-aware SQL analyzer:
- **Comments & CDATA Stripping**: Comments (`-- ...`, `/* ... */`) and XML CDATA sections are cleanly stripped prior to token inspection.
- **Column Name Safety**: Queries selecting columns named `deleted_at`, `is_deleted`, `insert_ts`, or `update_count` are recognized as standard queries and are **not** misclassified as DML statements.
- **Row-Returning Mutations**: Statements utilizing clauses like `RETURNING` (Postgres, SQLite, Oracle) or `OUTPUT` (SQL Server) are executed as queries so returned rows are preserved in `output_var`.
- **Summary Selects**: Complex multi-statement DML batches that conclude with a scalar summary (such as `SELECT @@ROWCOUNT;`) return that scalar result directly to `output_var`.

---

### 2. The `<sql_bulk>` Node

The `<sql_bulk>` node is an optimized streaming pipeline primitive that pipes rows from a source SQL query directly into a destination table.

#### Attributes

| Attribute | Required | Default | Description |
| :--- | :---: | :---: | :--- |
| `id` | **Yes** | — | Unique step identifier. |
| `db` / `database` | **Yes** | — | Source database connection handle. |
| `target_db` | No | Source `db` | Destination database connection handle. |
| `target_table` | **Yes** | — | Destination table name receiving bulk inserts. |
| `batch_size` | No | `10000` | Number of rows committed per network chunk. |
| `tablock` | No | `true` | *(SQL Server)* Acquires table-level lock for minimal transaction logging. |
| `check_constraints` | No | `false` | *(SQL Server)* Validates foreign keys/check constraints during bulk load. |
| `fire_triggers` | No | `false` | *(SQL Server)* Fires insert triggers on the target table during bulk load. |
| `keep_nulls` | No | `false` | *(SQL Server)* Preserves incoming `NULL` values instead of replacing with column defaults. |
| `output_var` | No | — | Stores total number of rows streamed to destination. |

---

## 💡 Practical Examples

### Example 1: Simple SQL Query and Bulk Copy

A clean, minimal example demonstrating table setup, standard `<sql>` querying into a pipeline variable, and cross-database `<sql_bulk>` replication.

```xml
<pipeline xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
          xsi:noNamespaceSchemaLocation="https://raw.githubusercontent.com/etl-madness/flow/main/xsd/pipeline.xsd">
    <databases>
        <database name="oltp_db" driver="postgres" connection_string="postgresql://app:secret@localhost:5432/orders_app" />
        <database name="reporting_db" driver="sqlite" connection_string="./reporting.db" />
    </databases>

    <flow>
        <!-- 1. Simple SQL: Initialize target reporting table -->
        <sql id="init_reporting_schema" db="reporting_db">
            CREATE TABLE IF NOT EXISTS customer_daily_spend (
                customer_id INTEGER PRIMARY KEY,
                total_amount REAL,
                order_date TEXT
            );
        </sql>

        <!-- 2. Simple SQL Bulk: Stream aggregated results from Postgres to SQLite -->
        <sql_bulk id="sync_daily_spend"
                  db="oltp_db"
                  target_db="reporting_db"
                  target_table="customer_daily_spend"
                  batch_size="5000"
                  output_var="RECORDS_SYNCED">
            SELECT 
                customer_id, 
                SUM(amount) AS total_amount, 
                CAST(order_date AS TEXT) AS order_date
            FROM orders
            WHERE order_date = CURRENT_DATE - INTERVAL '1 day'
            GROUP BY customer_id, order_date;
        </sql_bulk>

        <!-- 3. Simple SQL: Query the row count from the destination table -->
        <sql id="verify_sync" db="reporting_db" output_var="VERIFY_COUNT">
            SELECT COUNT(*) AS total_synced_customers FROM customer_daily_spend;
        </sql>
    </flow>
</pipeline>
```

---

### Example 2: Complex Enterprise SQL Transform on Millions of Rows Returning `SELECT @@ROWCOUNT`

This production-grade script executes a high-volume data warehouse consolidation across tens of millions of records on Microsoft SQL Server. It performs:
1. Temporary table creation with clustered columnstore and primary key hints.
2. Deduplication using window functions (`ROW_NUMBER()`).
3. High-volume `MERGE` into a target fact table (handling inserts and updates in a single pass).
4. Cascading updates to audit ledger status records.
5. Index reorganization on modified partitions.
6. Returns exclusively `SELECT @@ROWCOUNT` so the pipeline captures the total affected rows without transmitting massive intermediate datasets over the wire.

```xml
<pipeline xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
          xsi:noNamespaceSchemaLocation="https://raw.githubusercontent.com/etl-madness/flow/main/xsd/pipeline.xsd">
    <databases>
        <database name="dw_sqlserver" 
                  driver="sqlserver" 
                  connection_string="sqlserver://etl_user:P@ssw0rd123@sql-dw.corp.internal:1433?database=EnterpriseDW&amp;encrypt=true"
                  max_open_conns="50"
                  max_idle_conns="10"
                  workload="bulk" />
    </databases>

    <flow>
        <!-- Complex Enterprise Batch: Executes multi-stage transforms on millions of rows, returning only @@ROWCOUNT -->
        <sql id="execute_nightly_warehouse_consolidation" 
             db="dw_sqlserver" 
             timeout="45m"
             output_var="TOTAL_ROWS_AFFECTED">
            <![CDATA[
            SET NOCOUNT ON;
            SET XACT_ABORT ON;

            BEGIN TRANSACTION;

            -- 1. Create optimized staging structure in tempdb
            IF OBJECT_ID('tempdb..#StagedEvents') IS NOT NULL 
                DROP TABLE #StagedEvents;

            CREATE TABLE #StagedEvents (
                EventKey BIGINT NOT NULL,
                AccountID INT NOT NULL,
                EventCode VARCHAR(32) NOT NULL,
                PayloadAmount DECIMAL(18,4) NOT NULL,
                EventTimestamp DATETIME2(3) NOT NULL,
                RowHash VARBINARY(32) NOT NULL,
                DedupRank INT NOT NULL,
                INDEX IX_StagedEvents_Key CLUSTERED (EventKey, AccountID)
            );

            -- 2. Populate staging and filter duplicates across 15M+ raw ingest records
            INSERT INTO #StagedEvents WITH (TABLOCK)
            SELECT 
                r.EventKey,
                r.AccountID,
                r.EventCode,
                r.PayloadAmount,
                r.EventTimestamp,
                HASHBYTES('SHA2_256', CONCAT(r.EventKey, '|', r.AccountID, '|', r.PayloadAmount, '|', r.EventTimestamp)) AS RowHash,
                ROW_NUMBER() OVER (
                    PARTITION BY r.EventKey, r.AccountID 
                    ORDER BY r.IngestTimestamp DESC, r.SequenceID DESC
                ) AS DedupRank
            FROM RawIngest.IncomingEvents r WITH (NOLOCK)
            WHERE r.IngestDate >= CAST(DATEADD(DAY, -1, GETUTCDATE()) AS DATE)
              AND r.ProcessingStatus = 'PENDING';

            -- 3. Perform High-Volume MERGE into Production Fact Table
            MERGE INTO Fact.AccountTransactions WITH (TABLOCK) AS tgt
            USING (
                SELECT 
                    EventKey,
                    AccountID,
                    EventCode,
                    PayloadAmount,
                    EventTimestamp,
                    RowHash
                FROM #StagedEvents
                WHERE DedupRank = 1
            ) AS src
            ON (tgt.EventKey = src.EventKey AND tgt.AccountID = src.AccountID)
            
            -- Update existing records if payload or hash has evolved
            WHEN MATCHED AND tgt.RowHash <> src.RowHash THEN
                UPDATE SET 
                    tgt.EventCode = src.EventCode,
                    tgt.PayloadAmount = src.PayloadAmount,
                    tgt.EventTimestamp = src.EventTimestamp,
                    tgt.RowHash = src.RowHash,
                    tgt.LastModifiedUTC = GETUTCDATE(),
                    tgt.VersionNumber = tgt.VersionNumber + 1

            -- Insert newly recognized events
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (
                    EventKey,
                    AccountID,
                    EventCode,
                    PayloadAmount,
                    EventTimestamp,
                    RowHash,
                    CreatedUTC,
                    LastModifiedUTC,
                    VersionNumber
                )
                VALUES (
                    src.EventKey,
                    src.AccountID,
                    src.EventCode,
                    src.PayloadAmount,
                    src.EventTimestamp,
                    src.RowHash,
                    GETUTCDATE(),
                    GETUTCDATE(),
                    1
                );

            -- 4. Mark staging source rows as PROCESSED in the raw ingest ledger
            UPDATE raw
            SET 
                raw.ProcessingStatus = 'PROCESSED',
                raw.ProcessedUTC = GETUTCDATE()
            FROM RawIngest.IncomingEvents raw
            INNER JOIN #StagedEvents stg 
                ON raw.EventKey = stg.EventKey 
               AND raw.AccountID = stg.AccountID
            WHERE stg.DedupRank = 1;

            -- 5. Drop temp table to reclaim tempdb storage immediately
            DROP TABLE #StagedEvents;

            COMMIT TRANSACTION;

            -- 6. Return strictly @@ROWCOUNT to the Flow pipeline variable
            SELECT @@ROWCOUNT AS affected_rows;
            ]]>
        </sql>

        <!-- Verify execution metric in subsequent step -->
        <script id="log_affected_rows" language="go">
            package main
            import (
                "fmt"
                "host/vars"
            )
            func main() {
                count := vars.GetString("TOTAL_ROWS_AFFECTED")
                fmt.Printf("Warehouse consolidation complete. Affected rows reported: %s\n", count)
            }
        </script>
    </flow>
</pipeline>
```

#### Why This Pattern Works in Flow
1. **Intelligent Query Detection**: Flow strips the leading T-SQL commands (`SET NOCOUNT ON`, `BEGIN TRANSACTION`, `MERGE`, etc.) and detects that the script concludes with a row-returning statement (`SELECT @@ROWCOUNT`).
2. **Zero In-Memory Payload**: Instead of returning 15 million rows across the network into Go's memory, the database handles all merges and updates locally using its storage engine. Only the single scalar row count is returned and stored in `TOTAL_ROWS_AFFECTED`.
3. **Safe Timeouts**: The `timeout="45m"` attribute ensures that long-running operations are granted appropriate time to complete, while guaranteeing cleanup if a threshold is exceeded.

---

### Example 3: High-Performance Cross-Database Bulk Replication (`<sql_bulk>`)

This example replicates 20 million rows from a PostgreSQL transaction log into a Microsoft SQL Server data lake staging table with minimal transaction logging and optimized batch sizes.

```xml
<pipeline xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
          xsi:noNamespaceSchemaLocation="https://raw.githubusercontent.com/etl-madness/flow/main/xsd/pipeline.xsd">
    <databases>
        <database name="source_pg" 
                  driver="postgres" 
                  connection_string="postgresql://pg_read:Secret2026@pg-cluster.internal:5432/tx_db?sslmode=require"
                  max_open_conns="30"
                  workload="bulk" />

        <database name="dest_mssql" 
                  driver="sqlserver" 
                  connection_string="sqlserver://sa:Secret2026@mssql-dw.internal:1433?database=StagingLake"
                  max_open_conns="30"
                  workload="bulk" />
    </databases>

    <flow>
        <!-- Stream 20M+ rows directly between engines with zero RAM buffering -->
        <sql_bulk id="replicate_tx_log"
                  db="source_pg"
                  target_db="dest_mssql"
                  target_table="stg_transaction_log"
                  batch_size="25000"
                  tablock="true"
                  check_constraints="false"
                  fire_triggers="false"
                  keep_nulls="true"
                  output_var="REPLICATED_ROW_COUNT">
            SELECT 
                tx_id,
                account_id,
                merchant_id,
                amount,
                currency,
                created_at,
                metadata_json
            FROM transactions.ledger
            WHERE created_at >= '2026-01-01 00:00:00Z'
            ORDER BY tx_id ASC;
        </sql_bulk>
    </flow>
</pipeline>
```

#### Key Performance Features in this Configuration:
- `batch_size="25000"`: Minimizes network round-trip overhead while remaining within server memory buffer limits.
- `tablock="true"`: Enables table-level locking in SQL Server, switching ingestion from row-by-row logging to fast minimal bulk logging.
- `check_constraints="false"`: Bypasses constraint validation during the copy to maximize insertion throughput (constraints can be validated in bulk afterward).
- `keep_nulls="true"`: Ensures explicit `NULL` values from PostgreSQL are retained and not overwritten by SQL Server column default definitions.

---

## 🛠️ Summary Best Practices

1. **Keep Transformations in the Database Engine**: When transforming existing database records, use `<sql>` with in-database DML (`MERGE`, `INSERT INTO ... SELECT`, `UPDATE ... JOIN`) rather than pulling rows into Go to modify them. End your script with `SELECT @@ROWCOUNT` or `SELECT count(*)`.
2. **Use `<sql_bulk>` for Table-to-Table Transfers**: Whenever data must move between different tables or databases, always prefer `<sql_bulk>` over custom loops or scripts.
3. **Use Default Streaming in `<foreach>` for Large Data**: If you must loop over query results, rely on `<foreach>` default streaming mode ($O(1)$ memory). Only add `buffer="true"` if your loop contains <100,000 rows and you need to release the database connection immediately.
4. **Wrap Multi-Step Sequences in `<group transaction="true">`**: Ensure atomicity and eliminate deadlocks using Flow's automatic dialect-aware savepoint nesting (`SAVE TRANSACTION` on MSSQL, `SAVEPOINT` on PostgreSQL/SQLite/MySQL).
