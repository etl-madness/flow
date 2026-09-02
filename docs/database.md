# Database Operations in Flow Pipelines 🗄️

Flow features native, high-performance integration with SQL relational databases, enabling direct query execution, transactional grouping, rapid bulk data replication, and styled multi-tab spreadsheet generation.

---

## Key AST Nodes

### 1. `<sql>`
Used for executing standard SQL scripts (DDL, DML, standard queries).
- **`db` / `database`**: Name of the configured database connection to execute against.
- **`id`**: Unique identifier for this step.
- **`output_var` / `var` / `variable`**: (Optional) Pipeline variable where the query output (with column headers) will be stored as a newline-delimited text block.

### 2. `<sql_bulk>`
Optimized for streaming huge datasets directly from a source database query into a target table, bypassing CPU/memory bottlenecks.
- **`db` / `database`**: The source database connection name.
- **`target_db` / `target_database`**: The destination database connection name (defaults to the source database if omitted).
- **`target_table`**: The destination table to bulk insert records into.
- **`batch_size`**: The row batch size (defaults to `10000`).
- **`tablock`**: Acquire a table lock for minimal logging on SQL Server (`true` / `false`, defaults to `true`).
- **`check_constraints`**: Evaluate table constraints during bulk insert (`true` / `false`).
- **`fire_triggers`**: Execute target table triggers during insert (`true` / `false`).
- **`keep_nulls`**: Preserve explicit NULL values (`true` / `false`).

### Database connection tuning
Connection parameters live on the `<database>` definition itself, not on each SQL step.

- **`name`**: unique logical database identifier used by SQL nodes.
- **`driver`**: database driver (for example `sqlite`, `postgres`, `mysql`, `sqlserver`).
- **`connection_string`**: driver-specific DSN/connection string.
- **`max_open_conns`**: max number of simultaneous open connections in the pool.
- **`max_idle_conns`**: number of idle connections retained ready for reuse.
- **`conn_max_lifetime_seconds`**: maximum age of a pooled connection before it is recycled.
- **`workload`**: optional tuning profile label such as `oltp`, `bulk`, or `analytics`.

A workload is the operational pattern of a database connection. It describes how a connection is expected to be used at runtime so the pool can be tuned for the right trade-off between concurrency, latency, and throughput. In practice, Flow uses the workload label as a hint to choose sane defaults for connection count, idle retention, and lifetime without hardcoding one-size-fits-all values.

Common workload types:
- **`oltp`**: short, latency-sensitive reads and writes; keep a moderate number of open connections and shorter idle time to reduce cold-start latency.
- **`bulk`**: large ETL or ingestion jobs; favor larger pools and more throughput, with a shorter connection lifetime to handle high churn.
- **`analytics`**: read-heavy reporting workloads; balance concurrency and reuse to support many query workers without oversubscribing the server.
- **`batch`**: scheduled, non-interactive jobs that may run bursts; a tuned pool can temporarily scale higher but still recycle stale connections.

These labels are metadata, not required switches: they are optional and can be omitted. When omitted, the runtime falls back to the safe defaults already used by the registry.

Supported database engines and typical workload fit:
- **SQLite**: best for local dev, embedded apps, testing, and lightweight single-user workflows; commonly used with `oltp` or `batch`.
- **PostgreSQL**: strong fit for `oltp`, `analytics`, and `bulk` patterns; widely used for transactional apps, staging pipelines, and reporting.
- **MySQL**: commonly used for `oltp` and `batch` workloads; also suitable for bulk ingestion when configured with a larger pool.
- **SQL Server / MSSQL**: strong fit for `oltp`, `analytics`, and `bulk`, including large ETL copy operations with `tablock` and other MSSQL-specific bulk options.
- **Oracle**: commonly used for mission-critical `oltp` and `analytics` workloads with tuned connection reuse and larger query fan-out.

These workload labels are not limited to a single database engine; they describe the application behavior of the connection, so all supported drivers can use the same tuning model.

```xml
<databases>
    <database name="analytics_db"
              driver="postgres"
              connection_string="host=localhost port=5432 user=app password=secret dbname=analytics sslmode=disable"
              max_open_conns="25"
              max_idle_conns="10"
              conn_max_lifetime_seconds="300"
              workload="oltp" />
</databases>
```

### Example: Pool tuning for bulk ETL
Use a larger pool for high-throughput bulk workloads and a shorter idle lifetime for transient jobs.

```xml
<databases>
    <database name="warehouse_db"
              driver="postgres"
              connection_string="host=warehouse.internal port=5432 user=loader password=secret dbname=warehouse sslmode=disable"
              max_open_conns="80"
              max_idle_conns="20"
              conn_max_lifetime_seconds="180"
              workload="bulk" />
</databases>

<flow>
    <sql_bulk id="bulk_copy_orders"
              db="orders_db"
              target_db="warehouse_db"
              target_table="orders_fact"
              batch_size="5000">
        SELECT order_id, customer_id, amount, created_at FROM orders WHERE created_at >= CURRENT_DATE - INTERVAL '7 days';
    </sql_bulk>
</flow>
```

---

## Practical Examples

### Example 1: Schema Setup and Data Ingestion (`<sql>`)
Initialize a database table structure and load initial seed data.

```xml
<pipeline>
    <databases>
        <database name="analytics_db"
                  driver="sqlite"
                  connection_string="file::memory:?cache=shared"
                  max_open_conns="25"
                  max_idle_conns="10"
                  conn_max_lifetime_seconds="300"
                  workload="oltp" />
    </databases>
    <flow>
        <sql id="setup_analytics_schema" db="analytics_db">
            CREATE TABLE IF NOT EXISTS page_views (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL,
                visitor_ip TEXT,
                viewed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            INSERT INTO page_views (url, visitor_ip) VALUES 
            ('/home', '192.168.1.1'),
            ('/products', '192.168.1.100'),
            ('/checkout', '192.168.1.100');
        </sql>
    </flow>
</pipeline>
```

---

### Example 2: Dynamic Querying with Parameter Interpolation (`<sql>`)
Filter database records dynamically utilizing pipeline environment variables.

```xml
<pipeline>
    <databases>
        <database name="production_db" driver="postgres" connection_string="postgresql://app:secret@localhost:5432/prod" />
    </databases>
    <variables>
        <variable name="MIN_RATING" type="float" value="4.5" />
        <variable name="LIMIT_COUNT" type="int" value="10" />
    </variables>
    <flow>
        <sql id="fetch_top_products" db="production_db" output_var="RECOMMENDED_PRODUCTS">
            SELECT product_id, title, rating 
            FROM store.products 
            WHERE rating >= {{MIN_RATING}} 
            ORDER BY rating DESC 
            LIMIT {{LIMIT_COUNT}};
        </sql>
    </flow>
</pipeline>
```

---

### Example 3: High-Performance Data Archival (`<sql_bulk>`)
Bulk replicate records from a production database directly into a separate analytics cold storage target.

```xml
<pipeline>
    <databases>
        <database name="prod_db" driver="mysql" connection_string="root:secret@tcp(localhost:3306)/prod" />
        <database name="archive_db" driver="sqlite" connection_string="./archive.db" />
    </databases>
    <flow>
        <!-- Stream rows in batches of 5000 directly from source to target -->
        <sql_bulk id="archive_historical_logs" 
                  db="prod_db" 
                  target_db="archive_db" 
                  target_table="historical_logs" 
                  batch_size="5000">
            SELECT log_id, event, severity, created_at 
            FROM system.logs 
            WHERE created_at &lt; DATE_SUB(NOW(), INTERVAL 90 DAY);
        </sql_bulk>
    </flow>
</pipeline>
```

---

### Example 4: Exporting Queries to Multiple Excel Tabs (`<excel_write>`)
Consolidate multiple database reports into separate tabs inside a single `.xlsx` workbook using consecutive `<excel_write>` nodes.

```xml
<pipeline>
    <databases>
        <database name="retail_db" driver="postgres" connection_string="postgresql://app:secret@localhost:5432/retail" />
    </databases>
    <flow>
        <!-- Tab 1: Store Sales Overview -->
        <excel_write id="write_sales_overview" 
                     file="./reports/Retail_Dashboard.xlsx" 
                     sheet="Sales Summary" 
                     db="retail_db">
            SELECT date_trunc('month', sale_date) AS sale_month, SUM(revenue) AS total_revenue
            FROM sales.daily_ledger
            GROUP BY 1 ORDER BY 1 DESC;
        </excel_write>

        <!-- Tab 2: Best Performing Products -->
        <excel_write id="write_top_inventory" 
                     file="./reports/Retail_Dashboard.xlsx" 
                     sheet="Top Products" 
                     db="retail_db">
            SELECT sku, product_name, units_sold, stock_on_hand
            FROM sales.product_performance
            WHERE units_sold > 500
            ORDER BY units_sold DESC;
        </excel_write>
    </flow>
</pipeline>
```

---

### Example 5: Combined Ingestion, Bulk Replication, and Multi-Tab Reporting
Build a complete end-to-end data staging, bulk copy, and multi-tab Excel dashboard export workflow.

```xml
<pipeline>
    <databases>
        <database name="crm_db" driver="postgres" connection_string="postgresql://app:secret@localhost:5432/crm" />
        <database name="reporting_warehouse" driver="sqlite" connection_string="./warehouse.db" />
    </databases>
    <flow>
        <!-- 1. Staging tables creation -->
        <sql id="create_staging_structures" db="reporting_warehouse">
            CREATE TABLE IF NOT EXISTS stg_leads (
                lead_id INTEGER, 
                company TEXT, 
                deal_size REAL, 
                stage TEXT
            );
        </sql>

        <!-- 2. Bulk stream dynamic leads data directly to staging database -->
        <sql_bulk id="stage_crm_leads" 
                  db="crm_db" 
                  target_db="reporting_warehouse" 
                  target_table="stg_leads" 
                  batch_size="1000">
            SELECT id AS lead_id, company, deal_size, status AS stage 
            FROM public.leads 
            WHERE status IN ('Qualified', 'Proposal Sent');
        </sql_bulk>

        <!-- 3. Export Summary to Sheet 1 -->
        <excel_write id="report_summary" 
                     file="./reports/CRM_Leads_Report.xlsx" 
                     sheet="Summary Overview" 
                     db="reporting_warehouse">
            SELECT stage, COUNT(lead_id) AS total_leads, SUM(deal_size) AS pipeline_value
            FROM stg_leads
            GROUP BY stage;
        </excel_write>

        <!-- 4. Export Raw Details to Sheet 2 -->
        <excel_write id="report_raw_details" 
                     file="./reports/CRM_Leads_Report.xlsx" 
                     sheet="Lead Details" 
                     db="reporting_warehouse">
            SELECT lead_id, company, deal_size, stage
            FROM stg_leads
            ORDER BY deal_size DESC;
        </excel_write>
    </flow>
</pipeline>
```

---

### Example 6: Reading Excel Data and Writing to a Database
Extract data from a spreadsheet using `<excel_read>` and write it to a database using a Go interpreter script.

```xml
<pipeline>
    <databases>
        <database name="inventory_db" driver="postgres" connection_string="postgresql://app:secret@localhost:5432/inventory" />
    </databases>
    <flow>
        <!-- 1. Extract spreadsheet data into a pipeline variable containing JSON string -->
        <excel_read file="./uploads/inventory_updates.xlsx" sheet="New_Arrivals" header="true" output_var="EXCEL_JSON" />
        
        <!-- 2. Loop over extracted data programmatically and insert/update in database -->
        <script lang="go">
            package main
            import (
                "encoding/json"
                "fmt"
                "host/vars"
                "host/db"
            )
            
            type Item struct {
                SKU   string  `json:"sku"`
                Name  string  `json:"name"`
                Price float64 `json:"price"`
                Stock int     `json:"stock"`
            }
            
            func main() {
                jsonData := vars.GetString("EXCEL_JSON")
                var items []Item
                if err := json.Unmarshal([]byte(jsonData), &items); err != nil {
                    panic(err)
                }
                
                // Retrieve the database connection
                conn, err := db.Get("inventory_db")
                if err != nil {
                    panic(err)
                }
                
                for _, item := range items {
                    _, err := conn.Exec(`
                        INSERT INTO products (sku, name, price, stock) 
                        VALUES ($1, $2, $3, $4) 
                        ON CONFLICT (sku) 
                        DO UPDATE SET price = EXCLUDED.price, stock = EXCLUDED.stock`,
                        item.SKU, item.Name, item.Price, item.Stock,
                    )
                    if err != nil {
                        panic(err)
                    }
                }
                fmt.Printf("Successfully imported %d items from Excel to DB\n", len(items))
            }
        </script>
    </flow>
</pipeline>
```

---

### Example 7: Wrapping SQL Script Blocks inside Transactions
Use `<group>` with `transaction="true"` to wrap multiple SQL operations inside an atomic transaction, ensuring automatic rollback on any failure.

```xml
<pipeline>
    <databases>
        <database name="finance_db" driver="postgres" connection_string="postgresql://app:secret@localhost:5432/finance" />
    </databases>
    <flow>
        <!-- Transactions are enabled at the group node level -->
        <group id="transfer_funds_txn" transaction="true" db="finance_db" description="Transfers balance safely between accounts">
            
            <!-- Step 1: Withdraw from Account A -->
            <sql id="withdraw_acc_a" db="finance_db">
                UPDATE accounts SET balance = balance - 250.00 WHERE account_id = 'ACC-001' AND balance >= 250.00;
            </sql>
            
            <!-- Step 2: Deposit into Account B -->
            <sql id="deposit_acc_b" db="finance_db">
                UPDATE accounts SET balance = balance + 250.00 WHERE account_id = 'ACC-002';
            </sql>
            
            <!-- Step 3: Record transaction event -->
            <sql id="log_txn_event" db="finance_db">
                INSERT INTO transactions_ledger (from_account, to_account, amount) VALUES ('ACC-001', 'ACC-002', 250.00);
            </sql>
            
        </group>
    </flow>
</pipeline>
```

---

### Example 8: Inserting excel_read JSON Output directly using SQL JSON Parsing
If your database engine supports native JSON processing functions, you can pass the JSON output variable from `<excel_read>` directly into your `<sql>` node for inline database insertion.

#### Method A: PostgreSQL (Using `json_populate_recordset`)
```xml
<pipeline>
    <databases>
        <database name="store_db" driver="postgres" connection_string="postgresql://app:secret@localhost:5432/store" />
    </databases>
    <flow>
        <!-- 1. Extract spreadsheet rows to a variable containing a JSON array string -->
        <excel_read file="./uploads/catalog.xlsx" sheet="Products" header="true" output_var="CATALOG_JSON" />

        <!-- 2. Deserialize and insert directly inside SQL using postgres JSON engine functions -->
        <sql id="import_catalog" db="store_db">
            INSERT INTO store.inventory (sku, name, price)
            SELECT sku, name, price 
            FROM json_populate_recordset(NULL::store.inventory, '{{CATALOG_JSON}}');
        </sql>
    </flow>
</pipeline>
```

#### Method B: SQLite (Using `json_each`)
```xml
<pipeline>
    <databases>
        <database name="local_db" driver="sqlite" connection_string="./local.db" />
    </databases>
    <flow>
        <!-- 1. Extract worksheet data into a variable containing a JSON array string -->
        <excel_read file="./uploads/users.xlsx" sheet="Sheet1" header="true" output_var="USER_JSON" />

        <!-- 2. Parse and insert using SQLite's built-in json_each function -->
        <sql id="import_users" db="local_db">
            INSERT INTO users (username, role)
            SELECT 
                json_extract(value, '$.username') AS username,
                json_extract(value, '$.role') AS role
            FROM json_each('{{USER_JSON}}');
        </sql>
    </flow>
</pipeline>
```

#### Method C: Microsoft SQL Server / MSSQL (Using `OPENJSON`)
```xml
<pipeline>
    <databases>
        <database name="mssql_db" driver="sqlserver" connection_string="sqlserver://sa:secret@localhost:1433?database=store" />
    </databases>
    <flow>
        <!-- 1. Extract spreadsheet rows into a JSON array variable string -->
        <excel_read file="./uploads/catalog.xlsx" sheet="Products" header="true" output_var="CATALOG_JSON" />

        <!-- 2. Parse and insert using SQL Server's native OPENJSON function -->
        <sql id="import_catalog_mssql" db="mssql_db">
            INSERT INTO dbo.inventory (sku, name, price)
            SELECT sku, name, price
            FROM OPENJSON('{{CATALOG_JSON}}')
            WITH (
                sku VARCHAR(50) '$.sku',
                name VARCHAR(100) '$.name',
                price DECIMAL(10, 2) '$.price'
            );
        </sql>
    </flow>
</pipeline>
```



