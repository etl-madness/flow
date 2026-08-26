# Database Operations in Flow Pipelines 🗄️

Flow features native, high-performance integration with SQL relational databases, enabling direct query execution, transactional grouping, rapid bulk data replication, and styled multi-tab spreadsheet generation.

---

## Key AST Nodes

### 1. `<sql>`
Used for executing standard SQL scripts (DDL, DML, standard queries).
- **`db` / `database`**: Name of the configured database connection to execute against.
- **`id`**: Unique identifier for this step.
- **`output_var` / `var` / `variable`**: (Optional) Pipeline variable where the query output (with column headers) will be stored as a newline-delimited text block.

### 2. `<sql-bulk>`
Optimized for streaming huge datasets directly from a source database query into a target table, bypassing CPU/memory bottlenecks.
- **`db` / `database`**: The source database connection name.
- **`target_db` / `target_database`**: The destination database connection name (defaults to the source database if omitted).
- **`target_table`**: The destination table to bulk insert records into.
- **`batch_size`**: The row batch size (defaults to `10000`).
- **`tablock`**: Acquire a table lock for minimal logging on SQL Server (`true` / `false`, defaults to `true`).
- **`check_constraints`**: Evaluate table constraints during bulk insert (`true` / `false`).
- **`fire_triggers`**: Execute target table triggers during insert (`true` / `false`).
- **`keep_nulls`**: Preserve explicit NULL values (`true` / `false`).

---

## Practical Examples

### Example 1: Schema Setup and Data Ingestion (`<sql>`)
Initialize a database table structure and load initial seed data.

```xml
<pipeline>
    <databases>
        <database name="analytics_db" driver="sqlite" connection_string="file::memory:?cache=shared" />
    </databases>
    <scripts>
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
    </scripts>
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
    <scripts>
        <sql id="fetch_top_products" db="production_db" output_var="RECOMMENDED_PRODUCTS">
            SELECT product_id, title, rating 
            FROM store.products 
            WHERE rating >= {{MIN_RATING}} 
            ORDER BY rating DESC 
            LIMIT {{LIMIT_COUNT}};
        </sql>
    </scripts>
</pipeline>
```

---

### Example 3: High-Performance Data Archival (`<sql-bulk>`)
Bulk replicate records from a production database directly into a separate analytics cold storage target.

```xml
<pipeline>
    <databases>
        <database name="prod_db" driver="mysql" connection_string="root:secret@tcp(localhost:3306)/prod" />
        <database name="archive_db" driver="sqlite" connection_string="./archive.db" />
    </databases>
    <scripts>
        <!-- Stream rows in batches of 5000 directly from source to target -->
        <sql-bulk id="archive_historical_logs" 
                  db="prod_db" 
                  target_db="archive_db" 
                  target_table="historical_logs" 
                  batch_size="5000">
            SELECT log_id, event, severity, created_at 
            FROM system.logs 
            WHERE created_at &lt; DATE_SUB(NOW(), INTERVAL 90 DAY);
        </sql-bulk>
    </scripts>
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
    <scripts>
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
    </scripts>
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
    <scripts>
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
        <sql-bulk id="stage_crm_leads" 
                  db="crm_db" 
                  target_db="reporting_warehouse" 
                  target_table="stg_leads" 
                  batch_size="1000">
            SELECT id AS lead_id, company, deal_size, status AS stage 
            FROM public.leads 
            WHERE status IN ('Qualified', 'Proposal Sent');
        </sql-bulk>

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
    </scripts>
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
    <scripts>
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
    </scripts>
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
    <scripts>
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
    </scripts>
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
    <scripts>
        <!-- 1. Extract spreadsheet rows to a variable containing a JSON array string -->
        <excel_read file="./uploads/catalog.xlsx" sheet="Products" header="true" output_var="CATALOG_JSON" />

        <!-- 2. Deserialize and insert directly inside SQL using postgres JSON engine functions -->
        <sql id="import_catalog" db="store_db">
            INSERT INTO store.inventory (sku, name, price)
            SELECT sku, name, price 
            FROM json_populate_recordset(NULL::store.inventory, '{{CATALOG_JSON}}');
        </sql>
    </scripts>
</pipeline>
```

#### Method B: SQLite (Using `json_each`)
```xml
<pipeline>
    <databases>
        <database name="local_db" driver="sqlite" connection_string="./local.db" />
    </databases>
    <scripts>
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
    </scripts>
</pipeline>
```

#### Method C: Microsoft SQL Server / MSSQL (Using `OPENJSON`)
```xml
<pipeline>
    <databases>
        <database name="mssql_db" driver="sqlserver" connection_string="sqlserver://sa:secret@localhost:1433?database=store" />
    </databases>
    <scripts>
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
    </scripts>
</pipeline>
```



