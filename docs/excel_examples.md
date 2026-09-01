# Overview of the `<excel_read>` and `<excel_write>` Nodes in Flow

The `<excel_read>` and `<excel_write>` nodes in `flow` enable direct integration with `.xlsx` workbooks. You can extract data from Excel into JSON for APIs or downstream processing, and you can export database queries directly into styled, native Excel files.

## Key Attributes

### `<excel_read>`
* **`file`**: Target path of the `.xlsx` file to read. Supports `{{VarName}}` interpolation.
* **`sheet`**: (Optional) The name of the worksheet to read. Defaults to the first sheet if omitted.
* **`header`**: (Optional) Boolean (`true` / `false`). If `true` (default), the first row is treated as keys for the resulting JSON.
* **`output_var` / `var`**: The pipeline variable where the extracted JSON string will be stored.

### `<excel_write>`
* **`file`**: Target path on the local filesystem to save the `.xlsx` file. Supports interpolation.
* **`sheet`**: (Optional) Name of the worksheet where data will be written. Defaults to `Sheet1`.
* **`db`**: (Optional) Database connection name to execute the inline query against.
* **Inline Content**: The SQL query used to pull data from the database and populate the Excel file.

---

## Examples

### 1. Simple Database Export to Excel
Extract a report directly from a SQL database and save it as a native `.xlsx` file.

```xml
<pipeline>
    <databases>
        <database name="erp_db" connection_string="sqlserver://user:pass@localhost:1433" />
    </databases>
    <flow>
        <excel_write file="./exports/customer_list.xlsx" sheet="Customers" db="erp_db">
            SELECT id, first_name, last_name, email, created_at 
            FROM dbo.customers 
            WHERE active = 1
            ORDER BY created_at DESC;
        </excel_write>
    </flow>
</pipeline>
```

### 2. Reading Excel Data into a JSON API Payload
Extract configuration or bulk upload data from an Excel sheet and send it to an external API endpoint.

```xml
<pipeline>
    <variables>
        <variable name="FILE_PATH" type="string" value="./uploads/new_users.xlsx" />
    </variables>
    <flow>
        <!-- Reads the "Users" sheet into JSON (using row 1 as keys) -->
        <excel_read file="{{FILE_PATH}}" sheet="Users" output_var="USER_JSON_DATA" />
        
        <sql_bulk 
            method="POST" 
            uri="https://api.example.com/v1/users/bulk" 
            content_type="application/json" 
            data="{{USER_JSON_DATA}}" />
    </flow>
</pipeline>
```

### 3. Dynamic Excel Generation with Variables
Use pipeline variables to dynamically name the output file and filter the SQL query.

```xml
<pipeline>
    <variables>
        <variable name="REPORT_MONTH" type="string" value="August_2026" />
        <variable name="DEPT_ID" type="int" value="42" />
    </variables>
    <flow>
        <!-- The file path and query both use template variables -->
        <excel_write file="./reports/finance/Expenses_{{REPORT_MONTH}}.xlsx" sheet="Expenses" db="finance_db">
            SELECT expense_id, amount, category, date_filed 
            FROM dbo.expenses 
            WHERE department_id = {{DEPT_ID}} 
              AND FORMAT(date_filed, 'MMMM_yyyy') = '{{REPORT_MONTH}}'
        </excel_write>
    </flow>
</pipeline>
```

### 4. Reading Excel Data Without Headers
Sometimes data files don't have headers. You can set `header="false"` to process the raw rows programmatically in a later script.

```xml
<pipeline>
    <flow>
        <excel_read file="./raw_data/metrics.xlsx" sheet="RawMetrics" header="false" output_var="RAW_DATA" />
        
        <!-- Process the array of arrays in a Go script -->
        <script lang="go">
            package main
            import (
                "fmt"
                "encoding/json"
                "host/vars"
            )
            func main() {
                rawData := vars.GetString("RAW_DATA")
                fmt.Printf("Successfully loaded raw data: %d bytes\n", len(rawData))
                // Add custom JSON unmarshaling and validation here
            }
        </script>
    </flow>
</pipeline>
```

### 5. Multi-Sheet Export (Using Parallel execution)
Extract multiple datasets simultaneously from the database into different Excel files using the `<parallel>` node.

```xml
<pipeline>
    <flow>
        <parallel max_threads="3">
            <excel_write file="./exports/dashboard/Sales.xlsx" sheet="Sales_Data" db="warehouse">
                SELECT * FROM mart.fct_sales WHERE year = 2026;
            </excel_write>
            
            <excel_write file="./exports/dashboard/Inventory.xlsx" sheet="Stock" db="warehouse">
                SELECT item_code, quantity, warehouse_loc FROM mart.dim_inventory;
            </excel_write>
            
            <excel_write file="./exports/dashboard/Employees.xlsx" sheet="HR_Data" db="warehouse">
                SELECT emp_id, department, status FROM mart.dim_employees;
            </excel_write>
        </parallel>
    </flow>
</pipeline>
```
### 6. Adding different tabs to the same Excel file
You can create multiple sheets within a single Excel file by specifying different `sheet` attributes in multiple `<excel_write>` nodes pointing to the same file.

```xml
<?xml version="1.0" encoding="UTF-8"?>
<pipeline description="Monthly Sales and Inventory Report Pipeline">
    <databases>
        <database name="sales_db" 
                  driver="postgres" 
                  connection_string="host=localhost port=5432 user=postgres password=secret dbname=sales sslmode=disable" 
                  description="Primary Analytics Database" />
    </databases>

    <flow>
        <!-- Tab 1: Executive Summary -->
        <excel_write id="export_summary" 
                     file="reports/Monthly_Executive_Report.xlsx" 
                     sheet="Summary KPI" 
                     db="sales_db"
                     description="Writes top-level monthly KPIs to the first tab">
            SELECT 
                DATE_TRUNC('month', order_date) AS month,
                COUNT(order_id) AS total_orders,
                SUM(total_amount) AS total_revenue
            FROM orders
            GROUP BY 1
            ORDER BY 1 DESC;
        </excel_write>

        <!-- Tab 2: Customer Breakdown -->
        <excel_write id="export_customers" 
                     file="reports/Monthly_Executive_Report.xlsx" 
                     sheet="Customer Details" 
                     db="sales_db"
                     description="Appends customer breakdown to a second tab">
            SELECT 
                customer_id,
                company_name,
                country,
                total_spent
            FROM customer_summary
            WHERE total_spent > 10000;
        </excel_write>

        <!-- Tab 3: Regional Inventory -->
        <excel_write id="export_inventory" 
                     file="reports/Monthly_Executive_Report.xlsx" 
                     sheet="Inventory Status" 
                     db="sales_db"
                     description="Appends inventory levels to a third tab">
            SELECT 
                warehouse_id,
                product_sku,
                stock_on_hand,
                reorder_level
            FROM warehouse_inventory;
        </excel_write>
    </flow>
</pipeline>
```
