# New Pipeline Description Example
## Example Pipeline Description

This section provides an example of how to describe a pipeline, including its purpose, the databases it interacts with, and the scripts it executes. A well-documented pipeline helps in understanding its workflow and facilitates maintenance and collaboration.

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