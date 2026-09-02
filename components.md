# Components

## Database

The `<databases>` block registers named database connections used by scripts, bulk operations, and flow nodes. Each `<database>` entry supports the following attributes:

| Attribute | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `id` | string | No | - | Optional identifier for the database definition. |
| `name` | string | Yes | - | Unique database handle name referenced by `db`, `database`, `target_db`, and related attributes. |
| `driver` | string | No | - | Database driver name, such as `sqlite`, `postgres`, `mysql`, or `sqlserver`. |
| `type` | string | No | - | Optional logical database type or classification used to distinguish connection categories. |
| `connection_string` | string | Yes | - | Driver-specific DSN or connection string used to open the database connection. |
| `description` | string | No | - | Human-readable note describing the database purpose, environment, or usage. |

Example:

```xml
<databases>
  <database
    id="sales-db"
    name="sales_db"
    driver="postgres"
    type="primary"
    connection_string="host=localhost port=5432 user=postgres dbname=sales sslmode=disable"
    description="Primary sales database for reporting workloads" />
</databases>
```
