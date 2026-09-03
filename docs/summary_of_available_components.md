# `<pipeline>`

The `<pipeline>` element defines a sequence of steps to be executed in order. It serves as the top-level container for all pipeline components, including templates, SQL steps, and bulk SQL operations.

## `<databases>`

The `<databases>` element declares database connections through nested `<database>` elements.

| Attribute / field | Mandatory | Description |
| --- | --- | --- |
| `databases.description` | No | Human-readable description of the database collection. |
| `databases` child element | No | A database connection definition. Any number of database child elements may be included. |
| `databases.id` | No | Optional identifier for an individual database definition. |
| `databases.name` | Yes | Unique connection name used by database-aware pipeline nodes. |
 

Notes:
- `<databases>` is optional and may occur once within a `<pipeline>`.
- Reference a configured connection by its `database.name`, for example with `db="reporting_db"` on a database-aware node.


### `<database>`

The `<database>` element defines a named database connection and its connection pool settings within a `<databases>` container.

| Attribute | Mandatory | Description |
| --- | --- | --- |
| `id` | No | Optional identifier for the database definition. |
| `name` | Yes | Unique database handle name referenced by `db`, `database`, `target_db`, and related attributes. |
| `driver` | No | Database driver name, such as `sqlite`, `postgres`, `mysql`, `sqlserver`, or `oracle`. Defaults to `sqlserver` when omitted. |
| `type` | No | Alias for `driver`. |
| `connection_string` | Yes | Driver-specific connection string / DSN used to open the database connection. Supports `{{VariableName}}` variable interpolation. |
| `max_open_conns` | No | Maximum number of simultaneous open connections in the pool. Defaults to `25` if omitted or `<= 0`. |
| `max_idle_conns` | No | Maximum number of idle connections retained in the pool for reuse. Defaults to `10` if omitted or `<= 0`. Automatically clamped to `max_open_conns` if greater. |
| `conn_max_lifetime_seconds` | No | Maximum duration a connection may be reused before being recycled. Accepts duration strings (e.g., `5m`, `300s`) or integer seconds. Defaults to `5m` (`300s`) if omitted or `<= 0`. |
| `workload` | No | Optional tuning profile metadata label describing runtime usage patterns (`oltp`, `bulk`, `analytics`, `batch`). |
| `description` | No | Human-readable description of the database connection. |

Notes:
- Declare `<database>` elements inside the `<databases>` container.
- Attribute names are case-insensitive and support snake_case, kebab-case (`-`), and dot (`.`) separators.
- Connection strings support variable placeholders using `{{VariableName}}`.
- When pool tuning attributes are omitted, the runtime applies default pool values (`max_open_conns=25`, `max_idle_conns=10`, `conn_max_lifetime=5m`).

Example:

```xml
<databases>
  <database
    id="analytics-db"
    name="analytics_db"
    driver="postgres"
    connection_string="host=localhost port=5432 user=postgres password={{DB_PASSWORD}} dbname=analytics sslmode=disable"
    max_open_conns="50"
    max_idle_conns="20"
    conn_max_lifetime_seconds="300"
    workload="analytics"
    description="Analytics reporting replica database" />
</databases>
```


## `<variables>`

The `<variables>` element declares pipeline variables through nested `<variable>` elements.

| Attribute / field | Mandatory | Description |
| --- | --- | --- |
| `description` | No | Human-readable description of the variables collection. |
| `variable` child element | No | A variable definition. Any number of variable child elements may be included. |
| `variable.id` | No | Optional identifier for an individual variable definition. |
| `variable.name` | Yes | Name used to access the variable throughout the pipeline. |

Notes:
- `<variables>` is optional and may occur once within a `<pipeline>`.
- Initialized variables are available to pipeline nodes and may be interpolated where supported using `{{VariableName}}`.


### `<variable>`

The `<variable>` element defines an initial pipeline variable within a `<variables>` container.

| Attribute | Mandatory | Description |
| --- | --- | --- |
| `id` | No | Optional identifier for the variable definition. |
| `name` | Yes | Variable name used to retrieve or interpolate the value throughout the pipeline. |
| `type` | No | Value type. Defaults to `string`; supported values are `string`, `int` / `integer`, `bool` / `boolean`, `float` / `double` / `float64`, `datetime`, and `date`. |
| `value` | Yes | Initial variable value. Flow parses it according to `type`. |
| `description` | No | Human-readable description of the variable. |

Notes:
- Declare `<variable>` elements inside the `<variables>` container.
- Variable names can be interpolated in supported fields using `{{VariableName}}`.



## `<preflight>`

The `<preflight>` element contains setup and validation nodes that can be run before the main `<flow>` pipeline.

| Attribute / field | Mandatory | Description |
| --- | --- | --- |
| attributes | No | The `<preflight>` element does not define any attributes. |
| pipeline nodes | No | Zero or more executable child elements used for environment checks, initialization, connectivity validation, or other preparation steps. |

Notes:
- `<preflight>` is optional and may occur once, before `<flow>`, within a `<pipeline>`.
- Preflight and flow nodes are parsed into separate collections. The caller chooses whether to execute preflight nodes, flow nodes, or preflight followed by flow.

## `<flow>`

The `<flow>` element is the pipeline's main execution container. Its child nodes are parsed and executed in document order.

| Attribute / field | Mandatory | Description |
| --- | --- | --- |
| attributes | No | The `<flow>` element does not define any attributes. |
| pipeline nodes | No | Zero or more executable child elements, including `sql`, `sql_bulk`, `assert`, `http_client`, `template`, file, Excel, path-extraction, group, conditional, loop, and parallel nodes. |

Notes:
- `<flow>` is optional in the pipeline schema, but it is normally used to contain the pipeline's executable work.
- Child nodes execute sequentially unless a control-flow element such as `<parallel>`, `<foreach>`, or `<while>` changes that behavior.

## Child Actions allowed under `<flow>` or `<preflight>`

### Conditional Execution and Logical Grouping
#### `<foreach>`

The `<foreach>` element runs its nested pipeline nodes once for every row returned by its SQL driver query.

| Attribute / field | Mandatory | Description |
| --- | --- | --- |
| `id` | No | Unique identifier for the foreach step. If omitted, Flow generates an identifier in the form `foreach_N`. |
| `language` | No | Driver-query language. Defaults to `sql`; the current foreach runtime executes SQL drivers. |
| `lang` | No | Alias for `language`. |
| `db` | Yes | Name of the configured database connection used to execute the driver query. |
| `database` | No | Alias for `db`. |
| `var` | No | Name of a pipeline variable that supplies the SQL driver query. When it resolves to non-empty content, it takes precedence over inline query content. |
| `variable` | No | Alias for `var`. |
| `content` | Conditional | Inline SQL driver query placed before nested pipeline elements. Required when `var` or `variable` does not resolve to a non-empty SQL query. |
| nested pipeline nodes | No | Child steps executed sequentially for each returned row. The query's columns are available as pipeline variables in their original, lowercase, and uppercase forms. |

Notes:
- Each iteration sets `LOOP_INDEX` to the zero-based row index before executing child nodes.
- The foreach result reports the number of driver rows and completed iterations.

#### `<group>`

The `<group>` element organizes nested pipeline nodes into a sequential block. It can execute conditionally and can optionally wrap its child nodes in a database transaction.

| Attribute / field | Mandatory | Description |
| --- | --- | --- |
| `id` | No | Unique identifier for the group. |
| `var` | No | Name of the pipeline variable used for conditional execution. Alias for `if_var`. |
| `if_var` | No | Name of the pipeline variable used for conditional execution. |
| `equals` | No | Expected value for the condition. Alias for `if_equals`. |
| `value` | No | Alias for `if_equals`. |
| `if_val` | No | Alias for `if_equals`. |
| `if_equals` | No | Expected value required for the group to execute when `var` or `if_var` is set. |
| `condition` | No | Inline condition expression. Used as the condition variable when `var` and `if_var` are not set. |
| `cond` | No | Alias for `condition`. |
| `transaction` | No | Boolean that wraps child-node execution in a database transaction. Defaults to `false`. |
| `db` | Conditional | Name of the configured database connection for the transaction. Required when `transaction` is `true`. |
| `database` | Conditional | Alias for `db`. |
| nested pipeline nodes | No | Child steps executed sequentially when the condition passes, or unconditionally when no condition is supplied. |

Notes:
- If a transaction child step fails, Flow rolls back the transaction; otherwise it commits after all child steps complete.
- When a condition is present but does not pass, the group and its child nodes are skipped.

#### `<while>`

The `<while>` element repeatedly executes its nested pipeline nodes while its condition evaluates to true.

| Attribute / field | Mandatory | Description |
| --- | --- | --- |
| `id` | No | Unique identifier for the while step. If omitted, Flow generates an identifier in the form `while_N`. |
| `var` | Conditional | Name of the pipeline variable evaluated for each iteration. Alias for `if_var`. |
| `if_var` | Conditional | Name of the pipeline variable evaluated for each iteration. |
| `equals` | No | Expected condition value. Alias for `if_equals`. |
| `val` | No | Alias for `if_equals`. |
| `value` | No | Alias for `if_equals`. |
| `if_val` | No | Alias for `if_equals`. |
| `if_equals` | No | Expected condition value used with `var` or `if_var`. |
| `condition` | Conditional | Inline condition expression. Used as the condition variable when `var` and `if_var` are not set. |
| `cond` | Conditional | Alias for `condition`. |
| `max_iterations` | No | Positive maximum iteration count. Defaults to `1000` to prevent unbounded loops. |
| `max_loops` | No | Alias for `max_iterations`. |
| nested pipeline nodes | No | Child steps executed sequentially during each iteration. |

Notes:
- Each iteration sets `WHILE_INDEX` to its zero-based index before executing child nodes.
- Execution stops when the condition is false, the maximum iteration limit is reached, a child step fails, or the pipeline context is cancelled.



#### `<if>`, `<then>`, `<else>`

The `<if>` element conditionally executes one of two child branches. `<then>` contains the branch for a passing condition, and `<else>` contains the branch for a failing condition.

| Entity / attribute | Mandatory | Description |
| --- | --- | --- |
| `<if>` | No | Conditional container that evaluates a pipeline variable or condition expression. |
| `if.id` | No | Optional identifier for the conditional step. |
| `if.var` | Conditional | Name of the pipeline variable evaluated by the condition. Alias for `if_var`. |
| `if.if_var` | Conditional | Name of the pipeline variable evaluated by the condition. |
| `if.equals` | No | Expected condition value. Alias for `if_equals`. |
| `if.val` | No | Alias for `if_equals`. |
| `if.value` | No | Alias for `if_equals`. |
| `if.if_val` | No | Alias for `if_equals`. |
| `if.if_equals` | No | Expected condition value used with `var` or `if_var`. |
| `if.condition` | Conditional | Inline condition expression, used when `var` and `if_var` are not set. |
| `if.cond` | Conditional | Alias for `condition`. |
| `if.description` | No | Human-readable description of the conditional step. |
| `<then>` | No | Container for nodes executed when the condition passes. It does not define attributes. |
| `<else>` | No | Container for nodes executed when the condition fails. It does not define attributes. |
| inline child nodes | No | Child nodes placed directly inside `<if>` are treated as part of the then branch. |

Notes:
- Both `<then>` and `<else>` are optional; they can each appear at most once inside an `<if>` element.
- Flow executes only the selected branch, based on the evaluated condition.

### Currently Supported Tasks under `<flow>` and `<preflight>`
Support under `<flow>` and `<preflight>` including tasks within the conditional branches, groups and loops.
Some tasks may support their own sub-tasks or nested structure, such as `<assert>`.

#### `<template>`

The `<template>` element renders Go text templates using the current pipeline variables. It can read the template from an external file or from inline XML content, and it can optionally store the rendered output in a variable.

| Attribute / field | Mandatory | Description |
| --- | --- | --- |
| `id` | Yes | Unique identifier for the template node. This is the logical name used in execution results and diagnostics. |
| `name` | No | Friendly name for the template when it is parsed by Go's template engine. If omitted, the engine still works with a default internal name. |
| `file` | No | Path to an external template file. If provided, the file contents are used as the template source. This is optional when the template is provided inline. |
| `engine` | No | Declares the template engine. In the current implementation, execution uses Go `text/template`; this attribute is accepted for compatibility and documentation purposes. |
| `output_var` | No | Name of the pipeline variable that receives the rendered output. This is the primary output variable name. |
| `var` | No | Alias for `output_var`. If `output_var` is not set, this value is used instead. |
| `mode` | No | Output behavior for the result. `summary` returns a brief success message; any other value (including unset) returns the rendered template text itself. |
| `content` | Conditional | Inline template body text located between the opening and closing `<template>` tags. This is required when `file` is not set. |

Notes:
- `file` and inline `content` are mutually exclusive sources for the template body.
- The template is rendered with the current registry variables available in the pipeline.
- If both `output_var` and `var` are empty, the rendered text is still generated but not stored into a named variable. 


#### `<template_html>`

The `<template_html>` element renders Go HTML templates using the current pipeline variables. It can read the template from an external file or from inline XML content, and it can optionally store the rendered output in a variable.

The `<template>` element renders Go text templates using the current pipeline variables. It can read the template from an external file or from inline XML content, and it can optionally store the rendered output in a variable.

For `<template_html>`, the Go `html/template` engine is used, which provides automatic HTML escaping to help prevent XSS vulnerabilities.

| Attribute / field | Mandatory | Description |
| --- | --- | --- |
| `id` | Yes | Unique identifier for the template node. This is the logical name used in execution results and diagnostics. |
| `name` | No | Friendly name for the template when it is parsed by Go's template engine. If omitted, the engine still works with a default internal name. |
| `file` | No | Path to an external template file. If provided, the file contents are used as the template source. This is optional when the template is provided inline. |
| `engine` | No | Declares the template engine. In the current implementation, execution uses Go `text/template`; this attribute is accepted for compatibility and documentation purposes. |
| `output_var` | No | Name of the pipeline variable that receives the rendered output. This is the primary output variable name. |
| `var` | No | Alias for `output_var`. If `output_var` is not set, this value is used instead. |
| `mode` | No | Output behavior for the result. `summary` returns a brief success message; any other value (including unset) returns the rendered template text itself. |
| `content` | Conditional | Inline template body text located between the opening and closing `<template>` tags. This is required when `file` is not set. |

Notes:
- `file` and inline `content` are mutually exclusive sources for the template body.
- The template is rendered with the current registry variables available in the pipeline.
- If both `output_var` and `var` are empty, the rendered text is still generated but not stored into a named variable. 

#### `<sql>`

The `<sql>` element executes a SQL statement against a configured database connection. The statement can be supplied inline or read from a pipeline variable.

| Attribute / field | Mandatory | Description |
| --- | --- | --- |
| `id` | No | Unique identifier for the SQL step. If omitted, Flow generates an identifier in the form `sql_N`. |
| `db` | Yes | Name of the configured database connection on which to execute the SQL statement. |
| `database` | No | Alias for `db`. |
| `var` | No | Name of a pipeline variable that supplies the SQL statement. When its value is non-empty, it takes precedence over inline content. |
| `variable` | No | Alias for `var`. |
| `output_var` | No | Name of the pipeline variable that receives the query output. |
| `out_var` | No | Alias for `output_var`. |
| `output_variable` | No | Alias for `output_var`. |
| `content` | Conditional | SQL statement placed between the opening and closing `<sql>` tags. Required when `var` or `variable` does not resolve to a non-empty SQL statement. |

Notes:
- SQL output is always stored in `LAST_OUTPUT`; specify an output attribute to also store it under a named variable.
- Use either `db` or `database`, and use either inline `content` or a non-empty `var` / `variable` SQL source.

#### `<sql_bulk>`

The `<sql_bulk>` element streams the rows returned by a source SQL query into a destination table.

| Attribute / field | Mandatory | Description |
| --- | --- | --- |
| `id` | No | Unique identifier for the bulk SQL step. If omitted, Flow generates an identifier in the form `sql_bulk_N`. |
| `db` | Yes | Name of the configured source database connection that executes the query. |
| `database` | No | Alias for `db`. |
| `target_db` | No | Name of the configured destination database connection. If omitted, the source database is used. |
| `target_database` | No | Alias for `target_db`. |
| `target_table` | Yes | Destination table that receives the streamed rows. Pipeline variable placeholders are supported. |
| `batch_size` | No | Maximum number of rows written per batch. The underlying ETL operation uses its default when this is not specified. |
| `var` | No | Name of a pipeline variable that supplies the source SQL query. When its value is non-empty, it takes precedence over inline content. |
| `variable` | No | Alias for `var`. |
| `output_var` | No | Name of the pipeline variable that receives the number of rows copied. |
| `out_var` | No | Alias for `output_var`. |
| `tablock` | No | Enables or disables a table lock during SQL Server bulk insert. Defaults to `true`. |
| `check_constraints` | No | Whether to evaluate destination table constraints during SQL Server bulk insert. Defaults to `false`. |
| `fire_triggers` | No | Whether to execute destination table triggers during SQL Server bulk insert. Defaults to `false`. |
| `keep_nulls` | No | Whether explicit `NULL` values are preserved during SQL Server bulk insert. Defaults to `false`. |
| `content` | Conditional | Source SQL query placed between the opening and closing `<sql_bulk>` tags. Required when `var` or `variable` does not resolve to a non-empty SQL query. |

Notes:
- `target_table` is required because bulk SQL streams query results rather than returning them as a result set.
- The number of copied rows is always stored in `LAST_OUTPUT`; specify an output attribute to also store it under a named variable.

#### `<assert>` 

The `<assert>` element checks whether a pipeline variable meets an expected condition and can halt the pipeline, continue with a warning, set a failure variable, or run fallback nodes when the check fails.

| Attribute / field | Mandatory | Description |
| --- | --- | --- |
| `id` | No | Unique identifier for the assertion step. |
| `var` | Yes | Name of the pipeline variable evaluated by the assertion. |
| `equals` | Conditional | Expected value for the assertion. Use this or `value` when testing equality. `equals` takes precedence when both are set. |
| `value` | Conditional | Alias for the expected equality value. Used only when `equals` is empty. |
| `operator` | No | Declares a comparison operator. This attribute is accepted by the configuration model; assertion evaluation uses the variable and expected-value condition supported by Flow. |
| `message` | No | Custom message included in the assertion result when the check fails. A default message is generated when omitted. |
| `on_failure` | No | Failure action. `halt` (the default) stops the pipeline; `warn` and `continue` record a warning and allow it to continue. Any unrecognized value also halts the pipeline. |
| `fail_var` | No | Name of a pipeline variable set when the assertion fails. |
| `fail_val` | No | Value assigned to `fail_var` on failure. Defaults to `true` when omitted. |
| `on_failure` child element | No | Nested pipeline nodes to execute only when the assertion fails, such as cleanup or notification steps. |

Notes:
- `var` is mandatory; use `equals` or `value` to define an equality expectation.
- Failure child nodes execute before the `on_failure` action determines whether the pipeline stops or continues.

#### `<file_read>`

The `<file_read>` element loads a local file's contents into a pipeline variable.

| Attribute | Mandatory | Description |
| --- | --- | --- |
| `id` | No | Unique identifier for the file-read step. If omitted, Flow generates an identifier in the form `file_read_N`. |
| `file` | Conditional | Path of the file to read. This is the preferred path attribute and supports pipeline variable interpolation. |
| `path` | Conditional | Alias for `file`, used only when `file` is empty. |
| `filename` | Conditional | Alias for `file`, used only when both `file` and `path` are empty. |
| `var` | Conditional | Name of the pipeline variable that receives the file contents. This output alias has the highest precedence. |
| `variable` | Conditional | Alias for `var`, used when `var` is empty. |
| `output_var` | Conditional | Alias for the output variable, used when `var` and `variable` are empty. |
| `output_variable` | Conditional | Alias for the output variable, used when earlier output aliases are empty. |
| `out_var` | Conditional | Alias for the output variable, used when all earlier output aliases are empty. |

Notes:
- Provide exactly one path attribute (`file`, `path`, or `filename`) and one output attribute (`var`, `variable`, `output_var`, `output_variable`, or `out_var`).
- The file contents are stored in the selected output variable and in `LAST_OUTPUT`.

#### `<file_save>`

Flow implements file writing with the `<file_save>` element. It writes content from a pipeline variable or from its inline body to a local file.

| Attribute / field | Mandatory | Description |
| --- | --- | --- |
| `id` | No | Unique identifier for the file-write step. If omitted, Flow generates an identifier in the form `file_save_N`. |
| `file` | Conditional | Destination file path. This is the preferred path attribute and supports pipeline variable interpolation. |
| `path` | Conditional | Alias for `file`, used only when `file` is empty. |
| `filename` | Conditional | Alias for `file`, used only when both `file` and `path` are empty. |
| `var` | No | Name of a pipeline variable that supplies the content to write. When set, its value takes precedence over inline content. |
| `variable` | No | Alias for `var`. |
| `append` | No | Boolean controlling write mode. `true` appends to the existing file; omitted or `false` overwrites it. |
| `content` | Conditional | Inline text placed between the opening and closing `<file_save>` tags. Used when `var` and `variable` are not set. Pipeline variable interpolation is supported. |

Notes:
- Provide one path attribute (`file`, `path`, or `filename`) and either a variable source or inline content.
- Parent directories are created automatically before the file is written.

#### `<excel_write>`

Flow implements Excel saving with the `<excel_write>` element. It creates or updates an `.xlsx` workbook and can populate a worksheet with the rows returned by an inline SQL query.

| Attribute / field | Mandatory | Description |
| --- | --- | --- |
| `id` | No | Unique identifier for the Excel-save step. If omitted, Flow generates an identifier in the form `excel_write_N`. |
| `file` | Yes | Destination `.xlsx` file path. Pipeline variable interpolation is supported. An existing workbook is updated; otherwise a new workbook is created. |
| `sheet` | No | Name of the worksheet to create or update. Defaults to `Sheet1`. |
| `db` | Conditional | Name of the configured database connection used to execute the inline query. Required when supplying a query. |
| `var` | No | Accepted by the configuration model for compatibility, but the Excel write runtime currently populates sheets from inline query content. |
| `content` | Conditional | Inline SQL query placed between the opening and closing `<excel_write>` tags. Required with `db` to export query results into the worksheet. |

Notes:
- When both `db` and inline query content are provided, query columns become the first worksheet row and returned rows are written beneath them.
- Parent directories are created automatically before the workbook is saved.

#### `<excel_read>`

The `<excel_read>` element reads an `.xlsx` worksheet and serializes its rows as a JSON array of objects.

| Attribute | Mandatory | Description |
| --- | --- | --- |
| `id` | No | Unique identifier for the Excel-read step. If omitted, Flow generates an identifier in the form `excel_read_N`. |
| `file` | Yes | Path of the `.xlsx` workbook to read. Pipeline variable interpolation is supported. |
| `sheet` | No | Worksheet name to read. Defaults to the workbook's first worksheet. |
| `header` | No | Boolean that controls whether the first row contains column names. Defaults to `true`; the first row becomes the JSON object keys. |
| `output_var` | Conditional | Name of the pipeline variable that receives the serialized JSON. This is the preferred output variable name. |
| `var` | Conditional | Alias for `output_var`, used when `output_var` is empty. |

Notes:
- Provide `output_var` or `var` to retain the JSON under a named pipeline variable; the value is also stored in `LAST_OUTPUT`.
- With `header="false"`, the current runtime produces an empty JSON array because it serializes rows only when a header row is enabled.

#### `<json_path>`

The `<json_path>` element extracts values or nodes from JSON supplied by a file or pipeline variable.

| Attribute / field | Mandatory | Description |
| --- | --- | --- |
| `id` | No | Unique identifier for the JSON-path step. If omitted, Flow generates an identifier in the form `json_path_N`. |
| `file` | Conditional | Path to the source JSON file. Supports pipeline variable interpolation and takes precedence over `var`. |
| `var` | Conditional | Name of the pipeline variable containing source JSON. Used when `file` is not set. |
| `path` | Conditional | JSONPath query expression. Takes precedence over `jsonpath`; required when query content is not supplied inline. |
| `jsonpath` | Conditional | Alias for `path`, used when `path` is empty. |
| `mode` | No | Output format: `value` (default) joins matched values with newlines; `json` returns a single match as JSON or multiple matches as an array; `json_array` always returns matches as a JSON array. |
| `output_var` | Conditional | Name of the pipeline variable that receives the extracted value. This is the preferred output variable name. |
| `out_var` | Conditional | Alias for `output_var`, used when `output_var` is empty. |
| `content` | Conditional | Inline JSONPath query placed between the opening and closing `<json_path>` tags. Used when neither `path` nor `jsonpath` is set. |

Notes:
- Supply one JSON source (`file` or `var`), one JSONPath expression, and one output attribute (`output_var` or `out_var`).
- Extracted output is stored in the selected output variable and in `LAST_OUTPUT`.

#### `<yaml_path>`

The `<yaml_path>` element extracts values or nodes from YAML supplied by a file or pipeline variable.

| Attribute / field | Mandatory | Description |
| --- | --- | --- |
| `id` | No | Unique identifier for the YAML-path step. If omitted, Flow generates an identifier in the form `yaml_path_N`. |
| `file` | Conditional | Path to the source YAML file. Supports pipeline variable interpolation and takes precedence over `var`. |
| `var` | Conditional | Name of the pipeline variable containing source YAML. Used when `file` is not set. |
| `path` | Conditional | Path query expression evaluated against the YAML converted to normalized JSON. Takes precedence over `yamlpath`; required when query content is not supplied inline. |
| `yamlpath` | Conditional | Alias for `path`, used when `path` is empty. |
| `mode` | No | Output format: `value` (default) joins matched values with newlines; `json` returns a single match as JSON or multiple matches as an array; `json_array` always returns matches as a JSON array; `yaml` serializes matched nodes as YAML. |
| `output_var` | Conditional | Name of the pipeline variable that receives the extracted value. This is the preferred output variable name. |
| `out_var` | Conditional | Alias for `output_var`, used when `output_var` is empty. |
| `content` | Conditional | Inline path query placed between the opening and closing `<yaml_path>` tags. Used when neither `path` nor `yamlpath` is set. |

Notes:
- Supply one YAML source (`file` or `var`), one path expression, and one output attribute (`output_var` or `out_var`).
- Extracted output is stored in the selected output variable and in `LAST_OUTPUT`.

#### `<xml_xpath>`

Flow implements XML-path extraction with the `<xml_xpath>` element. It evaluates an XPath expression against XML supplied by a file or pipeline variable.

| Attribute / field | Mandatory | Description |
| --- | --- | --- |
| `id` | No | Unique identifier for the XML-path step. |
| `file` | Conditional | Path to the source XML file. Supports pipeline variable interpolation and takes precedence over `var`. |
| `var` | Conditional | Name of the pipeline variable containing source XML. Used when `file` is not set. |
| `xpath` | Conditional | XPath expression to evaluate. Required when the expression is not supplied as inline content. |
| `mode` | No | Output format: `text` (default) joins each matched node's inner text with newlines; `xml` joins serialized matched nodes with newlines; `json_array` returns the matched outputs as a JSON array of strings. |
| `output_var` | Yes | Name of the pipeline variable that receives the extracted output. |
| `content` | Conditional | Inline XPath expression placed between the opening and closing `<xml_xpath>` tags. Used when `xpath` is not set. |

Notes:
- Supply one XML source (`file` or `var`), one XPath expression, and `output_var`.
- Extracted output is stored in `output_var` and in `LAST_OUTPUT`.

#### `<http_client>`

The `<http_client>` element sends an HTTP request and can store its response body and status code in pipeline variables.

| Attribute / field | Mandatory | Description |
| --- | --- | --- |
| `id` | No | Unique identifier for the HTTP-client step. If omitted, Flow generates an identifier in the form `http_N`. |
| `uri` | Conditional | Target request URL. Takes precedence over `url`; one endpoint attribute is required. |
| `url` | Conditional | Alias for `uri`, used when `uri` is empty. |
| `method` | No | HTTP method. Defaults to `GET`. |
| `data` | No | Request payload. Takes precedence over inline content and supports pipeline variable interpolation. |
| `content` | Conditional | Inline request payload placed between the opening and closing `<http_client>` tags. Used when `data` is empty. |
| `headers` | No | Custom headers as comma-, semicolon-, or newline-separated `Name: Value` pairs. |
| `content_type` | No | Sets the request `Content-Type` header. |
| `var` | No | Response-body variable name. This alias has the highest precedence. |
| `variable` | No | Alias for the response-body variable, used when `var` is empty. |
| `output_var` | No | Alias for the response-body variable, used when `var` and `variable` are empty. |
| `output_variable` | No | Alias for the response-body variable, used when earlier aliases are empty. |
| `out_var` | No | Alias for the response-body variable, used when all earlier aliases are empty. |
| `status_code_var` | No | Variable name that receives the HTTP status code. This alias has the highest precedence. |
| `status_code_variable` | No | Alias for `status_code_var`. |
| `status_var` | No | Alias for `status_code_var`. |
| `status_variable` | No | Alias for `status_code_var`. |
| `timeout` | No | Total request timeout as a Go duration, such as `30s` or `2m`. |
| `follow_redirects` | No | Whether to automatically follow redirect responses. |
| `max_redirects` | No | Maximum number of redirects permitted before the request fails. |
| `cookie_jar` | No | Enables an in-memory cookie jar for the request client. |
| `proxy` | No | HTTP proxy URL. |
| `tls_insecure_skip_verify` | No | Disables TLS certificate verification. Use only for trusted development or test endpoints. |
| `tls_handshake_timeout` | No | Maximum TLS handshake duration. |
| `tls_server_name` | No | Server name used for TLS SNI and certificate verification. |
| `tls_min_version` / `tls_max_version` | No | Minimum or maximum permitted TLS version. |
| `disable_keep_alives` / `disable_compression` | No | Disable connection reuse or automatic compression handling. |
| `max_idle_conns` / `max_idle_conns_per_host` / `max_conns_per_host` | No | Connection-pool limits. |
| `idle_conn_timeout` / `response_header_timeout` / `expect_continue_timeout` | No | Transport timeouts expressed as Go durations. |
| `max_response_header_bytes` | No | Maximum response-header size in bytes. |
| `write_buffer_size` / `read_buffer_size` | No | HTTP transport buffer sizes in bytes. |
| `force_attempt_http2` | No | Forces HTTP/2 negotiation when supported by the transport. |

Notes:
- Endpoint, header, content type, and payload values support pipeline variable interpolation.
- The response body is always stored in `LAST_OUTPUT`; provide a response output alias to store it under a named variable.

