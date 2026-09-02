# Release Notes: Flow Engine Enhancements v1.2.18

## Key Enhancements

- **Database-Level Connection Pool Tuning**: Added optional connection pool attributes directly to `<database>` entries to tune pool sizing and lifecycle per workload:
  - `max_open_conns`
  - `max_idle_conns`
  - `conn_max_lifetime_seconds`
  - `workload`

  This allows different workloads such as `oltp`, `bulk`, `analytics`, and `batch` to apply the right pool strategy without changing the SQL node contract.

- **Performance-focused ETL refinements**: Reduced allocation churn in the hot row-reading path of `StreamETL` by simplifying the row collection/batch flush flow and avoiding extra producer/consumer indirection.

- **Safer Go runtime setup**: Centralized Yaegi interpreter setup while preserving correctness for repeated Go script execution without reintroducing stale or redeclared symbol issues.

## Bug Fixes
- **Removed Extra carriage returns**: Fixed an issue where unnecessary carriage return or new line characters (`\r`,`\n`) were present in the output, ensuring cleaner and more consistent formatting.

# Release Notes: Flow Engine Enhancements v1.2.17

## Key Enhancements

- **Added Support for Preflight**: Introduced a new preflight check mechanism to validate pipeline configurations and dependencies before execution, ensuring smoother and error-free ETL flows.

## Bug Fixes
- **Removed Extra carriage returns**: Fixed an issue where unnecessary carriage return or new line characters (`\r`,`\n`) were present in the output, ensuring cleaner and more consistent formatting.

# Release Notes: Flow Engine Enhancements v1.2.15🌊

## Key Enhancements

- **Native Assert Extraction (`<assert>`):** Introduced a new assertion mechanism to enforce validation rules and handle assertion failures within ETL flows. Supports basic assertions, failure handling, automatic ID assignment, and failure variables.
- **New Flow Node Types:** Added support for new flow high level node, to add semantic clarity and improve readability of ETL/workflow pipelines. The original scripts is still supported but deprecated in favor of the new high-level flow node.


# Release Notes: Flow Engine Enhancements v1.2.14🌊

We are pleased to introduce major architectural enhancements to the Flow Pipeline Engine: **Comprehensive `context.Context` Propagation**, **Thread-Isolated Parallel Variable Merging with Conflict Resolution**, **Dynamic Template Rendering**, and **Filesystem I/O Nodes**.

---

## 🚀 Key Enhancements

### 1. Robust `context.Context` Propagation
All execution boundaries and resource calls are now fully context-aware:
- **Database Contextualization:** Replaced standard queries, transactions, and inserts with their `Context` equivalents (`QueryContext`, `ExecContext`, `BeginTx`, `PrepareContext`). Database operations are now immediately aborted and rolled back if a cancellation or timeout is tripped.
- **Subprocess Cancellation:** Custom scripts (`dotnet-script`, `powershell`, Unix shells) are now spawned with `exec.CommandContext(...)`, guaranteeing immediate termination of child OS processes upon timeout or parent cancellation.
- **Embedded Script Control:** Injected the active pipeline `ctx` directly into the dynamic Go (Yaegi) interpreter exported closures.
- **Cooperative Loop Terminations:** Added select-drain cooperative cancellation checks to `<foreach>`, `<while>`, and heterogeneous database bulk streaming copies (`StreamETL`) to eradicate goroutine leaks.

### 2. Thread-Isolated Parallel Variable Merging & Conflict Resolution
Isolated executions inside `<parallel>` queues are now robustly tracked and merged:
- **Dirty Mutation Tracking:** Introduced a thread-safe `dirtyVars` tracking map inside the environment `Registry`.
- **Variable Mutation Isolation:** Only variables explicitly changed or added during a parallel worker's execution are registered as mutated. Stale variables from snapshot snapshots are safely discarded rather than overriding changes from concurrent tasks.
- **Automatic Namespacing:** If multiple parallel branches modify the exact same variable, a conflict resolution mechanism automatically namespaces them inside the parent registry as `WORKER_<id>_<variable_name>`. Non-colliding keys merge directly back into the parent registry.
- **Thread ID Injection:** A thread-specific `_THREAD_ID` variable is injected into each parallel worker's snapshot.

### 3. Highly Configurable `<http_client>` Node & Unified Identifiers (`id`)
To expand the pipeline engine's capabilities to external integrations, we have introduced a powerful new node type along with unified tracking support:
- **Unified Identifiers (`id` attribute):** Every major pipeline node or step now supports a unique `id` attribute. This drastically improves debuggability, verbose logging output, and schema validation error reporting.
- **Advanced `<http_client>` Node:** Fully exposes the Go HTTP standard library client and transport properties inside the XML pipeline AST, including:
  - Custom timeouts, redirect constraints, and session tracking (`cookie_jar`).
  - TLS specifications such as certificate verification overrides (`tls_insecure_skip_verify`), SNI ServerName, and min/max protocol restrictions (TLS 1.0 up to TLS 1.3).
  - Rich transport settings: customizable HTTP proxying, buffer sizes, connection pool limiters, keep-alive controls, and forced HTTP/2 negotiation.
  - Automatic template/variable interpolation (`{{VarName}}`) in URLs, headers, and request body content (either from `data` attribute or inner body text).
  - Response payload and integer status code persistence back to environment variables.

### 4. Dynamic Template Rendering & Filesystem I/O Nodes (`<template>`, `<file_save>`, `<file_read>`)
To streamline file manipulation, payload generation, and document processing, three new dedicated AST nodes have been introduced:
- **Dynamic `<template>` Node:** Uses Go's `text/template` engine (with `missingkey=zero`) to evaluate inline content or load external template files (`file` attribute) using current pipeline variables, persisting the output to a specified variable (`output_var` / `var`).
- **Filesystem Write Node (`<file_save>`):** Writes variable content (`var` / `variable`) or inline body text to a specified file path (`file`, `path`, or `filename`). Automatically creates missing parent directories and supports both overwrite (default) and append (`append="true"`) modes.
- **Filesystem Read Node (`<file_read>`):** Loads file contents directly from disk into pipeline environment variables (`output_var` / `var`) for use in downstream HTTP requests, scripts, or templates.
- **Dynamic File Path Interpolation:** Target file paths across both `<file_save>` and `<file_read>` nodes fully support `{{VarName}}` variable replacement.

### 5. Native Excel Import & Export (`<excel_read>`, `<excel_write>`)
Direct spreadsheet capabilities have been integrated to bridge relational database tables, workflow variables, and business reporting:
* **Excel Extraction (`<excel_read>`):** Parses specified worksheets (`sheet` attribute) from `.xlsx` files directly into JSON strings assigned to pipeline variables (`output_var` / `var`). Automatically processes row headers (`header="true"`) into object keys.
* **Database Query Export (`<excel_write>`):** Executes inline SQL queries against configured database connections (`db` attribute) and streams the query results directly into formatted `.xlsx` workbooks on disk (`file` attribute).
* **Multi-Tab Output Support:** Supports appending and writing different database queries into separate tabs/sheets of the same Excel workbook by specifying the same file path across sequential `<excel_write>` nodes with different `sheet` attributes.
* **Automated Directory Creation & Interpolation:** Destination paths automatically create missing directory structures and evaluate variable placeholders (`{{VarName}}`).

### 6. Native XML XPath Extraction (`<xml_xpath>`)
To support structured data processing from APIs and legacy configurations:
- **Flexible Sourcing:** Extracts nodes directly from inline body XML, pipeline environment variables (`var` attribute), or external files (`file` attribute).
- **XPath Queries:** Supports advanced XPath query syntax defined in attributes or within the node's body.
- **Multiple Output Formats:** Formats results as plaintext values (joined by newlines), original outer/inner XML tags (`mode="xml"`), or marshals them into serialized JSON string arrays (`mode="json_array"`).

### 7. Native JSONPath Extraction (`<json_path>`)
To handle lightweight modern data serialization formats:
- **Source Selection:** Queries raw JSON payloads sourced from disk files (`file` attribute) or environment variables (`var` attribute).
- **Flexible JSONPath Syntax:** Parses JSONPath expressions specified directly in attributes (`jsonpath` / `path`) or in the element body text.
- **Configurable Modes:** Supports default extraction as raw scalars/lines (`mode="value"`), a single JSON node (`mode="json"`), or serialized JSON arrays (`mode="json_array"`).
### 8. Native YAMLPath Extraction (`<yaml_path>`)
To query highly structured infrastructure and configuration payloads:
- **Unified Processing:** Loads YAML documents from disk (`file` attribute) or variables (`var` attribute), normalizing it into a JSON-compatible format internally.
- **Advanced Querying:** Runs powerful YAML path patterns defined in attributes (`yamlpath` / `path`) or tag body texts.
- **Rich Representation Modes:** Extracts queries into raw scalars (`mode="value"`), serialized JSON arrays (`mode="json_array"`), or formats nested subsets back into clean YAML format block strings (`mode="yaml"`).

### 9. Native SQL and Bulk Streaming SQL Nodes (`<sql>`, `<sql_bulk>`)
To streamline database execution and high-performance cross-database data synchronization:
- **Unified `<sql>` Node:** Directly executes SQL commands (DDL/DML or queries) against any target database. Supports capturing output streams into pipeline variables via `output_var`.
- **Streaming `<sql_bulk>` Node:** Automatically handles streaming query results from a source database directly to a destination table with batching configurations (`batch_size`, `tablock`, etc.), resolving high-volume ETL overhead.

### 10. Native Assert Extraction (`<assert>`)
To enforce validation rules and handle assertion failures within ETL flows:
- **Basic Assertions:** Checks conditions on pipeline variables using attributes like `var`, `equals`, `operator`, and `message`.
- **Failure Handling:** Supports immediate failure actions via `on_failure` attribute (`halt`, `warn`, etc.) and child `<on_failure>` blocks for custom scripts.
- **Automatic ID Assignment:** Generates unique IDs for assertions when the `id` attribute is omitted.
- **Failure Variables:** Allows specifying `fail_var` and `fail_val` to capture assertion failure states programmatically.

## 🛠️ API & Configuration Updates

- `Execute` signature updated:
  ```go
  func (e *Executor) Execute(ctx context.Context, nodes []PipelineNode) ([]ScriptResult, error)
  ```
- `StreamETL` signature updated:
  ```go
  func StreamETL(ctx context.Context, r *Registry, srcDBName, queryStr, dstDBName, targetTable string, opts ETLOptions) error
  ```

---

## 🧪 Verification

These enhancements are covered by a suite of tests inside [`executor_test.go`](./executor_test.go) and [`new_features_test.go`](./new_features_test.go), verifying:
1. Graceful termination under active context cancellation (`TestExecutorContextCancellation`).
2. Concurrent parallel variable isolation, non-colliding variable merging, and correct `WORKER_<id>_<key>` namespacing on key collisions (`TestParallelVariableIsolationAndNamespacing`).
3. Correct filesystem behaviors: writing dynamic payloads to paths, auto-creating directory trees, appending to files, and reading disk contents into state variables (`TestFileSaveAndRead`).
4. Template rendering: evaluating inline body text and loading external template documents with full path/variable interpolation (`TestTemplate`).
5. Native spreadsheet interoperability: executing database queries directly to Excel sheets, saving formatted spreadsheets, writing multiple sequential tabs to a single workbook, and parsing worksheets back into raw JSON object arrays (`TestExcelReadAndWrite`, `TestExcelMultiTabs`).
6. XPath node extraction: reading file-based or variable-based XML, matching node trees with XPath query patterns, and formatting outputs as plaintext, raw XML, or JSON lists (`TestXMLXPath`).
7. JSONPath value extraction: reading JSON payloads from variables or files, executing JSONPath matches, and formatting values in scalar, single JSON, or array representations (`TestJSONPath`).
8. YAMLPath extraction and formatting: querying structures from files or memory, formatting values into scalar joins, JSON lists, or marshalling nested maps back to clean YAML representations (`TestYAMLPath`).
9. Native SQL and bulk database synchronization: executing standard DDL/DML, and streaming high-performance bulk operations (`TestSQLAndSQLBulk`).

