# Flow Components

This document describes the main runtime components in the project and the recent configuration and performance updates that affect how pipelines execute.

## 1. XML configuration and schema model

The configuration layer turns XML into typed structs used by the runtime.

- `VariableConfig` stores environment variables loaded from `<variable>` elements.
- `DatabaseConfig` defines a named database target and its connection details.
- `ScriptItem` carries script metadata such as language, DB target, batching options, and output handling.
- `PipelineNode` is the execution tree for the pipeline AST.

Recent updates:
- Database pool settings are now defined on the `<database>` element itself instead of on SQL steps.
- Supported connection attributes:
  - `name`
  - `driver`
  - `connection_string`
  - `max_open_conns`
  - `max_idle_conns`
  - `conn_max_lifetime_seconds`
  - `workload`

Example:

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

This keeps connection-level tuning scoped to the actual resource owner and preserves compatibility with existing pipelines that omit the optional attributes.

## 2. Registry and connection management

The `Registry` is the thread-safe runtime state holder for variables and database pools.

Responsibilities:
- maintain the variable registry
- open and register `*sql.DB` connection pools
- expose `GetDB`, `GetVar`, and conversion helpers
- close all DB pools when execution ends

The database initialization path now applies defaults when pool settings are absent:
- `max_open_conns = 25`
- `max_idle_conns = 10`
- `conn_max_lifetime = 5m`

If a connection is configured with explicit values, those values override the defaults without changing any SQL node contract.

## 3. Executor engine

The `Executor` orchestrates the pipeline AST and runs the script and control-flow nodes.

Key responsibilities:
- evaluate sequential, conditional, parallel, foreach, and while flows
- execute SQL and Go script nodes
- handle transaction state per database
- capture script output in pipeline variables when `output_var` is provided

Go execution note:
- The Go runtime path is built around Yaegi and uses a small, safe setup strategy to avoid repeatedly rebuilding interpreter infrastructure for the same execution profile.
- Interpreter setup is centralized to reduce repeated initialization overhead while keeping interpreter state from being reused unsafely across incompatible script contexts.

## 4. ETL streaming layer

`StreamETL` moves rows from a source DB into a destination DB or bulk-insert target.

This path is a primary performance hotspot for large row counts because it processes data in batches and converts source values as they are scanned.

Recent improvements in the implementation:
- reduce unnecessary channel indirection in the row-reading path
- keep row collection in the same hot loop used for batch flushing
- preserve batch semantics while reducing allocation churn and GC pressure
- avoid repeated temporary row copies when the batch is already being built

This change is intentionally narrow: it improves throughput in large ETL jobs without altering pipeline XML behavior.

## 5. Runtime interaction model

A typical pipeline follows this lifecycle:

1. Parse XML into `PipelineConfig`
2. Initialize variables and databases via the `Registry`
3. Build the `Executor`
4. Execute AST nodes in order or according to flow control rules
5. Store results and output variables in the registry

This separation keeps configuration, runtime state, and execution logic independent and makes each component easier to test and optimize.

## 6. Performance notes

The most impactful tuning points in the project are:

- connection pool sizing on `<database>`
- ETL batch construction and row conversion in `StreamETL`
- script interpreter setup for Go execution

These settings are intentionally localized so they can be tuned by workload without altering the XML contract for SQL steps.
