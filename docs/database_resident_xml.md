# Database-backed Pipeline, Config, and Options Storage

This project can load pipeline XML, config XML, and CLI option XML content directly from a database instead of a local file path.

The runtime supports SQL-backed resources using the format:

```text
sql://<driver>@<dsn>#SELECT ...
```

or:

```text
db://<driver>@<dsn>#SELECT ...
```

The loader accepts a SQL query that returns the XML document as a single value. This makes it possible to store each artifact in a database table and execute the pipeline by reference.

---

## 1. Supported resource types

The application resolves XML inputs from the database in the same way it resolves local files:

- `-file` for the main pipeline XML
- `-config` for the override config XML
- `-options` for default CLI options XML

Examples:

```shell
flow.exe -file "sql://sqlserver@sqlserver://localhost:1433?database=example_db&integrated+security=true&trustServerCertificate=true#SELECT PipelineXML FROM dbo.flow_pipeline_content WHERE Name = 'MY_PIPELINE'"
flow.exe -config "sql://sqlserver@sqlserver://localhost:1433?database=example_db&integrated+security=true&trustServerCertificate=true#SELECT ConfigXML FROM dbo.flow_config_content WHERE Name = 'CONFIG_DEV'"
flow.exe -options "sql://sqlserver@sqlserver://localhost:1433?database=example_db&integrated+security=true&trustServerCertificate=true#SELECT OptionsXML FROM dbo.flow_options_content WHERE Name = 'OPTIONS_DEV'"
```

The loader also accepts `mssql`, `postgresql`, and `sqlite3` aliases and normalizes them internally to the proper Go SQL driver names.

---

## 2. Database table pattern

The importer uses this table layout:

```sql
CREATE TABLE dbo.flow_pipeline_content (
    Name NVARCHAR(255) PRIMARY KEY,
    Description NVARCHAR(4000) NULL,
    PipelineXML NVARCHAR(MAX) NOT NULL
);

CREATE TABLE dbo.flow_config_content (
    Name NVARCHAR(255) PRIMARY KEY,
    Description NVARCHAR(4000) NULL,
    ConfigXML NVARCHAR(MAX) NOT NULL
);

CREATE TABLE dbo.flow_options_content (
    Name NVARCHAR(255) PRIMARY KEY,
    Description NVARCHAR(4000) NULL,
    OptionsXML NVARCHAR(MAX) NOT NULL
);
```

A single XML document is stored as raw text in the corresponding column, and the database query returns that value directly to the loader.

---

## 3. Save a pipeline to the database

Use the importer helper under `db_importer/mssql_importer.go`:

```shell
cd db_importer
go run mssql_importer.go \
  -dsn "sqlserver://sa:Password123!@localhost:1433?database=master&trustServerCertificate=true" \
  -table pipeline \
  -name "github_ai_credit_usage" \
  -file "../examples/github_ai_credit_usage.xml" \
  -desc "GitHub Copilot Billing Usage Pipeline"
```

This reads the XML file, opens the SQL Server connection, and performs a MERGE/UPSERT into `dbo.flow_pipeline_content`.

---

## 4. Save config overrides to the database

```shell
cd db_importer
go run mssql_importer.go \
  -dsn "sqlserver://sa:Password123!@localhost:1433?database=master&trustServerCertificate=true" \
  -table config \
  -name "CONFIG_DEV_GITHUB" \
  -file "../examples/CONFIG.xml" \
  -desc "GitHub dev config override"
```

This stores the config XML in `dbo.flow_config_content` under the key `CONFIG_DEV_GITHUB`.

---

## 5. Save CLI options to the database

```shell
cd db_importer
go run mssql_importer.go \
  -dsn "sqlserver://sa:Password123!@localhost:1433?database=master&trustServerCertificate=true" \
  -table options \
  -name "OPTIONS_GITHUB_AI_CREDIT_USAGE" \
  -file "../db_importer/OPTIONS_GITHUB_AI_CREDIT_USAGE.xml" \
  -desc "Options for GitHub billing pipeline"
```

The options file can contain runtime arguments and also reference database-backed XML content via `file` and `config` entries.

Example option file:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<options>
  <file>sql://sqlserver@sqlserver://localhost:1433?database=example_db&amp;integrated+security=true&amp;trustServerCertificate=true#SELECT PipelineXML from dbo.flow_pipeline_content where Name = 'github_ai_credit_usage'</file>
  <config>sql://sqlserver@sqlserver://localhost:1433?database=example_db&amp;integrated+security=true&amp;trustServerCertificate=true#SELECT ConfigXML from dbo.flow_config_content where Name = 'CONFIG_DEV_GITHUB'</config>
  <vars>Environment=dev,RunID=nightly</vars>
</options>
```

This allows a single database-stored options document to bootstrap the pipeline, config, and runtime variables without a local filesystem dependency.

---

## 6. Run the pipeline from the database

### Run the pipeline XML from a database row

```shell
flow.exe -file "sql://sqlserver@sqlserver://sa:Password123!@localhost:1433?database=master&trustServerCertificate=true#SELECT PipelineXML from dbo.flow_pipeline_content where Name = 'github_ai_credit_usage'"
```

### Run with a database-backed config override

```shell
flow.exe \
  -file "sql://sqlserver@sqlserver://sa:Password123!@localhost:1433?database=master&trustServerCertificate=true#SELECT PipelineXML from dbo.flow_pipeline_content where Name = 'github_ai_credit_usage'" \
  -config "sql://sqlserver@sqlserver://sa:Password123!@localhost:1433?database=master&trustServerCertificate=true#SELECT ConfigXML from dbo.flow_config_content where Name = 'CONFIG_DEV_GITHUB'"
```

### Run with a database-backed options document

```shell
flow.exe \
  -options "sql://sqlserver@sqlserver://sa:Password123!@localhost:1433?database=master&trustServerCertificate=true#SELECT OptionsXML from dbo.flow_options_content where Name = 'OPTIONS_GITHUB_AI_CREDIT_USAGE'"
```

The `-options` path itself can point to a database row; the XML inside that options document can in turn point at database-stored pipeline and config content.

---

## 7. Typical production pattern

A common pattern is:

1. Save the pipeline XML to `dbo.flow_pipeline_content`
2. Save the environment-specific config XML to `dbo.flow_config_content`
3. Save the runtime defaults to `dbo.flow_options_content`
4. Execute with `-options` pointing to the database document that selects the right pipeline/config values for the environment

This keeps XML artifacts in versioned database tables while still allowing the command-line runtime to behave like a standard file-based execution model.

---

## 8. Notes

- The database query must return exactly one XML value.
- If the query returns no rows or an empty value, the loader returns an error.
- Use `-validate` and other standard Flow flags the same way you would with local XML files.
- Database-backed inputs are resolved by the same loader path used for HTTP and filesystem inputs.

For a quick example using the helper importer and the sample GitHub billing XML files, see `db_importer/README_DB_IMPORTER.md`.
