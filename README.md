# Flow 🌊

`flow` is an SSIS-like and embeddable data pipeline orchestration and stream ETL library for Go. It allows developers to programmatically load, validate, and execute complex pipeline AST nodes (such as loops, parallel batches, and dynamic SQL/Go scripts) from XML configuration files.

**go-flow** is a fully functional implementation that takes and executes XML files, with config.xml overrides, and can be found at [github.com/etl-madness/go-flow](https://github.com/etl-madness/go-flow)

**FLOW** Source code can be found at [github.com/etl-madness/flow](https://github.com/etl-madness/flow)

## Key Features

- **No Global State**: Fully isolated execution contexts (`Registry`) allowing you to run multiple pipelines concurrently in the same process without interference.
- **Dynamic Configuration Decoder**: Built-in support for XML parsers and optional XSD schema schema validations.
- **Direct Streaming ETL**: Copy bulk datasets line-by-line across heterogeneous engines (PostgreSQL, SQLite, MySQL, Oracle, SQL Server) with automatic parameter placeholder syntax correction.
- **Flexible Flow Controls**: Execution structures for Sequential steps, Parallel queues, If/Else branching, ForEach loops, and While loops.
- **Embedded Script Interpreter**: Dynamic runtime Go evaluations via Yaegi with closure-bound environment state injection.

---
## 🐚 OS Shell & Command Execution

`flow` supports, in addition to database connections, the execution of native host shell commands and binaries directly on the operating system without passing through the Go interpreter[cite: 4]. Supported `language` options on `<script>` tags include:

* **`shell`**: Cross-platform default shell (`cmd /C` on Windows, `sh -c` on Linux/macOS)[cite: 4].
* **`cmd`**: Windows Command Prompt (`cmd /C`)[cite: 4].
* **`powershell,pwsh`**: Windows PowerShell (`powershell -NoProfile -NonInteractive -Command`)[cite: 4].
* **`bash,zsh,ksh,csh,tcsh,dash,fish,sh`**: Various Unix shells (`bash -c`, `zsh -c`, etc.)[cite: 4].
* **`dotnet-script`** (or **`csx`**): Executed using C# script files with `dotnet-script` or `dotnet script`. Allows full inline C# execution including external NuGet package references (`#r "nuget: ..."`). This requires the `dotnet-script` tool to be installed on the host machine and the ability to create temporary files.

### Key Capabilities
* **Variable Interpolation**: Use `{{var_name}}` syntax inside script bodies to dynamically inject pipeline variables[cite: 3, 4].
* **Output Capture**: Define the `output_var` attribute to save standard output/error into a pipeline variable for downstream consumption by SQL or Go steps[cite: 2, 4].

### XML Examples

```xml
<pipeline>
    <variables>
        <variable name="export_dir" value="C:\exports" />
    </variables>
    <flow>
        <!-- Run external executable and store output in variable -->
        <script id="ExtractData" language="shell" output_var="GCLOUD_BILLING_JSON">
            ..\bqBilling.exe
        </script>

        <!-- PowerShell execution with variable interpolation -->
        <script id="PrepFolder" language="powershell">
            New-Item -ItemType Directory -Force -Path "{{export_dir}}"
        </script>

        <!-- Bash execution -->
        <script id="ArchiveLogs" language="bash" output_var="ARCHIVE_LOG">
            tar -czvf {{export_dir}}/archive.tar.gz {{export_dir}}/*.csv
        </script>
    </flow>
</pipeline>
```
## Package API Reference

### 1. Parsing & Validation
```go
// ParseXMLConfig parses a byte stream of XML into structured configuration blocks.
func ParseXMLConfig(xmlData []byte) ([]VariableConfig, []DatabaseConfig, []PipelineNode, error)

// ValidateAST performs semantic structure checks (uniqueness, reference integrity, loop bounds).
func ValidateAST(nodes []PipelineNode, registeredDBs []DatabaseConfig) error

// ValidateXSD invokes 'xmllint' to validate an XML configuration against schema standards.
func ValidateXSD(xmlPath string, xsdPath string) error

// GetSchemaXSD returns the compiled-in, embedded XSD schema file as a byte slice.
func GetSchemaXSD() []byte
```

### 2. State & Context Management
```go
// Registry holds thread-safe variable registries and database connection pools.
type Registry struct { ... }

func NewRegistry() *Registry
func (r *Registry) InitVariables(configs []VariableConfig) error
func (r *Registry) InitDatabases(configs []DatabaseConfig) error
func (r *Registry) CloseDatabases()

// Variables getters & setters
func (r *Registry) SetVar(name string, value interface{})
func (r *Registry) GetVar(name string) interface{}
func (r *Registry) GetVarString(name string) string
func (r *Registry) GetVarInt(name string) int
func (r *Registry) GetVarBool(name string) bool
```

### 3. Pipeline Executor
```go
// Executor orchestrates tree node executions.
type Executor struct { ... }

func NewExecutor(r *Registry) *Executor
func (e *Executor) Execute(ctx context.Context, nodes []PipelineNode) ([]ScriptResult, error)
func (e *Executor) ExecuteRun(ctx context.Context, nodes []PipelineNode) (RunResult, error)
func (e *Executor) SetVerbose(verbose bool)
func (e *Executor) SetEventSink(sink EventSink)
func (e *Executor) SetEventSinks(sinks ...EventSink)
func (e *Executor) SetGoPath(goPath string) // sets the GOPATH for the embedded Go interpreter (Yaegi) to resolve imports during script execution.

// ScriptResult represents the outcome of an executed script or loop step.
type ScriptResult struct {
	ScriptID      string `json:"script_id"`
	ReturnCode    any    `json:"return_code"`      // 0 on success, or an error string/code on failure
	ResultsString string `json:"results_string"`    // Output logs/data from query or script
	Duration      string `json:"duration,omitempty"` // Execution duration (e.g. "14.285ms")
}
```

---

## Quick Start Example

The following example demonstrates how to load, parse, validate, and execute an XML pipeline programmatically from custom Go code.

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/etl-madness/flow"
)

func main() {
	xsdSchema := flow.GetSchemaXSD() // Load embedded XSD schema for validation
	xmlConfig := []byte(`<?xml version="1.0" encoding="UTF-8"?>
	<pipeline>
		<variables>
			<variable name="TargetTable" value="processed_logs" />
			<variable name="Threshold" type="int" value="100" />
		</variables>
	</pipeline>`)
	xmlScript := []byte(`<?xml version="1.0" encoding="UTF-8"?>
	<pipeline>
		<variables>
			<variable name="TargetTable" value="processed_logs" />
			<variable name="Threshold" type="int" value="100" />
		</variables>
		<databases>
			<database name="sqlite_db" driver="sqlite" connection_string="./mydb.db" />
		</databases>
		<flow>
			<script id="SetupTable" language="sql" db="sqlite_db">
				CREATE TABLE IF NOT EXISTS processed_logs (id INTEGER PRIMARY KEY, status TEXT);
			</script>
			<script id="VerifyGo" language="go">
				package main
				import (
					"fmt"
					"host/vars"
				)
				func main() {
					tbl := vars.GetString("TargetTable")
					thresh := vars.GetInt("Threshold")
					fmt.Printf("Configured target table: %s with limit: %d\n", tbl, thresh)
				}
			</script>
		</flow>
	</pipeline>`)

	// 1. Parse XML to Pipeline AST
	varConfigs, dbConfigs, nodes, err := flow.ParseXMLConfig(xmlConfig)
	if err != nil {
		log.Fatalf("Parsing failed: %v", err)
	}
	if err := flow.ValidateXSD(xmlConfig, string(xsdSchema)); err != nil {
		log.Fatalf("XSD validation failed: %v", err)
	}
	if err := flow.ValidateXSD(xmlScript, string(xsdSchema)); err != nil {
		log.Fatalf("XSD validation failed: %v", err)
	}
	// 2. Perform semantic checks on the AST
	if err := flow.ValidateAST(nodes, dbConfigs); err != nil {
		log.Fatalf("Validation failed: %v", err)
	}

	// 3. Instantiate Registry and Register Connection Pools / Variables
	registry := flow.NewRegistry()
	if err := registry.InitVariables(varConfigs); err != nil {
		log.Fatalf("Variables initialization failed: %v", err)
	}
	if err := registry.InitDatabases(dbConfigs); err != nil {
		log.Fatalf("Databases initialization failed: %v", err)
	}
	defer registry.CloseDatabases()

	// 4. Instantiate Executor and run the pipeline
	executor := flow.NewExecutor(registry)
	results, err := executor.Execute(context.Background(), nodes)
	if err != nil {
		log.Fatalf("Execution encountered errors: %v", err)
	}

	// 5. Inspect Results
	fmt.Println("--- Pipeline Execution Results ---")
	for _, res := range results {
		fmt.Printf("Script [%s]: Return Code: %v\n", res.ScriptID, res.ReturnCode)
		if res.ResultsString != "" {
			fmt.Printf("Output:\n%s\n", res.ResultsString)
		}
	}
}
```

---

## Advanced: Shared Context Isolation

Since the state of connections and active variables is entirely held inside the `*flow.Registry` object rather than package globals, you can safely initialize multiple independent registries and run them in concurrent threads or separate executors:

```go
registryA := flow.NewRegistry()
registryB := flow.NewRegistry()

// Run independent pipelines in parallel
go flow.NewExecutor(registryA).Execute(context.Background(), nodesA)
go flow.NewExecutor(registryB).Execute(context.Background(), nodesB)
```

## 🔧 Customizing Go Interpreter Options

When running dynamic Go scripts, `flow` uses the [Yaegi](https://github.com/traefik/yaegi) interpreter under the hood. You can customize the `interp.Options` struct (e.g., to enable unrestricted code execution) by registering an interpreter configuration hook before executing your pipeline nodes.

### Example: Unrestricted Execution

By default, Yaegi restricts some types of execution for safety. If your scripts require complete system access (like reading files, starting subprocesses, or bypassing other sandbox restrictions), you can enable `Unrestricted: true`:

```go
import (
	"context"
	"github.com/traefik/yaegi/interp"
	"github.com/etl-madness/flow"
)

func main() {
	executor := flow.NewExecutor(registry)

	// Set a hook to modify Yaegi's interpreter options
	executor.SetInterpHook(func(opts *interp.Options) {
		opts.Unrestricted = true
	})

	results, err := executor.Execute(context.Background(), nodes)
}
```

---

## 📢 Verbose Execution Logging

To monitor the start, finish, duration, and outcome of each task in real-time as the pipeline processes them, you can enable verbose logging on the `Executor`.

### Enabling Verbose Mode
By default, the executor runs silently. Use the `SetVerbose(true)` method before triggering your pipeline to output execution summaries directly to the console:

```go
executor := flow.NewExecutor(registry)

// Enable verbose logging to console
executor.SetVerbose(true)

results, err := executor.Execute(context.Background(), nodes)
```

### Console Output Format
When verbose mode is enabled, the executor logs task lifecycles in the following format:

```text
Starting execution of script "SetupTable"
Finished execution of script "SetupTable" (duration: 4.812ms)
Starting execution of script "VerifyGo"
Finished execution of script "VerifyGo" (duration: 1.251ms)
```

If a task encounters an error during execution, the failure details are printed along with the elapsed time:

```text
Starting execution of script "FailedQuery"
Finished execution of script "FailedQuery" with error: table 'non_existent_table' not found (duration: 3.125ms)
```

---

## ⚡ Parallel Execution Engine

The `<parallel>` block allows you to execute multiple child nodes concurrently. It includes a built-in semaphore-based throttle and thread-safe error-handling behaviors.

### How it Works
1. **Concurrency Throttle (`max_threads`)**: You can specify the `max_threads` attribute on a `<parallel>` block. If unspecified or set to `<= 0`, it defaults to `4`. The engine uses a buffered channel semaphore to guarantee that no more than `max_threads` goroutines run concurrently.
2. **Fail-Fast Error & Context Cancellation**: If any concurrent child node encounters an error or if the pipeline `context.Context` is cancelled/timed out, execution halts immediately (`fail-fast`), preventing wasted compute resources.
3. **Variable Isolation & Conflict Resolution**:
   - Each parallel worker is isolated with a cloned registry snapshot containing the state of variables at the moment of start.
   - Each worker's registry receives a thread-specific variable named `_THREAD_ID`.
   - To prevent stale overwrites, only variables actually modified (`dirtyVars`) by a worker are considered during the merge phase.
   - If multiple parallel workers mutate the same variable name, they are automatically namespaced as `WORKER_<id>_<name>` to prevent race conditions or collisions. Non-colliding keys merge directly back into the parent registry.
4. **Thread-Safe Results Accumulation**: All script and task outcomes are safely accumulated into the final results list via internal mutex locking (`resultsMu`).

### XML Configuration Example
The following XML segment configures a parallel block of 3 tasks running with a maximum concurrency limit of 2:

```xml
<pipeline>
    <databases>
        <database name="main_db" driver="sqlite" connection_string="./mydb.db" />
    </databases>
    <flow>
        <parallel max_threads="2">
            <script id="ProcessBatchA" language="sql" db="main_db">
                UPDATE transactions SET processed = 1 WHERE batch_id = 'A';
            </script>
            <script id="ProcessBatchB" language="sql" db="main_db">
                UPDATE transactions SET processed = 1 WHERE batch_id = 'B';
            </script>
            <script id="ProcessBatchC" language="sql" db="main_db">
                UPDATE transactions SET processed = 1 WHERE batch_id = 'C';
            </script>
        </parallel>
    </flow>
</pipeline>
```

### Concurrently Nesting Loops (e.g. `<foreach>`)

Yes! Since the child elements in a `<parallel>` block are fully parsed as generic pipeline nodes, `<parallel>` natively supports **concurrently running multiple loops (such as `<foreach>`)** or nested structures.

When you nest `<foreach>` blocks inside `<parallel>`, **each loop executes concurrently in parallel** on its own thread, while the individual iterations within each loop run sequentially.

#### Example: Concurrently Running Independent Data Processors
Below is an XML pipeline configuring two `<foreach>` loops running simultaneously to import customers and products in parallel:

```xml
<pipeline>
    <databases>
        <database name="src_db" driver="sqlite" connection_string="./source.db" />
        <database name="target_db" driver="postgres" connection_string="postgresql://user:pass@localhost/db" />
    </databases>
	<script id="StreamData_MSSQL" language="sql" db="src_db" target_db="target_db" target_table="customers" batch_size="10000" tablock="true" check_constraints="false" fire_triggers="false" keep_nulls="true">
    SELECT id, name, email FROM source_customers;
    </script>
    <flow>
        <parallel max_threads="2">
            <!-- Loop 1: Import customer records -->
            <foreach id="SyncCustomers" db="src_db" var="customer_id">
                SELECT id FROM customers WHERE sync_pending = 1;
                <script id="MigrateCustomer" language="sql" db="src_db" target_db="target_db" target_table="customers" batch_size="100">
                    SELECT name, email, country FROM customers WHERE id = {{customer_id}};
                </script>
            </foreach>

            <!-- Loop 2: Import product records concurrently -->
            <foreach id="SyncProducts" db="src_db" var="product_id">
                SELECT id FROM products WHERE stock &gt; 0;
                <script id="MigrateProduct" language="sql" db="src_db" target_db="target_db" target_table="products" batch_size="50">
                    SELECT title, price, SKU FROM products WHERE id = {{product_id}};
                </script>
            </foreach>
        </parallel>
    </flow>
</pipeline>
```

> [!TIP]
> Use parallel blocks for network-bound tasks, independent bulk database loads, or concurrent loops (like syncing separate data tables) where workflows do not rely on each other's outputs.

---

## 🚀 High-Performance MSSQL Bulk Copy Support

`flow` supports high-performance native bulk stream copy operations when transferring datasets into Microsoft SQL Server (`sqlserver` or `mssql` drivers). When streaming data to a SQL Server target, `flow` bypasses standard parameterized multi-row `INSERT` operations (which are subject to the 2,100 parameter limit) and instead utilizes native TDS Bulk Copy Streams (`mssql.CopyIn`).

### XML Configuration Attributes
On any streaming `<script>` node (where both `target_db` and `target_table` are defined), you can configure the following bulk copy options:

- **`tablock`** (boolean, optional, default `true`): Acquires a table-level lock during the bulk insert, drastically reducing transaction log overhead and boosting throughput.
- **`check_constraints`** (boolean, optional, default `false`): Evaluates check and foreign key constraints on the target table during bulk insert.
- **`fire_triggers`** (boolean, optional, default `false`): Executes any insert triggers defined on the target table during bulk execution.
- **`keep_nulls`** (boolean, optional, default `false`): Retains explicit `NULL` values from the source dataset instead of utilizing target table default values.

### XML Example
```xml
<pipeline>
    <databases>
        <database name="src_db" driver="sqlite" connection_string="./source.db" />
        <database name="dst_mssql" driver="sqlserver" connection_string="sqlserver://user:pass@localhost:1433?database=target_db" />
    </databases>
    <flow>
        <script id="BulkSync" 
                language="sql" 
                db="src_db" 
                target_db="dst_mssql" 
                target_table="customers" 
                batch_size="25000" 
                tablock="true" 
                check_constraints="true" 
                fire_triggers="false" 
                keep_nulls="true">
            SELECT id, name, email, signup_date FROM raw_users;
        </script>
    </flow>
</pipeline>
```

### Fallback Driver Support
For non-MSSQL destination databases (e.g. PostgreSQL, MySQL, SQLite), `flow` automatically falls back to standard multi-row parameter-bound batch inserts. The batch sizes for fallback drivers are automatically throttled to ensure they never exceed the maximum 2,100 parameter limit (calculated dynamically as `2100 / column_count`).

---

## 📂 Project Structure

```
.
├── .gitignore          # Git exclusion rules
├── LICENSE             # MIT License
├── README.md           # This document
├── go.mod              # Go module definition
├── go.sum              # Go dependencies checksums
├── config.go           # XML parsing and schema validation functions
├── etl.go              # Database stream copying implementation
├── etl_test.go         # Core placeholder unit tests
├── executor.go         # Core AST walker and script runner
├── registry.go         # Environment variable and connection pool registry
├── transactions.md     # Documentation on database transactions
├── validator.go        # Semantic AST validator rules
├── variables.md        # Documentation on variable management & usage
└── xsd/
    └── schema.xsd      # XML validation schema
```
