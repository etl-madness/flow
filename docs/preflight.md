# Preflight Nodes: Setup and Validation

Preflight nodes are designed to run environment checks, initializations, and structural assertions **before** the main data-streaming pipelines (`<flow>`) are executed. This ensures that resources are available, configurations are correct, and the pipeline fails fast before performing heavy ETL operations.

> [!IMPORTANT]
> **No Flow Execution during Preflight**: Running a `<preflight>` block does **not** execute the main `<flow>` pipeline nodes. It is intended as a non-destructive runtime validation/test run to guarantee that running in a specific environment with those precise variables and configuration will execute exactly as intended. Once validation is complete, the pipeline can later be run in production without the preflight overhead.

---

## How It Works

1. **Separation of Concerns**: Preflight nodes and flow nodes are separated during XML parsing into `PreflightNodes` and `FlowNodes` respectively.
2. **Preflight-Only Execution**: Preflight checks are executed by passing `cfg.PreflightNodes` to the pipeline `Executor`. The `<flow>` block is ignored during this execution step.
3. **Fail-Fast Design**: If any preflight node fails or returns an error, the execution can be halted immediately, preventing the main `<flow>` from running with incorrect or missing parameters.

---

## Is the Flow Executed During Preflight?

> [!CAUTION]
> **NO. The main `<flow>` block is never executed when running a preflight check.**

Preflight is explicitly intended as a **runtime validation and test run**. Its core purposes are:
* To ensure that the application can run in the target environment.
* To verify that all required environment variables are set and reachable.
* To confirm that the specific configuration file compiles and executes safely.

Once the preflight validation passes successfully, you can be highly confident that running the same configuration file later with the preflight block omitted (or skipped) will execute exactly as intended.

---

## Is Preflight Executed When Running the Flow?

> [!IMPORTANT]
> **NO. The `<preflight>` block is never executed when running the flow normally.**

The executor only executes the slice of nodes explicitly passed to `Execute(ctx, nodes)`. If you wish to run the pipeline, you must choose whether to run preflight validation, flow execution, or both:

```go
// OPTION A: Run flow only (Skipping preflight checks entirely)
results, err := executor.Execute(ctx, cfg.FlowNodes)

// OPTION B: Safe Run (Preflight checks first, then Flow)
_, err := executor.Execute(ctx, cfg.PreflightNodes)
if err == nil {
    results, err := executor.Execute(ctx, cfg.FlowNodes)
}
```

---

## XML Schema Syntax

A preflight block is defined using the `<preflight>` element. Within this block, you can place standard pipeline AST nodes such as `<script>`, `<file_save>`, `<file_read>`, and `<assert>`:

```xml
<pipeline>
    <preflight>
        <!-- Preflight tasks here -->
    </preflight>

    <flow>
        <!-- Main ETL flow here -->
    </flow>
</pipeline>
```

---

## Detailed Examples

### Example 1: Environment Verification
This example checks if the correct database connectivity is established and that critical environment variables are set before proceeding.

```xml
<?xml version="1.0" encoding="UTF-8"?>
<pipeline>
    <variables>
        <variable name="ENV" type="string" value="staging" />
    </variables>

    <preflight>
        <!-- 1. Assert that we are not in production if running tests -->
        <assert id="check_env" var="ENV" equals="production" operator="!=" message="Warning: Running in production!" on_failure="warn" />

        <!-- 2. Verify we can fetch the system status -->
        <script id="ping_service" language="go">
            package main
            import (
                "net/http"
                "os"
            )
            func main() {
                resp, err := http.Head("https://api.yourdomain.com/health")
                if err != nil || resp.StatusCode != http.StatusOK {
                    println("API service is offline")
                    os.Exit(1)
                }
            }
        </script>
    </preflight>

    <flow>
        <!-- Flow starts only if preflight check passes -->
        <script id="load_data" language="go">
            package main
            func main() {
                println("Streaming data...")
            }
        </script>
    </flow>
</pipeline>
```

### Example 2: Directory Preparation
This example ensures that the destination folder structure exists and logs a marker file before downloading bulk records.

```xml
<?xml version="1.0" encoding="UTF-8"?>
<pipeline>
    <variables>
        <variable name="EXPORT_DIR" type="string" value="./exports" />
    </variables>

    <preflight>
        <!-- 1. Run shell script to create directories -->
        <script id="make_dir" language="bash">
            mkdir -p {{EXPORT_DIR}}
        </script>

        <!-- 2. Write a marker indicating preflight run started -->
        <file_save id="preflight_marker" file="{{EXPORT_DIR}}/preflight.log" append="false">
            Preflight started. Ready to receive files.
        </file_save>
    </preflight>

    <flow>
        <file_save id="data_payload" file="{{EXPORT_DIR}}/records.json">
            {"id": 1, "name": "ETL Payload"}
        </file_save>
    </flow>
</pipeline>
```

### Example 3: Schema & Version Assertion
This example reads a remote configuration file or local manifest to assert that the pipeline version is supported.

```xml
<?xml version="1.0" encoding="UTF-8"?>
<pipeline>
    <preflight>
        <!-- 1. Read local version manifest -->
        <file_read id="read_manifest" file="manifest.json" output_var="MANIFEST_DATA" />

        <!-- 2. Use JSONPath to extract the required version -->
        <json_path id="get_version" var="MANIFEST_DATA" jsonpath="$.version" output_var="CURRENT_VERSION" />

        <!-- 3. Assert that the version meets minimum compatibility -->
        <assert id="check_version" var="CURRENT_VERSION" equals="2.0.0" operator="==" message="Unsupported pipeline version" on_failure="halt" />
    </preflight>

    <flow>
        <script id="execute_migration" language="go">
            package main
            func main() {
                println("Running migration flow...")
            }
        </script>
    </flow>
</pipeline>
```

---

## Split Example: Pipeline Structure vs. Environment Configuration

It is common to separate your static pipeline execution structure (`pipeline.xml`) from dynamic, environment-specific databases and variables (`config.xml`). Below is an example demonstrating this setup.

### 1. `config.xml` (Environment Setup & Env-Specific Preflights)
This file defines environment-specific databases, parameters, and **preflight validation checks** tailored strictly to this staging environment (e.g., verifying that the staging API is online).

```xml
<?xml version="1.0" encoding="UTF-8"?>
<pipeline>
    <variables>
        <variable name="ENV" type="string" value="staging" />
        <variable name="API_ENDPOINT" type="string" value="https://staging.api.service.com" />
    </variables>

    <databases>
        <database name="primary_db" driver="postgres" connection_string="host=localhost port=5432 user=db_user dbname=staging_db sslmode=disable" />
    </databases>

    <!-- Staging environment preflight verification -->
    <preflight>
        <script id="verify_staging_api" language="go">
            package main
            import (
                "net/http"
                "os"
            )
            func main() {
                resp, err := http.Head("https://staging.api.service.com/health")
                if err != nil || resp.StatusCode != http.StatusOK {
                    println("Staging API is unreachable!")
                    os.Exit(1)
                }
            }
        </script>
    </preflight>
</pipeline>
```

### 2. `pipeline.xml` (Pipeline Logic & General Preflight)
This file defines the general validation logic (within `<preflight>`) and the heavy data movement tasks (within `<flow>`), completely independent of hardcoded credentials.

```xml
<?xml version="1.0" encoding="UTF-8"?>
<pipeline>
    <preflight>
        <!-- Ensure we are running against correct environment configuration -->
        <assert id="assert_staging" var="ENV" equals="staging" operator="==" message="Incorrect environment detected!" on_failure="halt" />

        <!-- Verify database connection from config.xml before flowing -->
        <script id="verify_db" language="sql" db="primary_db">
            SELECT 1;
        </script>
    </preflight>

    <flow>
        <!-- Execute ETL query against primary_db defined in config.xml -->
        <script id="stream_records" language="sql" db="primary_db">
            SELECT * FROM users_staging;
        </script>
    </flow>
</pipeline>
```

### 3. Orchestrating Split Files in Go

To parse and execute both files together, load them sequentially into your `ParseXMLConfig` parser, merge the `PreflightNodes` from both `PipelineConfig` outputs, and run them sequentially:

```go
package main

import (
	"context"
	"io/ioutil"
	"log"
	"github.com/etl-madness/flow"
)

func main() {
	// Read and parse environmental config
	configData, _ := ioutil.ReadFile("config.xml")
	envCfg, _ := flow.ParseXMLConfig(configData)

	// Read and parse structural pipeline
	pipelineData, _ := ioutil.ReadFile("pipeline.xml")
	pipeCfg, _ := flow.ParseXMLConfig(pipelineData)

	// Initialize Registry with both databases and variables
	registry := flow.NewRegistry()
	registry.InitVariables(envCfg.Variables)
	registry.InitDatabases(envCfg.Databases)

	executor := flow.NewExecutor(registry)

	// Merge env-specific and pipeline-specific preflights
	allPreflights := append(envCfg.PreflightNodes, pipeCfg.PreflightNodes...)

	// Safe Test Run / Preflight
	log.Println("Running combined preflight validation...")
	_, err := executor.Execute(context.Background(), allPreflights)
	if err != nil {
		log.Fatalf("Preflight failed! Flow aborted: %v", err)
	}

	// Execution of flow (only reaches here if all preflight checks pass)
	log.Println("Preflight passed successfully. Executing pipeline flow...")
	_, err = executor.Execute(context.Background(), pipeCfg.FlowNodes)
	if err != nil {
		log.Fatalf("Flow execution failed: %v", err)
	}
}
```

