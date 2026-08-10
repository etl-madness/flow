# Flow 🌊

[![Go Reference](https://pkg.go.dev/badge/github.com/etl-madness/flow/pkg/flow.svg)](https://pkg.go.dev/github.com/etl-madness/flow/pkg/flow)
[![Go Report Card](https://goreportcard.com/badge/github.com/etl-madness/flow)](https://goreportcard.com/report/github.com/etl-madness/flow)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

`flow` is a high-performance, modular, and embeddable data pipeline orchestration and stream ETL library for Go. It enables developers to programmatically load, validate, and execute complex pipeline AST nodes (such as loops, parallel batches, and dynamic SQL/Go scripts) from XML configuration files.

> [!NOTE]
> The engine is designed around **No Global State**, making it highly suitable for concurrent and multi-tenant systems.

---

## 🚀 Key Features

* **No Global State**: Fully isolated execution contexts (`Registry`) allowing you to run multiple pipelines concurrently in the same process without interference.
* **Direct Streaming ETL**: Copy bulk datasets line-by-line across heterogeneous engines (PostgreSQL, SQLite, MySQL, Oracle, SQL Server) with automatic parameter placeholder syntax correction.
* **Flexible Flow Controls**: Execution structures for sequential steps, parallel queues, If/Else branching, ForEach loops, and While loops.
* **Embedded Script Interpreter**: Dynamic runtime Go evaluations via Yaegi with closure-bound environment state injection.
* **XSD-Validated**: Built-in support for XML parsers and optional XSD schema validations.

---

## 📦 Installation

To start using `flow` in your Go project, add it to your `go.mod`:

```bash
go get github.com/etl-madness/flow
```

---

## 🛠️ Quick Start

Below is a complete, minimal example showing how to parse, validate, and execute an XML pipeline using `flow`:

```go
package main

import (
	"fmt"
	"log"

	"github.com/etl-madness/flow/pkg/flow"
)

func main() {
	xmlConfig := []byte(`<?xml version="1.0" encoding="UTF-8"?>
	<pipeline>
		<variables>
			<variable name="TargetTable" value="processed_logs" />
			<variable name="Threshold" type="int" value="100" />
		</variables>
		<databases>
			<database name="sqlite_db" driver="sqlite" connection_string="./mydb.db" />
		</databases>
		<scripts>
			<script id="SetupTable" language="sql" db="sqlite_db">
				CREATE TABLE IF NOT EXISTS processed_logs (id INTEGER PRIMARY KEY, status TEXT);
			</script>
			<script id="VerifyGo" language="go">
				package main
				import (
					"fmt"
					"host/vars/vars"
				)
				func main() {
					tbl := vars.GetString("TargetTable")
					thresh := vars.GetInt("Threshold")
					fmt.Printf("Configured target table: %s with limit: %d\n", tbl, thresh)
				}
			</script>
		</scripts>
	</pipeline>`)

	// 1. Parse XML to Pipeline AST
	varConfigs, dbConfigs, nodes, err := flow.ParseXMLConfig(xmlConfig)
	if err != nil {
		log.Fatalf("Parsing failed: %v", err)
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
	results, err := executor.Execute(nodes)
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

## 📂 Project Structure

```
.
├── .gitignore          # Git exclusion rules
├── go.mod              # Go module definition
├── go.sum              # Go dependencies checksums
├── pkg/
│   └── flow/           # Core library containing executor, registry, parser, and streaming ETL
└── xsd/
    └── schema.xsd      # XML validation schema
```

For detailed documentation of internal package implementation, check out [pkg/flow README](pkg/flow/README.md).
