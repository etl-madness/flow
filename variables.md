# Flow Variable Management & Usage Guide

Flow supports dynamic environment variables stored in a thread-safe registry (`*flow.Registry`). These variables enable you to parameterize your SQL queries, loop drivers, streaming configurations, and dynamic Go scripts.

---

## 1. Variable Hierarchy & Scope

Variables are kept in a single unified registry, but their behavior and scope change depending on the pipeline execution tree block:

| Scope | Context | Behavior |
| :--- | :--- | :--- |
| **Global / Registry** | Sequential execution | Read and write actions are immediate and shared. Scripts executing in a sequence (inside a standard group or root block) can read variables written by preceding scripts. |
| **Thread-Isolated** | `<parallel>` blocks | When running concurrent branches, Flow **snapshots (clones)** the registry's variables for each worker thread. Changes made to variables inside a parallel worker remain isolated to that worker and do not leak back or interfere with other concurrent workers. |

---

## 2. Using Variables

### SQL Scripts (Variable Interpolation)
For SQL scripts, variables are dynamically interpolated before execution using double curly brace placeholders: `{{VarName}}`.

```xml
<script id="QueryWithLimit" language="sql" db="app_db">
    SELECT * FROM orders WHERE status = 'PENDING' LIMIT {{MaxLimit}};
</script>
```

### Go Scripts (Yaegi `vars` Exports)
Inside dynamic Go scripts, variables are retrieved using the built-in host package `"host/vars/vars"`. The package exposes typed getters:

*   `vars.Get(name string) interface{}`
*   `vars.GetString(name string) string`
*   `vars.GetInt(name string) int`
*   `vars.GetBool(name string) bool`
*   `vars.GetFloat(name string) float64`

```go
package main

import (
    "fmt"
    "host/vars/vars"
)

func main() {
    threshold := vars.GetInt("Threshold")
    table := vars.GetString("TargetTable")
    fmt.Printf("Processing %s with threshold: %d\n", table, threshold)
}
```

---

## 3. Setting Variables in Scripts

### SQL Scripts (`output_var`)
To capture a value returned from a SQL query, use the `output_var` attribute.
*   If the SQL query returns a single row with a single column, `output_var` stores that value.
*   Otherwise, it captures the entire tab-separated results block.

```xml
<script id="GetMaxID" language="sql" db="app_db" output_var="LastProcessedID">
    SELECT COALESCE(MAX(id), 0) FROM logs;
</script>
```

### Go Scripts (`output_var` / Stdout Capture)
Because the Yaegi host bindings do not expose a variable mutation method, you write values to variables by printing them to **stdout**. The `output_var` attribute will capture the printed console output and store it as a string variable.

```xml
<script id="CalculateStats" language="go" output_var="ResultScore">
    package main
    import "fmt"
    func main() {
        score := 98.4
        // Captured directly by "ResultScore"
        fmt.Printf("%.1f", score)
    }
</script>
```

---

## 4. Loops (`<foreach>`)

When iterating over records using a `<foreach>` block, the loop driver query binds the following variables automatically for each iteration:

*   `LOOP_INDEX`: The current 0-indexed loop iteration index (integer).
*   **Columns**: The values of the current row are bound to variables matching the column names. To prevent casing issues, the engine binds them in three casings:
    1.  **Exact Case** (e.g. `UserId`)
    2.  **Lowercase** (e.g. `userid`)
    3.  **Uppercase** (e.g. `USERID`)

```xml
<foreach id="IterateUsers" db="app_db" var="UserId">
    SELECT id, username, email FROM users WHERE active = 1;
    
    <!-- Each iteration binds "id", "username", "email", and "LOOP_INDEX" -->
    <script id="ProcessUser" language="sql" db="app_db">
        INSERT INTO user_audit (user_id, action) 
        VALUES ({{id}}, 'Processed iteration {{LOOP_INDEX}}');
    </script>
</foreach>
```

---

## 5. Advanced Examples

### Example A: Exporting Multiple Parameters (Comma-Separated Output)
When you need to output multiple distinct values from a script to be consumed as separate parameters in a subsequent step, you can format the output as a delimited string and parse it inside the next Go script.

```xml
<pipeline>
    <scripts>
        <!-- Step 1: Export a delimited config from Go -->
        <script id="GenerateParams" language="go" output_var="MultiParams">
            package main
            import "fmt"
            func main() {
                env := "production"
                retries := 5
                timeout := 30
                fmt.Printf("%s,%d,%d", env, retries, timeout)
            }
        </script>

        <!-- Step 2: Consume and split parameters inside another Go script -->
        <script id="UseParams" language="go">
            package main
            import (
                "fmt"
                "strings"
                "strconv"
                "host/vars/vars"
            )
            func main() {
                raw := vars.GetString("MultiParams")
                parts := strings.Split(raw, ",")
                if len(parts) == 3 {
                    env := parts[0]
                    retries, _ := strconv.Atoi(parts[1])
                    timeout, _ := strconv.Atoi(parts[2])
                    fmt.Printf("Env: %s, Retries: %d, Timeout: %d\n", env, retries, timeout)
                }
            }
        </script>
    </scripts>
</pipeline>
```

### Example B: Passing JSON Output Between Scripts
For complex, structured data, you can output a JSON string, capture it, and parse it back into typed structs in subsequent dynamic Go scripts.

```xml
<pipeline>
    <scripts>
        <!-- Step 1: Query database config details, formatting output as JSON -->
        <script id="FetchServiceConfig" language="go" output_var="ServiceJSON">
            package main
            import (
                "encoding/json"
                "fmt"
            )
            type ConnectionDetails struct {
                Host string `json:"host"`
                Port int    `json:"port"`
                SSL  bool   `json:"ssl"`
            }
            func main() {
                cfg := ConnectionDetails{
                    Host: "db-replica.internal",
                    Port: 5432,
                    SSL:  true,
                }
                bytes, _ := json.Marshal(cfg)
                fmt.Println(string(bytes))
            }
        </script>

        <!-- Step 2: Unmarshal and utilize the JSON payload in a subsequent step -->
        <script id="ConnectToService" language="go">
            package main
            import (
                "encoding/json"
                "fmt"
                "host/vars/vars"
            )
            type ConnectionDetails struct {
                Host string `json:"host"`
                Port int    `json:"port"`
                SSL  bool   `json:"ssl"`
            }
            func main() {
                jsonStr := vars.GetString("ServiceJSON")
                var cfg ConnectionDetails
                if err := json.Unmarshal([]byte(jsonStr), &cfg); err != nil {
                    fmt.Printf("Failed to unmarshal config: %v\n", err)
                    return
                }
                fmt.Printf("Successfully established connection to %s:%d (SSL: %t)\n", cfg.Host, cfg.Port, cfg.SSL)
            }
        </script>
    </scripts>
</pipeline>
```
