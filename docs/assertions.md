# Pipeline Assertions in Flow 🚨

Flow pipelines support native `<assert>` tags to perform data quality checks, environment checks, and business rule validations. With assertions, you can gracefully halt execution on critical issues or run fallback pipelines to recover or log warnings.

---

## 🛠️ Configuration Syntax

The `<assert>` tag supports the following attributes:

*   **`var`**: The name of the variable inside the pipeline registry to evaluate. (Required)
*   **`equals`** / **`value`**: The expected value to compare against (case-insensitive string comparison).
*   **`message`**: A custom error/warning message to log when the assertion fails.
*   **`on_failure`**: Behavior on assertion failure. Options:
    *   `halt` (default): Immediately stops pipeline execution and returns a non-zero exit code.
    *   `warn` / `continue`: Logs a warning to the execution results but allows the pipeline to continue.
*   **`fail_var`**: (Optional) Pipeline variable name to set when the assertion fails.
*   **`fail_val`**: (Optional) The value to set in `fail_var` (defaults to `"true"` if omitted).
*   **`<on_failure>`**: (Optional) A nested child element containing sequence nodes (e.g., `<script>`, `<sql>`, `<http_client>`) to execute only if the assertion fails.

---

## 💡 Practical Examples

### Example 1: Basic Environment Check (Fail-Fast)
Ensure the active pipeline environment variable matches the production expectation. If not, halt the pipeline immediately.

```xml
<pipeline>
    <variables>
        <variable name="ENV" type="string" value="staging" />
    </variables>
    <flow>
        <!-- This will halt execution since ENV is staging instead of production -->
        <assert id="check_production" 
                var="ENV" 
                equals="production" 
                message="Pipeline must be run in production environment!" 
                on_failure="halt" />
                
        <!-- Subsequent nodes will not run -->
        <script id="prod_deploy" language="powershell">
            Write-Host "Deploying to production..."
        </script>
    </flow>
</pipeline>
```

---

### Example 2: API Status Verification with Warnings
Run a warning-only assertion that checks an HTTP client's status code, logging a warning and setting a failure flag, but allowing execution to continue.

```xml
<pipeline>
    <flow>
        <http_client id="fetch_health" 
                     url="https://api.example.com/health" 
                     status_code_var="STATUS_CODE" />

        <!-- Log a warning and set PIPELINE_DIRTY to true if status is not 200 -->
        <assert id="assert_api_health" 
                var="STATUS_CODE" 
                equals="200" 
                message="Health check returned non-200 code!" 
                on_failure="warn"
                fail_var="PIPELINE_DIRTY" 
                fail_val="true" />
                
        <script id="log_results" language="go">
            package main
            import (
                "fmt"
                "host/vars"
            )
            func main() {
                isDirty := vars.GetString("PIPELINE_DIRTY")
                if isDirty == "true" {
                    fmt.Println("Warning: Pipeline marked as dirty.")
                } else {
                    fmt.Println("Health checks passed successfully!")
                }
            }
        </script>
    </flow>
</pipeline>
```

---

### Example 3: Fallback and Cleanup on Failure (`<on_failure>`)
Use the nested `<on_failure>` block to run fallback scripts or cleanup actions when an assertion fails.

```xml
<pipeline>
    <databases>
        <database name="temp_db" driver="sqlite" connection_string="./temp.db" />
    </databases>
    <variables>
        <variable name="RECORD_COUNT" type="int" value="0" />
    </variables>
    <flow>
        <!-- Check if we ingested any records. If not, trigger the <on_failure> block -->
        <assert id="verify_ingested_records" 
                var="RECORD_COUNT" 
                equals="1000" 
                on_failure="halt" 
                message="Ingested record count does not match expected threshold!">
            <on_failure>
                <!-- Run a SQL cleanup script before halting -->
                <sql id="rollback_staging" db="temp_db">
                    DELETE FROM staging_records;
                </sql>
                <script id="notify_slack" language="go">
                    package main
                    import "fmt"
                    func main() {
                        fmt.Println("Sending Slack alert: Ingestion failed, staging table rolled back.")
                    }
                </script>
            </on_failure>
        </assert>
    </flow>
</pipeline>
```
