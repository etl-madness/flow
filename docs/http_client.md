# HTTP_CLIENT Node Reference

### Core Execution Flow
1. **Variable Snapshot & Interpolation**: Reads current registry variables and replaces `{{VarName}}` placeholders in `uri`, `url`, `headers`, `content_type`, `data`, and inner element body text.
2. **Transport & Client Assembly**: Constructs `http.Transport` with TLS/proxy settings and wraps it inside an `http.Client` with custom redirect and timeout policies via `BuildClientAndRequest`.
3. **Request Execution**: Issues the HTTP request bound to the parent pipeline execution context (`context.Context`).
4. **Response Handling & State Persistence**: Reads the response body and writes data to target pipeline variables (`output_var`, `status_code_var`), updating `LAST_OUTPUT`.

---

## 2. Complete Attribute Specification

### 2.1 Core Request & Variable Attributes

| Attribute | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `id` | `xs:string` | Auto (`http_N`) | Unique node identifier for logging and AST validation. |
| `uri` / `url` | `xs:string` | **Required** | Target endpoint URL. Supports variable interpolation (`{{VarName}}`). |
| `method` | `xs:string` | `GET` | HTTP verb (`GET`, `POST`, `PUT`, `DELETE`, `PATCH`, `HEAD`, `OPTIONS`). |
| `data` | `xs:string` | Optional | Direct payload data. Takes precedence over element body content. |
| `headers` | `xs:string` | Optional | Custom request headers (`K1:V1,K2:V2` or newline-separated). |
| `content_type` | `xs:string` | Optional | Shortcut header setter for `Content-Type`. |
| `var` / `variable` / `output_var` / `output_variable` / `out_var` | `xs:string` | Optional | Registry variable key to store the response body. |
| `status_code_var` / `status_code_variable` / `status_var` / `status_variable` | `xs:string` | Optional | Registry variable key to store the integer HTTP status code. |

### 2.2 Go `http.Client` Settings

| Attribute | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `timeout` | `xs:string` | Unlimited | Total request timeout duration (e.g., `5s`, `30s`, `2m`). |
| `follow_redirects` | `xs:boolean` | `true` | Follow HTTP 3xx redirect responses automatically. |
| `max_redirects` | `xs:nonNegativeInteger` | Unlimited | Maximum redirect chain limit before erroring. |
| `cookie_jar` | `xs:boolean` | `false` | Enable in-memory `cookiejar` session state. |

### 2.3 Go `http.Transport` & TLS Tuning

| Attribute | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `proxy` | `xs:string` | Optional | Proxy URL (e.g., `http://proxy.internal:8080`). |
| `tls_insecure_skip_verify` | `xs:boolean` | `false` | Disable SSL/TLS certificate validation checks. |
| `tls_server_name` | `xs:string` | Optional | SNI host verification override. |
| `tls_min_version` | `xs:string` | `1.2` | Minimum TLS version (`1.0`, `1.1`, `1.2`, `1.3`). |
| `tls_max_version` | `xs:string` | `1.3` | Maximum TLS version restriction. |
| `tls_handshake_timeout` | `xs:string` | Optional | Maximum wait duration for TLS handshake. |
| `disable_keep_alives` | `xs:boolean` | `false` | Disable HTTP keep-alive connection reuse. |
| `disable_compression` | `xs:boolean` | `false` | Disable `Accept-Encoding: gzip` headers. |
| `max_idle_conns` | `xs:nonNegativeInteger` | Default | Global maximum idle connection pool capacity. |
| `max_idle_conns_per_host` | `xs:nonNegativeInteger` | Default | Host-level maximum idle connection limit. |
| `max_conns_per_host` | `xs:nonNegativeInteger` | Unlimited | Total connection pool limit per host. |
| `idle_conn_timeout` | `xs:string` | Optional | Maximum idle duration before closing pooled connection. |
| `response_header_timeout` | `xs:string` | Optional | Wait duration limit for server response headers. |
| `expect_continue_timeout` | `xs:string` | Optional | Wait time limit for `100-continue` header responses. |
| `max_response_header_bytes` | `xs:long` | Default | Maximum allowed size of response headers. |
| `write_buffer_size` | `xs:nonNegativeInteger` | Default | Size of the write buffer in bytes. |
| `read_buffer_size` | `xs:nonNegativeInteger` | Default | Size of the read buffer in bytes. |
| `force_attempt_http2` | `xs:boolean` | Default | Force HTTP/2 protocol negotiation on TLS connections. |

---

## 3. Payload Resolution & Variable Interpolation

When preparing an outgoing payload (e.g., `POST`, `PUT`, `PATCH`), the engine applies a two-tier resolution strategy:

1. **Attribute Data (`data="..."`)**: If the `data` attribute is populated, it is selected as the payload body. `{{VarName}}` tokens inside the attribute are interpolated.
2. **Inner Body Content**: If `data` is omitted, inner XML text is processed. Any `{{VarName}}` tags are replaced with corresponding values from the registry context.

---

## 4. Control Flow & Loop Integration

### 4.1 SQL Driver Iteration (`<foreach>`)
Inside a `<foreach>` block, every returned database row populates registry variables with column values (bound in exact, lowercase, and uppercase forms) alongside `LOOP_INDEX`. `<http_client>` consumes these variables per iteration:

```xml
<foreach id="IterateUsers" db="app_db">
    SELECT user_id, email, account_tier FROM users WHERE sync_pending = 1;

    <http_client 
        id="PostUserToApi"
        uri="[https://api.example.com/v1/sync](https://api.example.com/v1/sync)"
        method="POST"
        content_type="application/json"
        data='{"id": {{user_id}}, "email": "{{email}}", "tier": "{{account_tier}}"}'
        output_var="ApiResponseBody"
        status_code_var="ApiResponseCode" />

    <sql id="LogSync" db="app_db">
        UPDATE users 
        SET synced = 1, sync_code = {{ApiResponseCode}} 
        WHERE user_id = {{user_id}};
    </sql>
</foreach>
```
### 4.2 Polling Loops (`<while>`)
Inside a `<while>` loop, `<http_client>` can poll remote job status endpoints until a termination condition is met:
```xml
<pipeline xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
          xsi:noNamespaceSchemaLocation="https://raw.githubusercontent.com/etl-madness/flow/main/xsd/pipeline.xsd">
    <variables>
        <variable name="JobState" type="string" value="RUNNING" />
        <variable name="JobID" type="string" value="JOB-99201" />
    </variables>

    <flow>
        <while id="PollStatus" var="JobState" equals="RUNNING" max_iterations="20">
            <http_client 
                id="CheckStatus"
                uri="[https://api.example.com/v1/jobs/](https://api.example.com/v1/jobs/){{JobID}}"
                method="GET"
                output_var="JobState" />

            <script id="Sleep" language="go">
                package main
                import "time"
                func main() { time.Sleep(2 * time.Second) }
            </script>
        </while>
    </flow>
</pipeline>
```

### 4.3 Concurrent Requests (`<parallel>`)
Inside `<parallel>` blocks, each child branch runs in an isolated worker thread with a cloned registry snapshot. Output variables updated by `<http_client>` in thread branches are automatically merged or namespaced (`WORKER_<ID>_<VAR>`) back to the main registry upon completion.

```xml
<parallel max_threads="2">
    <http_client 
        id="FetchServiceA"
        uri="[https://service-a.internal/health](https://service-a.internal/health)"
        output_var="HealthA" />

    <http_client 
        id="FetchServiceB"
        uri="[https://service-b.internal/health](https://service-b.internal/health)"
        output_var="HealthB" />
</parallel>
```

### 5. Comprehensive Pipeline Examples

#### Example A: SQL -> HTTP POST -> Go Response Parser
```xml
<pipeline xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
          xsi:noNamespaceSchemaLocation="https://raw.githubusercontent.com/etl-madness/flow/main/xsd/pipeline.xsd">
    <flow>
        <!-- 1. Extract JSON payload from database -->
        <sql id="GetPayload" db="analytics_db" output_var="JsonPayload">
            SELECT JSON_OBJECT('sensor_id': id, 'reading': value) 
            FROM telemetry 
            WHERE processed = 0 
            LIMIT 1;
        </sql>

        <!-- 2. Post JSON payload to cloud endpoint -->
        <http_client 
            id="SendTelemetry"
            uri="[https://cloud.example.com/v1/telemetry](https://cloud.example.com/v1/telemetry)"
            method="POST"
            content_type="application/json"
            timeout="10s"
            data="{{JsonPayload}}"
            output_var="CloudResponse"
            status_code_var="CloudStatus" />

        <!-- 3. Parse cloud response in Go -->
        <script id="ParseResponse" language="go">
            package main
            import (
                "fmt"
                "host/vars"
            )
            func main() {
                status := vars.GetInt("CloudStatus")
                resp := vars.GetString("CloudResponse")
                fmt.Printf("Uploaded with status %d: %s\n", status, resp)
            }
        </script>
    </flow>
</pipeline>

```
#### Example B: Secure Enterprise Connection (TLS & Proxy Setup)
```xml
<pipeline xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
          xsi:noNamespaceSchemaLocation="https://raw.githubusercontent.com/etl-madness/flow/main/xsd/pipeline.xsd">
    <flow>
        <http_client 
            id="SecureEnterprisePost"
            uri="[https://secure.partner.com/api/v2/ingest](https://secure.partner.com/api/v2/ingest)"
            method="POST"
            content_type="application/json"
            headers="Authorization: Bearer secret-token-key, X-Client-ID: Pipeline01"
            proxy="[http://proxy.corp.internal:8080](http://proxy.corp.internal:8080)"
            tls_min_version="1.3"
            tls_insecure_skip_verify="false"
            tls_server_name="secure.partner.com"
            timeout="30s"
            max_idle_conns="50"
            output_var="IngestResult"
            status_code_var="IngestCode">
            {
                "timestamp": "{{CURRENT_TIMESTAMP}}",
                "environment": "production"
            }
        </http_client>
    </flow>
</pipeline>

```
