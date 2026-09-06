# Key-Value (KV) Store Integration

This guide covers how to configure and execute operations against Key-Value databases within the `flow` pipeline orchestration library. The engine supports both embedded key-value stores (`bbolt`, `BadgerDB`) and server-based instances (`Redis/Valkey`, `etcd`)[cite: 1].

---

## Overview

The KV engine provides two primary execution nodes:
* **`<kv>`**: Executes individual atomic operations (`get`, `put`/`set`, `delete`/`del`, `scan`/`list`). Returns logs and outputs compatible with standard `<sql>` nodes[cite: 1, 3].
* **`<kv_bulk>`**: Performs high-throughput streaming and ETL between SQL databases and Key-Value stores. Reports progress compatible with `<sql_bulk>` nodes.

---

## Supported Drivers

| Driver Name | Type | Connection String Syntax | Primary Use Case |
| :--- | :--- | :--- | :--- |
| **`bbolt`** / **`bolt`** | Embedded | `/path/to/database.db` | Lightweight local caching, transactional state persistence[cite: 1] |
| **`badger`** | Embedded | `/path/to/data_dir` | High-write throughput embedded storage[cite: 1] |
| **`redis`** / **`valkey`** | Server | `redis://:password@localhost:6379/0` | High-speed shared in-memory caching and messaging[cite: 1] |
| **`etcd`** | Server | `http://127.0.0.1:2379,http://127.0.0.1:22379` | Distributed consensus, dynamic configuration management[cite: 1] |

---

## Database Configuration

Define KV connections inside the `<databases>` XML container[cite: 1, 8]. Connection strings support standard variable interpolation (`{{VarName}}`)[cite: 1, 5, 8].

### Embedded Key-Value Database (`bbolt`)
For embedded stores like `bbolt`, pass the target file path in `connection_string`[cite: 1, 8]. The engine creates parent directories automatically if they do not exist.

```xml
<databases>
    <database 
        name="local_cache" 
        driver="bbolt" 
        connection_string="./data/app_cache.db" />
</databases>
```
### Server-Based Key-Value Database (Redis)
```xml
<databases>
    <database 
        name="redis_store" 
        driver="redis" 
        connection_string="redis://:secret_pass@127.0.0.1:6379/0" />
</databases>
```

## XML Reference & Execution Nodes
### 1.Single Operations (<kv>)
Executes point reads, writes, deletions, or range scans against a target bucket/namespace.

|Attribute|Required|Default|Description|
| :--- | :--- | :--- | :--- |
|id|No|Auto|Unique execution step identifier.|
|db|Yes|—|Target database connection handle.|
|bucket|No|default|Bucket name (bbolt/BadgerDB) or key prefix namespace (Redis/etcd).|
|op|No|*|Operation type: get, put/set, delete/del, scan/list.|
|key|No|*|Target record key.|
|value|No|*|Record value payload for write operations.|
|var|No|—|Environment variable containing a dynamic DSL string.|
|output_var|No|LAST_OUTPUT|Variable that receives the command output or value.|
* Note: Operations, keys, and values can be defined via XML attributes or inline body DSL text.
  
### Result Formatting

`<kv>` returns standard result logs compatible with `<sql>` execution observability:
- **Write Ops (put, delete)**: Logs `(1 row(s) affected)`.
- **Read Ops (get, scan)**: Formats tabular outputs (`KEY\tVALUE\n...`) and logs `(N row(s) returned)`.

### 2. Bulk Operations (<kv_bulk>)
Streams data sets directly between relational SQL queries and Key-Value buckets.
|Attribute|Required|Default|Description|
| :--- | :--- | :--- | :--- |
|id|No|Auto|Unique execution step identifier.|
|db|Yes|—|Source database connection handle.|
|target_db|No|db|Destination database connection handle (KV or SQL).|
|target_bucket|No|—|Target KV bucket or namespace receiving streamed records.|
|batch_size|No|10000|Number of key-value pairs written per commit chunk.|
|output_var|No|—|Variable receiving the total count of transferred records.|

* Note: Bulk operations are optimized for high-throughput data transfer between SQL and KV stores.
* Note: The `target_db` and `target_bucket` attributes must be correctly specified to ensure successful data transfer.

## Usage Examples

### 1. Embedded Write & Read (bbolt)
```xml
<pipeline description="Embedded KV Operations Example">
    <variables>
        <variable name="CACHE_FILE" type="string" value="./data/pipeline.db" />
        <variable name="USER_ID" type="string" value="usr_9901" />
        <variable name="USER_DATA" type="string" value='{"status":"active","tier":"gold"}' />
    </variables>

    <databases>
        <database name="embedded_store" driver="bbolt" connection_string="{{CACHE_FILE}}" />
    </databases>

    <flow>
        <!-- Write record to "user_sessions" bucket -->
        <kv id="write_session" 
            db="embedded_store" 
            bucket="user_sessions" 
            op="put" 
            key="{{USER_ID}}" 
            value="{{USER_DATA}}" />

        <!-- Read record from "user_sessions" bucket into variable "RETRIEVED_SESSION" -->
        <kv id="read_session" 
            db="embedded_store" 
            bucket="user_sessions" 
            op="get" 
            key="{{USER_ID}}" 
            output_var="RETRIEVED_SESSION" />

        <!-- Output assertion -->
        <assert id="check_session" var="RETRIEVED_SESSION" operator="ne" value="" />
    </flow>
</pipeline>
```

### 2. Inline Command DSL Usage
You can supply key-value commands using inline text body instead of XML attributes:
```xml
<flow>
    <!-- Inline PUT command -->
    <kv id="inline_write" db="embedded_store" bucket="config">
        PUT system_status operational
    </kv>

    <!-- Inline GET command -->
    <kv id="inline_read" db="embedded_store" bucket="config" output_var="SYS_STATUS">
        GET system_status
    </kv>
</flow>
```

### 3. Key Prefix Scanning

```xml
<flow>
    <!-- Scan all keys starting with 'usr_' in the 'accounts' bucket -->
    <kv id="scan_users" 
        db="embedded_store" 
        bucket="accounts" 
        op="scan" 
        key="usr_" 
        output_var="USER_LIST" />
</flow>
```

### 4. High-Performance Bulk ETL (PostgreSQL to bbolt)
Stream 100,000+ user records directly from PostgreSQL into a local bbolt embedded database chunked in batches of 5,000 records.

```xml
<pipeline description="PostgreSQL to bbolt Streaming ETL">
    <databases>
        <database name="postgres_db" driver="postgres" connection_string="postgres://user:pass@localhost:5432/app?sslmode=disable" />
        <database name="cache_db" driver="bbolt" connection_string="./cache/lookup.db" />
    </databases>

    <flow>
        <!-- The SQL query MUST return at least 2 columns: Column 1 = Key, Column 2 = Value -->
        <kv_bulk id="cache_user_tokens" 
                 db="postgres_db" 
                 target_db="cache_db" 
                 target_bucket="tokens" 
                 batch_size="5000" 
                 output_var="COPIED_COUNT">
            SELECT 
                user_id AS key, 
                auth_token AS value 
            FROM active_user_tokens 
            WHERE is_valid = true
        </kv_bulk>

        <assert id="verify_bulk_transfer" var="COPIED_COUNT" operator="ne" value="0" />
    </flow>
</pipeline>
```

## Observability & Error Handling

All <kv> and <kv_bulk> operations emit lifecycle telemetry events captured by standard observers and event sinks:  
* Attempt Tracking: Execution start/finish duration, row count metrics (read, written, affected), and attempt status (succeeded, failed, canceled).
* Error Classification: File lock errors, invalid buckets, missing connection handles, or network disconnects are mapped to standard ErrorClass definitions (e.g., ErrorClassFileSystem, ErrorClassDatabase).
* Redaction: Sensitive keys or value strings matching security patterns are masked in error outputs.
