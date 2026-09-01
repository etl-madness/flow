# Overview of the `<yaml_path>` Node in Flow

The `<yaml_path>` node in `flow` enables evaluating path queries against raw YAML content loaded from disk files or stored inside pipeline variables[cite: 1, 2, 4]. It converts YAML structures into normalized JSON in memory and evaluates path expressions, writing the output back to downstream pipeline variables.

## Key Attributes

* **`file`**: (Optional) Path to a `.yaml` or `.yml` file on disk[cite: 1, 2]. Supports `{{VarName}}` variable interpolation.
* **`var`**: (Optional) Pipeline environment variable containing raw YAML text[cite: 1, 2].
* **`path` / `yamlpath`**: (Optional) The path query expression to evaluate[cite: 1, 2]. Alternatively, the path expression can be written inside the element body[cite: 2, 4].
* **`mode`**: (Optional) Format of the extracted output[cite: 1, 2, 4]:
  * `value` (Default): Returns scalar text or line-separated value matches[cite: 1, 2, 4].
  * `json`: Returns extracted sub-objects as a formatted JSON string[cite: 2, 4].
  * `json_array`: Serializes all matched nodes into a JSON array string[cite: 1, 2, 4].
  * `yaml`: Serializes matched nodes back into clean YAML block format[cite: 2, 4].
* **`output_var` / `out_var`**: Target pipeline environment variable where extracted results will be saved[cite: 1, 2, 4].

---

## Examples

### 1. Extracting a Database Host from a Configuration File
Load a service configuration file and extract a scalar database connection string[cite: 2, 4].

```xml
<pipeline>
    <variables>
        <variable name="CONFIG_FILE" type="string" value="./config/app.yaml" />
    </variables>

    <flow>
        <!-- Extract the database host string into variable DB_HOST -->
        <yaml_path 
            file="{{CONFIG_FILE}}" 
            path="$.database.connection.host" 
            output_var="DB_HOST" />

        <script lang="bash">
            echo "Connecting to database at: $DB_HOST"
        </script>
    </flow>
</pipeline>
```

### 2. Extracting Nested Objects as JSON (mode="json")
Extract a sub-map or configuration block from a YAML file directly into a JSON string to pass into an HTTP client node.

```xml
<pipeline>
    <flow>
        <!-- Extract the entire telemetry configuration block as JSON -->
        <yaml_path 
            file="./config/services.yaml" 
            path="$.telemetry.metrics" 
            mode="json" 
            output_var="METRICS_CONFIG_JSON" />

        <sql_bulk 
            method="POST" 
            uri="[https://api.example.com/v1/config](https://api.example.com/v1/config)" 
            content_type="application/json" 
            data="{{METRICS_CONFIG_JSON}}" />
    </flow>
</pipeline>
```

### 3. Multi-Line Body Expression with Variable Interpolation
Place dynamic or complex path queries directly inside the element body using template variable substitution.

```xml
<pipeline>
    <variables>
        <variable name="ENVIRONMENT" type="string" value="production" />
    </variables>

    <flow>
        <file_read file="./config/deployments.yaml" output_var="MANIFEST_YAML" />

        <!-- Filter deployment settings based on pipeline environment -->
        <yaml_path var="MANIFEST_YAML" output_var="ACTIVE_REPLICAS">
            $.environments[?(@.name == '{{ENVIRONMENT}}')].replicaCount
        </yaml_path>
    </flow>
</pipeline>
```

### 4. Extracting Kubernetes Container Images (mode="json_array")
Extract a list of container images from a Kubernetes manifest file as a clean JSON array string.

```xml
<pipeline>
    <flow>
        <!-- Query all container images across all specs -->
        <yaml_path 
            file="./k8s/deployment.yaml" 
            mode="json_array" 
            output_var="CONTAINER_IMAGES">
            $.spec.template.spec.containers[*].image
        </yaml_path>

        <script lang="bash">
            echo "Deployed Images Array: $CONTAINER_IMAGES"
        </script>
    </flow>
</pipeline>
```

### 5. Re-serializing Matched Nodes to YAML (mode="yaml")
Extract a sub-section of a larger YAML document and output it formatted as valid YAML for writing to disk.

```xml
<pipeline>
    <flow>
        <file_read file="./config/cluster.yaml" output_var="FULL_CLUSTER_YAML" />

        <!-- Extract storage spec back out as pure YAML -->
        <yaml_path 
            var="FULL_CLUSTER_YAML" 
            path="$.cluster.storage" 
            mode="yaml" 
            output_var="STORAGE_YAML" />

        <!-- Write the isolated storage configuration to a new file -->
        <file_save file="./output/storage_spec.yaml" var="STORAGE_YAML" />
    </flow>
</pipeline>
```