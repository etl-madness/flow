# Overview of the `<file_read>` and `<file_save>` Nodes in Flow

The `<file_read>` and `<file_save>` nodes in `flow` allow you to interact directly with the local filesystem. You can read configuration files or datasets into pipeline variables, and you can write generated templates, script outputs, or log data back to disk.

## Key Attributes

### `<file_save>`
* **`file` / `path` / `filename`**: Target path on the local filesystem. Supports `{{VarName}}` interpolation.
* **`var` / `variable`**: (Optional) Source variable containing the text to write. If omitted, the inline text inside the element is used.
* **`append`**: (Optional) Boolean (`true` / `false`). If `true`, text is appended; if `false` (default), the file is overwritten. 

*Note: `<file_save>` automatically creates missing parent directories.*

### `<file_read>`
* **`file` / `path` / `filename`**: Target path of the file to load. Supports `{{VarName}}` interpolation.
* **`output_var` / `var`**: The pipeline variable where the file's contents will be stored as a string.

---

## Examples

### 1. Reading a JSON Config and Sending it via HTTP
Load a configuration file into memory and POST it to a remote endpoint.

```xml
<pipeline>
    <scripts>
        <file_read file="./data/payload.json" output_var="JSON_DATA" />
        
        <sql_bulk 
            method="POST" 
            uri="https://api.example.com/sync" 
            content_type="application/json" 
            data="{{JSON_DATA}}" />
    </scripts>
</pipeline>
```

### 2. Saving Inline Formatted Text to a Log File
Write inline text with variable interpolation directly to a log file, appending to it rather than overwriting.

```xml
<pipeline>
    <variables>
        <variable name="USER" type="string" value="Alice" />
        <variable name="LOG_DIR" type="string" value="/var/log/flow" />
    </variables>
    <scripts>
        <file_save file="{{LOG_DIR}}/audit.log" append="true">
            [{{USER}}] Pipeline step completed at 2026-08-25T18:24:51Z
        </file_save>
    </scripts>
</pipeline>
```

### 3. Rendering a Template and Saving the Output
Generate a dynamic HTML report using the `<template>` node and save the resulting variable to disk.

```xml
<pipeline>
    <variables>
        <variable name="REPORT_DATE" type="string" value="2026-08-25" />
    </variables>
    <scripts>
        <!-- Generate HTML content into REPORT_HTML -->
        <template id="gen_report" output_var="REPORT_HTML">
            <html>
                <body>
                    <h1>Daily Report: {{.REPORT_DATE}}</h1>
                    <p>All systems operational.</p>
                </body>
            </html>
        </template>
        
        <!-- Save the HTML to an output directory -->
        <file_save file="./output/reports/daily_{{REPORT_DATE}}.html" var="REPORT_HTML" />
    </scripts>
</pipeline>
```

### 4. Reading a SQL Query from File and Executing It
Keep your SQL scripts organized in files, load them dynamically, and execute them on your database.

```xml
<pipeline>
    <databases>
        <database name="primary_db" connection_string="sqlserver://user:pass@localhost:1433" />
    </databases>
    <scripts>
        <!-- Read SQL text into variable QUERY_TEXT -->
        <file_read file="./queries/nightly_cleanup.sql" output_var="QUERY_TEXT" />
        
        <!-- Execute the SQL query -->
        <script lang="sql" db="primary_db" var="QUERY_TEXT" />
    </scripts>
</pipeline>
```

### 5. Modifying and Resaving a File using Scripts
Read an existing file, process or modify it using a script (like Go, Bash, or PowerShell), and save the results to a new file.

```xml
<pipeline>
    <scripts>
        <!-- 1. Read input CSV -->
        <file_read file="./data/raw_users.csv" output_var="RAW_CSV" />
        
        <!-- 2. Process it via shell script, outputting to CLEAN_CSV -->
        <script lang="bash" output_var="CLEAN_CSV">
            # Using grep to remove empty lines and invalid data
            echo "$RAW_CSV" | grep -v "^$" | grep -i "@example.com"
        </script>
        
        <!-- 3. Save processed results to a new file -->
        <file_save file="./data/clean_users.csv" var="CLEAN_CSV" />
    </scripts>
</pipeline>
```
