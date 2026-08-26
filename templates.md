# Overview of the `<template>` Node in Flow

The `<template>` node in `flow` allows you to dynamically generate text, configuration files, emails, or payloads by leveraging Go's powerful `text/template` engine. It evaluates your current pipeline variables and renders them into the desired output format, which can then be saved to a variable for use in subsequent pipeline steps.

## Key Attributes
* **`id`**: Unique identifier for the template node.
* **`name`**: (Optional) Name of the template being parsed.
* **`file`**: (Optional) Path to an external template file. If omitted, the node uses inline text.
* **`output_var`** (or **`var`**): The variable where the rendered text will be stored.

---

## Examples

### 1. Simple Inline Template
Use the template element to quickly generate text using pipeline variables.

```xml
<pipeline>
    <variables>
        <variable name="USER_NAME" type="string" value="Alice" />
        <variable name="ROLE" type="string" value="Admin" />
    </variables>
    <scripts>
        <template id="welcome_msg" output_var="EMAIL_BODY">
            Hello {{.USER_NAME}}, 
            Welcome back! Your current role is: {{.ROLE}}.
        </template>
        <script lang="bash">
            echo "$EMAIL_BODY"
        </script>
    </scripts>
</pipeline>
```

### 2. Loading an External Template File
For larger templates (like HTML emails or complex configurations), you can point the node to an external file.

```xml
<pipeline>
    <variables>
        <variable name="REPORT_DATE" type="string" value="2026-08-25" />
        <variable name="TOTAL_SALES" type="float" value="15430.50" />
    </variables>
    <scripts>
        <template id="gen_report" file="templates/monthly_report.tmpl" output_var="HTML_REPORT" />
    </scripts>
</pipeline>
```

### 3. Conditional Rendering
Because it uses Go's `text/template`, you can use native template logic like `if/else` directly inside the template.

```xml
<pipeline>
    <variables>
        <variable name="IS_PREMIUM" type="bool" value="true" />
        <variable name="USER" type="string" value="Bob" />
    </variables>
    <scripts>
        <template id="promo_msg" output_var="PROMO_TEXT">
            Hi {{.USER}},
            {{if .IS_PREMIUM}}
            Thank you for being a premium subscriber! Here is your 20% discount code.
            {{else}}
            Upgrade to premium today to get exclusive discounts!
            {{end}}
        </template>
    </scripts>
</pipeline>
```

### 4. Generating a JSON Payload for HTTP POST
A great use case for templates is cleanly generating JSON bodies for subsequent `<http-client>` calls.

```xml
<pipeline>
    <variables>
        <variable name="ORDER_ID" type="int" value="9924" />
        <variable name="STATUS" type="string" value="SHIPPED" />
    </variables>
    <scripts>
        <template id="build_json" output_var="REQ_BODY">
            {
                "order_id": {{.ORDER_ID}},
                "status": "{{.STATUS}}",
                "timestamp": "2026-08-25T18:00:00Z"
            }
        </template>
        
        <http-client method="POST" uri="https://api.example.com/webhook" content_type="application/json" data="{{REQ_BODY}}" />
    </scripts>
</pipeline>
```

### 5. Dynamic SQL Query Generation
You can render complex SQL statements and pass them into SQL scripts or stream ETL steps.

```xml
<pipeline>
    <variables>
        <variable name="TARGET_TABLE" type="string" value="sales_q3" />
        <variable name="MIN_AMOUNT" type="int" value="1000" />
    </variables>
    <scripts>
        <template id="build_sql" output_var="DYNAMIC_SQL">
            SELECT id, amount, customer_id 
            FROM dbo.raw_sales 
            WHERE amount > {{.MIN_AMOUNT}}
            INTO {{.TARGET_TABLE}};
        </template>
        
        <!-- Assuming your script supports reading from a var -->
        <script lang="sql" db="analytics" var="DYNAMIC_SQL" />
    </scripts>
</pipeline>
```
