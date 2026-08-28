# Guide: JSONPath Extraction Node (`<json_path>`) 

The `<json_path>` node provides lightweight, native querying and filtering over JSON documents in the Flow Pipeline Engine. It supports reading inputs from files or environment variables, evaluates expressions using JSONPath syntax, and structures outputs in multiple formats.

---

## ⚙️ Node Reference

### Attributes
- `id` (string, optional): Unique step identifier for logging and tracking.
- `file` (string, optional): Path to the source JSON file on disk. Supports variable interpolation (e.g. `{{json_path}}`).
- `var` (string, optional): Name of the pipeline variable containing the raw JSON string to query.
- `jsonpath` or `path` (string, optional): The JSONPath query expression to execute.
- `mode` (string, optional): Defines the formatting style of the output:
  - `"value"` (default): Returns string values or stringified raw nodes. Multiple matched nodes are joined by newlines.
  - `"json"`: Returns a single JSON-serialized element (if exactly one is matched).
  - `"json_array"`: Returns all matching elements serialized as a single JSON array list.
- `output_var` or `out_var` (string, required): Variable key to store the resulting output.

> [!NOTE]
> If neither `jsonpath` nor `path` attributes are defined, the engine automatically extracts the JSONPath expression from the inner tag text of the node.

---

## 💡 Examples

The following examples demonstrate common workflows using `<json_path>`.

### Example 1: Standard Extraction (Default `"value"` Mode)
Extracts simple scalar values from an external file and joins the matched results into a newline-separated string.

**Source JSON File (`books.json`):**
```json
{
  "store": {
    "books": [
      { "title": "Sayings of the Century", "category": "reference" },
      { "title": "Sword of Honour", "category": "fiction" }
    ]
  }
}
```

**XML Pipeline Definition:**
```xml
<pipeline>
  <variables>
    <variable name="src_file" type="string" value="books.json" />
  </variables>
<flow>
  <!-- Query all titles in default text-join mode -->
  <json_path id="get_titles" file="{{src_file}}" jsonpath="$.store.books[*].title" output_var="book_titles" />
</flow>
</pipeline>
```

**Resulting Output (`book_titles`):**
```text
Sayings of the Century
Sword of Honour
```

---

### Example 2: Extracting Lists (`mode="json_array"`)
Extracts and serialized matched objects or numeric arrays as a structured JSON array suitable for passing to downstream APIs.

**XML Pipeline Definition:**
```xml
<pipeline>
  <flow>
  <!-- Extract all book categories as a serialized JSON string array -->
  <json_path id="get_categories" 
             file="books.json" 
             jsonpath="$.store.books[*].category" 
             mode="json_array" 
             output_var="categories_json" />
  </flow>
</pipeline>
```

**Resulting Output (`categories_json`):**
```json
["reference", "fiction"]
```

---

### Example 3: Extracting Single Objects (`mode="json"`)
Extracts a specific nested object node and serializes it directly to a clean JSON string block.

**XML Pipeline Definition:**
```xml
<pipeline>
  <flow>
  <!-- Extract only the first book object -->
  <json_path id="get_first_book" 
             file="books.json" 
             jsonpath="$.store.books[0]" 
             mode="json" 
             output_var="first_book" />
  </flow>
</pipeline>
```

**Resulting Output (`first_book`):**
```json
{"title": "Sayings of the Century", "category": "reference"}
```

---

### Example 4: Expression Filters & Variable Interpolation
Combines expression-based filtering (such as price filters) with dynamic variable interpolation to query a document based on runtime pipeline values.

**Source JSON File (`store.json`):**
```json
{
  "store": {
    "bicycle": { "color": "red", "price": 19.95 },
    "unicycle": { "color": "blue", "price": 45.00 }
  }
}
```

**XML Pipeline Definition:**
```xml
<pipeline>
  <variables>
    <variable name="max_price" type="string" value="25.00" />
  </variables>

  <flow>
    <!-- Select only items with price less than the max_price variable value -->
    <json_path id="filter_by_price" 
               file="store.json" 
               jsonpath="$.store.*[?(@.price &lt; {{max_price}})].color" 
               output_var="cheap_colors" />
  </flow>
</pipeline>
```

**Resulting Output (`cheap_colors`):**
```text
red
```

---

### Example 5: Inline Expression Definitions
For complex JSONPath syntax that contains many special characters, specify the query expression inside the tag body content rather than as an attribute.

**XML Pipeline Definition:**
```xml
<pipeline>
  <variables>
    <variable name="raw_payload" type="string" value='{"employees": [{"name": "Alice", "role": "developer"}, {"name": "Bob", "role": "manager"}]}' />
  </variables>
  <flow>
  <!-- Uses chardata body text as the JSONPath expression and stores output variables -->
  <json_path id="get_developers" var="raw_payload" output_var="dev_names">
    $.employees[?(@.role == "developer")].name
  </json_path>
  </flow>
</pipeline>
```

**Resulting Output (`dev_names`):**
```text
Alice
```
