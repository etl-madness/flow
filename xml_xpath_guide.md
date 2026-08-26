# Guide: XML XPath Extraction Node (`<xml_xpath>`)

The `<xml_xpath>` node provides advanced XML querying and parsing functionality within the Flow Pipeline Engine. Powered by `xmlquery`, it allows users to execute standard XPath expressions over XML files or environment variables and extract matched elements into plaintext, raw XML, or JSON formats.

---

## ⚙️ Node Reference

### Attributes
- `id` (string, optional): Unique step identifier.
- `file` (string, optional): Path to the source XML file on disk. Supports dynamic path interpolation (e.g. `{{xml_path}}`).
- `var` (string, optional): Variable key containing the raw XML string payload to parse.
- `xpath` (string, optional): The XPath query expression to execute.
- `mode` (string, optional): Output serialization format:
  - `"text"` (default): Returns the inner text of matched elements, joined by newlines.
  - `"xml"`: Returns raw XML strings (including element tags and nested content), joined by newlines.
  - `"json_array"`: Returns matched elements or text contents serialized into a single JSON array list of strings.
- `output_var` (string, required): Variable key to store the resulting output.

> [!NOTE]
> If the `xpath` attribute is omitted, the engine automatically extracts the XPath query from the inner body text of the element.

---

## 💡 Practical Examples

The following examples demonstrate common workflows using `<xml_xpath>`.

### Example 1: Extracting Inner Text (Default `"text"` Mode)
Retrieves the text inside targeted tags and joins the results together with newlines.

**Source XML File (`inventory.xml`):**
```xml
<inventory>
  <item id="101">
    <name>Wireless Mouse</name>
    <category>Peripherals</category>
  </item>
  <item id="102">
    <name>Mechanical Keyboard</name>
    <category>Peripherals</category>
  </item>
</inventory>
```

**XML Pipeline Definition:**
```xml
<pipeline>
  <variables>
    <variable name="xml_file" type="string" value="inventory.xml" />
  </variables>

  <!-- Query names in default plaintext join mode -->
  <xml_xpath id="get_names" file="{{xml_file}}" xpath="//item/name" output_var="item_names" />
</pipeline>
```

**Resulting Output (`item_names`):**
```text
Wireless Mouse
Mechanical Keyboard
```

---

### Example 2: Extracting Outer XML Elements (`mode="xml"`)
Selects whole node trees and outputs them with their XML tags preserved.

**XML Pipeline Definition:**
```xml
<pipeline>
  <!-- Select the full raw XML structures for all items -->
  <xml_xpath id="get_raw_items" 
             file="inventory.xml" 
             xpath="//item" 
             mode="xml" 
             output_var="raw_items" />
</pipeline>
```

**Resulting Output (`raw_items`):**
```xml
<item id="101"><name>Wireless Mouse</name><category>Peripherals</category></item>
<item id="102"><name>Mechanical Keyboard</name><category>Peripherals</category></item>
```

---

### Example 3: Exporting to JSON Lists (`mode="json_array"`)
Extracts attribute values or text nodes and packs them into a marshalled JSON string array.

**XML Pipeline Definition:**
```xml
<pipeline>
  <!-- Query the 'id' attribute values of all items and output as JSON list -->
  <xml_xpath id="get_ids" 
             file="inventory.xml" 
             xpath="//item/@id" 
             mode="json_array" 
             output_var="item_ids_json" />
</pipeline>
```

**Resulting Output (`item_ids_json`):**
```json
["101", "102"]
```

---

### Example 4: XPath Expressions with Variable Interpolation
Leverages runtime pipeline variables inside the XPath query itself for dynamic filtering.

**XML Pipeline Definition:**
```xml
<pipeline>
  <variables>
    <variable name="target_id" type="string" value="102" />
  </variables>

  <!-- Query item name dynamically using target_id variable -->
  <xml_xpath id="get_target_name" 
             file="inventory.xml" 
             xpath="//item[@id='{{target_id}}']/name" 
             output_var="target_name" />
</pipeline>
```

**Resulting Output (`target_name`):**
```text
Mechanical Keyboard
```

---

### Example 5: Inline Query Specifications (Inner Text)
Allows writing long, complex, or formatted XPath expressions cleanly inside the node body instead of cluttering attributes.

**XML Pipeline Definition:**
```xml
<pipeline>
  <variables>
    <variable name="raw_xml" type="string" value="&lt;root&gt;&lt;user role='admin'&gt;Alice&lt;/user&gt;&lt;user role='user'&gt;Bob&lt;/user&gt;&lt;/root&gt;" />
  </variables>

  <!-- Executes query written directly within body text -->
  <xml_xpath id="query_admins" var="raw_xml" output_var="admin_users">
    //user[@role='admin']
  </xml_xpath>
</pipeline>
```

**Resulting Output (`admin_users`):**
```text
Alice
```
