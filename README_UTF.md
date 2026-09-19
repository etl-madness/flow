# UTF-8 & UTF-16 Encoding Guide for Flow

This document describes how the **Flow** engine handles character encodings across pipeline definitions, XML schema validation, file operations, variable interpolation, and XPath parsing.

---

## 1. Overview

Enterprise pipelines frequently ingest or produce XML documents from various sources:
* **Microsoft SQL Server / Windows systems** (`NVARCHAR(MAX)`, PowerShell outputs, SSIS migrations) that store or emit **UTF-16LE** (often with a Byte Order Mark `0xFF 0xFE`).
* **Unix / Cloud systems** that produce standard **UTF-8** (with or without BOM `0xEF 0xBB 0xBF`).
* **XML declarations** declaring `<?xml version="1.0" encoding="UTF-16"?>` even when transcoded or passed as database text strings.

Flow provides **transparent, zero-configuration character encoding normalization**. Pipeline authors do not need to add special encoding attributes or run external conversion scripts.

---

## 2. Encoding Auto-Detection & Normalization

Flow implements auto-detection and byte stream normalization in [`encoding.go`](file:///c:/Users/U00001/source/repos/etl-madness/flow/encoding.go).

### Supported Ingestion Formats

| Encoding Flavor | BOM Bytes | Auto-Detection Heuristic | Handling |
| :--- | :--- | :--- | :--- |
| **UTF-8 Plain** | None | Standard ASCII / UTF-8 code points | Read as-is |
| **UTF-8 with BOM** | `0xEF 0xBB 0xBF` | Strips BOM prefix | Stripped to clean UTF-8 |
| **UTF-16LE with BOM** | `0xFF 0xFE` | Matches BOM prefix | Transcoded to UTF-8 |
| **UTF-16BE with BOM** | `0xFE 0xFF` | Matches BOM prefix | Transcoded to UTF-8 |
| **UTF-16LE without BOM** | None | Leading `<` unit (`0x3C 0x00`) | Transcoded to UTF-8 |
| **UTF-16BE without BOM** | None | Leading `<` unit (`0x00 0x3C`) | Transcoded to UTF-8 |
| **UTF-8 declaring UTF-16** | N/A | Regex matches `encoding="utf-16"` | Rewritten to `encoding="UTF-8"` |

### Why XML Declarations Are Rewritten
Go's standard library `encoding/xml` decoder expects input matching its byte stream. If an XML stream has been transcoded into valid UTF-8, but its header still states `encoding="UTF-16"`, Go's XML parser will invoke `CharsetReader` or fail expecting 16-bit units. Flow dynamically rewrites `encoding="UTF-16"` (and single-quoted variants) to `encoding="UTF-8"` during normalization.

---

## 3. Core Engine APIs

Flow provides two primary encoding helpers in package `flow`:

### `NormalizeXMLBytes(data []byte) []byte`
Prepares raw XML byte streams for Go's standard XML parser and external tools like `xmllint`:
1. Strips any UTF-8 BOM.
2. Detects UTF-16 (LE or BE, with or without BOM) and transcodes UTF-16 code units to UTF-8.
3. Rewrites any `encoding="UTF-16"` XML declaration to `encoding="UTF-8"`.

```go
rawBytes, err := os.ReadFile("pipeline_utf16.xml")
if err != nil {
    log.Fatal(err)
}

// normalized is clean UTF-8 with updated XML declaration
normalized := flow.NormalizeXMLBytes(rawBytes)
```

### `DecodeTextBytes(data []byte) string`
Decodes arbitrary text files (such as logs, SQL scripts, or data files) into clean Go `string` values:
* Automatically strips UTF-8 BOMs.
* Detects UTF-16LE and UTF-16BE BOMs and decodes to UTF-8 string.
* Falls back to standard UTF-8 string conversion if no BOM is present.

```go
fileBytes, _ := os.ReadFile("exported_data.txt")
cleanString := flow.DecodeTextBytes(fileBytes)
```

---

## 4. Pipeline Execution & AST Integration

### `ParseXMLConfig(xmlData []byte)`
When parsing pipeline configuration definitions:
* `ParseXMLConfig` immediately passes input bytes through `NormalizeXMLBytes`.
* Pipelines formatted as UTF-16LE (with BOM from PowerShell or SQL Server exports) parse seamlessly without raising `XML syntax error on line 1: invalid UTF-8`.

### `ValidateXSD(xmlPath, xsdPath)`
When validating pipelines against `pipeline.xsd`:
* `xmllint` is invoked using standard input (`xmllint --schema <xsdPath> --noout -`).
* Flow reads `xmlPath`, normalizes the byte stream via `NormalizeXMLBytes`, and pipes the UTF-8 bytes to `xmllint`.
* Any errors reported on stdin (`-:12: error`) are automatically mapped back to `xmlPath:12: error` for user readability.
* Files on disk are never altered.

### `<file_read>` Node
When reading files into pipeline variables:
```xml
<file_read id="read_config" file="C:\exports\data.txt" var="MY_DATA" />
```
* Uses `DecodeTextBytes` internally.
* Eliminates null-byte corruption when reading Windows UTF-16 files into downstream variable replacements (`{{MY_DATA}}`).

### `<xml_xpath>` Node
When evaluating XPath queries against XML files or variables:
```xml
<xml_xpath id="get_token" file="response.xml" xpath="//auth/token/text()" output_var="AUTH_TOKEN" />
```
* Input bytes from either `file` or `var` are normalized with `NormalizeXMLBytes` prior to DOM tree construction in `xmlquery`.
* Supports both `output_var` and `out_var` destination attributes.

---

## 5. Schema Best Practices (`pipeline.xsd`)

* **Schema Document Header**: Always retain `<?xml version="1.0" encoding="UTF-8"?>` on line 1 of `xsd/pipeline.xsd`. 
  > **Note**: Do **not** declare `encoding="UTF-16"` on `pipeline.xsd` itself. XSD files on disk are 8-bit UTF-8 text; declaring UTF-16 will cause schema compilation tools like `xmllint` to misinterpret 8-bit ASCII characters as 16-bit code units, causing fatal compilation errors (`parser error: Blank needed here`).
* **Pipeline Instance Documents**: Pipeline XML files can declare either `encoding="UTF-8"` or `encoding="UTF-16"`; both validate and execute cleanly.
