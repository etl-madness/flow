package flow

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func encodeUTF16LE(s string, withBOM bool) []byte {
	var buf bytes.Buffer
	if withBOM {
		buf.Write([]byte{0xFF, 0xFE})
	}
	for _, r := range s {
		buf.WriteByte(byte(r))
		buf.WriteByte(byte(r >> 8))
	}
	return buf.Bytes()
}

func encodeUTF16BE(s string, withBOM bool) []byte {
	var buf bytes.Buffer
	if withBOM {
		buf.Write([]byte{0xFE, 0xFF})
	}
	for _, r := range s {
		buf.WriteByte(byte(r >> 8))
		buf.WriteByte(byte(r))
	}
	return buf.Bytes()
}

func TestNormalizeXMLBytes_AllEncodings(t *testing.T) {
	rawXML := `<?xml version="1.0" encoding="UTF-16"?><pipeline><variables><variable name="APP" value="FLOW"/></variables></pipeline>`

	tests := []struct {
		name     string
		input    []byte
		mustHave string
	}{
		{
			name:     "UTF-16LE with BOM",
			input:    encodeUTF16LE(rawXML, true),
			mustHave: `<variable name="APP" value="FLOW"/>`,
		},
		{
			name:     "UTF-16BE with BOM",
			input:    encodeUTF16BE(rawXML, true),
			mustHave: `<variable name="APP" value="FLOW"/>`,
		},
		{
			name:     "UTF-16LE without BOM",
			input:    encodeUTF16LE(rawXML, false),
			mustHave: `<variable name="APP" value="FLOW"/>`,
		},
		{
			name:     "UTF-16BE without BOM",
			input:    encodeUTF16BE(rawXML, false),
			mustHave: `<variable name="APP" value="FLOW"/>`,
		},
		{
			name:     "UTF-8 with UTF-8 BOM",
			input:    append([]byte{0xEF, 0xBB, 0xBF}, []byte(rawXML)...),
			mustHave: `<variable name="APP" value="FLOW"/>`,
		},
		{
			name:     "UTF-8 with UTF-16 declaration",
			input:    []byte(rawXML),
			mustHave: `<variable name="APP" value="FLOW"/>`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			normalized := NormalizeXMLBytes(tc.input)
			s := string(normalized)
			if !strings.Contains(s, tc.mustHave) {
				t.Fatalf("expected output to contain %q, got: %q", tc.mustHave, s)
			}
			if strings.Contains(s, "\x00") {
				t.Fatalf("output still contains null bytes: %q", s)
			}
			if strings.Contains(strings.ToLower(s), `encoding="utf-16"`) {
				t.Fatalf("output still contains encoding=\"utf-16\": %q", s)
			}
			if strings.Contains(strings.ToLower(s), `encoding="utf-8"`) == false {
				t.Fatalf("output should have encoding=\"UTF-8\", got: %q", s)
			}
		})
	}
}

func TestDecodeTextBytes(t *testing.T) {
	text := "Hello, World! Welcome to ETL."

	tests := []struct {
		name  string
		input []byte
	}{
		{"UTF-8 Plain", []byte(text)},
		{"UTF-8 with BOM", append([]byte{0xEF, 0xBB, 0xBF}, []byte(text)...)},
		{"UTF-16LE with BOM", encodeUTF16LE(text, true)},
		{"UTF-16BE with BOM", encodeUTF16BE(text, true)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := DecodeTextBytes(tc.input)
			if got != text {
				t.Fatalf("expected %q, got %q", text, got)
			}
		})
	}
}

func TestParseXMLConfig_UTF16Encodings(t *testing.T) {
	xmlText := `<?xml version="1.0" encoding="UTF-16"?>
<pipeline>
  <variables>
    <variable name="TEST_KEY" value="UTF16_SUCCESS"/>
  </variables>
  <flow>
    <script id="s1" language="shell">echo "ok"</script>
  </flow>
</pipeline>`

	tests := []struct {
		name  string
		input []byte
	}{
		{"UTF-16LE with BOM", encodeUTF16LE(xmlText, true)},
		{"UTF-16BE with BOM", encodeUTF16BE(xmlText, true)},
		{"UTF-16LE without BOM", encodeUTF16LE(xmlText, false)},
		{"UTF-16BE without BOM", encodeUTF16BE(xmlText, false)},
		{"UTF-8 with UTF-16 declaration", []byte(xmlText)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := ParseXMLConfig(tc.input)
			if err != nil {
				t.Fatalf("ParseXMLConfig failed for %s: %v", tc.name, err)
			}
			if len(cfg.Variables) != 1 || cfg.Variables[0].Name != "TEST_KEY" || cfg.Variables[0].Value != "UTF16_SUCCESS" {
				t.Fatalf("Unexpected parsed variables: %+v", cfg.Variables)
			}
			if len(cfg.FlowNodes) != 1 || cfg.FlowNodes[0].Kind != NodeScript {
				t.Fatalf("Unexpected parsed flow nodes: %+v", cfg.FlowNodes)
			}
		})
	}
}

func TestValidateXSD_UTF16(t *testing.T) {
	xsdPath := filepath.Join("xsd", "pipeline.xsd")
	if _, err := os.Stat(xsdPath); os.IsNotExist(err) {
		t.Skip("xsd/pipeline.xsd not found, skipping xmllint test")
	}

	xmlContent := `<?xml version="1.0" encoding="UTF-16"?>
<pipeline>
  <variables>
    <variable name="DB_NAME" value="analytics" />
  </variables>
  <flow>
    <script id="step_1">echo "valid"</script>
  </flow>
</pipeline>`

	tests := []struct {
		name     string
		data     []byte
		filename string
	}{
		{"UTF-16LE with BOM", encodeUTF16LE(xmlContent, true), "test_utf16le_bom.xml"},
		{"UTF-16BE with BOM", encodeUTF16BE(xmlContent, true), "test_utf16be_bom.xml"},
		{"UTF-16LE without BOM", encodeUTF16LE(xmlContent, false), "test_utf16le_nobom.xml"},
		{"UTF-8 with UTF-16 declaration", []byte(xmlContent), "test_utf8_decl.xml"},
	}

	tempDir := t.TempDir()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			filePath := filepath.Join(tempDir, tc.filename)
			if err := os.WriteFile(filePath, tc.data, 0644); err != nil {
				t.Fatalf("failed to write test xml file: %v", err)
			}

			if err := ValidateXSD(filePath, xsdPath); err != nil {
				t.Fatalf("ValidateXSD failed for %s: %v", tc.name, err)
			}
		})
	}
}

func TestExecutor_XMLXPath_UTF16File(t *testing.T) {
	dataXML := `<?xml version="1.0" encoding="UTF-16"?>
<catalog>
  <book id="bk101">
    <title>ETL Architecture in Go</title>
    <price>49.99</price>
  </book>
</catalog>`

	tempDir := t.TempDir()
	xmlFile := filepath.Join(tempDir, "books_utf16.xml")
	if err := os.WriteFile(xmlFile, encodeUTF16LE(dataXML, true), 0644); err != nil {
		t.Fatalf("failed to write UTF-16 xml file: %v", err)
	}

	pipelineXML := `<?xml version="1.0" encoding="UTF-16"?>
<pipeline>
  <variables>
    <variable name="BOOK_TITLE" value="" />
  </variables>
  <flow>
    <xml_xpath id="get_title" file="` + filepath.ToSlash(xmlFile) + `" xpath="//book[@id='bk101']/title/text()" output_var="BOOK_TITLE" />
  </flow>
</pipeline>`

	cfg, err := ParseXMLConfig([]byte(pipelineXML))
	if err != nil {
		t.Fatalf("ParseXMLConfig failed: %v", err)
	}

	registry := NewRegistry()
	if err := registry.InitVariables(cfg.Variables); err != nil {
		t.Fatalf("InitVariables failed: %v", err)
	}

	executor := NewExecutor(registry)
	results, err := executor.Execute(context.Background(), cfg.FlowNodes)
	if err != nil {
		t.Fatalf("executor.Execute failed: %v", err)
	}

	if len(results) != 1 || results[0].ReturnCode != 0 {
		t.Fatalf("xml_xpath failed: %+v", results)
	}

	val := registry.GetVarString("BOOK_TITLE")
	if val != "ETL Architecture in Go" {
		t.Fatalf("expected 'ETL Architecture in Go', got %q", val)
	}
}

func TestExecutor_FileRead_UTF16(t *testing.T) {
	textContent := "Database Migration Pipeline v2.0"
	tempDir := t.TempDir()
	txtFile := filepath.Join(tempDir, "migration_utf16.txt")
	if err := os.WriteFile(txtFile, encodeUTF16LE(textContent, true), 0644); err != nil {
		t.Fatalf("failed to write UTF-16 text file: %v", err)
	}

	pipelineXML := `<?xml version="1.0" encoding="UTF-16"?>
<pipeline>
  <variables>
    <variable name="FILE_CONTENT" value="" />
  </variables>
  <flow>
    <file_read id="read_utf16" file="` + filepath.ToSlash(txtFile) + `" var="FILE_CONTENT" />
  </flow>
</pipeline>`

	cfg, err := ParseXMLConfig([]byte(pipelineXML))
	if err != nil {
		t.Fatalf("ParseXMLConfig failed: %v", err)
	}

	registry := NewRegistry()
	if err := registry.InitVariables(cfg.Variables); err != nil {
		t.Fatalf("InitVariables failed: %v", err)
	}

	executor := NewExecutor(registry)
	results, err := executor.Execute(context.Background(), cfg.FlowNodes)
	if err != nil {
		t.Fatalf("executor.Execute failed: %v", err)
	}

	if len(results) != 1 || results[0].ReturnCode != 0 {
		t.Fatalf("file_read failed: %+v", results)
	}

	val := registry.GetVarString("FILE_CONTENT")
	if val != textContent {
		t.Fatalf("expected %q, got %q", textContent, val)
	}
}
