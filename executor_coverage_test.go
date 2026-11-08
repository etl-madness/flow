package flow

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/xuri/excelize/v2"
)

func TestGoScriptExecution(t *testing.T) {
	registry := NewRegistry()
	registry.SetVar("message", "hello")
	executor := NewExecutor(registry)

	var results []ScriptResult
	script := ScriptItem{
		ID: "go", Language: "go", OutputVar: "out",
		Code: `package main
import ("fmt"; "host/vars")
func main() { fmt.Println(vars.GetString("message")) }`,
	}
	if executor.executeScriptNode(context.Background(), script, &results) {
		t.Fatalf("Go script failed: %#v", results)
	}
	if got := registry.GetVarString("out"); got != "hello" {
		t.Fatalf("Go script output = %q, want hello", got)
	}
	results = nil
	script.Code = "not valid Go code !"
	if !executor.executeScriptNode(context.Background(), script, &results) || len(results) != 1 {
		t.Fatalf("invalid Go script unexpectedly succeeded: %#v", results)
	}
}

func TestShellAndDotnetScriptExecution(t *testing.T) {
	registry := NewRegistry()
	registry.SetVar("message", "hello")
	executor := NewExecutor(registry)
	language := "sh"
	if runtime.GOOS == "windows" {
		language = "cmd"
	}
	var results []ScriptResult
	script := ScriptItem{ID: "shell", Language: language, Code: "echo {{message}}", OutputVar: "out"}
	if executor.executeScriptNode(context.Background(), script, &results) || registry.GetVarString("out") != "hello" {
		t.Fatalf("shell script output = %q; results = %#v", registry.GetVarString("out"), results)
	}
	script.Code = "exit 3"
	results = nil
	if !executor.executeScriptNode(context.Background(), script, &results) || len(results) != 1 {
		t.Fatalf("shell failure not reported: %#v", results)
	}
	t.Setenv("SHELL", "")
	script.Language = "shell"
	script.Code = "echo fallback"
	results = nil
	if executor.executeScriptNode(context.Background(), script, &results) || registry.GetVarString("out") != "fallback" {
		t.Fatalf("default shell output = %q; results = %#v", registry.GetVarString("out"), results)
	}
	t.Setenv("PATH", t.TempDir())
	results = nil
	if !executor.executeScriptNode(context.Background(), ScriptItem{ID: "dotnet", Language: "csx", Code: "1 + 1"}, &results) || len(results) != 1 {
		t.Fatalf("missing dotnet not reported: %#v", results)
	}
}

func TestForEachStreamingAndBufferLimit(t *testing.T) {
	registry := NewRegistry()
	if err := registry.InitDatabases([]DatabaseConfig{
		{Name: "db", Driver: "sqlite", ConnectionString: filepath.Join(t.TempDir(), "foreach.db")},
	}); err != nil {
		t.Fatal(err)
	}
	defer registry.CloseDatabases()
	executor := NewExecutor(registry)
	node := PipelineNode{
		Kind: NodeForEach, GroupID: "stream",
		ForEachScript: &ScriptItem{ID: "driver", Language: "sql", DBName: "db",
			Code: "SELECT CAST('hello' AS BLOB) AS value UNION ALL SELECT NULL"},
	}
	var results []ScriptResult
	if executor.executeForEachNode(context.Background(), node, &results) || len(results) != 1 {
		t.Fatalf("streaming foreach failed: %#v", results)
	}
	if !strings.Contains(results[0].ResultsString, "2 iteration") {
		t.Fatalf("unexpected streaming summary: %#v", results)
	}
	node.Children = []PipelineNode{{Kind: NodeAssert, Assert: &AssertElement{ID: "child", Var: "value", Equals: "not hello"}}}
	results = nil
	if !executor.executeForEachNode(context.Background(), node, &results) {
		t.Fatalf("foreach child failure not reported: %#v", results)
	}
	node.Buffer = true
	results = nil
	if !executor.executeForEachNode(context.Background(), node, &results) {
		t.Fatalf("buffered foreach child failure not reported: %#v", results)
	}
	node.Children = nil
	node.ForEachScript.Code = "WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM n WHERE x < 100001) SELECT x FROM n"
	results = nil
	if !executor.executeForEachNode(context.Background(), node, &results) || len(results) != 1 ||
		!strings.Contains(results[0].ReturnCode.(string), "buffer mode cap exceeded") {
		t.Fatalf("buffer limit failure not reported: %#v", results)
	}
}

func TestExcelWriteQueryAndErrors(t *testing.T) {
	registry := NewRegistry()
	if err := registry.InitDatabases([]DatabaseConfig{
		{Name: "db", Driver: "sqlite", ConnectionString: filepath.Join(t.TempDir(), "data.db")},
	}); err != nil {
		t.Fatal(err)
	}
	defer registry.CloseDatabases()
	executor := NewExecutor(registry)
	path := filepath.Join(t.TempDir(), "nested", "report.xlsx")
	elem := ExcelWriteElement{
		ID: "write", File: path, Sheet: "Report", DBName: "db",
		Query: "SELECT CAST('hello' AS BLOB) AS bytes, 42 AS number",
	}
	var results []ScriptResult
	if executor.executeExcelWriteNode(context.Background(), elem, &results) {
		t.Fatalf("Excel write failed: %#v", results)
	}
	book, err := excelize.OpenFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for cell, want := range map[string]string{"A1": "bytes", "B1": "number", "A2": "hello", "B2": "42"} {
		got, err := book.GetCellValue("Report", cell)
		if err != nil || got != want {
			t.Errorf("%s = %q, %v; want %q", cell, got, err, want)
		}
	}
	if err := book.Close(); err != nil {
		t.Fatal(err)
	}
	results = nil
	if executor.executeExcelWriteNode(context.Background(), elem, &results) {
		t.Fatalf("writing existing workbook failed: %#v", results)
	}

	badFile := filepath.Join(t.TempDir(), "bad.xlsx")
	if err := os.WriteFile(badFile, []byte("not an Excel file"), 0600); err != nil {
		t.Fatal(err)
	}
	blockedDir := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(blockedDir, []byte("not a directory"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		elem ExcelWriteElement
		want string
	}{
		{"corrupt file", ExcelWriteElement{File: badFile}, "failed to open existing excel file"},
		{"invalid sheet", ExcelWriteElement{File: path, Sheet: "invalid/name"}, "failed to create sheet"},
		{"missing db", ExcelWriteElement{File: path, DBName: "missing", Query: "SELECT 1"}, "query error"},
		{"blocked directory", ExcelWriteElement{File: filepath.Join(blockedDir, "out.xlsx")}, "failed to create directory"},
		{"unsupported extension", ExcelWriteElement{File: filepath.Join(t.TempDir(), "out.invalid")}, "failed to save excel file"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var results []ScriptResult
			if !executor.executeExcelWriteNode(context.Background(), tc.elem, &results) || len(results) != 1 {
				t.Fatalf("expected error, got %#v", results)
			}
			if got, ok := results[0].ReturnCode.(string); !ok || !strings.Contains(got, tc.want) {
				t.Fatalf("error = %v, want %q", results[0].ReturnCode, tc.want)
			}
		})
	}
}

func TestDecodeTextBytesEncodings(t *testing.T) {
	for _, tc := range []struct {
		name string
		data []byte
		want string
	}{
		{"empty", nil, ""},
		{"utf8 bom", []byte{0xef, 0xbb, 0xbf, 'x'}, "x"},
		{"utf16 little endian", []byte{0xff, 0xfe, 'a', 0, 'b', 0}, "ab"},
		{"utf16 big endian", []byte{0xfe, 0xff, 0, 'a', 0, 'b'}, "ab"},
		{"little endian no bom", []byte{' ', 0, '<', 0, 'x', 0}, " <x"},
		{"big endian no bom", []byte{0, ' ', 0, '<', 0, 'x'}, " <x"},
		{"plain", []byte("hello"), "hello"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := DecodeTextBytes(tc.data); got != tc.want {
				t.Fatalf("DecodeTextBytes() = %q, want %q", got, tc.want)
			}
		})
	}
}
