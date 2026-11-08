package flow

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xuri/excelize/v2"
)

func TestFileSaveAndExcelReadErrors(t *testing.T) {
	registry := NewRegistry()
	executor := NewExecutor(registry)
	dir := t.TempDir()
	blockingFile := filepath.Join(dir, "blocker")
	if err := os.WriteFile(blockingFile, []byte("block"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		elem FileSaveElement
		want string
	}{
		{"directory blocked", FileSaveElement{File: filepath.Join(blockingFile, "out")}, "failed to create directory structure"},
		{"destination is directory", FileSaveElement{File: dir}, "failed to open destination file"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var results []ScriptResult
			if !executor.executeFileSaveNode(context.Background(), tc.elem, &results) || len(results) != 1 ||
				!strings.Contains(results[0].ReturnCode.(string), tc.want) {
				t.Fatalf("file save error = %#v; want %q", results, tc.want)
			}
		})
	}

	book := excelize.NewFile()
	if err := book.SetCellValue("Sheet1", "A1", "first"); err != nil {
		t.Fatal(err)
	}
	if err := book.SetCellValue("Sheet1", "B1", "second"); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "book.xlsx")
	if err := book.SaveAs(path); err != nil {
		t.Fatal(err)
	}
	if err := book.Close(); err != nil {
		t.Fatal(err)
	}
	noHeader := false
	var results []ScriptResult
	if executor.executeExcelReadNode(context.Background(), ExcelReadElement{
		File: path, Header: &noHeader, OutputVar: "rows",
	}, &results) {
		t.Fatalf("reading workbook failed: %#v", results)
	}
	if got := registry.GetVarString("rows"); got != `[{"col1":"first","col2":"second"}]` {
		t.Fatalf("workbook data = %s", got)
	}
	results = nil
	if !executor.executeExcelReadNode(context.Background(), ExcelReadElement{
		File: path, Sheet: "missing", OutputVar: "rows",
	}, &results) || len(results) != 1 || !strings.Contains(results[0].ReturnCode.(string), "failed to read sheet") {
		t.Fatalf("missing sheet error = %#v", results)
	}
}
