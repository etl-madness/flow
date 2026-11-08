package flow

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

var errTestRows = errors.New("injected row iteration failure")

type failingRowsConnector struct{}

func (failingRowsConnector) Connect(context.Context) (driver.Conn, error) {
	return failingRowsConn{}, nil
}
func (failingRowsConnector) Driver() driver.Driver { return bulkTestDriver{} }

type failingRowsConn struct{}

func (failingRowsConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepare")
}
func (failingRowsConn) Close() error              { return nil }
func (failingRowsConn) Begin() (driver.Tx, error) { return nil, errors.New("unexpected transaction") }
func (failingRowsConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return &failingRows{}, nil
}

type failingRows struct {
	called bool
}

func (*failingRows) Columns() []string { return []string{"key", "value"} }
func (*failingRows) Close() error      { return nil }
func (r *failingRows) Next(dest []driver.Value) error {
	if !r.called {
		r.called = true
		dest[0], dest[1] = "key", "value"
		return nil
	}
	return errTestRows
}

func TestRowIterationFailuresAreReported(t *testing.T) {
	registry := NewRegistry()
	if err := registry.InitDatabases([]DatabaseConfig{
		{Name: "destination", Driver: "sqlite", ConnectionString: filepath.Join(t.TempDir(), "dest.db")},
	}); err != nil {
		t.Fatal(err)
	}
	registry.dbRegistry["source"] = DBHandle{Conn: sql.OpenDB(failingRowsConnector{}), Driver: "test"}
	defer registry.CloseDatabases()
	executor := NewExecutor(registry)

	for _, buffer := range []bool{false, true} {
		node := PipelineNode{Kind: NodeForEach, GroupID: "loop", Buffer: buffer,
			ForEachScript: &ScriptItem{ID: "source", Language: "sql", DBName: "source", Code: "SELECT"}}
		var results []ScriptResult
		if !executor.executeForEachNode(context.Background(), node, &results) || len(results) != 1 ||
			!strings.Contains(results[0].ReturnCode.(string), errTestRows.Error()) {
			t.Errorf("foreach buffer=%t failed to report rows error: %#v", buffer, results)
		}
	}
	var results []ScriptResult
	if !executor.executeExcelWriteNode(context.Background(), ExcelWriteElement{
		ID: "excel", DBName: "source", Query: "SELECT", File: filepath.Join(t.TempDir(), "out.xlsx"),
	}, &results) || len(results) != 1 || !strings.Contains(results[0].ReturnCode.(string), errTestRows.Error()) {
		t.Errorf("Excel write failed to report rows error: %#v", results)
	}
	count, err := StreamETL(context.Background(), registry, "source", "SELECT", "destination", "unused", ETLOptions{BatchSize: 10})
	if count != 0 || !errors.Is(err, errTestRows) {
		t.Errorf("StreamETL() = %d, %v; want iteration error", count, err)
	}
}
