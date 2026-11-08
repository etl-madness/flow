package flow

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/traefik/yaegi/interp"
)

func TestGoInterpreterStreamETLUsesActiveTargets(t *testing.T) {
	registry := NewRegistry()
	if err := registry.InitDatabases([]DatabaseConfig{
		{Name: "source", Driver: "sqlite", ConnectionString: filepath.Join(t.TempDir(), "source.db")},
		{Name: "destination", Driver: "sqlite", ConnectionString: filepath.Join(t.TempDir(), "destination.db")},
	}); err != nil {
		t.Fatal(err)
	}
	defer registry.CloseDatabases()
	source, err := registry.GetDB("source")
	if err != nil {
		t.Fatal(err)
	}
	destination, err := registry.GetDB("destination")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.Exec("CREATE TABLE source (value TEXT); INSERT INTO source VALUES ('one'), ('two')"); err != nil {
		t.Fatal(err)
	}
	if _, err := destination.Exec("CREATE TABLE target (value TEXT)"); err != nil {
		t.Fatal(err)
	}
	executor := NewExecutor(registry)
	interpreter, err := executor.getGoInterpreter(context.Background(), ScriptItem{}, interp.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := interpreter.Eval(`import "host/db"`); err != nil {
		t.Fatal(err)
	}
	value, err := interpreter.Eval("db.StreamETL")
	if err != nil {
		t.Fatal(err)
	}
	stream, ok := value.Interface().(func(string, string, string, string, ETLOptions) (int64, error))
	if !ok {
		t.Fatalf("unexpected StreamETL type %T", value.Interface())
	}
	run := func() {
		t.Helper()
		count, err := stream("source", "SELECT value FROM source", "destination", "target", ETLOptions{BatchSize: 1})
		if err != nil || count != 2 {
			t.Fatalf("StreamETL() = %d, %v; want 2 rows", count, err)
		}
	}
	run()

	srcConn, err := source.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer srcConn.Close()
	dstConn, err := destination.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer dstConn.Close()
	executor.pushConn("source", srcConn)
	executor.pushConn("destination", dstConn)
	run()
	executor.popConn("source")
	executor.popConn("destination")

	srcTx, err := srcConn.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	dstTx, err := dstConn.BeginTx(context.Background(), nil)
	if err != nil {
		srcTx.Rollback()
		t.Fatal(err)
	}
	executor.pushTx("source", srcTx)
	executor.pushTx("destination", dstTx)
	run()
	executor.popTx("source")
	executor.popTx("destination")
	if err := srcTx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := dstTx.Commit(); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := dstConn.QueryRowContext(context.Background(), "SELECT count(*) FROM target").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 6 {
		t.Fatalf("target count = %d, want 6", count)
	}
}
