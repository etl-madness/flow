package flow

import (
	"context"
	"path/filepath"
	"testing"
)

func TestNestedTransactionCommitAndRollback(t *testing.T) {
	registry := NewRegistry()
	if err := registry.InitDatabases([]DatabaseConfig{
		{Name: "db", Driver: "sqlite", ConnectionString: filepath.Join(t.TempDir(), "transactions.db")},
	}); err != nil {
		t.Fatal(err)
	}
	defer registry.CloseDatabases()
	db, err := registry.GetDB("db")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE items (id INTEGER)"); err != nil {
		t.Fatal(err)
	}
	executor := NewExecutor(registry)
	insert := func(id string) PipelineNode {
		return PipelineNode{Kind: NodeSQL, SQL: &SQLElement{ID: id, DBName: "db", Code: "INSERT INTO items VALUES (" + id + ")"}}
	}
	group := func(children ...PipelineNode) PipelineNode {
		return PipelineNode{Kind: NodeGroup, GroupID: "tx", DBName: "db", Transaction: true, Children: children}
	}
	_, err = executor.Execute(context.Background(), []PipelineNode{group(insert("1"), group(insert("2")))})
	if err != nil {
		t.Fatalf("nested commit failed: %v", err)
	}
	_, err = executor.Execute(context.Background(), []PipelineNode{group(insert("3"), group(insert("4"),
		PipelineNode{Kind: NodeAssert, Assert: &AssertElement{ID: "failure", Var: "unset", Equals: "expected"}},
	))})
	if err == nil {
		t.Fatal("failed nested transaction unexpectedly succeeded")
	}
	var count, sum int
	if err := db.QueryRow("SELECT count(*), sum(id) FROM items").Scan(&count, &sum); err != nil {
		t.Fatal(err)
	}
	if count != 2 || sum != 3 {
		t.Fatalf("transaction results = count %d, sum %d; want 2 and 3", count, sum)
	}
}
