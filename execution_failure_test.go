package flow

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestTransactionGroupDriverFailures(t *testing.T) {
	for _, stage := range []string{"begin", "commit"} {
		t.Run(stage, func(t *testing.T) {
			registry := NewRegistry()
			registry.dbRegistry["db"] = DBHandle{Conn: sql.OpenDB(&bulkTestConnector{stage: stage}), Driver: "sqlite"}
			defer registry.CloseDatabases()
			executor := NewExecutor(registry)
			results, err := executor.Execute(context.Background(), []PipelineNode{{
				Kind: NodeGroup, GroupID: "tx", DBName: "db", Transaction: true,
			}})
			if err == nil || len(results) != 1 || !strings.Contains(results[0].ReturnCode.(string), "failed to "+stage+" transaction") {
				t.Fatalf("transaction %s error = %v, %#v", stage, err, results)
			}
		})
	}
	registry := NewRegistry()
	db := sql.OpenDB(&bulkTestConnector{})
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	registry.dbRegistry["db"] = DBHandle{Conn: db, Driver: "sqlite"}
	executor := NewExecutor(registry)
	results, err := executor.Execute(context.Background(), []PipelineNode{{
		Kind: NodeGroup, GroupID: "session", DBName: "db", Session: true,
	}})
	if err == nil || len(results) != 1 || !strings.Contains(results[0].ReturnCode.(string), "failed to pin") {
		t.Fatalf("closed session error = %v, %#v", err, results)
	}
}

func TestYAMLNonFiniteNumberError(t *testing.T) {
	registry := NewRegistry()
	registry.SetVar("yaml", "number: .nan")
	executor := NewExecutor(registry)
	var results []ScriptResult
	if !executor.executeYAMLPathNode(context.Background(), YamlPathElement{Var: "yaml", Path: "$.number", OutputVar: "out"}, &results) ||
		len(results) != 1 || !strings.Contains(results[0].ReturnCode.(string), "failed to convert YAML to JSON") {
		t.Fatalf("non-finite YAML conversion error = %#v", results)
	}
}

func TestKVBulkMissingTarget(t *testing.T) {
	executor := NewExecutor(NewRegistry())
	var results []ScriptResult
	if !executor.executeKVBulkNode(context.Background(), KVBulkElement{DBName: "source", TargetDB: "missing"}, &results) ||
		len(results) != 1 || !strings.Contains(results[0].ReturnCode.(string), "target key-value database") {
		t.Fatalf("missing bulk target error = %#v", results)
	}
}

func TestHTTPTruncatedResponseError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "10")
		_, _ = fmt.Fprint(w, "short")
	}))
	defer server.Close()
	executor := NewExecutor(NewRegistry())
	var results []ScriptResult
	if !executor.executeHTTPClientNode(context.Background(), HTTPClientElement{URL: server.URL}, &results) ||
		len(results) != 1 || !strings.Contains(results[0].ReturnCode.(string), "failed to read response body") {
		t.Fatalf("truncated HTTP response error = %#v", results)
	}
}
