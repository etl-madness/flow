package flow

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSQLNodesQueriesAndBulk(t *testing.T) {
	registry := NewRegistry()
	if err := registry.InitDatabases([]DatabaseConfig{
		{Name: "db", Driver: "sqlite", ConnectionString: filepath.Join(t.TempDir(), "test.db")},
		{Name: "destination", Driver: "sqlite", ConnectionString: filepath.Join(t.TempDir(), "destination.db")},
	}); err != nil {
		t.Fatal(err)
	}
	defer registry.CloseDatabases()
	executor := NewExecutor(registry)
	registry.SetVar("query", "SELECT 42")

	for _, tc := range []struct {
		name string
		elem SQLElement
		fail bool
		want string
	}{
		{name: "driver variable", elem: SQLElement{ID: "variable", DBName: "db", VarName: "query", OutputVar: "out", Timeout: "5s"}, want: "42"},
		{name: "empty database", elem: SQLElement{ID: "missing", Code: "SELECT 1"}, fail: true},
		{name: "invalid query", elem: SQLElement{ID: "invalid", DBName: "db", Code: "SELECT * FROM missing_table"}, fail: true},
		{name: "create", elem: SQLElement{ID: "create", DBName: "db", Code: "CREATE TABLE rows (id INTEGER, name TEXT)"}},
		{name: "insert", elem: SQLElement{ID: "insert", DBName: "db", Code: "INSERT INTO rows VALUES (1, 'one'), (2, 'two')"}},
		{name: "select", elem: SQLElement{ID: "select", DBName: "db", Code: "SELECT name FROM rows ORDER BY id", OutputVar: "out"}, want: "name\none\ntwo"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var results []ScriptResult
			failed := executor.executeSQLNode(context.Background(), tc.elem, &results)
			if failed != tc.fail || len(results) != 1 {
				t.Fatalf("executeSQLNode() failed=%t results=%#v", failed, results)
			}
			if tc.want != "" && registry.GetVarString("out") != tc.want {
				t.Fatalf("SQL output = %q, want %q", registry.GetVarString("out"), tc.want)
			}
		})
	}
	if _, _, err := executor.executeSQLQuery(context.Background(), "missing", "UPDATE x SET y=1"); err == nil {
		t.Fatal("DML against missing DB unexpectedly succeeded")
	}

	var results []ScriptResult
	if executor.executeSQLNode(context.Background(), SQLElement{ID: "create-target", DBName: "destination", Code: "CREATE TABLE target (id INTEGER, name TEXT)"}, &results) {
		t.Fatalf("creating target table failed: %#v", results)
	}
	registry.SetVar("bulk-query", "SELECT id, name FROM rows ORDER BY id")
	results = nil
	if failed := executor.executeSQLBulkNode(context.Background(), SQLBulkElement{
		ID: "copy", DBName: "db", TargetDB: "destination", TargetTable: "target", VarName: "bulk-query", BatchSize: 1, OutputVar: "copied", Timeout: "5s",
	}, &results); failed {
		t.Fatalf("SQL bulk copy failed: %#v", results)
	}
	if registry.GetVarString("copied") != "2" {
		t.Fatalf("bulk copied = %q, want 2", registry.GetVarString("copied"))
	}
	results = nil
	if failed := executor.executeSQLBulkNode(context.Background(), SQLBulkElement{
		ID: "bad-copy", DBName: "missing", TargetTable: "target", Code: "SELECT 1",
	}, &results); !failed {
		t.Fatal("bulk copy from missing DB unexpectedly succeeded")
	}
}

func TestControlFlowExecutionAndFailures(t *testing.T) {
	registry := NewRegistry()
	if err := registry.InitDatabases([]DatabaseConfig{
		{Name: "db", Driver: "sqlite", ConnectionString: filepath.Join(t.TempDir(), "control.db")},
	}); err != nil {
		t.Fatal(err)
	}
	defer registry.CloseDatabases()
	executor := NewExecutor(registry)
	registry.SetVar("condition", "yes")

	assertNode := func(id, value string) PipelineNode {
		return PipelineNode{Kind: NodeAssert, Assert: &AssertElement{ID: id, Var: "condition", Equals: value}}
	}
	nodes := []PipelineNode{
		{Kind: NodeIf, IfVar: "condition", IfEquals: "yes", Children: []PipelineNode{assertNode("then", "yes")}, ElseNodes: []PipelineNode{assertNode("not-then", "no")}},
		{Kind: NodeIf, IfVar: "condition", IfEquals: "no", Children: []PipelineNode{assertNode("not-else", "no")}, ElseNodes: []PipelineNode{assertNode("else", "yes")}},
		{Kind: NodeGroup, IfVar: "condition", IfEquals: "no", Children: []PipelineNode{assertNode("skipped", "no")}},
		{Kind: NodeGroup, GroupID: "session", DBName: "db", Session: true, Timeout: "5s", Children: []PipelineNode{
			{Kind: NodeGroup, GroupID: "nested-session", DBName: "db", Session: true, Children: []PipelineNode{assertNode("nested", "yes")}},
		}},
	}
	results, err := executor.Execute(context.Background(), nodes)
	if err != nil {
		t.Fatalf("control flow failed: %v, %#v", err, results)
	}
	if len(results) != 3 || results[0].ScriptID != "then" || results[1].ScriptID != "else" || results[2].ScriptID != "nested" {
		t.Fatalf("unexpected control-flow results: %#v", results)
	}

	for _, node := range []PipelineNode{
		{Kind: NodeGroup, GroupID: "missing-session-db", Session: true},
		{Kind: NodeGroup, GroupID: "unknown-session-db", Session: true, DBName: "missing"},
		{Kind: NodeGroup, GroupID: "missing-tx-db", Transaction: true},
		{Kind: NodeGroup, GroupID: "unknown-tx-db", Transaction: true, DBName: "missing"},
		{Kind: NodeWhile, GroupID: "limit", IfVar: "condition", IfEquals: "yes", MaxIterations: 2},
		{Kind: NodeWhile, GroupID: "child-error", IfVar: "condition", IfEquals: "yes", Children: []PipelineNode{assertNode("fail", "no")}},
	} {
		results, err := executor.Execute(context.Background(), []PipelineNode{node})
		if err == nil || len(results) == 0 {
			t.Errorf("expected failure for %s: err=%v results=%#v", node.GroupID, err, results)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var canceledResults []ScriptResult
	if !executor.executeWhileNode(ctx, PipelineNode{GroupID: "cancelled", IfVar: "condition", IfEquals: "yes"}, &canceledResults) ||
		len(canceledResults) != 1 || !strings.Contains(canceledResults[0].ReturnCode.(string), "canceled") {
		t.Fatalf("while cancellation result = %#v", canceledResults)
	}
}

func TestForEachDriverAndErrorPaths(t *testing.T) {
	registry := NewRegistry()
	if err := registry.InitDatabases([]DatabaseConfig{
		{Name: "db", Driver: "sqlite", ConnectionString: filepath.Join(t.TempDir(), "foreach.db")},
	}); err != nil {
		t.Fatal(err)
	}
	defer registry.CloseDatabases()
	executor := NewExecutor(registry)
	registry.SetVar("query", "SELECT NULL AS value UNION ALL SELECT 'ok'")
	node := PipelineNode{
		Kind: NodeForEach, GroupID: "rows", Buffer: true,
		ForEachScript: &ScriptItem{ID: "driver", Language: "sql", DBName: "db", VarName: "query"},
	}
	var results []ScriptResult
	if executor.executeForEachNode(context.Background(), node, &results) || len(results) != 1 ||
		!strings.Contains(results[0].ResultsString, "2 iteration") {
		t.Fatalf("buffered foreach failed: %#v", results)
	}
	if registry.GetVarString("value") != "ok" {
		t.Fatalf("last foreach value = %q", registry.GetVarString("value"))
	}

	node.ForEachScript.DBName = "missing"
	results = nil
	if !executor.executeForEachNode(context.Background(), node, &results) || len(results) != 1 {
		t.Fatalf("foreach missing DB unexpectedly succeeded: %#v", results)
	}
	if executor.executeForEachNode(context.Background(), PipelineNode{}, &results) {
		t.Fatal("foreach without a driver unexpectedly failed")
	}
}

func TestDatabasePoolWorkloads(t *testing.T) {
	for _, tc := range []struct {
		workload string
		open     int
		idle     int
		lifetime time.Duration
		idleTime time.Duration
	}{
		{"bulk", 50, 25, 30 * time.Minute, 10 * time.Minute},
		{"analytics", 15, 8, 30 * time.Minute, 10 * time.Minute},
		{"batch", 20, 10, 15 * time.Minute, 5 * time.Minute},
		{"oltp", 25, 10, 5 * time.Minute, 2 * time.Minute},
	} {
		open, idle, lifetime, idleTime := resolveDatabasePoolSettings(DatabaseConfig{Workload: tc.workload})
		if open != tc.open || idle != tc.idle || lifetime != tc.lifetime || idleTime != tc.idleTime {
			t.Errorf("%s pool settings = %d, %d, %v, %v", tc.workload, open, idle, lifetime, idleTime)
		}
	}
	open, idle, lifetime, idleTime := resolveDatabasePoolSettings(DatabaseConfig{
		MaxOpenConns: 2, MaxIdleConns: 4, ConnMaxLifetime: time.Second, ConnMaxIdleTime: 2 * time.Second,
	})
	if open != 2 || idle != 2 || lifetime != time.Second || idleTime != time.Second {
		t.Fatalf("pool clamping = %d, %d, %v, %v", open, idle, lifetime, idleTime)
	}
}

func TestScriptFileAndVariableExecution(t *testing.T) {
	registry := NewRegistry()
	executor := NewExecutor(registry)
	registry.SetVar("script", `print("variable")`)
	scriptPath := filepath.Join(t.TempDir(), "script.lua")
	if err := os.WriteFile(scriptPath, []byte(`print("file")`), 0600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		elem ScriptItem
		want string
		fail bool
	}{
		{elem: ScriptItem{ID: "file", Language: "lua", FilePath: scriptPath, OutputVar: "out"}, want: "file"},
		{elem: ScriptItem{ID: "variable", Language: "lua", VarName: "script", Code: `print("wrong")`, OutputVar: "out"}, want: "variable"},
		{elem: ScriptItem{ID: "missing", Language: "lua", FilePath: filepath.Join(t.TempDir(), "missing")}, fail: true},
	} {
		var results []ScriptResult
		if failed := executor.executeScriptNode(context.Background(), tc.elem, &results); failed != tc.fail {
			t.Errorf("script %q failed=%t results=%#v", tc.elem.ID, failed, results)
		}
		if tc.want != "" && registry.GetVarString("out") != tc.want {
			t.Errorf("script %q output = %q, want %q", tc.elem.ID, registry.GetVarString("out"), tc.want)
		}
	}
}
