package flow

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
)

func TestKVBulkBackendsMultipleColumnsAndFailures(t *testing.T) {
	redisAddr, stopRedis := startMockRedisServer(t)
	defer stopRedis()
	etcdAddr, stopEtcd := startMockEtcdServer(t)
	defer stopEtcd()

	registry := NewRegistry()
	configs := []DatabaseConfig{
		{Name: "source", Driver: "sqlite", ConnectionString: filepath.Join(t.TempDir(), "source.db")},
		{Name: "bolt", Driver: "bbolt", ConnectionString: filepath.Join(t.TempDir(), "store.db")},
		{Name: "badger", Driver: "badger", ConnectionString: filepath.Join(t.TempDir(), "store")},
		{Name: "redis", Driver: "redis", ConnectionString: redisAddr},
		{Name: "etcd", Driver: "etcd", ConnectionString: etcdAddr},
	}
	if err := registry.InitDatabases(configs); err != nil {
		t.Fatal(err)
	}
	registry.dbRegistry["failing"] = DBHandle{Conn: sql.OpenDB(failingRowsConnector{}), Driver: "test"}
	defer registry.CloseDatabases()
	executor := NewExecutor(registry)

	for _, backend := range []string{"bolt", "badger", "redis", "etcd"} {
		t.Run(backend, func(t *testing.T) {
			node := KVBulkElement{
				ID: "bulk-" + backend, DBName: "source", TargetDB: backend, TargetBucket: "items",
				BatchSize: 1, OutputVar: "copied",
				Code: "SELECT 'key1', 'value1', 17 UNION ALL SELECT 'key2', 'value2', 18",
			}
			var results []ScriptResult
			if failed := executor.executeKVBulkNode(context.Background(), node, &results); failed {
				t.Fatalf("bulk write failed: %#v", results)
			}
			if got := registry.GetVarString("copied"); got != "2" {
				t.Fatalf("bulk wrote %q rows, want 2", got)
			}
			for _, key := range []string{"key1", "key2"} {
				results = nil
				if failed := executor.executeKVNode(context.Background(), KVElement{
					ID: "get", DBName: backend, Bucket: "items", Op: "get", Key: key, OutputVar: "output",
				}, &results); failed {
					t.Fatalf("read back %q failed: %#v", key, results)
				}
				if got := registry.GetVarString("output"); got != "value"+key[len(key)-1:] {
					t.Errorf("value for %q = %q", key, got)
				}
			}

			for _, tc := range []struct {
				name  string
				src   string
				query string
				want  string
			}{
				{name: "unknown-source", src: "missing", query: "SELECT 1, 2", want: "source database error"},
				{name: "invalid-query", src: "source", query: "SELECT * FROM missing_table", want: "source query error"},
				{name: "one-column", src: "source", query: "SELECT 1", want: "at least 2 columns"},
				{name: "null-value", src: "source", query: "SELECT 'key', NULL", want: "row scan error"},
				{name: "iteration-error", src: "failing", query: "SELECT", want: "injected row iteration failure"},
			} {
				t.Run(tc.name, func(t *testing.T) {
					results = nil
					badNode := node
					badNode.DBName = tc.src
					badNode.Code = tc.query
					if !executor.executeKVBulkNode(context.Background(), badNode, &results) || len(results) != 1 {
						t.Fatalf("expected failure for %s: %#v", tc.name, results)
					}
					if result, ok := results[0].ReturnCode.(string); !ok || !strings.Contains(result, tc.want) {
						t.Errorf("error = %v, want %q", results[0].ReturnCode, tc.want)
					}
				})
			}

			for _, tc := range []struct {
				code string
				want string
			}{
				{"set implicit-key implicit-value", "1"},
				{"get implicit-key", "implicit-value"},
				{"delete implicit-key", "1"},
			} {
				results = nil
				if executor.executeKVNode(context.Background(), KVElement{
					DBName: backend, Code: tc.code, OutputVar: "code-output",
				}, &results) || registry.GetVarString("code-output") != tc.want {
					t.Errorf("KV command %q output = %q, results = %#v", tc.code, registry.GetVarString("code-output"), results)
				}
			}
			results = nil
			if !executor.executeKVNode(context.Background(), KVElement{DBName: backend, Op: "invalid"}, &results) ||
				len(results) != 1 || !strings.Contains(results[0].ReturnCode.(string), "unsupported") {
				t.Fatalf("invalid KV operation was not reported: %#v", results)
			}
			if backend == "bolt" || backend == "badger" {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				results = nil
				if !executor.executeKVNode(ctx, KVElement{DBName: backend, Bucket: "items", Op: "scan"}, &results) ||
					len(results) != 1 || !strings.Contains(results[0].ReturnCode.(string), "canceled") {
					t.Fatalf("canceled KV scan was not reported: %#v", results)
				}
			}
		})
	}
}

func TestKVHandlerMissingDatabase(t *testing.T) {
	executor := NewExecutor(NewRegistry())
	for name, execute := range map[string]func(context.Context, KVElement, *[]ScriptResult) bool{
		"bolt":   executor.executeBoltKVNode,
		"badger": executor.executeBadgerKVNode,
		"redis":  executor.executeRedisKVNode,
		"etcd":   executor.executeEtcdKVNode,
	} {
		t.Run(name, func(t *testing.T) {
			var results []ScriptResult
			if !execute(context.Background(), KVElement{DBName: "missing"}, &results) || len(results) != 1 ||
				!strings.Contains(results[0].ReturnCode.(string), "not registered") {
				t.Fatalf("missing database was not reported: %#v", results)
			}
		})
	}
	for _, key := range []string{string(badgerKey("", "key")), redisKey("", "key"), etcdKey("", "key")} {
		if key != "default:key" {
			t.Errorf("default namespaced key = %q", key)
		}
	}
}
