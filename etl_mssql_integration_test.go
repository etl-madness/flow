package flow

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
)

func TestStreamETLMSSQLBulkExecution(t *testing.T) {
	for _, tc := range []struct {
		stage string
		want  string
	}{
		{"success", ""},
		{"row", "batch bulk copy error"},
		{"commit", "batch bulk copy error"},
	} {
		t.Run(tc.stage, func(t *testing.T) {
			registry := NewRegistry()
			if err := registry.InitDatabases([]DatabaseConfig{
				{Name: "source", Driver: "sqlite", ConnectionString: filepath.Join(t.TempDir(), "source.db")},
			}); err != nil {
				t.Fatal(err)
			}

			registry.dbRegistry["destination"] = DBHandle{Conn: sql.OpenDB(&bulkTestConnector{stage: tc.stage}), Driver: "mssql"}
			defer registry.CloseDatabases()
			count, err := StreamETL(context.Background(), registry, "source",
				"SELECT 'one' AS name UNION ALL SELECT 'two'", "destination", "items", ETLOptions{BatchSize: 1})
			if tc.want == "" {
				if err != nil || count != 2 {
					t.Fatalf("StreamETL() = %d, %v; want 2 rows", count, err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("StreamETL() error = %v; want %q", err, tc.want)
			}
		})
	}
}

func TestStreamETLPrepareAndInsertFailures(t *testing.T) {
	registry := NewRegistry()
	if err := registry.InitDatabases([]DatabaseConfig{
		{Name: "source", Driver: "sqlite", ConnectionString: filepath.Join(t.TempDir(), "source.db")},
	}); err != nil {
		t.Fatal(err)
	}
	registry.dbRegistry["destination"] = DBHandle{Conn: sql.OpenDB(&bulkTestConnector{stage: "prepare"}), Driver: "sqlite"}
	defer registry.CloseDatabases()
	count, err := StreamETL(context.Background(), registry, "source", "SELECT 'one' AS name", "destination", "items", ETLOptions{BatchSize: 1})
	if count != 0 || err == nil || !strings.Contains(err.Error(), "batch insert error") {
		t.Fatalf("StreamETL() = %d, %v; want insert error", count, err)
	}
}
