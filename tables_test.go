package flow

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDefaultTableConfigAndResolve(t *testing.T) {
	cfg := DefaultTableConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("DefaultTableConfig.Validate: %v", err)
	}
	if got := cfg.Resolve("sqlserver", cfg.PipelineRuns); got != "dbo.pipeline_runs" {
		t.Fatalf("sqlserver resolve = %q, want dbo.pipeline_runs", got)
	}
	if got := cfg.Resolve("sqlite", cfg.PipelineRuns); got != "pipeline_runs" {
		t.Fatalf("sqlite resolve = %q, want pipeline_runs", got)
	}
	if got := cfg.Resolve("mysql", cfg.PipelineRuns); got != "pipeline_runs" {
		t.Fatalf("mysql resolve = %q, want pipeline_runs", got)
	}
	if got := BaseTableName("etl_flow.my_runs"); got != "my_runs" {
		t.Fatalf("BaseTableName = %q, want my_runs", got)
	}
	if got := BaseTableName("plain_table"); got != "plain_table" {
		t.Fatalf("BaseTableName = %q, want plain_table", got)
	}
	m := cfg.Map("")
	if m["pipeline_runs"] != "dbo.pipeline_runs" {
		t.Fatalf("Map('') pipeline_runs = %q", m["pipeline_runs"])
	}
	mSQLite := cfg.Map("sqlite")
	if mSQLite["api_users"] != "flow_api_users" {
		t.Fatalf("Map('sqlite') api_users = %q", mSQLite["api_users"])
	}
}

func TestLoadTableConfigJSONAndYAML(t *testing.T) {
	dir := t.TempDir()

	// JSON override with custom schema & per-table explicit schema
	jsonPath := filepath.Join(dir, "tables.json")
	jsonBody := `{
  "schema": "etl_custom",
  "pipeline_runs": "custom_runs",
  "api_events": "audit_schema.api_log"
}`
	if err := os.WriteFile(jsonPath, []byte(jsonBody), 0644); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadTableConfigFile(jsonPath)
	if err != nil {
		t.Fatalf("LoadTableConfigFile(json): %v", err)
	}
	if got := cfg.Resolve("sqlserver", cfg.PipelineRuns); got != "etl_custom.custom_runs" {
		t.Fatalf("sqlserver custom_runs = %q", got)
	}
	if got := cfg.Resolve("postgres", cfg.APIAuditEvents); got != "audit_schema.api_log" {
		t.Fatalf("postgres api_log = %q", got)
	}
	if got := cfg.Resolve("sqlite", cfg.APIAuditEvents); got != "api_log" {
		t.Fatalf("sqlite api_log = %q", got)
	}
	if got := cfg.Resolve("mysql", cfg.APIAuditEvents); got != "audit_schema.api_log" {
		t.Fatalf("mysql api_log = %q", got)
	}

	// YAML override with explicit empty schema
	yamlPath := filepath.Join(dir, "tables.yaml")
	yamlBody := "schema: \"\"\npipeline_events: my_events\n"
	if err := os.WriteFile(yamlPath, []byte(yamlBody), 0644); err != nil {
		t.Fatal(err)
	}
	yCfg, err := LoadTableConfigFile(yamlPath)
	if err != nil {
		t.Fatalf("LoadTableConfigFile(yaml): %v", err)
	}
	if yCfg.Schema != "" || yCfg.SchemaPrefix() != "" {
		t.Fatalf("expected empty schema, got %q", yCfg.Schema)
	}
	if got := yCfg.Resolve("sqlserver", yCfg.PipelineEvents); got != "my_events" {
		t.Fatalf("unqualified sqlserver = %q, want my_events", got)
	}

	// Empty bytes fall back to defaults
	if emptyCfg, err := ParseTableConfigBytes([]byte("  ")); err != nil || emptyCfg.Schema != "dbo" {
		t.Fatalf("empty bytes: got (%+v, %v)", emptyCfg, err)
	}
}

func TestTableConfigValidationRejectsInjection(t *testing.T) {
	badInputs := []string{
		`{"schema": "dbo; DROP TABLE users;--"}`,
		`{"pipeline_runs": "runs; DELETE FROM x"}`,
		`{"pipeline_runs": "a.b.c"}`,
		`{"pipeline_runs": "123bad"}`,
		`{"pipeline_runs": ""}`,
		`{invalid json`,
		":::bad yaml:::\n\t[",
	}
	for _, raw := range badInputs {
		if _, err := ParseTableConfigBytes([]byte(raw)); err == nil {
			t.Errorf("expected error for invalid input %q", raw)
		}
	}
	if _, err := LoadTableConfigFile(filepath.Join(t.TempDir(), "missing.json")); err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestTableConfigRenderDDL(t *testing.T) {
	cfg, err := ParseTableConfigBytes([]byte(`{
		"schema": "etl_flow",
		"pipeline_content": "my_pipelines",
		"pipeline_runs": "my_runs",
		"pipeline_acl": "my_acl"
	}`))
	if err != nil {
		t.Fatalf("ParseTableConfigBytes: %v", err)
	}

	for _, dialect := range []string{"sqlserver", "postgres", "mysql", "sqlite", "oracle"} {
		ddl, err := cfg.RenderDDL(dialect)
		if err != nil {
			t.Fatalf("RenderDDL(%s): %v", dialect, err)
		}
		if !strings.Contains(ddl, "my_pipelines") || !strings.Contains(ddl, "my_runs") || !strings.Contains(ddl, "my_acl") {
			t.Errorf("RenderDDL(%s) missing custom table names", dialect)
		}
		if (dialect == "sqlserver" || dialect == "postgres") && !strings.Contains(ddl, "etl_flow") {
			t.Errorf("RenderDDL(%s) missing custom schema etl_flow", dialect)
		}
	}

	if _, err := cfg.RenderDDL("unsupported"); err == nil {
		t.Fatal("expected error for unsupported dialect")
	}

	// Execute rendered SQLite DDL against an in-memory SQLite DB to prove valid syntax!
	sqliteDDL, err := cfg.RenderDDL("sqlite")
	if err != nil {
		t.Fatalf("RenderDDL(sqlite): %v", err)
	}
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(sqliteDDL); err != nil {
		t.Fatalf("executing rendered SQLite DDL failed: %v", err)
	}
}
