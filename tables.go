package flow

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// Package-level default schema and table names.
// These are declared as vars (not consts) so enterprise builds can override them
// at compile time using Go linker flags, for example:
//
//	go build -ldflags="-X github.com/etl-madness/flow.DefaultSchema=etl_flow" .
var (
	DefaultSchema = "dbo"

	DefaultTablePipelineContent      = "flow_pipeline_content"
	DefaultTableOptionsContent       = "flow_options_content"
	DefaultTableConfigContent        = "flow_config_content"
	DefaultTablePipelineRuns         = "pipeline_runs"
	DefaultTablePipelineEvents       = "pipeline_events"
	DefaultTableAPIAuditEvents       = "flow_api_events"
	DefaultTableAPIUsers             = "flow_api_users"
	DefaultTableAPIGroups            = "flow_api_groups"
	DefaultTableAPIUserGroups        = "flow_api_user_groups"
	DefaultTablePipelineGroups       = "flow_pipeline_groups"
	DefaultTablePipelineGroupMembers = "flow_pipeline_group_members"
	DefaultTablePipelineACL          = "flow_pipeline_acl"
)

// TableConfig holds the database schema and table names used across go-flow,
// db_importer, builder, and go-flowAPI. Omitted fields inherit the build-time
// Default* values.
type TableConfig struct {
	Schema               string `json:"schema,omitempty" yaml:"schema,omitempty"`
	PipelineContent      string `json:"pipeline_content,omitempty" yaml:"pipeline_content,omitempty"`
	OptionsContent       string `json:"options_content,omitempty" yaml:"options_content,omitempty"`
	ConfigContent        string `json:"config_content,omitempty" yaml:"config_content,omitempty"`
	PipelineRuns         string `json:"pipeline_runs,omitempty" yaml:"pipeline_runs,omitempty"`
	PipelineEvents       string `json:"pipeline_events,omitempty" yaml:"pipeline_events,omitempty"`
	APIAuditEvents       string `json:"api_events,omitempty" yaml:"api_events,omitempty"`
	APIUsers             string `json:"api_users,omitempty" yaml:"api_users,omitempty"`
	APIGroups            string `json:"api_groups,omitempty" yaml:"api_groups,omitempty"`
	APIUserGroups        string `json:"api_user_groups,omitempty" yaml:"api_user_groups,omitempty"`
	PipelineGroups       string `json:"pipeline_groups,omitempty" yaml:"pipeline_groups,omitempty"`
	PipelineGroupMembers string `json:"pipeline_group_members,omitempty" yaml:"pipeline_group_members,omitempty"`
	PipelineACL          string `json:"pipeline_acl,omitempty" yaml:"pipeline_acl,omitempty"`
}

// DefaultTableConfig returns a TableConfig populated from the build-time Default* vars.
func DefaultTableConfig() TableConfig {
	return TableConfig{
		Schema:               strings.TrimSuffix(strings.TrimSpace(DefaultSchema), "."),
		PipelineContent:      strings.TrimSpace(DefaultTablePipelineContent),
		OptionsContent:       strings.TrimSpace(DefaultTableOptionsContent),
		ConfigContent:        strings.TrimSpace(DefaultTableConfigContent),
		PipelineRuns:         strings.TrimSpace(DefaultTablePipelineRuns),
		PipelineEvents:       strings.TrimSpace(DefaultTablePipelineEvents),
		APIAuditEvents:       strings.TrimSpace(DefaultTableAPIAuditEvents),
		APIUsers:             strings.TrimSpace(DefaultTableAPIUsers),
		APIGroups:            strings.TrimSpace(DefaultTableAPIGroups),
		APIUserGroups:        strings.TrimSpace(DefaultTableAPIUserGroups),
		PipelineGroups:       strings.TrimSpace(DefaultTablePipelineGroups),
		PipelineGroupMembers: strings.TrimSpace(DefaultTablePipelineGroupMembers),
		PipelineACL:          strings.TrimSpace(DefaultTablePipelineACL),
	}
}

// LoadTableConfigFile reads a JSON or YAML table mapping file and overlays any
// specified fields onto DefaultTableConfig(), validating every identifier.
func LoadTableConfigFile(path string) (TableConfig, error) {
	data, err := os.ReadFile(strings.TrimSpace(path))
	if err != nil {
		return TableConfig{}, fmt.Errorf("failed reading tables config %q: %w", path, err)
	}
	return ParseTableConfigBytes(data)
}

// ParseTableConfigBytes parses JSON or YAML bytes into a validated TableConfig.
func ParseTableConfigBytes(data []byte) (TableConfig, error) {
	cfg := DefaultTableConfig()
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		return cfg, cfg.Validate()
	}

	// Track explicit empty "schema": "" so callers can intentionally clear DefaultSchema.
	var rawMap map[string]any
	if trimmed[0] == '{' {
		if err := json.Unmarshal(trimmed, &rawMap); err != nil {
			return TableConfig{}, fmt.Errorf("invalid JSON in tables config: %w", err)
		}
		if err := json.Unmarshal(trimmed, &cfg); err != nil {
			return TableConfig{}, fmt.Errorf("invalid JSON in tables config: %w", err)
		}
	} else {
		if err := yaml.Unmarshal(trimmed, &rawMap); err != nil {
			return TableConfig{}, fmt.Errorf("invalid YAML in tables config: %w", err)
		}
		if err := yaml.Unmarshal(trimmed, &cfg); err != nil {
			return TableConfig{}, fmt.Errorf("invalid YAML in tables config: %w", err)
		}
	}

	// Normalize schema trailing dot; preserve explicit empty schema if provided.
	if val, ok := rawMap["schema"]; ok {
		if val == nil {
			cfg.Schema = ""
		} else if s, ok := val.(string); ok {
			cfg.Schema = strings.TrimSuffix(strings.TrimSpace(s), ".")
		}
	} else {
		cfg.Schema = strings.TrimSuffix(strings.TrimSpace(cfg.Schema), ".")
	}

	if err := cfg.Validate(); err != nil {
		return TableConfig{}, err
	}
	return cfg, nil
}

const tableIdentChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_"

func isValidSQLIdent(ident string) bool {
	if len(ident) == 0 || len(ident) > 128 {
		return false
	}
	for i := 0; i < len(ident); i++ {
		ch := ident[i]
		if strings.IndexByte(tableIdentChars, ch) < 0 {
			return false
		}
		if i == 0 && ch >= '0' && ch <= '9' {
			return false
		}
	}
	return true
}

func validateSQLTableRef(fieldName, raw string) error {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return fmt.Errorf("table config field %q cannot be empty", fieldName)
	}
	parts := strings.Split(trimmed, ".")
	if len(parts) == 1 {
		if !isValidSQLIdent(parts[0]) {
			return fmt.Errorf("invalid table identifier %q for %s", raw, fieldName)
		}
		return nil
	}
	if len(parts) == 2 {
		if !isValidSQLIdent(parts[0]) || !isValidSQLIdent(parts[1]) {
			return fmt.Errorf("invalid qualified table identifier %q for %s", raw, fieldName)
		}
		return nil
	}
	return fmt.Errorf("invalid table identifier %q for %s (expected <table> or <schema>.<table>)", raw, fieldName)
}

// Validate checks that Schema (if non-empty) and all 12 table names are valid SQL identifiers.
func (c TableConfig) Validate() error {
	schema := strings.TrimSuffix(strings.TrimSpace(c.Schema), ".")
	if schema != "" && !isValidSQLIdent(schema) {
		return fmt.Errorf("invalid schema identifier %q", c.Schema)
	}
	checks := []struct {
		name string
		val  string
	}{
		{"pipeline_content", c.PipelineContent},
		{"options_content", c.OptionsContent},
		{"config_content", c.ConfigContent},
		{"pipeline_runs", c.PipelineRuns},
		{"pipeline_events", c.PipelineEvents},
		{"api_events", c.APIAuditEvents},
		{"api_users", c.APIUsers},
		{"api_groups", c.APIGroups},
		{"api_user_groups", c.APIUserGroups},
		{"pipeline_groups", c.PipelineGroups},
		{"pipeline_group_members", c.PipelineGroupMembers},
		{"pipeline_acl", c.PipelineACL},
	}
	for _, chk := range checks {
		if err := validateSQLTableRef(chk.name, chk.val); err != nil {
			return err
		}
	}
	return nil
}

// SchemaPrefix returns "<schema>." when Schema is non-empty, or "" otherwise.
func (c TableConfig) SchemaPrefix() string {
	s := strings.TrimSuffix(strings.TrimSpace(c.Schema), ".")
	if s == "" {
		return ""
	}
	return s + "."
}

// Qualify prepends SchemaPrefix() when tableName does not already include a schema.
func (c TableConfig) Qualify(tableName string) string {
	t := strings.TrimSpace(tableName)
	if t == "" || strings.Contains(t, ".") {
		return t
	}
	return c.SchemaPrefix() + t
}

// Resolve returns the dialect-appropriate table identifier for tableName.
//   - sqlite / sqlite3: strips any schema prefix (single-file SQLite has no custom schemas).
//   - mysql / mariadb: strips "dbo." while preserving explicit custom database/schema prefixes.
//   - sqlserver / postgres / oracle / others: qualifies with SchemaPrefix() if unqualified.
func (c TableConfig) Resolve(dialect, tableName string) string {
	qualified := c.Qualify(tableName)
	switch strings.ToLower(strings.TrimSpace(dialect)) {
	case "sqlite", "sqlite3":
		if idx := strings.LastIndex(qualified, "."); idx >= 0 && idx+1 < len(qualified) {
			return qualified[idx+1:]
		}
		return qualified
	case "mysql", "mariadb":
		if strings.HasPrefix(strings.ToLower(qualified), "dbo.") {
			return qualified[4:]
		}
		return qualified
	default:
		return qualified
	}
}

// BaseName strips any leading "<schema>." prefix from a table name.
func BaseTableName(tableName string) string {
	t := strings.TrimSpace(tableName)
	if idx := strings.LastIndex(t, "."); idx >= 0 && idx+1 < len(t) {
		return t[idx+1:]
	}
	return t
}

// Map returns a logical-key -> resolved-table-name map for the given dialect (or qualified if dialect is empty).
func (c TableConfig) Map(dialect string) map[string]string {
	res := func(t string) string {
		if strings.TrimSpace(dialect) == "" {
			return c.Qualify(t)
		}
		return c.Resolve(dialect, t)
	}
	return map[string]string{
		"pipeline_content":       res(c.PipelineContent),
		"options_content":        res(c.OptionsContent),
		"config_content":         res(c.ConfigContent),
		"pipeline_runs":          res(c.PipelineRuns),
		"pipeline_events":        res(c.PipelineEvents),
		"api_events":             res(c.APIAuditEvents),
		"api_users":              res(c.APIUsers),
		"api_groups":             res(c.APIGroups),
		"api_user_groups":        res(c.APIUserGroups),
		"pipeline_groups":        res(c.PipelineGroups),
		"pipeline_group_members": res(c.PipelineGroupMembers),
		"pipeline_acl":           res(c.PipelineACL),
	}
}

// RenderDDL generates ready-to-execute CREATE SCHEMA / CREATE TABLE / CREATE INDEX DDL
// for the requested dialect ("sqlserver", "postgres", "mysql", "sqlite", or "oracle")
// using the configured schema and table names.
func (c TableConfig) RenderDDL(dialect string) (string, error) {
	if err := c.Validate(); err != nil {
		return "", err
	}
	d := strings.ToLower(strings.TrimSpace(dialect))
	tPipe := c.Resolve(d, c.PipelineContent)
	tOpt := c.Resolve(d, c.OptionsContent)
	tCfg := c.Resolve(d, c.ConfigContent)
	tRuns := c.Resolve(d, c.PipelineRuns)
	tEvts := c.Resolve(d, c.PipelineEvents)
	tAudit := c.Resolve(d, c.APIAuditEvents)
	tUsers := c.Resolve(d, c.APIUsers)
	tGroups := c.Resolve(d, c.APIGroups)
	tUserGrps := c.Resolve(d, c.APIUserGroups)
	tPipeGrps := c.Resolve(d, c.PipelineGroups)
	tPipeGrpMem := c.Resolve(d, c.PipelineGroupMembers)
	tACL := c.Resolve(d, c.PipelineACL)

	bUserGrps := BaseTableName(tUserGrps)
	bPipeGrpMem := BaseTableName(tPipeGrpMem)
	bACL := BaseTableName(tACL)
	bGroups := BaseTableName(tGroups)
	bPipeGrps := BaseTableName(tPipeGrps)

	schema := strings.TrimSuffix(strings.TrimSpace(c.Schema), ".")

	switch d {
	case "sqlite", "sqlite3":
		return fmt.Sprintf(`-- SQLite DDL generated by ETL Madness Flow
CREATE TABLE IF NOT EXISTS %s (
    ID          INTEGER PRIMARY KEY AUTOINCREMENT,
    Name        TEXT NOT NULL UNIQUE,
    Description TEXT,
    PipelineXML TEXT,
    CreatedAt   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UpdatedAt   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS %s (
    ID          INTEGER PRIMARY KEY AUTOINCREMENT,
    Name        TEXT NOT NULL UNIQUE,
    Description TEXT,
    OptionsXML  TEXT,
    CreatedAt   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UpdatedAt   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS %s (
    ID          INTEGER PRIMARY KEY AUTOINCREMENT,
    Name        TEXT NOT NULL UNIQUE,
    Description TEXT,
    ConfigXML   TEXT,
    CreatedAt   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UpdatedAt   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS %s (
    run_id           TEXT PRIMARY KEY,
    file_path        TEXT,
    config_path      TEXT,
    status           TEXT NOT NULL,
    started_at       DATETIME NOT NULL,
    finished_at      DATETIME NOT NULL,
    duration_ms      INTEGER NOT NULL,
    task_count       INTEGER NOT NULL,
    user_name        TEXT,
    os_user_name     TEXT,
    db_user_name     TEXT,
    db_server        TEXT,
    spid             INTEGER,
    application_name TEXT,
    hostname         TEXT,
    options_path     TEXT,
    error_class      TEXT,
    error_message    TEXT
);

CREATE TABLE IF NOT EXISTS %s (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id           TEXT NOT NULL,
    execution_id     TEXT,
    sequence_num     INTEGER NOT NULL,
    occurred_at      DATETIME NOT NULL,
    event_type       TEXT NOT NULL,
    node_kind        TEXT,
    node_id          TEXT,
    status           TEXT,
    user_name        TEXT,
    os_user_name     TEXT,
    db_user_name     TEXT,
    db_server        TEXT,
    spid             TEXT,
    application_name TEXT,
    hostname         TEXT,
    options_path     TEXT,
    error_message    TEXT,
    rows_read        INTEGER,
    rows_written     INTEGER,
    rows_affected    INTEGER,
    node_output      TEXT
);

CREATE TABLE IF NOT EXISTS %s (
    event_id      TEXT PRIMARY KEY,
    occurred_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    event_type    TEXT NOT NULL,
    http_method   TEXT NOT NULL,
    path          TEXT NOT NULL,
    artifact_type TEXT,
    artifact_name TEXT,
    run_id        TEXT,
    auth_user     TEXT,
    remote_addr   TEXT,
    status_code   INTEGER NOT NULL,
    status        TEXT NOT NULL,
    duration_ms   INTEGER NOT NULL,
    error_message TEXT,
    details_json  TEXT
);

CREATE TABLE IF NOT EXISTS %s (
    username      TEXT PRIMARY KEY,
    password_hash TEXT NOT NULL,
    permission    TEXT NOT NULL,
    is_active     INTEGER NOT NULL DEFAULT 1,
    description   TEXT,
    created_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS %s (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL UNIQUE,
    description TEXT,
    created_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS %s (
    group_name TEXT NOT NULL,
    username   TEXT NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (group_name, username)
);
CREATE INDEX IF NOT EXISTS ix_%s_username ON %s (username);

CREATE TABLE IF NOT EXISTS %s (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    name         TEXT NOT NULL UNIQUE,
    description  TEXT,
    name_pattern TEXT,
    created_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS %s (
    group_name    TEXT NOT NULL,
    pipeline_name TEXT NOT NULL,
    created_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (group_name, pipeline_name)
);
CREATE INDEX IF NOT EXISTS ix_%s_pipeline ON %s (pipeline_name);

CREATE TABLE IF NOT EXISTS %s (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    principal_type TEXT NOT NULL,
    principal_name TEXT NOT NULL,
    target_type    TEXT NOT NULL,
    target_name    TEXT NOT NULL,
    permissions    TEXT NOT NULL,
    effect         TEXT NOT NULL DEFAULT 'allow',
    description    TEXT,
    created_at     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (principal_type, principal_name, target_type, target_name, effect)
);
CREATE INDEX IF NOT EXISTS ix_%s_target ON %s (target_type, target_name);
CREATE INDEX IF NOT EXISTS ix_%s_principal ON %s (principal_type, principal_name);
`, tPipe, tOpt, tCfg, tRuns, tEvts, tAudit, tUsers, tGroups, tUserGrps, bUserGrps, tUserGrps, tPipeGrps, tPipeGrpMem, bPipeGrpMem, tPipeGrpMem, tACL, bACL, tACL, bACL, tACL), nil

	case "postgres", "postgresql", "pq", "pgx":
		schemaStmt := ""
		if schema != "" {
			schemaStmt = fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s;\n\n", schema)
		}
		return fmt.Sprintf(`-- PostgreSQL DDL generated by ETL Madness Flow
%sCREATE TABLE IF NOT EXISTS %s (
    ID          SERIAL       PRIMARY KEY,
    Name        VARCHAR(255) NOT NULL UNIQUE,
    Description TEXT,
    PipelineXML TEXT,
    CreatedAt   TIMESTAMPTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UpdatedAt   TIMESTAMPTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS %s (
    ID          SERIAL       PRIMARY KEY,
    Name        VARCHAR(255) NOT NULL UNIQUE,
    Description TEXT,
    OptionsXML  TEXT,
    CreatedAt   TIMESTAMPTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UpdatedAt   TIMESTAMPTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS %s (
    ID          SERIAL       PRIMARY KEY,
    Name        VARCHAR(255) NOT NULL UNIQUE,
    Description TEXT,
    ConfigXML   TEXT,
    CreatedAt   TIMESTAMPTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UpdatedAt   TIMESTAMPTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS %s (
    run_id           VARCHAR(64)  PRIMARY KEY,
    file_path        VARCHAR(512),
    config_path      VARCHAR(512),
    status           VARCHAR(32)  NOT NULL,
    started_at       TIMESTAMPTZ  NOT NULL,
    finished_at      TIMESTAMPTZ  NOT NULL,
    duration_ms      BIGINT       NOT NULL,
    task_count       INT          NOT NULL,
    user_name        VARCHAR(255),
    os_user_name     VARCHAR(255),
    db_user_name     VARCHAR(255),
    db_server        VARCHAR(255),
    spid             BIGINT,
    application_name VARCHAR(255),
    hostname         VARCHAR(255),
    options_path     VARCHAR(512),
    error_class      VARCHAR(128),
    error_message    TEXT
);

CREATE TABLE IF NOT EXISTS %s (
    id               BIGSERIAL    PRIMARY KEY,
    run_id           VARCHAR(64)  NOT NULL,
    execution_id     VARCHAR(64),
    sequence_num     BIGINT       NOT NULL,
    occurred_at      TIMESTAMPTZ  NOT NULL,
    event_type       VARCHAR(64)  NOT NULL,
    node_kind        VARCHAR(64),
    node_id          VARCHAR(255),
    status           VARCHAR(64),
    user_name        VARCHAR(255),
    os_user_name     VARCHAR(255),
    db_user_name     VARCHAR(255),
    db_server        VARCHAR(255),
    spid             VARCHAR(64),
    application_name VARCHAR(255),
    hostname         VARCHAR(255),
    options_path     VARCHAR(512),
    error_message    TEXT,
    rows_read        BIGINT,
    rows_written     BIGINT,
    rows_affected    BIGINT,
    node_output      TEXT
);

CREATE TABLE IF NOT EXISTS %s (
    event_id      VARCHAR(64)  PRIMARY KEY,
    occurred_at   TIMESTAMPTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP,
    event_type    VARCHAR(128) NOT NULL,
    http_method   VARCHAR(16)  NOT NULL,
    path          VARCHAR(512) NOT NULL,
    artifact_type VARCHAR(64),
    artifact_name VARCHAR(255),
    run_id        VARCHAR(64),
    auth_user     VARCHAR(255),
    remote_addr   VARCHAR(128),
    status_code   INT          NOT NULL,
    status        VARCHAR(32)  NOT NULL,
    duration_ms   BIGINT       NOT NULL,
    error_message TEXT,
    details_json  TEXT
);

CREATE TABLE IF NOT EXISTS %s (
    username      VARCHAR(255) PRIMARY KEY,
    password_hash VARCHAR(512) NOT NULL,
    permission    VARCHAR(128) NOT NULL,
    is_active     BOOLEAN      NOT NULL DEFAULT TRUE,
    description   VARCHAR(512),
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at    TIMESTAMPTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS %s (
    id          SERIAL       PRIMARY KEY,
    name        VARCHAR(128) NOT NULL UNIQUE,
    description VARCHAR(512),
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS %s (
    group_name VARCHAR(128) NOT NULL,
    username   VARCHAR(255) NOT NULL,
    created_at TIMESTAMPTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (group_name, username)
);
CREATE INDEX IF NOT EXISTS ix_%s_username ON %s (username);

CREATE TABLE IF NOT EXISTS %s (
    id           SERIAL       PRIMARY KEY,
    name         VARCHAR(128) NOT NULL UNIQUE,
    description  VARCHAR(512),
    name_pattern VARCHAR(255),
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS %s (
    group_name    VARCHAR(128) NOT NULL,
    pipeline_name VARCHAR(255) NOT NULL,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (group_name, pipeline_name)
);
CREATE INDEX IF NOT EXISTS ix_%s_pipeline ON %s (pipeline_name);

CREATE TABLE IF NOT EXISTS %s (
    id             SERIAL       PRIMARY KEY,
    principal_type VARCHAR(32)  NOT NULL,
    principal_name VARCHAR(255) NOT NULL,
    target_type    VARCHAR(32)  NOT NULL,
    target_name    VARCHAR(255) NOT NULL,
    permissions    VARCHAR(128) NOT NULL,
    effect         VARCHAR(16)  NOT NULL DEFAULT 'allow',
    description    VARCHAR(512),
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMPTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_%s_rule UNIQUE (principal_type, principal_name, target_type, target_name, effect)
);
CREATE INDEX IF NOT EXISTS ix_%s_target ON %s (target_type, target_name);
CREATE INDEX IF NOT EXISTS ix_%s_principal ON %s (principal_type, principal_name);
`, schemaStmt, tPipe, tOpt, tCfg, tRuns, tEvts, tAudit, tUsers, tGroups, tUserGrps, bUserGrps, tUserGrps, tPipeGrps, tPipeGrpMem, bPipeGrpMem, tPipeGrpMem, tACL, bACL, bACL, tACL, bACL, tACL), nil

	case "sqlserver", "mssql", "azuresql", "azuread":
		schemaStmt := ""
		if schema != "" && !strings.EqualFold(schema, "dbo") {
			schemaStmt = fmt.Sprintf("IF SCHEMA_ID(N'%s') IS NULL EXEC(N'CREATE SCHEMA [%s]');\nGO\n\n", schema, schema)
		}
		return fmt.Sprintf(`-- SQL Server DDL generated by ETL Madness Flow
%sIF OBJECT_ID('%s', 'U') IS NULL
CREATE TABLE %s (
    ID          INT IDENTITY(1,1) PRIMARY KEY,
    Name        NVARCHAR(255) NOT NULL UNIQUE,
    Description NVARCHAR(MAX) NULL,
    PipelineXML NVARCHAR(MAX) NULL,
    CreatedAt   DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
    UpdatedAt   DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

IF OBJECT_ID('%s', 'U') IS NULL
CREATE TABLE %s (
    ID          INT IDENTITY(1,1) PRIMARY KEY,
    Name        NVARCHAR(255) NOT NULL UNIQUE,
    Description NVARCHAR(MAX) NULL,
    OptionsXML  NVARCHAR(MAX) NULL,
    CreatedAt   DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
    UpdatedAt   DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

IF OBJECT_ID('%s', 'U') IS NULL
CREATE TABLE %s (
    ID          INT IDENTITY(1,1) PRIMARY KEY,
    Name        NVARCHAR(255) NOT NULL UNIQUE,
    Description NVARCHAR(MAX) NULL,
    ConfigXML   NVARCHAR(MAX) NULL,
    CreatedAt   DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
    UpdatedAt   DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

IF OBJECT_ID('%s', 'U') IS NULL
CREATE TABLE %s (
    run_id           NVARCHAR(64)  NOT NULL PRIMARY KEY,
    file_path        NVARCHAR(512) NULL,
    config_path      NVARCHAR(512) NULL,
    status           NVARCHAR(32)  NOT NULL,
    started_at       DATETIME2     NOT NULL,
    finished_at      DATETIME2     NOT NULL,
    duration_ms      BIGINT        NOT NULL,
    task_count       INT           NOT NULL,
    user_name        NVARCHAR(255) NULL,
    os_user_name     NVARCHAR(255) NULL,
    db_user_name     NVARCHAR(255) NULL,
    db_server        NVARCHAR(255) NULL,
    spid             BIGINT        NULL,
    application_name NVARCHAR(255) NULL,
    hostname         NVARCHAR(255) NULL,
    options_path     NVARCHAR(512) NULL,
    error_class      NVARCHAR(128) NULL,
    error_message    NVARCHAR(MAX) NULL
);
GO

IF OBJECT_ID('%s', 'U') IS NULL
CREATE TABLE %s (
    id               BIGINT IDENTITY(1,1) PRIMARY KEY,
    run_id           NVARCHAR(64)  NOT NULL,
    execution_id     NVARCHAR(64)  NULL,
    sequence_num     BIGINT        NOT NULL,
    occurred_at      DATETIME2     NOT NULL,
    event_type       NVARCHAR(64)  NOT NULL,
    node_kind        NVARCHAR(64)  NULL,
    node_id          NVARCHAR(255) NULL,
    status           NVARCHAR(64)  NULL,
    user_name        NVARCHAR(255) NULL,
    os_user_name     NVARCHAR(255) NULL,
    db_user_name     NVARCHAR(255) NULL,
    db_server        NVARCHAR(255) NULL,
    spid             NVARCHAR(64)  NULL,
    application_name NVARCHAR(255) NULL,
    hostname         NVARCHAR(255) NULL,
    options_path     NVARCHAR(512) NULL,
    error_message    NVARCHAR(MAX) NULL,
    rows_read        BIGINT        NULL,
    rows_written     BIGINT        NULL,
    rows_affected    BIGINT        NULL,
    node_output      NVARCHAR(MAX) NULL
);
GO

IF OBJECT_ID('%s', 'U') IS NULL
CREATE TABLE %s (
    event_id      NVARCHAR(64)  NOT NULL PRIMARY KEY,
    occurred_at   DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
    event_type    NVARCHAR(128) NOT NULL,
    http_method   NVARCHAR(16)  NOT NULL,
    path          NVARCHAR(512) NOT NULL,
    artifact_type NVARCHAR(64)  NULL,
    artifact_name NVARCHAR(255) NULL,
    run_id        NVARCHAR(64)  NULL,
    auth_user     NVARCHAR(255) NULL,
    remote_addr   NVARCHAR(128) NULL,
    status_code   INT           NOT NULL,
    status        NVARCHAR(32)  NOT NULL,
    duration_ms   BIGINT        NOT NULL,
    error_message NVARCHAR(MAX) NULL,
    details_json  NVARCHAR(MAX) NULL
);
GO

IF OBJECT_ID('%s', 'U') IS NULL
CREATE TABLE %s (
    username      NVARCHAR(255) NOT NULL PRIMARY KEY,
    password_hash NVARCHAR(512) NOT NULL,
    permission    NVARCHAR(128) NOT NULL,
    is_active     BIT           NOT NULL DEFAULT 1,
    description   NVARCHAR(512) NULL,
    created_at    DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at    DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

IF OBJECT_ID('%s', 'U') IS NULL
CREATE TABLE %s (
    id          INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    name        NVARCHAR(128)     NOT NULL CONSTRAINT UQ_%s_name UNIQUE,
    description NVARCHAR(512)     NULL,
    created_at  DATETIME2         NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at  DATETIME2         NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

IF OBJECT_ID('%s', 'U') IS NULL
CREATE TABLE %s (
    group_name NVARCHAR(128) NOT NULL,
    username   NVARCHAR(255) NOT NULL,
    created_at DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_%s PRIMARY KEY CLUSTERED (group_name, username)
);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_%s_username' AND object_id = OBJECT_ID('%s'))
CREATE NONCLUSTERED INDEX IX_%s_username ON %s (username);
GO

IF OBJECT_ID('%s', 'U') IS NULL
CREATE TABLE %s (
    id           INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    name         NVARCHAR(128)     NOT NULL CONSTRAINT UQ_%s_name UNIQUE,
    description  NVARCHAR(512)     NULL,
    name_pattern NVARCHAR(255)     NULL,
    created_at   DATETIME2         NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at   DATETIME2         NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

IF OBJECT_ID('%s', 'U') IS NULL
CREATE TABLE %s (
    group_name    NVARCHAR(128) NOT NULL,
    pipeline_name NVARCHAR(255) NOT NULL,
    created_at    DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_%s PRIMARY KEY CLUSTERED (group_name, pipeline_name)
);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_%s_pipeline' AND object_id = OBJECT_ID('%s'))
CREATE NONCLUSTERED INDEX IX_%s_pipeline ON %s (pipeline_name);
GO

IF OBJECT_ID('%s', 'U') IS NULL
CREATE TABLE %s (
    id             INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    principal_type NVARCHAR(32)      NOT NULL,
    principal_name NVARCHAR(255)     NOT NULL,
    target_type    NVARCHAR(32)      NOT NULL,
    target_name    NVARCHAR(255)     NOT NULL,
    permissions    NVARCHAR(128)     NOT NULL,
    effect         NVARCHAR(16)      NOT NULL CONSTRAINT DF_%s_effect DEFAULT N'allow',
    description    NVARCHAR(512)     NULL,
    created_at     DATETIME2         NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at     DATETIME2         NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT CK_%s_principal_type CHECK (principal_type IN (N'user', N'group')),
    CONSTRAINT CK_%s_target_type    CHECK (target_type IN (N'pipeline', N'pipeline_group', N'*')),
    CONSTRAINT CK_%s_effect         CHECK (effect IN (N'allow', N'deny')),
    CONSTRAINT UQ_%s_rule           UNIQUE NONCLUSTERED (principal_type, principal_name, target_type, target_name, effect)
);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_%s_target' AND object_id = OBJECT_ID('%s'))
CREATE NONCLUSTERED INDEX IX_%s_target ON %s (target_type, target_name);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_%s_principal' AND object_id = OBJECT_ID('%s'))
CREATE NONCLUSTERED INDEX IX_%s_principal ON %s (principal_type, principal_name);
GO
`, schemaStmt,
			tPipe, tPipe,
			tOpt, tOpt,
			tCfg, tCfg,
			tRuns, tRuns,
			tEvts, tEvts,
			tAudit, tAudit,
			tUsers, tUsers,
			tGroups, tGroups, bGroups,
			tUserGrps, tUserGrps, bUserGrps,
			bUserGrps, tUserGrps, bUserGrps, tUserGrps,
			tPipeGrps, tPipeGrps, bPipeGrps,
			tPipeGrpMem, tPipeGrpMem, bPipeGrpMem,
			bPipeGrpMem, tPipeGrpMem, bPipeGrpMem, tPipeGrpMem,
			tACL, tACL, bACL, bACL, bACL, bACL, bACL,
			bACL, tACL, bACL, tACL,
			bACL, tACL, bACL, tACL), nil

	case "mysql", "mariadb":
		return fmt.Sprintf(`-- MySQL / MariaDB DDL generated by ETL Madness Flow
CREATE TABLE IF NOT EXISTS %s (
    ID          INT AUTO_INCREMENT PRIMARY KEY,
    Name        VARCHAR(255) NOT NULL UNIQUE,
    Description LONGTEXT,
    PipelineXML LONGTEXT,
    CreatedAt   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UpdatedAt   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS %s (
    ID          INT AUTO_INCREMENT PRIMARY KEY,
    Name        VARCHAR(255) NOT NULL UNIQUE,
    Description LONGTEXT,
    OptionsXML  LONGTEXT,
    CreatedAt   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UpdatedAt   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS %s (
    ID          INT AUTO_INCREMENT PRIMARY KEY,
    Name        VARCHAR(255) NOT NULL UNIQUE,
    Description LONGTEXT,
    ConfigXML   LONGTEXT,
    CreatedAt   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UpdatedAt   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS %s (
    run_id           VARCHAR(64) PRIMARY KEY,
    file_path        VARCHAR(512),
    config_path      VARCHAR(512),
    status           VARCHAR(32) NOT NULL,
    started_at       DATETIME NOT NULL,
    finished_at      DATETIME NOT NULL,
    duration_ms      BIGINT NOT NULL,
    task_count       INT NOT NULL,
    user_name        VARCHAR(255),
    os_user_name     VARCHAR(255),
    db_user_name     VARCHAR(255),
    db_server        VARCHAR(255),
    spid             BIGINT,
    application_name VARCHAR(255),
    hostname         VARCHAR(255),
    options_path     VARCHAR(512),
    error_class      VARCHAR(128),
    error_message    LONGTEXT
);

CREATE TABLE IF NOT EXISTS %s (
    id               BIGINT AUTO_INCREMENT PRIMARY KEY,
    run_id           VARCHAR(64) NOT NULL,
    execution_id     VARCHAR(64),
    sequence_num     BIGINT NOT NULL,
    occurred_at      DATETIME NOT NULL,
    event_type       VARCHAR(64) NOT NULL,
    node_kind        VARCHAR(64),
    node_id          VARCHAR(255),
    status           VARCHAR(64),
    user_name        VARCHAR(255),
    os_user_name     VARCHAR(255),
    db_user_name     VARCHAR(255),
    db_server        VARCHAR(255),
    spid             VARCHAR(64),
    application_name VARCHAR(255),
    hostname         VARCHAR(255),
    options_path     VARCHAR(512),
    error_message    LONGTEXT,
    rows_read        BIGINT,
    rows_written     BIGINT,
    rows_affected    BIGINT,
    node_output      LONGTEXT
);

CREATE TABLE IF NOT EXISTS %s (
    event_id      VARCHAR(64) PRIMARY KEY,
    occurred_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    event_type    VARCHAR(128) NOT NULL,
    http_method   VARCHAR(16) NOT NULL,
    path          VARCHAR(512) NOT NULL,
    artifact_type VARCHAR(64),
    artifact_name VARCHAR(255),
    run_id        VARCHAR(64),
    auth_user     VARCHAR(255),
    remote_addr   VARCHAR(128),
    status_code   INT NOT NULL,
    status        VARCHAR(32) NOT NULL,
    duration_ms   BIGINT NOT NULL,
    error_message LONGTEXT,
    details_json  LONGTEXT
);

CREATE TABLE IF NOT EXISTS %s (
    username      VARCHAR(255) PRIMARY KEY,
    password_hash VARCHAR(512) NOT NULL,
    permission    VARCHAR(128) NOT NULL,
    is_active     TINYINT(1) NOT NULL DEFAULT 1,
    description   VARCHAR(512),
    created_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS %s (
    id          INT AUTO_INCREMENT PRIMARY KEY,
    name        VARCHAR(128) NOT NULL UNIQUE,
    description VARCHAR(512),
    created_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS %s (
    group_name VARCHAR(128) NOT NULL,
    username   VARCHAR(255) NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (group_name, username),
    INDEX ix_%s_username (username)
);

CREATE TABLE IF NOT EXISTS %s (
    id           INT AUTO_INCREMENT PRIMARY KEY,
    name         VARCHAR(128) NOT NULL UNIQUE,
    description  VARCHAR(512),
    name_pattern VARCHAR(255),
    created_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS %s (
    group_name    VARCHAR(128) NOT NULL,
    pipeline_name VARCHAR(255) NOT NULL,
    created_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (group_name, pipeline_name),
    INDEX ix_%s_pipeline (pipeline_name)
);

CREATE TABLE IF NOT EXISTS %s (
    id             INT AUTO_INCREMENT PRIMARY KEY,
    principal_type VARCHAR(32) NOT NULL,
    principal_name VARCHAR(255) NOT NULL,
    target_type    VARCHAR(32) NOT NULL,
    target_name    VARCHAR(255) NOT NULL,
    permissions    VARCHAR(128) NOT NULL,
    effect         VARCHAR(16) NOT NULL DEFAULT 'allow',
    description    VARCHAR(512),
    created_at     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_%s_rule (principal_type, principal_name, target_type, target_name, effect),
    INDEX ix_%s_target (target_type, target_name),
    INDEX ix_%s_principal (principal_type, principal_name)
);
`, tPipe, tOpt, tCfg, tRuns, tEvts, tAudit, tUsers, tGroups, tUserGrps, bUserGrps, tPipeGrps, tPipeGrpMem, bPipeGrpMem, tACL, bACL, bACL, bACL), nil

	case "oracle", "ora", "godror":
		return fmt.Sprintf(`-- Oracle DDL generated by ETL Madness Flow
CREATE TABLE %s (
    ID          NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    Name        VARCHAR2(255) NOT NULL UNIQUE,
    Description CLOB,
    PipelineXML CLOB,
    CreatedAt   TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    UpdatedAt   TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL
);

CREATE TABLE %s (
    ID          NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    Name        VARCHAR2(255) NOT NULL UNIQUE,
    Description CLOB,
    OptionsXML  CLOB,
    CreatedAt   TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    UpdatedAt   TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL
);

CREATE TABLE %s (
    ID          NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    Name        VARCHAR2(255) NOT NULL UNIQUE,
    Description CLOB,
    ConfigXML   CLOB,
    CreatedAt   TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    UpdatedAt   TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL
);

CREATE TABLE %s (
    run_id           VARCHAR2(64) PRIMARY KEY,
    file_path        VARCHAR2(512),
    config_path      VARCHAR2(512),
    status           VARCHAR2(32) NOT NULL,
    started_at       TIMESTAMP WITH TIME ZONE NOT NULL,
    finished_at      TIMESTAMP WITH TIME ZONE NOT NULL,
    duration_ms      NUMBER(19) NOT NULL,
    task_count       NUMBER(10) NOT NULL,
    user_name        VARCHAR2(255),
    os_user_name     VARCHAR2(255),
    db_user_name     VARCHAR2(255),
    db_server        VARCHAR2(255),
    spid             NUMBER(19),
    application_name VARCHAR2(255),
    hostname         VARCHAR2(255),
    options_path     VARCHAR2(512),
    error_class      VARCHAR2(128),
    error_message    CLOB
);

CREATE TABLE %s (
    id               NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    run_id           VARCHAR2(64) NOT NULL,
    execution_id     VARCHAR2(64),
    sequence_num     NUMBER(19) NOT NULL,
    occurred_at      TIMESTAMP WITH TIME ZONE NOT NULL,
    event_type       VARCHAR2(64) NOT NULL,
    node_kind        VARCHAR2(64),
    node_id          VARCHAR2(255),
    status           VARCHAR2(64),
    user_name        VARCHAR2(255),
    os_user_name     VARCHAR2(255),
    db_user_name     VARCHAR2(255),
    db_server        VARCHAR2(255),
    spid             VARCHAR2(64),
    application_name VARCHAR2(255),
    hostname         VARCHAR2(255),
    options_path     VARCHAR2(512),
    error_message    CLOB,
    rows_read        NUMBER(19),
    rows_written     NUMBER(19),
    rows_affected    NUMBER(19),
    node_output      CLOB
);

CREATE TABLE %s (
    event_id      VARCHAR2(64) PRIMARY KEY,
    occurred_at   TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    event_type    VARCHAR2(128) NOT NULL,
    http_method   VARCHAR2(16) NOT NULL,
    path          VARCHAR2(512) NOT NULL,
    artifact_type VARCHAR2(64),
    artifact_name VARCHAR2(255),
    run_id        VARCHAR2(64),
    auth_user     VARCHAR2(255),
    remote_addr   VARCHAR2(128),
    status_code   NUMBER(10) NOT NULL,
    status        VARCHAR2(32) NOT NULL,
    duration_ms   NUMBER(19) NOT NULL,
    error_message CLOB,
    details_json  CLOB
);

CREATE TABLE %s (
    username      VARCHAR2(255) PRIMARY KEY,
    password_hash VARCHAR2(512) NOT NULL,
    permission    VARCHAR2(128) NOT NULL,
    is_active     NUMBER(1) DEFAULT 1 NOT NULL,
    description   VARCHAR2(512),
    created_at    TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    updated_at    TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL
);

CREATE TABLE %s (
    id          NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    name        VARCHAR2(128) NOT NULL UNIQUE,
    description VARCHAR2(512),
    created_at  TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    updated_at  TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL
);

CREATE TABLE %s (
    group_name VARCHAR2(128) NOT NULL,
    username   VARCHAR2(255) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    PRIMARY KEY (group_name, username)
);

CREATE TABLE %s (
    id           NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    name         VARCHAR2(128) NOT NULL UNIQUE,
    description  VARCHAR2(512),
    name_pattern VARCHAR2(255),
    created_at   TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    updated_at   TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL
);

CREATE TABLE %s (
    group_name    VARCHAR2(128) NOT NULL,
    pipeline_name VARCHAR2(255) NOT NULL,
    created_at    TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    PRIMARY KEY (group_name, pipeline_name)
);

CREATE TABLE %s (
    id             NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    principal_type VARCHAR2(32) NOT NULL,
    principal_name VARCHAR2(255) NOT NULL,
    target_type    VARCHAR2(32) NOT NULL,
    target_name    VARCHAR2(255) NOT NULL,
    permissions    VARCHAR2(128) NOT NULL,
    effect         VARCHAR2(16) DEFAULT 'allow' NOT NULL,
    description    VARCHAR2(512),
    created_at     TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    updated_at     TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT uq_%s_rule UNIQUE (principal_type, principal_name, target_type, target_name, effect)
);
`, tPipe, tOpt, tCfg, tRuns, tEvts, tAudit, tUsers, tGroups, tUserGrps, tPipeGrps, tPipeGrpMem, tACL, bACL), nil

	default:
		return "", fmt.Errorf("unsupported DDL dialect %q (expected sqlserver, postgres, mysql, sqlite, or oracle)", dialect)
	}
}
