package flow

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func TestFormatPlaceholder(t *testing.T) {
	tests := []struct {
		driver   string
		paramIdx int
		expected string
	}{
		{"postgres", 1, "$1"},
		{"postgresql", 5, "$5"},
		{"mysql", 2, "?"},
		{"sqlite3", 3, "?"},
		{"oracle", 4, ":4"},
		{"mssql", 2, "@p2"},
		{"sqlserver", 10, "@p10"},
		{"unknown", 1, "@p1"},
	}

	for _, tt := range tests {
		t.Run(tt.driver, func(t *testing.T) {
			got := formatPlaceholder(tt.driver, tt.paramIdx)
			if got != tt.expected {
				t.Errorf("formatPlaceholder(%q, %d) = %q; want %q", tt.driver, tt.paramIdx, got, tt.expected)
			}
		})
	}
}

func TestMaxParametersForDriver(t *testing.T) {
	tests := []struct {
		driver string
		want   int
	}{
		{"postgres", 65535},
		{"postgresql", 65535},
		{"pgx", 65535},
		{"mysql", 65535},
		{"sqlite", 32766},
		{"sqlite3", 32766},
		{"oracle", 32767},
		{"godror", 32767},
		{"sqlserver", 2100},
		{"mssql", 2100},
		{"unknown", 2100},
	}

	for _, tt := range tests {
		if got := maxParametersForDriver(tt.driver); got != tt.want {
			t.Errorf("maxParametersForDriver(%q) = %d; want %d", tt.driver, got, tt.want)
		}
	}
}

func TestBuildMultiRowInsertSQL(t *testing.T) {
	gotPG := buildMultiRowInsertSQL("postgres", "users", "id, name", 2, 2)
	wantPG := "INSERT INTO users (id, name) VALUES ($1, $2), ($3, $4)"
	if gotPG != wantPG {
		t.Fatalf("got %q, want %q", gotPG, wantPG)
	}

	gotSQLite := buildMultiRowInsertSQL("sqlite", "users", "id, name", 2, 2)
	wantSQLite := "INSERT INTO users (id, name) VALUES (?, ?), (?, ?)"
	if gotSQLite != wantSQLite {
		t.Fatalf("got %q, want %q", gotSQLite, wantSQLite)
	}

	gotOracle := buildMultiRowInsertSQL("oracle", "users", "id, name", 2, 2)
	wantOracle := "INSERT INTO users (id, name) VALUES (:1, :2), (:3, :4)"
	if gotOracle != wantOracle {
		t.Fatalf("got %q, want %q", gotOracle, wantOracle)
	}
}

func TestInterpolateVarsSinglePass(t *testing.T) {
	vars := map[string]interface{}{
		"A":     "{{B}}",
		"B":     "expanded_B",
		"NAME":  "alice",
		"COUNT": 42,
	}

	// 1. Fast path without placeholders
	plain := "SELECT id, name FROM users WHERE active = 1;"
	if got := interpolateVars(plain, vars); got != plain {
		t.Fatalf("expected plain string unchanged, got %q", got)
	}

	// 2. Single-pass non-recursive behavior: {{A}} expands to literal "{{B}}", not "expanded_B"
	if got := interpolateVars("val={{A}}", vars); got != "val={{B}}" {
		t.Fatalf("expected non-recursive expansion 'val={{B}}', got %q", got)
	}

	// 3. Unknown tokens and Go template syntax preserved
	tmpl := "Hello {{NAME}}, goField={{.Field}}, count={{COUNT}}, unknown={{MISSING}}"
	wantTmpl := "Hello alice, goField={{.Field}}, count=42, unknown={{MISSING}}"
	if got := interpolateVars(tmpl, vars); got != wantTmpl {
		t.Fatalf("got %q, want %q", got, wantTmpl)
	}

	// 4. Innermost brace handling {{{NAME}}} -> {alice}
	if got := interpolateVars("json={{{NAME}}}", vars); got != "json={alice}" {
		t.Fatalf("expected 'json={alice}', got %q", got)
	}

	// 5. Unclosed '{{' preserved
	if got := interpolateVars("prefix {{NAME}} trailing {{unclosed", vars); got != "prefix alice trailing {{unclosed" {
		t.Fatalf("unexpected unclosed result: %q", got)
	}
}

func TestRegistrySetVarsAndSetRowVars(t *testing.T) {
	parent := NewRegistry()
	parent.SetVar("INIT", "1")

	child := parent.Snapshot()
	child.SetVars(map[string]interface{}{
		"K1": "V1",
		"K2": 99,
	})
	if child.GetVarString("K1") != "V1" || child.GetVarInt("K2") != 99 {
		t.Fatalf("SetVars failed to populate variables")
	}
	if _, ok := child.dirtyVars["K1"]; !ok {
		t.Fatalf("SetVars failed to mark K1 dirty: %#v", child.dirtyVars)
	}
	if _, ok := child.dirtyVars["K2"]; !ok {
		t.Fatalf("SetVars failed to mark K2 dirty: %#v", child.dirtyVars)
	}

	cols := []string{"UserId", "UserName", "NullableCol"}
	lowerCols := []string{"userid", "username", "nullablecol"}
	upperCols := []string{"USERID", "USERNAME", "NULLABLECOL"}
	vals := []interface{}{101, []byte("bob"), nil}

	parent.SetRowVars(cols, lowerCols, upperCols, vals, 7)
	if parent.GetVarInt("LOOP_INDEX") != 7 {
		t.Fatalf("expected LOOP_INDEX=7, got %v", parent.GetVar("LOOP_INDEX"))
	}
	if parent.GetVarString("UserId") != "101" || parent.GetVarString("userid") != "101" || parent.GetVarString("USERID") != "101" {
		t.Fatalf("expected UserId/userid/USERID='101'")
	}
	if parent.GetVarString("username") != "bob" || parent.GetVarString("USERNAME") != "bob" {
		t.Fatalf("expected username/USERNAME='bob'")
	}
	if parent.GetVarString("nullablecol") != "NULL" {
		t.Fatalf("expected nullablecol='NULL', got %q", parent.GetVarString("nullablecol"))
	}
}

func TestResolveDatabasePoolSettings(t *testing.T) {
	maxOpen, maxIdle, maxLife, maxIdleTime := resolveDatabasePoolSettings(DatabaseConfig{Workload: "bulk"})
	if maxOpen != 50 || maxIdle != 25 || maxLife != 30*time.Minute || maxIdleTime != 10*time.Minute {
		t.Fatalf("unexpected bulk workload defaults: open=%d idle=%d life=%v idleTime=%v", maxOpen, maxIdle, maxLife, maxIdleTime)
	}

	maxOpen2, maxIdle2, _, maxIdleTime2 := resolveDatabasePoolSettings(DatabaseConfig{Workload: "oltp", MaxOpenConns: 12})
	if maxOpen2 != 12 || maxIdle2 != 10 || maxIdleTime2 != 2*time.Minute {
		t.Fatalf("unexpected oltp workload settings with override: open=%d idle=%d idleTime=%v", maxOpen2, maxIdle2, maxIdleTime2)
	}

	xmlData := []byte(`<pipeline>
		<databases>
			<database name="d1" driver="sqlite" connection_string=":memory:" workload="analytics" conn_max_idle_time_seconds="45" />
		</databases>
		<flow><sql id="s1" db="d1">SELECT 1;</sql></flow>
	</pipeline>`)
	parsedCfg, err := ParseXMLConfig(xmlData)
	if err != nil {
		t.Fatalf("ParseXMLConfig failed: %v", err)
	}
	if len(parsedCfg.Databases) != 1 || parsedCfg.Databases[0].ConnMaxIdleTime != 45*time.Second || parsedCfg.Databases[0].Workload != "analytics" {
		t.Fatalf("unexpected parsed DatabaseConfig: %+v", parsedCfg.Databases)
	}
}

func TestHTTPTransportPoolingAndIsolation(t *testing.T) {
	skipTrue := true
	elem1 := HTTPClientElement{URI: "https://example.com/a", Timeout: "5s"}
	elem2 := HTTPClientElement{URI: "https://example.com/b", Timeout: "10s"}
	elemInsecure := HTTPClientElement{URI: "https://example.com/c", TLSInsecureSkipVerify: &skipTrue}

	t1, err := getOrCreateTransport(elem1)
	if err != nil {
		t.Fatalf("getOrCreateTransport(elem1) error: %v", err)
	}
	t2, err := getOrCreateTransport(elem2)
	if err != nil {
		t.Fatalf("getOrCreateTransport(elem2) error: %v", err)
	}
	if t1 != t2 {
		t.Fatalf("expected identical transport configs (differing only in URI/client timeout) to share pooled *http.Transport")
	}
	if t1.MaxIdleConnsPerHost != 10 {
		t.Fatalf("expected default MaxIdleConnsPerHost=10, got %d", t1.MaxIdleConnsPerHost)
	}

	t3, err := getOrCreateTransport(elemInsecure)
	if err != nil {
		t.Fatalf("getOrCreateTransport(elemInsecure) error: %v", err)
	}
	if t1 == t3 {
		t.Fatalf("expected distinct TLSInsecureSkipVerify setting to produce isolated *http.Transport")
	}

	CloseIdleHTTPConnections()
}

func TestRuntimeIdentityCached(t *testing.T) {
	resetRuntimeIdentityForTest()
	u1, h1 := runtimeIdentity()
	start := time.Now()
	u2, h2 := runtimeIdentity()
	elapsed := time.Since(start)

	if u1 != u2 || h1 != h2 {
		t.Fatalf("cached runtimeIdentity mismatch: (%q,%q) vs (%q,%q)", u1, h1, u2, h2)
	}
	if elapsed > 5*time.Millisecond {
		t.Fatalf("expected cached runtimeIdentity call to be sub-millisecond, took %v", elapsed)
	}
}

func TestTemplateCaching(t *testing.T) {
	e := NewExecutor(NewRegistry())
	tmpl1, err := e.getOrParseTextTemplate("t1", "Hello {{.NAME}}")
	if err != nil {
		t.Fatalf("getOrParseTextTemplate failed: %v", err)
	}
	tmpl2, err := e.getOrParseTextTemplate("t1", "Hello {{.NAME}}")
	if err != nil {
		t.Fatalf("getOrParseTextTemplate second call failed: %v", err)
	}
	if tmpl1 != tmpl2 {
		t.Fatalf("expected cached text template pointer to be reused")
	}

	tmpl3, err := e.getOrParseTextTemplate("t1", "Goodbye {{.NAME}}")
	if err != nil {
		t.Fatalf("getOrParseTextTemplate modified content failed: %v", err)
	}
	if tmpl1 == tmpl3 {
		t.Fatalf("expected different template content under same name to parse a distinct template")
	}
}

func TestStreamETLRowIntegrityAndPreparedReuse(t *testing.T) {
	tmpDir := t.TempDir()
	srcPath := filepath.Join(tmpDir, "src.db")
	dstPath := filepath.Join(tmpDir, "dst.db")

	r := NewRegistry()
	err := r.InitDatabases([]DatabaseConfig{
		{Name: "src", Driver: "sqlite", ConnectionString: srcPath, Workload: "bulk"},
		{Name: "dst", Driver: "sqlite", ConnectionString: dstPath, Workload: "bulk"},
	})
	if err != nil {
		t.Fatalf("InitDatabases failed: %v", err)
	}
	defer r.CloseDatabases()

	srcDB, _ := r.GetDB("src")
	dstDB, _ := r.GetDB("dst")

	if _, err := srcDB.Exec(`CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT, raw_bytes BLOB);`); err != nil {
		t.Fatalf("create src table: %v", err)
	}
	if _, err := dstDB.Exec(`CREATE TABLE items_copy (id INTEGER PRIMARY KEY, label TEXT, raw_bytes BLOB);`); err != nil {
		t.Fatalf("create dst table: %v", err)
	}

	const totalRows = 1250
	tx, err := srcDB.Begin()
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	stmt, err := tx.Prepare(`INSERT INTO items (id, label, raw_bytes) VALUES (?, ?, ?)`)
	if err != nil {
		t.Fatalf("prepare insert: %v", err)
	}
	for i := 1; i <= totalRows; i++ {
		raw := []byte{0xff, byte(i % 256), byte((i / 256) % 256)}
		if _, err := stmt.Exec(i, fmt.Sprintf("label_%d", i), raw); err != nil {
			t.Fatalf("insert row %d: %v", i, err)
		}
	}
	_ = stmt.Close()
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit src rows: %v", err)
	}

	copied, err := StreamETL(context.Background(), r, "src", "SELECT id, label, raw_bytes FROM items ORDER BY id", "dst", "items_copy", ETLOptions{
		BatchSize: 300, // 4 full batches of 300 (reusing prepared statement) + 1 partial batch of 50
	})
	if err != nil {
		t.Fatalf("StreamETL failed: %v", err)
	}
	if copied != totalRows {
		t.Fatalf("expected %d copied rows, got %d", totalRows, copied)
	}

	rows, err := dstDB.Query(`SELECT id, label, raw_bytes FROM items_copy ORDER BY id`)
	if err != nil {
		t.Fatalf("query dst: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
		var id int
		var label string
		var raw []byte
		if err := rows.Scan(&id, &label, &raw); err != nil {
			t.Fatalf("scan dst row %d: %v", count, err)
		}
		wantLabel := fmt.Sprintf("label_%d", count)
		if id != count || label != wantLabel {
			t.Fatalf("row %d mismatch: got (%d, %q), want (%d, %q)", count, id, label, count, wantLabel)
		}
		if len(raw) != 3 || raw[0] != 0xff || raw[1] != byte(count%256) {
			t.Fatalf("row %d binary payload corrupted: %v", count, raw)
		}
	}
	if count != totalRows {
		t.Fatalf("expected %d rows in destination, found %d", totalRows, count)
	}
}

func BenchmarkInterpolateVars(b *testing.B) {
	vars := make(map[string]interface{}, 100)
	for i := 0; i < 100; i++ {
		vars[fmt.Sprintf("VAR_%d", i)] = fmt.Sprintf("value_%d", i)
	}
	query := "SELECT id, name FROM orders WHERE tenant = '{{VAR_5}}' AND region = '{{VAR_72}}' LIMIT 100;"

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = interpolateVars(query, vars)
	}
}

func BenchmarkStreamETL(b *testing.B) {
	tmpDir := b.TempDir()
	srcPath := filepath.Join(tmpDir, "bench_src.db")
	dstPath := filepath.Join(tmpDir, "bench_dst.db")

	r := NewRegistry()
	if err := r.InitDatabases([]DatabaseConfig{
		{Name: "src", Driver: "sqlite", ConnectionString: srcPath, Workload: "bulk"},
		{Name: "dst", Driver: "sqlite", ConnectionString: dstPath, Workload: "bulk"},
	}); err != nil {
		b.Fatalf("InitDatabases: %v", err)
	}
	defer r.CloseDatabases()

	srcDB, _ := r.GetDB("src")
	dstDB, _ := r.GetDB("dst")
	_, _ = srcDB.Exec(`CREATE TABLE src_items (id INT, c1 TEXT, c2 TEXT, c3 INT);`)
	_, _ = dstDB.Exec(`CREATE TABLE dst_items (id INT, c1 TEXT, c2 TEXT, c3 INT);`)

	tx, _ := srcDB.Begin()
	stmt, _ := tx.Prepare(`INSERT INTO src_items VALUES (?, ?, ?, ?)`)
	for i := 0; i < 1000; i++ {
		_, _ = stmt.Exec(i, "alpha", "beta", i*10)
	}
	_ = stmt.Close()
	_ = tx.Commit()

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = dstDB.Exec(`DELETE FROM dst_items;`)
		if _, err := StreamETL(ctx, r, "src", "SELECT id, c1, c2, c3 FROM src_items", "dst", "dst_items", ETLOptions{BatchSize: 500}); err != nil {
			b.Fatalf("StreamETL: %v", err)
		}
	}
}
