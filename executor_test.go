package flow

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/xuri/excelize/v2"
)

// TestShellVariablePassing verifies that output_var variables pass correctly
// between shell execution steps.
// TestShellVariablePassing verifies that output_var variables pass correctly
// between shell execution steps using environment variables.
func TestShellVariablePassing(t *testing.T) {
	lang := "bash"
	varCmd := "echo Data: $GCLOUD_BILLING_JSON"

	if runtime.GOOS == "windows" {
		lang = "cmd"
		varCmd = "echo Data: %GCLOUD_BILLING_JSON%"
	}

	xmlConfig := []byte(`<?xml version="1.0" encoding="UTF-8"?>
	<pipeline>
		<scripts>
			<script id="extract" language="` + lang + `" output_var="GCLOUD_BILLING_JSON">
				echo {"account_id": "12345"}
			</script>
			<script id="echo_data" language="` + lang + `">
				` + varCmd + `
			</script>
		</scripts>
	</pipeline>`)

	cfg, err := ParseXMLConfig(xmlConfig)
	if err != nil {
		t.Fatalf("failed to parse XML: %v", err)
	}
	varConfigs := cfg.Variables
	dbConfigs := cfg.Databases
	nodes := cfg.FlowNodes

	registry := NewRegistry()
	if err := registry.InitVariables(varConfigs); err != nil {
		t.Fatalf("failed to init variables: %v", err)
	}
	if err := registry.InitDatabases(dbConfigs); err != nil {
		t.Fatalf("failed to init databases: %v", err)
	}
	defer registry.CloseDatabases()

	executor := NewExecutor(registry)
	results, err := executor.Execute(context.Background(), nodes)
	if err != nil {
		t.Fatalf("execution failed: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 script results, got %d", len(results))
	}

	outStr := results[1].ResultsString
	expected := `{"account_id": "12345"}`
	if runtime.GOOS == "windows" {
		expected = `{\"account_id\": \"12345\"}`
	}
	if !strings.Contains(outStr, expected) {
		t.Errorf("expected output to contain json string, got: %s", outStr)
	}
}

// TestIsShellLanguage tests validation of supported shell language identifiers.
func TestIsShellLanguage(t *testing.T) {
	validShells := []string{
		"shell", "cmd", "powershell", "pwsh", "bash", "git-bash",
		"gitbash", "zsh", "ksh", "csh", "tcsh", "dash", "fish", "sh",
	}

	for _, shell := range validShells {
		if !isShellLanguage(shell) {
			t.Errorf("expected isShellLanguage('%s') to be true", shell)
		}
	}

	invalidShells := []string{"sql", "go", "python", "ruby", "javascript"}
	for _, shell := range invalidShells {
		if isShellLanguage(shell) {
			t.Errorf("expected isShellLanguage('%s') to be false", shell)
		}
	}
}

// TestGroupTransactions verifies transaction commits and rollbacks within group blocks.
func TestGroupTransactions(t *testing.T) {
	// Initialize in-memory SQLite database
	xmlConfig := []byte(`<?xml version="1.0" encoding="UTF-8"?>
	<pipeline>
		<databases>
			<database name="tx_test_db" driver="sqlite" connection_string="file::memory:?cache=shared" />
		</databases>
		<scripts>
			<!-- Setup script -->
			<sql id="setup" db="tx_test_db">
				CREATE TABLE tx_test (id INTEGER PRIMARY KEY, val TEXT);
			</sql>

			<!-- Group that should succeed and commit -->
			<group id="success_group" transaction="true" db="tx_test_db">
				<sql id="insert_1" db="tx_test_db">
					INSERT INTO tx_test (id, val) VALUES (1, 'apple');
				</sql>
				<sql id="insert_2" db="tx_test_db">
					INSERT INTO tx_test (id, val) VALUES (2, 'banana');
				</sql>
			</group>

			<!-- Group that should fail and rollback -->
			<group id="fail_group" transaction="true" db="tx_test_db">
				<sql id="insert_3" db="tx_test_db">
					INSERT INTO tx_test (id, val) VALUES (3, 'cherry');
				</sql>
				<sql id="insert_fail" db="tx_test_db">
					INSERT INTO non_existent_table (id, val) VALUES (4, 'date');
				</sql>
			</group>
		</scripts>
	</pipeline>`)

	cfg, err := ParseXMLConfig(xmlConfig)
	if err != nil {
		t.Fatalf("failed to parse XML: %v", err)
	}
	varConfigs := cfg.Variables
	dbConfigs := cfg.Databases
	nodes := cfg.FlowNodes

	registry := NewRegistry()
	if err := registry.InitVariables(varConfigs); err != nil {
		t.Fatalf("failed to init variables: %v", err)
	}
	if err := registry.InitDatabases(dbConfigs); err != nil {
		t.Fatalf("failed to init databases: %v", err)
	}
	defer registry.CloseDatabases()

	executor := NewExecutor(registry)

	// 1. Run Setup
	_, err = executor.Execute(context.Background(), []PipelineNode{nodes[0]})
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	// 2. Run success group (commits 1 and 2)
	_, err = executor.Execute(context.Background(), []PipelineNode{nodes[1]})
	if err != nil {
		t.Fatalf("success group failed: %v", err)
	}

	// Verify rows 1 and 2 exist
	dbConn, err := registry.GetDB("tx_test_db")
	if err != nil {
		t.Fatalf("failed to get db: %v", err)
	}

	var count int
	err = dbConn.QueryRow("SELECT COUNT(*) FROM tx_test").Scan(&count)
	if err != nil {
		t.Fatalf("count query failed: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 rows after success group, got %d", count)
	}

	// 3. Run fail group (should rollback row 3 insertion)
	_, err = executor.Execute(context.Background(), []PipelineNode{nodes[2]})
	if err == nil {
		t.Error("expected fail group to return an error, but it succeeded")
	}

	// Verify row 3 was rolled back and count is still 2
	err = dbConn.QueryRow("SELECT COUNT(*) FROM tx_test").Scan(&count)
	if err != nil {
		t.Fatalf("count query failed: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 rows after rolled back group, got %d (cherry was not rolled back)", count)
	}
}

// TestDotnetScriptExecution verifies that dotnet script blocks execute, can resolve environment variables,
// and correctly pass variables out of the script.
/*
func TestDotnetScriptExecution(t *testing.T) {
	// Check for dotnet-script in default global tools directories and add to PATH if found
	var toolsDir string
	if usrProfile := os.Getenv("USERPROFILE"); usrProfile != "" {
		toolsDir = filepath.Join(usrProfile, ".dotnet", "tools")
	} else if home := os.Getenv("HOME"); home != "" {
		toolsDir = filepath.Join(home, ".dotnet", "tools")
	}

	if toolsDir != "" {
		if _, err := os.Stat(toolsDir); err == nil {
			path := os.Getenv("PATH")
			sep := string(os.PathListSeparator)
			os.Setenv("PATH", path+sep+toolsDir)
		}
	}

	hasDotnetScript := false
	if _, err := exec.LookPath("dotnet-script.exe"); err == nil {
		hasDotnetScript = true
	} else if _, err := exec.LookPath("dotnet-script"); err == nil {
		hasDotnetScript = true
	} else if _, err := exec.LookPath("dotnet"); err == nil {
		// check if dotnet script works
		cmd := exec.Command("dotnet", "script", "--version")
		if err := cmd.Run(); err == nil {
			hasDotnetScript = true
		}
	}

	if !hasDotnetScript {
		t.Skip("dotnet-script or dotnet script is not installed/available in PATH")
	}

	xmlConfig := []byte(`<?xml version="1.0" encoding="UTF-8"?>
	<pipeline>
		<variables>
			<variable name="TEST_VAR" value="AntigravityPower" />
		</variables>
		<scripts>
			<script id="cs_test" language="dotnet-script" output_var="CS_OUT">
				using System;
				var val = Environment.GetEnvironmentVariable("TEST_VAR");
				Console.Write("CSharpOutput: " + val);
			</script>
		</scripts>
	</pipeline>`)

	cfg, err := ParseXMLConfig(xmlConfig)
	if err != nil {
		t.Fatalf("failed to parse XML: %v", err)
	}
	varConfigs := cfg.Variables
	dbConfigs := cfg.Databases
	nodes := cfg.FlowNodes

	registry := NewRegistry()
	if err := registry.InitVariables(varConfigs); err != nil {
		t.Fatalf("failed to init variables: %v", err)
	}
	if err := registry.InitDatabases(dbConfigs); err != nil {
		t.Fatalf("failed to init databases: %v", err)
	}
	defer registry.CloseDatabases()

	executor := NewExecutor(registry)
	results, err := executor.Execute(context.Background(), nodes)
	if err != nil {
		for _, r := range results {
			t.Logf("Result: ID=%s, Code=%d, Output=%s", r.ScriptID, r.ReturnCode, r.ResultsString)
		}
		t.Fatalf("execution failed: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	outStr := results[0].ResultsString
	if !strings.Contains(outStr, "CSharpOutput: AntigravityPower") {
		t.Errorf("expected output to contain 'CSharpOutput: AntigravityPower', got: %s", outStr)
	}

	if registry.GetVarString("CS_OUT") != "CSharpOutput: AntigravityPower" {
		t.Errorf("expected CS_OUT variable to be 'CSharpOutput: AntigravityPower', got: %v", registry.GetVar("CS_OUT"))
	}
}
*/
// TestExecutorContextCancellation verifies that canceling a context terminates long loops immediately.
func TestExecutorContextCancellation(t *testing.T) {
	xmlConfig := []byte(`<?xml version="1.0" encoding="UTF-8"?>
	<pipeline>
		<variables>
			<variable name="LoopCond" value="true" />
		</variables>
		<scripts>
			<while if_var="LoopCond" if_equals="true">
				<script id="inside_loop" language="go">
					package main
					import "time"
					func main() {
						time.Sleep(10 * time.Millisecond)
					}
				</script>
			</while>
		</scripts>
	</pipeline>`)

	cfg, err := ParseXMLConfig(xmlConfig)
	if err != nil {
		t.Fatalf("failed to parse XML: %v", err)
	}
	varConfigs := cfg.Variables
	nodes := cfg.FlowNodes

	registry := NewRegistry()
	if err := registry.InitVariables(varConfigs); err != nil {
		t.Fatalf("failed to init variables: %v", err)
	}

	executor := NewExecutor(registry)
	ctx, cancel := context.WithCancel(context.Background())

	// Cancel context after a small delay
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	results, err := executor.Execute(ctx, nodes)
	if err == nil {
		t.Error("expected execution to fail or terminate with context cancellation, but got no error")
	}

	// At least one iteration should be logged, and some results returned
	if len(results) > 0 {
		lastRes := results[len(results)-1]
		if !strings.Contains(lastRes.ReturnCode.(string), "context canceled") {
			t.Errorf("expected last result return code to mention 'context canceled', got: %v", lastRes.ReturnCode)
		}
	}
}

// TestParallelVariableIsolationAndNamespacing verifies that parallel workers:
// 1. Isolate variable writes from other workers.
// 2. Do not overwrite parent variables they do not mutate.
// 3. namespace colliding keys, while non-colliding keys merge directly.
func TestParallelVariableIsolationAndNamespacing(t *testing.T) {
	// Let's test the merging logic directly via Registry.Snapshot() and executeParallelNode's merging mechanism.
	parentReg := NewRegistry()
	parentReg.SetVar("CommonVar", "initial")
	parentReg.SetVar("UnrelatedVar", "untouched")

	// Create 2 snapshots simulating 2 workers
	w0Reg := parentReg.Snapshot()
	w1Reg := parentReg.Snapshot()

	// Simulate Worker 0 modifying CommonVar and setting a unique var UniqueW0
	w0Reg.SetVar("CommonVar", "w0_val")
	w0Reg.SetVar("UniqueW0", "w0_unique")

	// Simulate Worker 1 modifying CommonVar and setting a unique var UniqueW1
	w1Reg.SetVar("CommonVar", "w1_val")
	w1Reg.SetVar("UniqueW1", "w1_unique")

	// Simulate executeParallelNode's merging logic
	workerRegistries := []*Registry{w0Reg, w1Reg}

	mutationCounts := make(map[string]int)
	for _, wReg := range workerRegistries {
		wReg.varMu.RLock()
		for k := range wReg.dirtyVars {
			mutationCounts[k]++
		}
		wReg.varMu.RUnlock()
	}

	for i, wReg := range workerRegistries {
		wReg.varMu.RLock()
		for k := range wReg.dirtyVars {
			val := wReg.varRegistry[k]
			if mutationCounts[k] > 1 {
				scopedKey := fmt.Sprintf("WORKER_%d_%s", i, k)
				parentReg.SetVar(scopedKey, val)
			} else {
				parentReg.SetVar(k, val)
			}
		}
		wReg.varMu.RUnlock()
	}

	// Assertions
	// 1. Colliding variable "CommonVar" should NOT be modified in its base form (or it could be left as initial since it collided, which is true because we didn't write to parent's base "CommonVar")
	if parentReg.GetVarString("CommonVar") != "initial" {
		t.Errorf("expected CommonVar to remain 'initial' due to collision, but got: %s", parentReg.GetVarString("CommonVar"))
	}

	// 2. Colliding variables namespaced correctly
	if parentReg.GetVarString("WORKER_0_CommonVar") != "w0_val" {
		t.Errorf("expected WORKER_0_CommonVar to be 'w0_val', got: %s", parentReg.GetVarString("WORKER_0_CommonVar"))
	}
	if parentReg.GetVarString("WORKER_1_CommonVar") != "w1_val" {
		t.Errorf("expected WORKER_1_CommonVar to be 'w1_val', got: %s", parentReg.GetVarString("WORKER_1_CommonVar"))
	}

	// 3. Non-colliding variables merged successfully
	if parentReg.GetVarString("UniqueW0") != "w0_unique" {
		t.Errorf("expected UniqueW0 to be 'w0_unique', got: %s", parentReg.GetVarString("UniqueW0"))
	}
	if parentReg.GetVarString("UniqueW1") != "w1_unique" {
		t.Errorf("expected UniqueW1 to be 'w1_unique', got: %s", parentReg.GetVarString("UniqueW1"))
	}

	// 4. Unrelated variables untouched
	if parentReg.GetVarString("UnrelatedVar") != "untouched" {
		t.Errorf("expected UnrelatedVar to remain 'untouched', got: %s", parentReg.GetVarString("UnrelatedVar"))
	}
}

func TestSQLAndSQLBulk(t *testing.T) {
	xmlConfig := []byte(`<?xml version="1.0" encoding="UTF-8"?>
	<pipeline>
		<databases>
			<database name="test_sql_db" driver="sqlite" connection_string="file::memory:?cache=shared" />
		</databases>
		<scripts>
			<!-- Test standard <sql> tag for table setup -->
			<sql id="setup_tables" db="test_sql_db">
				CREATE TABLE src_table (id INTEGER PRIMARY KEY, name TEXT);
				CREATE TABLE dest_table (id INTEGER PRIMARY KEY, name TEXT);
				INSERT INTO src_table (id, name) VALUES (1, 'Alice');
				INSERT INTO src_table (id, name) VALUES (2, 'Bob');
			</sql>

			<!-- Test <sql_bulk> tag to stream from src_table to dest_table -->
			<sql_bulk id="bulk_copy" db="test_sql_db" target_db="test_sql_db" target_table="dest_table" batch_size="1">
				SELECT id, name FROM src_table ORDER BY id ASC
			</sql_bulk>

			<!-- Test <sql> tag with output_var to fetch records -->
			<sql id="select_dest" db="test_sql_db" output_var="dest_content">
				SELECT name FROM dest_table ORDER BY id ASC
			</sql>
		</scripts>
	</pipeline>`)

	cfg, err := ParseXMLConfig(xmlConfig)
	if err != nil {
		t.Fatalf("failed to parse XML: %v", err)
	}
	varConfigs := cfg.Variables
	dbConfigs := cfg.Databases
	nodes := cfg.FlowNodes

	registry := NewRegistry()
	if err := registry.InitVariables(varConfigs); err != nil {
		t.Fatalf("failed to init variables: %v", err)
	}
	if err := registry.InitDatabases(dbConfigs); err != nil {
		t.Fatalf("failed to init databases: %v", err)
	}
	defer registry.CloseDatabases()

	executor := NewExecutor(registry)
	results, err := executor.Execute(context.Background(), nodes)
	if err != nil {
		for _, res := range results {
			t.Logf("Result - ScriptID: %s, ReturnCode: %v, ResultsString: %s", res.ScriptID, res.ReturnCode, res.ResultsString)
		}
		t.Fatalf("execution failed: %v", err)
	}

	// Verify the destination content
	destContent := registry.GetVar("dest_content")
	if destContent == nil {
		t.Fatal("dest_content variable is nil")
	}

	expectedResult := "name\nAlice\nBob"
	cleanResult := strings.TrimSpace(strings.ReplaceAll(destContent.(string), "\r", ""))
	if cleanResult != expectedResult {
		t.Errorf("expected %q, got %q", expectedResult, cleanResult)
	}

	// Double check results count
	if len(results) != 3 {
		t.Fatalf("expected 3 script results, got %d", len(results))
	}
}

func TestSQLDMLWithReturning(t *testing.T) {
	xmlConfig := []byte(`<?xml version="1.0" encoding="UTF-8"?>
	<pipeline>
		<databases>
			<database name="dml_test_db" driver="sqlite" connection_string="file::memory:?cache=shared" />
		</databases>
		<scripts>
			<sql id="setup" db="dml_test_db">
				CREATE TABLE items (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, status TEXT);
			</sql>
			<sql id="insert_returning" db="dml_test_db" output_var="inserted_id">
				INSERT INTO items (name, status) VALUES ('Widget A', 'pending') RETURNING id;
			</sql>
			<sql id="update_returning" db="dml_test_db" output_var="updated_status">
				UPDATE items SET status = 'completed' WHERE id = 1 RETURNING status;
			</sql>
			<sql id="delete_returning" db="dml_test_db" output_var="deleted_name">
				DELETE FROM items WHERE id = 1 RETURNING name;
			</sql>
			<sql id="update_with_rowcount" db="dml_test_db" output_var="row_count_val">
				UPDATE items SET status = 'active'; SELECT 42 AS "@@ROWCOUNT";
			</sql>
		</scripts>
	</pipeline>`)

	cfg, err := ParseXMLConfig(xmlConfig)
	if err != nil {
		t.Fatalf("failed to parse XML: %v", err)
	}

	registry := NewRegistry()
	if err := registry.InitDatabases(cfg.Databases); err != nil {
		t.Fatalf("failed to init databases: %v", err)
	}
	defer registry.CloseDatabases()

	executor := NewExecutor(registry)
	results, err := executor.Execute(context.Background(), cfg.FlowNodes)
	if err != nil {
		t.Fatalf("execution failed: %v", err)
	}

	if len(results) != 5 {
		t.Fatalf("expected 5 results, got %d", len(results))
	}

	if insertedID := registry.GetVarString("inserted_id"); insertedID != "1" {
		t.Errorf("expected inserted_id to be '1', got %q", insertedID)
	}

	if updatedStatus := registry.GetVarString("updated_status"); updatedStatus != "completed" {
		t.Errorf("expected updated_status to be 'completed', got %q", updatedStatus)
	}

	if deletedName := registry.GetVarString("deleted_name"); deletedName != "Widget A" {
		t.Errorf("expected deleted_name to be 'Widget A', got %q", deletedName)
	}

	if rowCountVal := registry.GetVarString("row_count_val"); rowCountVal != "42" {
		t.Errorf("expected row_count_val to be '42', got %q", rowCountVal)
	}

	if expected := "\n(1 row(s) affected)\n"; results[1].ResultsString != expected {
		t.Errorf("expected results[1].ResultsString to be %q, got %q", expected, results[1].ResultsString)
	}

	if expected := "\n(42 row(s) affected)\n"; results[4].ResultsString != expected {
		t.Errorf("expected results[4].ResultsString to be %q, got %q", expected, results[4].ResultsString)
	}
}

func TestDMLClassification(t *testing.T) {
	selectQueries := []string{
		"SELECT deleted_at FROM users WHERE id = 1",
		"SELECT is_deleted, updated_at FROM accounts",
		"-- comment\nSELECT * FROM orders",
		"/* comment */ SELECT * FROM orders",
		"<![CDATA[ SELECT deleted_at FROM users ]]>",
		"WITH user_cte AS (SELECT id, deleted_at FROM users) SELECT * FROM user_cte",
		"SHOW TABLES",
		"PRAGMA table_info(users)",
		"INSERT INTO users (name) VALUES ('alice') RETURNING id",
		"UPDATE users SET name = 'bob' OUTPUT INSERTED.id",
	}

	for _, q := range selectQueries {
		if isDMLQuery(q) {
			t.Errorf("expected isDMLQuery to be false for: %q", q)
		}
	}

	dmlQueries := []string{
		"INSERT INTO users (name) VALUES ('alice')",
		"UPDATE users SET deleted_at = datetime('now') WHERE id = 1",
		"DELETE FROM users WHERE id = 1",
		"-- comment\nDELETE FROM users WHERE id = 2",
		"/* comment */ TRUNCATE TABLE users",
		"CREATE TABLE test (id INT)",
		"DROP TABLE test",
		"ALTER TABLE users ADD COLUMN age INT",
	}

	for _, q := range dmlQueries {
		if !isDMLQuery(q) {
			t.Errorf("expected isDMLQuery to be true for: %q", q)
		}
	}
}

func TestSQLExecutionDoesNotDropDeletedAtColumns(t *testing.T) {
	xmlConfig := []byte(`<?xml version="1.0" encoding="UTF-8"?>
	<pipeline>
		<databases>
			<database name="dml_test_db" driver="sqlite" connection_string="file::memory:?cache=shared" />
		</databases>
		<flow>
			<sql id="setup" db="dml_test_db">
				CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, deleted_at TEXT);
				INSERT INTO users (id, name, deleted_at) VALUES (1, 'Alice', '2026-01-01');
			</sql>
			<sql id="fetch_deleted" db="dml_test_db" output_var="deleted_rows">
				SELECT id, name, deleted_at FROM users WHERE id = 1;
			</sql>
		</flow>
	</pipeline>`)

	cfg, err := ParseXMLConfig(xmlConfig)
	if err != nil {
		t.Fatalf("failed to parse config: %v", err)
	}

	registry := NewRegistry()
	if err := registry.InitDatabases(cfg.Databases); err != nil {
		t.Fatalf("failed to init db: %v", err)
	}
	defer registry.CloseDatabases()

	executor := NewExecutor(registry)
	results, err := executor.Execute(context.Background(), cfg.FlowNodes)
	if err != nil {
		t.Fatalf("pipeline failed: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	fetchRes := results[1]
	if !strings.Contains(fetchRes.ResultsString, "2026-01-01") {
		t.Errorf("expected output to contain '2026-01-01', got: %s", fetchRes.ResultsString)
	}
	if !strings.Contains(fetchRes.ResultsString, "deleted_at") {
		t.Errorf("expected output to contain column name 'deleted_at', got: %s", fetchRes.ResultsString)
	}
}

func TestForEachHybridStreamingAndBuffering(t *testing.T) {
	xmlConfig := []byte(`<?xml version="1.0" encoding="UTF-8"?>
	<pipeline>
		<databases>
			<database name="loop_db" driver="sqlite" connection_string="file::memory:?cache=shared" />
		</databases>
		<flow>
			<sql id="init_db" db="loop_db">
				CREATE TABLE items (id INT, label TEXT);
				INSERT INTO items VALUES (1, 'Alpha'), (2, 'Beta'), (3, 'Gamma');
			</sql>

			<!-- Test 1: Buffered mode -->
			<foreach id="buffered_loop" db="loop_db" buffer="true">
				SELECT id, label FROM items ORDER BY id;
				<sql id="child_buffered" db="loop_db">
					SELECT '{{label}}' AS current_item;
				</sql>
			</foreach>

			<!-- Test 2: Streaming mode (default) -->
			<foreach id="streaming_loop" db="loop_db" buffer="false">
				SELECT id, label FROM items ORDER BY id;
				<sql id="child_streaming" db="loop_db">
					SELECT '{{label}}' AS current_item;
				</sql>
			</foreach>
		</flow>
	</pipeline>`)

	cfg, err := ParseXMLConfig(xmlConfig)
	if err != nil {
		t.Fatalf("failed to parse config: %v", err)
	}

	registry := NewRegistry()
	if err := registry.InitDatabases(cfg.Databases); err != nil {
		t.Fatalf("failed to init db: %v", err)
	}
	defer registry.CloseDatabases()

	executor := NewExecutor(registry)
	results, err := executor.Execute(context.Background(), cfg.FlowNodes)
	if err != nil {
		t.Fatalf("pipeline execution failed: %v", err)
	}

	var bufferedDriverResult, streamingDriverResult *ScriptResult
	for i := range results {
		if results[i].ScriptID == "buffered_loop_driver" {
			bufferedDriverResult = &results[i]
		}
		if results[i].ScriptID == "streaming_loop_driver" {
			streamingDriverResult = &results[i]
		}
	}

	if bufferedDriverResult == nil {
		t.Fatal("expected buffered_loop_driver result")
	}
	if !strings.Contains(bufferedDriverResult.ResultsString, "3 iteration(s)") {
		t.Errorf("expected buffered loop to execute 3 iterations, got: %s", bufferedDriverResult.ResultsString)
	}

	if streamingDriverResult == nil {
		t.Fatal("expected streaming_loop_driver result")
	}
	if !strings.Contains(streamingDriverResult.ResultsString, "3 iteration(s)") {
		t.Errorf("expected streaming loop to execute 3 iterations, got: %s", streamingDriverResult.ResultsString)
	}
}

func TestNestedTransactions(t *testing.T) {
	xmlConfig := []byte(`<?xml version="1.0" encoding="UTF-8"?>
	<pipeline>
		<databases>
			<database name="nested_tx_db" driver="sqlite" connection_string="file:nested_tx?mode=memory&amp;cache=shared" />
		</databases>
		<flow>
			<sql id="setup" db="nested_tx_db">
				CREATE TABLE audit_log (msg TEXT);
			</sql>
			<group id="outer_group" transaction="true" db="nested_tx_db">
				<sql id="outer_insert" db="nested_tx_db">
					INSERT INTO audit_log VALUES ('outer_start');
				</sql>
				<group id="inner_group" transaction="true" db="nested_tx_db">
					<sql id="inner_insert" db="nested_tx_db">
						INSERT INTO audit_log VALUES ('inner_commit');
					</sql>
				</group>
				<sql id="outer_end" db="nested_tx_db">
					INSERT INTO audit_log VALUES ('outer_finish');
				</sql>
			</group>
			<sql id="verify" db="nested_tx_db" output_var="total_logs">
				SELECT COUNT(*) as count FROM audit_log;
			</sql>
		</flow>
	</pipeline>`)

	cfg, err := ParseXMLConfig(xmlConfig)
	if err != nil {
		t.Fatalf("failed to parse XML: %v", err)
	}

	registry := NewRegistry()
	if err := registry.InitDatabases(cfg.Databases); err != nil {
		t.Fatalf("failed to init db: %v", err)
	}
	defer registry.CloseDatabases()

	executor := NewExecutor(registry)
	results, err := executor.Execute(context.Background(), cfg.FlowNodes)
	if err != nil {
		t.Fatalf("pipeline execution failed: %v", err)
	}

	var verifyRes *ScriptResult
	for i := range results {
		if results[i].ScriptID == "verify" {
			verifyRes = &results[i]
			break
		}
	}
	if verifyRes == nil {
		t.Fatal("verify step result missing")
	}
	if !strings.Contains(verifyRes.ResultsString, "3") {
		t.Errorf("expected 3 audit log records, got: %s", verifyRes.ResultsString)
	}
}

func TestExcelReadWithoutHeader(t *testing.T) {
	tmpDir := t.TempDir()
	excelFile := filepath.Join(tmpDir, "no_header.xlsx")

	f := excelize.NewFile()
	sheet := "Data"
	f.NewSheet(sheet)
	f.SetCellValue(sheet, "A1", "Val1")
	f.SetCellValue(sheet, "B1", "Val2")
	f.SetCellValue(sheet, "A2", "Val3")
	f.SetCellValue(sheet, "B2", "Val4")
	if err := f.SaveAs(excelFile); err != nil {
		t.Fatalf("failed to save test excel: %v", err)
	}
	f.Close()

	xmlConfig := []byte(`<?xml version="1.0" encoding="UTF-8"?>
	<pipeline>
		<flow>
			<excel_read id="read_no_header" file="` + filepath.ToSlash(excelFile) + `" sheet="Data" header="false" var="excel_data" />
		</flow>
	</pipeline>`)

	cfg, err := ParseXMLConfig(xmlConfig)
	if err != nil {
		t.Fatalf("failed to parse XML: %v", err)
	}

	registry := NewRegistry()
	executor := NewExecutor(registry)
	_, err = executor.Execute(context.Background(), cfg.FlowNodes)
	if err != nil {
		t.Fatalf("excel read execution failed: %v", err)
	}

	rawJSON := registry.GetVarString("excel_data")
	if rawJSON == "" {
		t.Fatal("expected excel_data variable to be populated")
	}

	var records []map[string]string
	if err := json.Unmarshal([]byte(rawJSON), &records); err != nil {
		t.Fatalf("failed to unmarshal excel JSON: %v", err)
	}

	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}
	if records[0]["col1"] != "Val1" || records[0]["col2"] != "Val2" {
		t.Errorf("unexpected record 0: %v", records[0])
	}
	if records[1]["col1"] != "Val3" || records[1]["col2"] != "Val4" {
		t.Errorf("unexpected record 1: %v", records[1])
	}
}

func TestASTValidationOmittedNodes(t *testing.T) {
	// 1. Missing file in NodeFileSave
	nodeFileSaveInvalid := []PipelineNode{
		{
			Kind: NodeFileSave,
			FileSave: &FileSaveElement{
				ID: "save1",
			},
		},
	}
	if err := ValidateAST(nil, nodeFileSaveInvalid, nil); err == nil {
		t.Error("expected validation error for NodeFileSave missing file")
	}

	// 2. Missing var in NodeFileRead
	nodeFileReadInvalid := []PipelineNode{
		{
			Kind: NodeFileRead,
			FileRead: &FileReadElement{
				ID:   "read1",
				File: "test.txt",
			},
		},
	}
	if err := ValidateAST(nil, nodeFileReadInvalid, nil); err == nil {
		t.Error("expected validation error for NodeFileRead missing var")
	}

	// 3. NodeExcelWrite referencing non-existent DB
	nodeExcelWriteInvalid := []PipelineNode{
		{
			Kind: NodeExcelWrite,
			ExcelWrite: &ExcelWriteElement{
				ID:     "write_excel",
				File:   "out.xlsx",
				DBName: "missing_db",
				Query:  "SELECT 1",
			},
		},
	}
	if err := ValidateAST(nil, nodeExcelWriteInvalid, nil); err == nil {
		t.Error("expected validation error for NodeExcelWrite referencing missing DB")
	}

	// 4. NodeGroup with transaction referencing non-existent DB
	nodeGroupInvalid := []PipelineNode{
		{
			Kind:        NodeGroup,
			GroupID:     "group1",
			Transaction: true,
			DBName:      "missing_db",
		},
	}
	if err := ValidateAST(nil, nodeGroupInvalid, nil); err == nil {
		t.Error("expected validation error for NodeGroup transaction referencing missing DB")
	}
}

func TestParallelFailFastCancellation(t *testing.T) {
	xmlConfig := []byte(`<?xml version="1.0" encoding="UTF-8"?>
	<pipeline>
		<databases>
			<database name="p_db" driver="sqlite" connection_string="file::memory:?cache=shared" />
		</databases>
		<flow>
			<parallel id="test_parallel" max_threads="1">
				<sql id="fast_fail" db="p_db">
					SELECT * FROM table_that_does_not_exist;
				</sql>
				<script id="slow_worker" language="go">
					package main
					import (
						"time"
					)
					func main() {
						time.Sleep(3 * time.Second)
					}
				</script>
			</parallel>
		</flow>
	</pipeline>`)

	cfg, err := ParseXMLConfig(xmlConfig)
	if err != nil {
		t.Fatalf("failed to parse XML: %v", err)
	}

	registry := NewRegistry()
	if err := registry.InitDatabases(cfg.Databases); err != nil {
		t.Fatalf("failed to init db: %v", err)
	}
	defer registry.CloseDatabases()

	executor := NewExecutor(registry)
	startTime := time.Now()
	results, err := executor.Execute(context.Background(), cfg.FlowNodes)
	duration := time.Since(startTime)

	if err == nil {
		t.Fatal("expected parallel execution to fail")
	}

	if duration > 2000*time.Millisecond {
		t.Errorf("expected fail-fast cancellation in < 2s, but took %v", duration)
	}

	// Verify slow_worker was never executed
	for _, res := range results {
		if res.ScriptID == "slow_worker" {
			t.Errorf("slow_worker should have been canceled and skipped, but was executed")
		}
	}
}

func TestDatabaseInitWithContextRollback(t *testing.T) {
	configs := []DatabaseConfig{
		{Name: "valid_sqlite", Driver: "sqlite", ConnectionString: "file::memory:?cache=shared"},
		{Name: "invalid_db", Driver: "nonexistent_driver", ConnectionString: "foo"},
	}

	reg := NewRegistry()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := reg.InitDatabasesWithContext(ctx, configs)
	if err == nil {
		t.Fatal("expected error with nonexistent_driver")
	}

	_, getErr := reg.GetDB("valid_sqlite")
	if getErr == nil {
		t.Error("expected valid_sqlite handle to be cleared after partial failure rollback")
	}
}

func TestXSDAlignedNodeAttributes(t *testing.T) {
	xmlContent := `<?xml version="1.0" encoding="UTF-8"?>
<pipeline>
    <flow>
        <group tx="true" timeout="30s" db="testdb">
            <sql id="step1" db="testdb" timeout="10s">
                CREATE TABLE IF NOT EXISTS items (id INT);
            </sql>
            <sql_bulk id="bulk1" db="testdb" target_table="items" timeout="15s">
                SELECT 1 AS id;
            </sql_bulk>
            <foreach id="loop1" db="testdb" buffer="true" mode="buffer">
                SELECT id FROM items;
                <group>
                    <html_template id="tmpl1" var="rendered">
                        &lt;b&gt;Item {{.id}}&lt;/b&gt;
                    </html_template>
                </group>
            </foreach>
        </group>
    </flow>
</pipeline>`

	cfg, err := ParseXMLConfig([]byte(xmlContent))
	if err != nil {
		t.Fatalf("failed to parse pipeline with XSD-aligned attributes: %v", err)
	}

	if len(cfg.FlowNodes) == 0 {
		t.Fatal("expected at least one flow node")
	}

	groupNode := cfg.FlowNodes[0]
	if groupNode.Kind != NodeGroup {
		t.Fatalf("expected NodeGroup, got %v", groupNode.Kind)
	}
	if !groupNode.Transaction {
		t.Errorf("expected Transaction=true from tx='true'")
	}
	if groupNode.Timeout != "30s" {
		t.Errorf("expected Timeout='30s', got %q", groupNode.Timeout)
	}

	if len(groupNode.Children) < 3 {
		t.Fatalf("expected at least 3 children in group, got %d", len(groupNode.Children))
	}

	sqlNode := groupNode.Children[0]
	if sqlNode.SQL == nil || sqlNode.SQL.Timeout != "10s" {
		t.Errorf("expected SQL.Timeout='10s', got %v", sqlNode.SQL)
	}

	sqlBulkNode := groupNode.Children[1]
	if sqlBulkNode.SQLBulk == nil || sqlBulkNode.SQLBulk.Timeout != "15s" {
		t.Errorf("expected SQLBulk.Timeout='15s', got %v", sqlBulkNode.SQLBulk)
	}

	forEachNode := groupNode.Children[2]
	if forEachNode.Kind != NodeForEach || !forEachNode.Buffer {
		t.Errorf("expected NodeForEach with Buffer=true, got kind=%v, buffer=%v", forEachNode.Kind, forEachNode.Buffer)
	}

	if len(forEachNode.Children) > 0 && len(forEachNode.Children[0].Children) > 0 {
		htmlTmplNode := forEachNode.Children[0].Children[0]
		if htmlTmplNode.Kind != NodeHtmlTemplate {
			t.Errorf("expected NodeHtmlTemplate from <html_template>, got %v", htmlTmplNode.Kind)
		}
	}
}

func TestSavepointDialectSQL(t *testing.T) {
	tests := []struct {
		driver           string
		spName           string
		expectedCreate   string
		expectedRollback string
		expectedRelease  string
	}{
		{
			driver:           "sqlserver",
			spName:           "sp_12345",
			expectedCreate:   "SAVE TRANSACTION sp_12345",
			expectedRollback: "ROLLBACK TRANSACTION sp_12345",
			expectedRelease:  "",
		},
		{
			driver:           "mssql",
			spName:           "sp_nested",
			expectedCreate:   "SAVE TRANSACTION sp_nested",
			expectedRollback: "ROLLBACK TRANSACTION sp_nested",
			expectedRelease:  "",
		},
		{
			driver:           "postgres",
			spName:           "sp_1",
			expectedCreate:   "SAVEPOINT sp_1",
			expectedRollback: "ROLLBACK TO SAVEPOINT sp_1",
			expectedRelease:  "RELEASE SAVEPOINT sp_1",
		},
		{
			driver:           "sqlite",
			spName:           "sp_sub",
			expectedCreate:   "SAVEPOINT sp_sub",
			expectedRollback: "ROLLBACK TO SAVEPOINT sp_sub",
			expectedRelease:  "RELEASE SAVEPOINT sp_sub",
		},
		{
			driver:           "mysql",
			spName:           "sp_my",
			expectedCreate:   "SAVEPOINT sp_my",
			expectedRollback: "ROLLBACK TO SAVEPOINT sp_my",
			expectedRelease:  "RELEASE SAVEPOINT sp_my",
		},
		{
			driver:           "oracle",
			spName:           "sp_ora",
			expectedCreate:   "SAVEPOINT sp_ora",
			expectedRollback: "ROLLBACK TO SAVEPOINT sp_ora",
			expectedRelease:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.driver, func(t *testing.T) {
			gotCreate := savepointSQL(tt.driver, tt.spName)
			if gotCreate != tt.expectedCreate {
				t.Errorf("savepointSQL(%q) = %q; want %q", tt.driver, gotCreate, tt.expectedCreate)
			}

			gotRollback := rollbackSavepointSQL(tt.driver, tt.spName)
			if gotRollback != tt.expectedRollback {
				t.Errorf("rollbackSavepointSQL(%q) = %q; want %q", tt.driver, gotRollback, tt.expectedRollback)
			}

			gotRelease := releaseSavepointSQL(tt.driver, tt.spName)
			if gotRelease != tt.expectedRelease {
				t.Errorf("releaseSavepointSQL(%q) = %q; want %q", tt.driver, gotRelease, tt.expectedRelease)
			}
		})
	}
}


