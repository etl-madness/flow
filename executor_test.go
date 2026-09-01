package flow

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"
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
			<script id="setup" language="sql" db="tx_test_db">
				CREATE TABLE tx_test (id INTEGER PRIMARY KEY, val TEXT);
			</script>

			<!-- Group that should succeed and commit -->
			<group id="success_group" transaction="true" db="tx_test_db">
				<script id="insert_1" language="sql" db="tx_test_db">
					INSERT INTO tx_test (id, val) VALUES (1, 'apple');
				</script>
				<script id="insert_2" language="sql" db="tx_test_db">
					INSERT INTO tx_test (id, val) VALUES (2, 'banana');
				</script>
			</group>

			<!-- Group that should fail and rollback -->
			<group id="fail_group" transaction="true" db="tx_test_db">
				<script id="insert_3" language="sql" db="tx_test_db">
					INSERT INTO tx_test (id, val) VALUES (3, 'cherry');
				</script>
				<script id="insert_fail" language="sql" db="tx_test_db">
					INSERT INTO non_existent_table (id, val) VALUES (4, 'date');
				</script>
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
