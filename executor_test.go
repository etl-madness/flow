package flow

import (
	"os/exec"
	"runtime"
	"strings"
	"testing"
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

	varConfigs, dbConfigs, nodes, err := ParseXMLConfig(xmlConfig)
	if err != nil {
		t.Fatalf("failed to parse XML: %v", err)
	}

	registry := NewRegistry()
	if err := registry.InitVariables(varConfigs); err != nil {
		t.Fatalf("failed to init variables: %v", err)
	}
	if err := registry.InitDatabases(dbConfigs); err != nil {
		t.Fatalf("failed to init databases: %v", err)
	}
	defer registry.CloseDatabases()

	executor := NewExecutor(registry)
	results, err := executor.Execute(nodes)
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

	varConfigs, dbConfigs, nodes, err := ParseXMLConfig(xmlConfig)
	if err != nil {
		t.Fatalf("failed to parse XML: %v", err)
	}

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
	_, err = executor.Execute([]PipelineNode{nodes[0]})
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	// 2. Run success group (commits 1 and 2)
	_, err = executor.Execute([]PipelineNode{nodes[1]})
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
	_, err = executor.Execute([]PipelineNode{nodes[2]})
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
func TestDotnetScriptExecution(t *testing.T) {
	hasDotnetScript := false
	if _, err := exec.LookPath("dotnet-script"); err == nil {
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

	varConfigs, dbConfigs, nodes, err := ParseXMLConfig(xmlConfig)
	if err != nil {
		t.Fatalf("failed to parse XML: %v", err)
	}

	registry := NewRegistry()
	if err := registry.InitVariables(varConfigs); err != nil {
		t.Fatalf("failed to init variables: %v", err)
	}
	if err := registry.InitDatabases(dbConfigs); err != nil {
		t.Fatalf("failed to init databases: %v", err)
	}
	defer registry.CloseDatabases()

	executor := NewExecutor(registry)
	results, err := executor.Execute(nodes)
	if err != nil {
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

