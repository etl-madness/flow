package flow

import (
	"testing"
)

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
