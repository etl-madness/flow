package flow

import (
	"context"
	"strings"
	"testing"
)

// TestParseXMLConfig_Assert tests XML unmarshaling of <assert> tags,
// including default ID assignment, attribute parsing, and <on_failure> blocks.
func TestParseXMLConfig_Assert(t *testing.T) {
	xmlData := []byte(`
	<flow>
		<variable name="ENV" type="string" value="production"/>
		
		<!-- Explicit ID and basic attributes -->
		<assert id="check_env" var="ENV" equals="production" operator="==" message="Environment must be production" on_failure="halt"/>

		<!-- Omitted ID (auto-generated ID) with failure variables and child nodes -->
		<assert var="ENV" value="staging" on_failure="warn" fail_var="IS_STAGING" fail_val="false">
			<on_failure>
				<script id="fallback_script" language="go">
					println("Handling assertion failure...")
				</script>
			</on_failure>
		</assert>
	</flow>
	`)

	vars, _, nodes, err := ParseXMLConfig(xmlData)
	if err != nil {
		t.Fatalf("ParseXMLConfig failed unexpectedly: %v", err)
	}

	if len(vars) != 1 {
		t.Fatalf("Expected 1 variable, got %d", len(vars))
	}
	if len(nodes) != 2 {
		t.Fatalf("Expected 2 AST nodes, got %d", len(nodes))
	}

	// 1. Verify Node 1 (Explicit attributes)
	n1 := nodes[0]
	if n1.Kind != NodeAssert {
		t.Fatalf("Expected first node kind to be NodeAssert, got %v", n1.Kind)
	}
	a1 := n1.Assert
	if a1 == nil {
		t.Fatal("Expected Assert payload to be non-nil")
	}
	if a1.ID != "check_env" {
		t.Errorf("Expected ID 'check_env', got %q", a1.ID)
	}
	if a1.Var != "ENV" || a1.Equals != "production" || a1.Operator != "==" {
		t.Errorf("Attributes mismatch on n1: var=%q, equals=%q, operator=%q", a1.Var, a1.Equals, a1.Operator)
	}
	if a1.OnFailure != "halt" || a1.Message != "Environment must be production" {
		t.Errorf("Failure handler mismatch on n1: on_failure=%q, message=%q", a1.OnFailure, a1.Message)
	}

	// 2. Verify Node 2 (Auto-generated ID & <on_failure> child block)
	n2 := nodes[1]
	if n2.Kind != NodeAssert {
		t.Fatalf("Expected second node kind to be NodeAssert, got %v", n2.Kind)
	}
	a2 := n2.Assert
	if a2 == nil {
		t.Fatal("Expected Assert payload on second node to be non-nil")
	}
	if a2.ID != "assert_1" {
		t.Errorf("Expected auto-assigned ID 'assert_1', got %q", a2.ID)
	}
	if a2.Value != "staging" || a2.FailVar != "IS_STAGING" || a2.FailVal != "false" {
		t.Errorf("Attributes mismatch on n2: value=%q, fail_var=%q, fail_val=%q", a2.Value, a2.FailVar, a2.FailVal)
	}
	if len(a2.FailureNodes) != 1 {
		t.Fatalf("Expected 1 node inside <on_failure>, got %d", len(a2.FailureNodes))
	}
	if a2.FailureNodes[0].Kind != NodeScript {
		t.Errorf("Expected failure node to be NodeScript, got %v", a2.FailureNodes[0].Kind)
	}
}

// TestValidateAST_Assert validates semantic constraints for <assert> nodes.
func TestValidateAST_Assert(t *testing.T) {
	t.Run("Valid Assert Node", func(t *testing.T) {
		nodes := []PipelineNode{
			{
				Kind: NodeAssert,
				Assert: &AssertElement{
					ID:  "valid_assert",
					Var: "STATUS_CODE",
				},
			},
		}
		if err := ValidateAST(nodes, nil); err != nil {
			t.Errorf("Expected AST validation to pass, got error: %v", err)
		}
	})

	t.Run("Missing Var Attribute", func(t *testing.T) {
		nodes := []PipelineNode{
			{
				Kind: NodeAssert,
				Assert: &AssertElement{
					ID: "invalid_assert",
				},
			},
		}
		err := ValidateAST(nodes, nil)
		if err == nil {
			t.Fatal("Expected error for missing 'var' attribute, got nil")
		}
		if !strings.Contains(err.Error(), "missing required 'var' attribute") {
			t.Errorf("Unexpected error message: %v", err)
		}
	})

	t.Run("Duplicate Assert ID", func(t *testing.T) {
		nodes := []PipelineNode{
			{Kind: NodeAssert, Assert: &AssertElement{ID: "dup_id", Var: "VAR1"}},
			{Kind: NodeAssert, Assert: &AssertElement{ID: "dup_id", Var: "VAR2"}},
		}
		err := ValidateAST(nodes, nil)
		if err == nil {
			t.Fatal("Expected error for duplicate assertion ID, got nil")
		}
		if !strings.Contains(err.Error(), "duplicate ID found") {
			t.Errorf("Unexpected error message: %v", err)
		}
	})

	t.Run("Recursive FailureNodes Validation", func(t *testing.T) {
		nodes := []PipelineNode{
			{
				Kind: NodeAssert,
				Assert: &AssertElement{
					ID:  "parent_assert",
					Var: "VAR1",
					FailureNodes: []PipelineNode{
						{
							Kind: NodeAssert,
							Assert: &AssertElement{
								ID:  "parent_assert", // Duplicate ID inside <on_failure>
								Var: "VAR2",
							},
						},
					},
				},
			},
		}
		err := ValidateAST(nodes, nil)
		if err == nil {
			t.Fatal("Expected error for duplicate ID inside failure nodes, got nil")
		}
	})
}

// TestExecutor_Assert_Execution tests runtime evaluation of assertions under various conditions.
func TestExecutor_Assert_Execution(t *testing.T) {
	t.Run("Successful Assertion", func(t *testing.T) {
		reg := NewRegistry()
		reg.SetVar("COUNT", "10")

		nodes := []PipelineNode{
			{
				Kind: NodeAssert,
				Assert: &AssertElement{
					ID:     "assert_success",
					Var:    "COUNT",
					Equals: "10",
				},
			},
		}

		executor := NewExecutor(reg)
		results, err := executor.Execute(context.Background(), nodes)
		if err != nil {
			t.Fatalf("Executor failed: %v", err)
		}

		if len(results) != 1 {
			t.Fatalf("Expected 1 result, got %d", len(results))
		}
		if results[0].ReturnCode != 0 {
			t.Errorf("Expected ReturnCode 0, got %v", results[0].ReturnCode)
		}
	})

	t.Run("Failed Assertion - Halt Behavior (Default)", func(t *testing.T) {
		reg := NewRegistry()
		reg.SetVar("COUNT", "5")

		executedNextStep := false
		nodes := []PipelineNode{
			{
				Kind: NodeAssert,
				Assert: &AssertElement{
					ID:        "assert_halt",
					Var:       "COUNT",
					Equals:    "10",
					OnFailure: "halt",
					Message:   "Count does not match expected value",
				},
			},
			{
				Kind: NodeScript,
				Script: &ScriptItem{
					ID:       "subsequent_script",
					Language: "go",
					Code:     `println("Should not execute")`,
				},
			},
		}

		executor := NewExecutor(reg)
		results, err := executor.Execute(context.Background(), nodes)

		if err == nil {
			t.Fatal("Expected pipeline execution error on assertion halt, got nil")
		}
		if len(results) != 1 {
			t.Fatalf("Expected 1 result (execution halted before second node), got %d", len(results))
		}
		if results[0].ReturnCode == 0 {
			t.Errorf("Expected non-zero ReturnCode on failed assertion")
		}
		if executedNextStep {
			t.Error("Subsequent step was executed after halt assertion failed")
		}
	})

	t.Run("Failed Assertion - Warn/Continue Behavior", func(t *testing.T) {
		reg := NewRegistry()
		reg.SetVar("ENV", "development")

		nodes := []PipelineNode{
			{
				Kind: NodeAssert,
				Assert: &AssertElement{
					ID:        "assert_warn",
					Var:       "ENV",
					Equals:    "production",
					OnFailure: "warn",
					Message:   "Not running in production",
				},
			},
			{
				Kind: NodeAssert,
				Assert: &AssertElement{
					ID:     "subsequent_assert",
					Var:    "ENV",
					Equals: "development",
				},
			},
		}

		executor := NewExecutor(reg)
		results, err := executor.Execute(context.Background(), nodes)

		if err != nil {
			t.Fatalf("Execution should not fail when on_failure is set to warn, got: %v", err)
		}
		if len(results) != 2 {
			t.Fatalf("Expected 2 results (continued execution), got %d", len(results))
		}
		if results[0].ReturnCode != 0 {
			t.Errorf("Expected warning node ReturnCode to be 0, got %v", results[0].ReturnCode)
		}
		if !strings.Contains(results[0].ResultsString, "WARNING:") {
			t.Errorf("Expected result string to contain WARNING prefix, got %q", results[0].ResultsString)
		}
	})

	t.Run("Failed Assertion - Sets FailVar and Runs FailureNodes", func(t *testing.T) {
		reg := NewRegistry()
		reg.SetVar("STATUS", "FAILED")

		nodes := []PipelineNode{
			{
				Kind: NodeAssert,
				Assert: &AssertElement{
					ID:        "assert_fallback",
					Var:       "STATUS",
					Equals:    "SUCCESS",
					OnFailure: "warn",
					FailVar:   "PIPELINE_HAS_ERRORS",
					FailVal:   "true",
					FailureNodes: []PipelineNode{
						{
							Kind: NodeAssert,
							Assert: &AssertElement{
								ID:     "nested_failure_handler",
								Var:    "STATUS",
								Equals: "FAILED",
							},
						},
					},
				},
			},
		}

		executor := NewExecutor(reg)
		results, err := executor.Execute(context.Background(), nodes)
		if err != nil {
			t.Fatalf("Execution failed unexpectedly: %v", err)
		}

		// 1. Check if FailVar was stored in the registry
		failVarVal := reg.GetVarString("PIPELINE_HAS_ERRORS")
		if failVarVal != "true" {
			t.Errorf("Expected registry variable 'PIPELINE_HAS_ERRORS' to be 'true', got %q", failVarVal)
		}

		// 2. Check if FailureNodes were executed (Primary Assert + Nested Assert = 2 results)
		if len(results) != 2 {
			t.Fatalf("Expected 2 execution results (including nested failure node), got %d", len(results))
		}
		if results[0].ScriptID != "nested_failure_handler" {
			t.Errorf("Expected first result ID to be 'nested_failure_handler', got %q", results[0].ScriptID)
		}
		if results[1].ScriptID != "assert_fallback" {
			t.Errorf("Expected second result ID to be 'assert_fallback', got %q", results[1].ScriptID)
		}
	})
}
