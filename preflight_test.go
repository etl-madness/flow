package flow

import (
	"context"
	"strings"
	"testing"
)

func TestPreflight_ParsingAndExecution(t *testing.T) {
	xmlConfig := []byte(`<?xml version="1.0" encoding="UTF-8"?>
	<pipeline>
		<variables>
			<variable name="preflight_done" type="string" value="no" />
			<variable name="flow_done" type="string" value="no" />
		</variables>

		<preflight>
			<script id="preflight_script" language="go">
				package main
				import "fmt"
				func main() {
					fmt.Println("Executing Preflight Script")
				}
			</script>
			<file_save id="preflight_save" file="preflight_marker.txt" var="preflight_done" />
		</preflight>

		<flow>
			<script id="flow_script" language="go">
				package main
				import "fmt"
				func main() {
					fmt.Println("Executing Flow Script")
				}
			</script>
		</flow>
	</pipeline>`)

	cfg, err := ParseXMLConfig(xmlConfig)
	if err != nil {
		t.Fatalf("ParseXMLConfig failed unexpectedly: %v", err)
	}

	// 1. Verify Preflight parsing
	if len(cfg.PreflightNodes) != 2 {
		t.Fatalf("Expected 2 preflight nodes, got %d", len(cfg.PreflightNodes))
	}
	if len(cfg.FlowNodes) != 1 {
		t.Fatalf("Expected 1 flow node, got %d", len(cfg.FlowNodes))
	}

	// 2. Validate AST
	if err := ValidateAST(cfg.PreflightNodes, cfg.FlowNodes, cfg.Databases); err != nil {
		t.Fatalf("ValidateAST failed unexpectedly: %v", err)
	}

	// 3. Execute Preflight nodes
	registry := NewRegistry()
	if err := registry.InitVariables(cfg.Variables); err != nil {
		t.Fatalf("failed to init variables: %v", err)
	}

	executor := NewExecutor(registry)
	results, err := executor.Execute(context.Background(), cfg.PreflightNodes)
	if err != nil {
		t.Fatalf("execution of preflight nodes failed: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 execution results from preflight, got %d", len(results))
	}
}

func TestPreflight_ValidationDuplicateID(t *testing.T) {
	// Preflight script ID matches Flow script ID
	nodes1 := []PipelineNode{
		{
			Kind: NodeScript,
			Script: &ScriptItem{
				ID:       "duplicate_id",
				Language: "go",
				Code:     `package main; func main() {}`,
			},
		},
	}
	nodes2 := []PipelineNode{
		{
			Kind: NodeScript,
			Script: &ScriptItem{
				ID:       "duplicate_id",
				Language: "go",
				Code:     `package main; func main() {}`,
			},
		},
	}

	err := ValidateAST(nodes1, nodes2, nil)
	if err == nil {
		t.Fatal("Expected validation error for duplicate ID across preflight and flow, but got nil")
	}
	if !strings.Contains(err.Error(), "duplicate ID found") && !strings.Contains(err.Error(), "duplicate script ID found") {
		t.Errorf("Expected duplicate ID error message, got: %v", err)
	}
}
