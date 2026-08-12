package flow

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"
)

func TestExecutorVerboseMode(t *testing.T) {
	xmlConfig := []byte(`<?xml version="1.0" encoding="UTF-8"?>
	<pipeline>
		<variables>
			<variable name="TestVar" value="hello" />
		</variables>
		<scripts>
			<script id="TestVerboseGo" language="go">
				package main
				import "fmt"
				func main() {
					fmt.Println("Running internal go test")
				}
			</script>
		</scripts>
	</pipeline>`)

	varConfigs, _, nodes, err := ParseXMLConfig(xmlConfig)
	if err != nil {
		t.Fatalf("failed to parse XML: %v", err)
	}

	registry := NewRegistry()
	if err := registry.InitVariables(varConfigs); err != nil {
		t.Fatalf("failed to init variables: %v", err)
	}

	executor := NewExecutor(registry)
	executor.SetVerbose(true)

	// Capture stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	_, err = executor.Execute(nodes)
	w.Close()
	os.Stdout = oldStdout

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		t.Fatalf("failed to copy captured stdout: %v", err)
	}

	output := buf.String()

	// Assertions
	if !strings.Contains(output, `Starting execution of script "TestVerboseGo"`) {
		t.Errorf("expected output to contain start message, got:\n%s", output)
	}

	if !strings.Contains(output, `Finished execution of script "TestVerboseGo"`) {
		t.Errorf("expected output to contain finish message, got:\n%s", output)
	}
}

