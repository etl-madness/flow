package flow

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	lua "github.com/yuin/gopher-lua"
)

func TestRegistryTypedValuesAndSnapshot(t *testing.T) {
	registry := NewRegistry()
	registry.SetVars(map[string]interface{}{
		"bool":        true,
		"bool_string": "TRUE",
		"bad_bool":    "not-bool",
		"int64":       int64(12),
		"float64-int": float64(8),
		"string-int":  "9",
		"bad-int":     "not-int",
		"float":       float32(2.5),
		"float64":     float64(3.5),
		"bad_float":   "not-float",
		"time":        "2025-01-02T03:04:05Z",
		"empty_time":  " ",
	})

	if !registry.GetVarBool("bool") || !registry.GetVarBool("bool_string") {
		t.Fatal("expected boolean values to be converted")
	}
	if registry.GetVarBool("bad_bool") || registry.GetVarBool("missing") {
		t.Fatal("invalid and missing boolean values should be false")
	}
	if got := registry.GetVarInt("int64"); got != 12 {
		t.Fatalf("GetVarInt(int64) = %d, want 12", got)
	}
	if registry.GetVarInt("float64-int") != 8 || registry.GetVarInt("string-int") != 9 ||
		registry.GetVarInt("bad-int") != 0 || registry.GetVarInt("missing") != 0 {
		t.Fatal("GetVarInt() did not handle conversion and fallback cases")
	}
	if got := registry.GetVarFloat("float"); got != 2.5 {
		t.Fatalf("GetVarFloat(float32) = %v, want 2.5", got)
	}
	if registry.GetVarFloat("float64") != 3.5 || registry.GetVarFloat("int64") != 12 ||
		registry.GetVarFloat("float64-int") != 8 || registry.GetVarFloat("string-int") != 9 {
		t.Fatal("GetVarFloat() did not handle numeric values")
	}
	if registry.GetVarFloat("bad_float") != 0 || registry.GetVarFloat("missing") != 0 {
		t.Fatal("invalid and missing float values should be zero")
	}
	if got := registry.GetVarTime("time"); !got.Equal(time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)) {
		t.Fatalf("GetVarTime() = %v, want parsed timestamp", got)
	}
	if !registry.GetVarTime("empty_time").IsZero() || !registry.GetVarTime("missing").IsZero() {
		t.Fatal("empty and missing times should be zero")
	}

	registry.MergeVariables(map[string]interface{}{"merged": "value"})
	registry.dbRegistry["db"] = DBHandle{Driver: "sqlite"}
	registry.boltRegistry["bolt"] = nil
	registry.badgerRegistry["badger"] = nil
	registry.etcdRegistry["etcd"] = nil
	registry.redisRegistry["redis"] = nil
	snapshot := registry.Snapshot()
	snapshot.SetVar("bool", false)
	if !registry.GetVarBool("bool") || snapshot.GetVarBool("bool") {
		t.Fatal("snapshot variable storage should be isolated")
	}
	if got := registry.CopyVariables()["merged"]; got != "value" {
		t.Fatalf("merged variable = %v, want value", got)
	}
}

func TestRegistryInitVariablesConversions(t *testing.T) {
	registry := NewRegistry()
	err := registry.InitVariables([]VariableConfig{
		{Name: "integer", Type: "integer", Value: "42"},
		{Name: "boolean", Type: "boolean", Value: "true"},
		{Name: "decimal", Type: "double", Value: "1.25"},
		{Name: "text", Type: "other", Value: "plain"},
	})
	if err != nil {
		t.Fatalf("InitVariables() error = %v", err)
	}
	if registry.GetVarInt("integer") != 42 || !registry.GetVarBool("boolean") ||
		registry.GetVarFloat("decimal") != 1.25 || registry.GetVarString("text") != "plain" {
		t.Fatal("InitVariables() did not preserve converted values")
	}
	for _, cfg := range []VariableConfig{
		{Name: "bad-int", Type: "int", Value: "x"},
		{Name: "bad-bool", Type: "bool", Value: "x"},
		{Name: "bad-float", Type: "float", Value: "x"},
	} {
		if err := registry.InitVariables([]VariableConfig{cfg}); err == nil {
			t.Errorf("InitVariables(%q) unexpectedly succeeded", cfg.Name)
		}
	}
}

func TestConfigElementAccessors(t *testing.T) {
	if got := (&KVElement{OutputVar: "out"}).GetOutputVar(); got != "out" {
		t.Errorf("KV output accessor = %q", got)
	}
	if got := (&KVElement{VarName: "fallback"}).GetOutputVar(); got != "fallback" {
		t.Errorf("KV fallback output accessor = %q", got)
	}
	if got := (&KVBulkElement{OutputVar: "out"}).GetOutputVar(); got != "out" {
		t.Errorf("KV bulk output accessor = %q", got)
	}
	if got := (&KVBulkElement{VarName: "fallback"}).GetOutputVar(); got != "fallback" {
		t.Errorf("KV bulk fallback output accessor = %q", got)
	}
	if got := (&HtmlTemplateElement{OutputVar: "out"}).GetOutputVar(); got != "out" {
		t.Errorf("HTML template output accessor = %q", got)
	}
	if got := (&HtmlTemplateElement{Var: "fallback"}).GetOutputVar(); got != "fallback" {
		t.Errorf("HTML template fallback output accessor = %q", got)
	}
	if got := (&TemplateElement{OutputVar: "out"}).GetOutputVar(); got != "out" {
		t.Errorf("template output accessor = %q", got)
	}
	if got := (&ExcelReadElement{Var: "fallback"}).GetOutputVar(); got != "fallback" {
		t.Errorf("Excel read fallback output accessor = %q", got)
	}
	if got := (&FileReadElement{Filename: "file", OutVar: "output"}).GetFilePath(); got != "file" {
		t.Errorf("file read path accessor = %q", got)
	}
	if got := (&FileReadElement{Variable: "output"}).GetOutputVar(); got != "output" {
		t.Errorf("file read output accessor = %q", got)
	}
	if got := (&JsonPathElement{JSONPath: "$.name"}).GetJSONPath(); got != "$.name" {
		t.Errorf("JSON path fallback accessor = %q", got)
	}
	if got := (&YamlPathElement{YAMLPath: "$.name"}).GetYAMLPath(); got != "$.name" {
		t.Errorf("YAML path fallback accessor = %q", got)
	}
	if got := stripCDATAWrapper("  <![cdata[ value ]]>\t"); got != "value" {
		t.Errorf("CDATA wrapper = %q", got)
	}
	if got := stripCDATAWrapper("not cdata"); got != "not cdata" {
		t.Errorf("unchanged CDATA input = %q", got)
	}
	if got := (&XmlXPathElement{OutVar: "xml-output"}).GetOutputVar(); got != "xml-output" {
		t.Errorf("XML XPath output accessor = %q", got)
	}
	if got := (&JsonPathElement{OutVar: "json-output"}).GetOutputVar(); got != "json-output" {
		t.Errorf("JSON path output accessor = %q", got)
	}
	if got := (&YamlPathElement{OutVar: "yaml-output"}).GetOutputVar(); got != "yaml-output" {
		t.Errorf("YAML path output accessor = %q", got)
	}
	if got := (&FileSaveElement{Path: "path", Variable: "input"}).GetFilePath(); got != "path" {
		t.Errorf("file save path accessor = %q", got)
	}
	if got := (&FileSaveElement{Variable: "input"}).GetInputVar(); got != "input" {
		t.Errorf("file save input accessor = %q", got)
	}
	if got := (&JsonPathElement{Content: "  $.content  "}).GetJSONPath(); got != "$.content" {
		t.Errorf("JSON path content accessor = %q", got)
	}
	if got := (&YamlPathElement{Content: "  $.content  "}).GetYAMLPath(); got != "$.content" {
		t.Errorf("YAML path content accessor = %q", got)
	}
	if got := (&XmlXPathElement{Content: "  /root  "}).GetXPath(); got != "/root" {
		t.Errorf("XML XPath content accessor = %q", got)
	}
	if got := (&HTTPClientElement{Variable: "body"}).GetOutputVariable(); got != "body" {
		t.Errorf("HTTP output accessor = %q", got)
	}
	if got := (&HTTPClientElement{StatusVariable: "status"}).GetStatusCodeVariable(); got != "status" {
		t.Errorf("HTTP status accessor = %q", got)
	}
}

func TestClassifyErrorCategories(t *testing.T) {
	cases := []struct {
		err  error
		want ErrorClass
	}{
		{err: nil, want: ""},
		{err: context.Canceled, want: ErrorClassCanceled},
		{err: &os.PathError{Op: "open", Path: "missing", Err: os.ErrNotExist}, want: ErrorClassFileSystem},
		{err: &url.Error{Op: "Get", URL: "https://example.test", Err: errors.New("failed")}, want: ErrorClassHTTP},
		{err: errors.New("assertion failed"), want: ErrorClassValidation},
		{err: errors.New("database transaction failed"), want: ErrorClassDatabase},
		{err: errors.New("HTTP request failed"), want: ErrorClassHTTP},
		{err: errors.New("file read failed"), want: ErrorClassFileSystem},
		{err: errors.New("template parse failed"), want: ErrorClassTemplate},
		{err: errors.New("invalid JSON document"), want: ErrorClassDataFormat},
		{err: errors.New("script execution failed"), want: ErrorClassScript},
		{err: errors.New("unclassified"), want: ErrorClassUnknown},
	}
	for _, tc := range cases {
		if got := ClassifyError(tc.err); got != tc.want {
			t.Errorf("ClassifyError(%v) = %q, want %q", tc.err, got, tc.want)
		}
	}
}

func TestExecutorSettersAndSinkRegistration(t *testing.T) {
	executor := NewExecutor(NewRegistry())
	sink := &recordingEventSink{}
	executor.SetGoPath("custom-go-path")
	executor.SetInterpreterHook(nil)
	executor.SetInterpHook(nil)
	executor.AddEventSink(nil)
	executor.AddEventSink(sink)
	executor.SetEventSink(sink)
	if executor.goPath != "custom-go-path" || len(executor.eventSinks) != 1 || executor.eventSinks[0] != sink {
		t.Fatalf("executor setters did not update configuration: %+v", executor)
	}
}

func TestExecutorConditionEvaluation(t *testing.T) {
	registry := NewRegistry()
	registry.SetVars(map[string]interface{}{
		"bool": true, "int": 1, "float": float64(1.5),
		"yes": " yes ", "other": "value", "false": false,
	})
	executor := NewExecutor(registry)
	cases := []struct {
		name     string
		varName  string
		expected string
		want     bool
	}{
		{name: "boolean", varName: "bool", want: true},
		{name: "integer", varName: "int", want: true},
		{name: "float", varName: "float", want: true},
		{name: "string truth", varName: "yes", want: true},
		{name: "expected value", varName: "other", expected: `"VALUE"`, want: true},
		{name: "equals syntax", varName: " other == value ", want: true},
		{name: "not equals syntax", varName: "other != different", want: true},
		{name: "false", varName: "false", want: false},
		{name: "missing", varName: "missing", want: false},
		{name: "unknown string", varName: "other", want: false},
		{name: "not equals false", varName: "other != value", want: false},
		{name: "numeric zero", varName: "zero", want: false},
		{name: "unsupported value", varName: "object", want: false},
	}
	registry.SetVar("zero", 0)
	registry.SetVar("object", []string{"value"})
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := executor.evalCondition(tc.varName, tc.expected); got != tc.want {
				t.Errorf("evalCondition(%q, %q) = %t, want %t", tc.varName, tc.expected, got, tc.want)
			}
		})
	}
}

func TestParseXMLConfigConditionalBranches(t *testing.T) {
	xmlConfig := []byte(`<pipeline>
		<flow>
			<if condition="enabled" equals="yes">
				<then><script id="then-script" lang="go">println("then")</script></then>
				<else><script id="else-script" lang="go">println("else")</script></else>
			</if>
		</flow>
	</pipeline>`)
	cfg, err := ParseXMLConfig(xmlConfig)
	if err != nil {
		t.Fatalf("ParseXMLConfig() error = %v", err)
	}
	if len(cfg.FlowNodes) != 1 || cfg.FlowNodes[0].Kind != NodeIf {
		t.Fatalf("parsed nodes = %#v, want one conditional", cfg.FlowNodes)
	}
	node := cfg.FlowNodes[0]
	if node.IfVar != "enabled" || node.IfEquals != "yes" ||
		len(node.Children) != 1 || len(node.ElseNodes) != 1 {
		t.Fatalf("conditional branches were not parsed correctly: %#v", node)
	}
	if err := ValidateAST(nil, cfg.FlowNodes, nil); err != nil {
		t.Fatalf("ValidateAST() rejected valid conditional: %v", err)
	}
}

func TestParseXMLConfigNodeFamilies(t *testing.T) {
	xmlConfig := []byte(`<config>
		<variables><variable name="count" type="int" value="4"/><variable value="ignored"/></variables>
		<databases>
			<database name="db" driver="SQLite" connection-string=":memory:"
				max-open-conns="3" max_idle_conns="2" conn_max_lifetime_seconds="10s"
				conn_max_idle_time="15" workload="batch"/>
			<database driver="postgres" connection_string="missing-name"/>
		</databases>
		<preflight><sql id="preflight-sql">SELECT 1</sql></preflight>
		<flow>
			<kv db="db" operation="GET" key="k">body</kv>
			<kv_bulk database="db" target_database="db" batch_size="2" var="source">scan</kv_bulk>
			<assert var="count" equals="4" on_failure="continue">
				<on_failure><script lang="go">println("failed")</script></on_failure>
			</assert>
			<sql database="db" timeout="1s">SELECT 1</sql>
			<sql_bulk db="db" target_db="db" target_table="items" batch_size="2"
				tablock="false" check_constraints="true" fire_triggers="true" keep_nulls="true">SELECT 1</sql_bulk>
			<yaml_path var="yaml" yamlpath="$.name"/>
			<json_path var="json" jsonpath="$.name"/>
			<xml_xpath var="xml" xpath="/root"/>
			<excel_read file="in.xlsx" header="false"/>
			<excel_write file="out.xlsx" db="db">SELECT 1</excel_write>
			<file_save path="out.txt" append="true">content</file_save>
			<file_read filename="in.txt" variable="input"/>
			<html_template content="&lt;p&gt;{{.name}}&lt;/p&gt;"/>
			<template name="text">hello</template>
			<http_client url="https://example.test"/>
			<script language="Lua" file_path="script.lua" var="code" output_var="output"><![CDATA[print("ok")]]></script>
			<group name="group" condition="ready" if_equals="yes" transaction="true" session="true" db="db" timeout="2s">
				<script lang="go">println("group")</script>
			</group>
			<parallel concurrency="2"><script lang="go">println("parallel")</script></parallel>
			<foreach id="rows" lang="sql" db="db" mode="buffer">SELECT 1<script lang="go">println("row")</script></foreach>
			<while condition="ready" equals="yes" max_loops="3"><script lang="go">println("loop")</script></while>
		</flow>
	</config>`)

	cfg, err := ParseXMLConfig(xmlConfig)
	if err != nil {
		t.Fatalf("ParseXMLConfig() error = %v", err)
	}
	if len(cfg.Variables) != 1 || len(cfg.Databases) != 1 || len(cfg.PreflightNodes) != 1 {
		t.Fatalf("top-level config parse = %#v", cfg)
	}
	if cfg.Databases[0].Driver != "sqlite" || cfg.Databases[0].MaxOpenConns != 3 ||
		cfg.Databases[0].ConnMaxLifetime != 10*time.Second ||
		cfg.Databases[0].ConnMaxIdleTime != 15*time.Second {
		t.Fatalf("database config parse = %#v", cfg.Databases[0])
	}
	if len(cfg.FlowNodes) != 20 {
		t.Fatalf("parsed %d flow nodes, want 20", len(cfg.FlowNodes))
	}
	if cfg.FlowNodes[2].Assert == nil || len(cfg.FlowNodes[2].Assert.FailureNodes) != 1 {
		t.Fatalf("assert failure nodes were not parsed: %#v", cfg.FlowNodes[2])
	}
	if cfg.FlowNodes[16].GroupID != "group" || !cfg.FlowNodes[16].Transaction || !cfg.FlowNodes[16].Session {
		t.Fatalf("group attributes were not parsed: %#v", cfg.FlowNodes[16])
	}
	if cfg.FlowNodes[18].ForEachScript == nil || !cfg.FlowNodes[18].Buffer {
		t.Fatalf("foreach attributes were not parsed: %#v", cfg.FlowNodes[18])
	}
	if cfg.FlowNodes[19].MaxIterations != 3 {
		t.Fatalf("while max iterations = %d, want 3", cfg.FlowNodes[19].MaxIterations)
	}
	if !isLikelyUTF8XML([]byte("<root/>")) || isLikelyUTF8XML([]byte{0, '<', 'x', '>'}) {
		t.Fatal("isLikelyUTF8XML() returned an incorrect result")
	}
}

func TestExecuteTextTemplateNodePaths(t *testing.T) {
	registry := NewRegistry()
	registry.SetVar("name", "Ada")
	executor := NewExecutor(registry)
	var results []ScriptResult

	if failed := executor.executeTemplateNode(context.Background(), TemplateElement{
		ID: "inline", Content: `Hello {{.name}}`, OutputVar: "rendered",
	}, &results); failed {
		t.Fatalf("inline template failed: %#v", results)
	}
	if registry.GetVarString("rendered") != "Hello Ada" {
		t.Fatalf("inline template output = %q", registry.GetVarString("rendered"))
	}

	path := filepath.Join(t.TempDir(), "template.txt")
	if err := os.WriteFile(path, []byte(`{{.name}}`), 0600); err != nil {
		t.Fatal(err)
	}
	results = nil
	if failed := executor.executeTemplateNode(context.Background(), TemplateElement{
		ID: "file", File: path, Var: "file-result", Mode: "summary",
	}, &results); failed {
		t.Fatalf("file template failed: %#v", results)
	}
	if registry.GetVarString("file-result") != "Ada" ||
		!strings.Contains(results[0].ResultsString, "Successfully rendered template") {
		t.Fatalf("file template result = %#v", results)
	}

	for _, elem := range []TemplateElement{
		{ID: "missing-file", File: filepath.Join(t.TempDir(), "missing.txt")},
		{ID: "bad-template", Content: `{{`},
		{ID: "execution-error", Content: `{{call .name}}`},
	} {
		results = nil
		if failed := executor.executeTemplateNode(context.Background(), elem, &results); !failed {
			t.Errorf("template %q unexpectedly succeeded", elem.ID)
		}
		if len(results) != 1 || results[0].ReturnCode == 0 {
			t.Errorf("template %q result = %#v, want error", elem.ID, results)
		}
	}
}

func TestExecuteFileNodesPaths(t *testing.T) {
	registry := NewRegistry()
	registry.SetVars(map[string]interface{}{"name": "Ada", "contents": "variable text"})
	executor := NewExecutor(registry)
	path := filepath.Join(t.TempDir(), "nested", "output.txt")
	var results []ScriptResult

	if failed := executor.executeFileSaveNode(context.Background(), FileSaveElement{
		ID: "save-inline", Path: path, Content: "Hello {{name}}",
	}, &results); failed {
		t.Fatalf("inline file save failed: %#v", results)
	}
	if data, err := os.ReadFile(path); err != nil || string(data) != "Hello Ada" {
		t.Fatalf("saved file = %q, err = %v", data, err)
	}

	appendMode := true
	results = nil
	if failed := executor.executeFileSaveNode(context.Background(), FileSaveElement{
		ID: "save-variable", File: path, Var: "contents", Append: &appendMode,
	}, &results); failed {
		t.Fatalf("variable file append failed: %#v", results)
	}
	if data, err := os.ReadFile(path); err != nil || string(data) != "Hello Adavariable text" {
		t.Fatalf("appended file = %q, err = %v", data, err)
	}

	results = nil
	if failed := executor.executeFileReadNode(context.Background(), FileReadElement{
		ID: "read", Filename: path, OutputVar: "read-result",
	}, &results); failed {
		t.Fatalf("file read failed: %#v", results)
	}
	if registry.GetVarString("read-result") != "Hello Adavariable text" {
		t.Fatalf("read output = %q", registry.GetVarString("read-result"))
	}

	blockingFile := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(blockingFile, []byte("file"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, elem := range []FileSaveElement{
		{ID: "missing-path"},
		{ID: "missing-directory", File: filepath.Join(blockingFile, "file")},
	} {
		results = nil
		if failed := executor.executeFileSaveNode(context.Background(), elem, &results); !failed {
			t.Errorf("file save %q unexpectedly succeeded", elem.ID)
		}
	}
	for _, elem := range []FileReadElement{
		{ID: "missing-path", Var: "output"},
		{ID: "missing-output", File: path},
		{ID: "missing-file", File: filepath.Join(t.TempDir(), "missing"), Var: "output"},
	} {
		results = nil
		if failed := executor.executeFileReadNode(context.Background(), elem, &results); !failed {
			t.Errorf("file read %q unexpectedly succeeded", elem.ID)
		}
	}
}

func TestExecuteStructuredDataPathModesAndErrors(t *testing.T) {
	registry := NewRegistry()
	registry.SetVars(map[string]interface{}{
		"xml":  `<root><item>Ada</item><item>Bob</item></root>`,
		"json": `{"items":[{"name":"Ada"},{"name":"Bob"}],"count":2}`,
		"yaml": "items:\n  - name: Ada\n  - name: Bob\ncount: 2\n",
	})
	executor := NewExecutor(registry)

	for _, elem := range []XmlXPathElement{
		{ID: "xml-text", Var: "xml", XPath: "//item", OutputVar: "xml-text"},
		{ID: "xml-markup", Var: "xml", XPath: "//item", Mode: "xml", OutVar: "xml-markup"},
		{ID: "xml-array", Var: "xml", XPath: "//item", Mode: "json_array", OutputVar: "xml-array"},
	} {
		var results []ScriptResult
		if failed := executor.executeXMLXPathNode(context.Background(), elem, &results); failed {
			t.Errorf("XML XPath %q failed: %#v", elem.ID, results)
		}
	}
	if registry.GetVarString("xml-text") != "Ada\nBob" ||
		registry.GetVarString("xml-markup") != "<item>Ada</item>\n<item>Bob</item>" ||
		registry.GetVarString("xml-array") != `["Ada","Bob"]` {
		t.Fatalf("XML XPath modes returned unexpected outputs: %#v", registry.CopyVariables())
	}

	for _, elem := range []JsonPathElement{
		{ID: "json-text", Var: "json", Path: "$.items[*].name", OutputVar: "json-text"},
		{ID: "json-one", Var: "json", Path: "$.count", Mode: "json", OutputVar: "json-one"},
		{ID: "json-array", Var: "json", Path: "$.items[*].name", Mode: "json_array", OutputVar: "json-array"},
		{ID: "json-unknown-mode", Var: "json", Path: "$.items[*].name", Mode: "YAML", OutputVar: "json-unknown-mode"},
		{ID: "json-empty-array", Var: "json", Path: "$.missing", Mode: "json_array", OutputVar: "json-empty-array"},
	} {
		var results []ScriptResult
		if failed := executor.executeJSONPathNode(context.Background(), elem, &results); failed {
			t.Errorf("JSONPath %q failed: %#v", elem.ID, results)
		}
	}
	if registry.GetVarString("json-text") != "Ada\nBob" ||
		registry.GetVarString("json-one") != "2" ||
		registry.GetVarString("json-array") != `["Ada","Bob"]` ||
		registry.GetVarString("json-unknown-mode") != "Ada\nBob" ||
		registry.GetVarString("json-empty-array") != "null" {
		t.Fatalf("JSONPath modes returned unexpected outputs: %#v", registry.CopyVariables())
	}

	for _, elem := range []YamlPathElement{
		{ID: "yaml-text", Var: "yaml", Path: "$.items[*].name", OutputVar: "yaml-text"},
		{ID: "yaml-one", Var: "yaml", Path: "$.count", Mode: "json", OutputVar: "yaml-one"},
		{ID: "yaml-array", Var: "yaml", Path: "$.items[*].name", Mode: "json_array", OutputVar: "yaml-array"},
		{ID: "yaml-output", Var: "yaml", Path: "$.count", Mode: "yaml", OutputVar: "yaml-output"},
		{ID: "yaml-empty-array", Var: "yaml", Path: "$.missing", Mode: "json_array", OutputVar: "yaml-empty-array"},
	} {
		var results []ScriptResult
		if failed := executor.executeYAMLPathNode(context.Background(), elem, &results); failed {
			t.Errorf("YAML path %q failed: %#v", elem.ID, results)
		}
	}
	if registry.GetVarString("yaml-text") != "Ada\nBob" ||
		registry.GetVarString("yaml-one") != "2" ||
		registry.GetVarString("yaml-array") != `["Ada","Bob"]` ||
		registry.GetVarString("yaml-output") != "- 2\n" ||
		registry.GetVarString("yaml-empty-array") != "null" {
		t.Fatalf("YAML path modes returned unexpected outputs: %#v", registry.CopyVariables())
	}

	xmlFile := filepath.Join(t.TempDir(), "data.xml")
	if err := os.WriteFile(xmlFile, []byte(`<root><item>file</item></root>`), 0600); err != nil {
		t.Fatal(err)
	}
	var results []ScriptResult
	if failed := executor.executeXMLXPathNode(context.Background(), XmlXPathElement{
		ID: "xml-file", File: xmlFile, Content: "//item", OutputVar: "xml-file",
	}, &results); failed || registry.GetVarString("xml-file") != "file" {
		t.Fatalf("XML file query failed: results=%#v value=%q", results, registry.GetVarString("xml-file"))
	}

	for _, elem := range []XmlXPathElement{
		{ID: "missing-expression", Var: "xml"},
		{ID: "missing-file", File: filepath.Join(t.TempDir(), "missing"), XPath: "/root"},
		{ID: "bad-xml", Var: "invalid-xml", XPath: "/root"},
		{ID: "bad-xpath", Var: "xml", XPath: "["},
	} {
		if elem.ID == "bad-xml" {
			registry.SetVar("invalid-xml", "<root>")
		}
		results = nil
		if !executor.executeXMLXPathNode(context.Background(), elem, &results) {
			t.Errorf("XML XPath %q unexpectedly succeeded", elem.ID)
		}
	}
	for _, elem := range []JsonPathElement{
		{ID: "missing-expression", Var: "json", OutputVar: "out"},
		{ID: "missing-output", Var: "json", Path: "$.count"},
		{ID: "empty-json", Var: "empty", Path: "$.count", OutputVar: "out"},
		{ID: "bad-json", Var: "invalid-json", Path: "$.count", OutputVar: "out"},
		{ID: "bad-path", Var: "json", Path: "[", OutputVar: "out"},
		{ID: "missing-file", File: filepath.Join(t.TempDir(), "missing"), Path: "$.count", OutputVar: "out"},
	} {
		registry.SetVar("empty", "")
		registry.SetVar("invalid-json", "{")
		results = nil
		if !executor.executeJSONPathNode(context.Background(), elem, &results) {
			t.Errorf("JSONPath %q unexpectedly succeeded", elem.ID)
		}
	}
	for _, elem := range []YamlPathElement{
		{ID: "missing-expression", Var: "yaml", OutputVar: "out"},
		{ID: "missing-output", Var: "yaml", Path: "$.count"},
		{ID: "empty-yaml", Var: "empty", Path: "$.count", OutputVar: "out"},
		{ID: "bad-yaml", Var: "invalid-yaml", Path: "$.count", OutputVar: "out"},
		{ID: "bad-path", Var: "yaml", Path: "[", OutputVar: "out"},
		{ID: "missing-file", File: filepath.Join(t.TempDir(), "missing"), Path: "$.count", OutputVar: "out"},
	} {
		registry.SetVar("empty", "")
		registry.SetVar("invalid-yaml", "key: [")
		results = nil
		if !executor.executeYAMLPathNode(context.Background(), elem, &results) {
			t.Errorf("YAML path %q unexpectedly succeeded", elem.ID)
		}
	}
}

func TestLuaGoValueConversion(t *testing.T) {
	state := lua.NewState()
	defer state.Close()

	cases := []struct {
		name string
		in   interface{}
		want lua.LValue
	}{
		{name: "nil", in: nil, want: lua.LNil},
		{name: "bool", in: true, want: lua.LTrue},
		{name: "false", in: false, want: lua.LFalse},
		{name: "int", in: int(1), want: lua.LNumber(1)},
		{name: "int", in: int8(3), want: lua.LNumber(3)},
		{name: "int16", in: int16(5), want: lua.LNumber(5)},
		{name: "int32", in: int32(6), want: lua.LNumber(6)},
		{name: "int64", in: int64(7), want: lua.LNumber(7)},
		{name: "uint", in: uint(8), want: lua.LNumber(8)},
		{name: "uint8", in: uint8(9), want: lua.LNumber(9)},
		{name: "uint16", in: uint16(10), want: lua.LNumber(10)},
		{name: "uint32", in: uint32(11), want: lua.LNumber(11)},
		{name: "uint", in: uint64(4), want: lua.LNumber(4)},
		{name: "float", in: float32(2.5), want: lua.LNumber(2.5)},
		{name: "float64", in: float64(3.5), want: lua.LNumber(3.5)},
		{name: "string", in: "text", want: lua.LString("text")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := goValueToLua(state, tc.in); got != tc.want {
				t.Fatalf("goValueToLua(%v) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}

	array := goValueToLua(state, []any{"item", int(2)}).(*lua.LTable)
	if array.Len() != 2 || array.RawGetInt(1) != lua.LString("item") || array.RawGetInt(2) != lua.LNumber(2) {
		t.Fatalf("array conversion = %v", array)
	}
	object := goValueToLua(state, map[string]any{"key": "value"}).(*lua.LTable)
	if object.RawGetString("key") != lua.LString("value") {
		t.Fatalf("map conversion = %v", object)
	}
	if got := goValueToLua(state, struct{ Value string }{"fallback"}); got != lua.LString("{fallback}") {
		t.Fatalf("fallback conversion = %v", got)
	}

	arrayValue := lua.LTable{}
	arrayValue.Append(lua.LNumber(1))
	arrayValue.Append(lua.LString("two"))
	if got := luaValueToGo(&arrayValue); !reflect.DeepEqual(got, []any{1, "two"}) {
		t.Fatalf("lua array conversion = %#v", got)
	}
	objectValue := state.NewTable()
	state.SetField(objectValue, "key", lua.LString("value"))
	if got := luaValueToGo(objectValue); !reflect.DeepEqual(got, map[string]any{"key": "value"}) {
		t.Fatalf("lua map conversion = %#v", got)
	}
	if got := luaValueToGo(lua.LNumber(2.5)); got != 2.5 {
		t.Fatalf("fractional Lua number conversion = %#v", got)
	}
	if luaValueToGo(lua.LNil) != nil || luaValueToGo(lua.LTrue) != true ||
		luaValueToGo(lua.LString("text")) != "text" || luaValueToGo(lua.LNumber(2)) != 2 {
		t.Fatal("Lua primitive values were not converted to Go values")
	}
}

func TestExecuteLuaScriptHelpers(t *testing.T) {
	registry := NewRegistry()
	registry.SetVars(map[string]interface{}{
		"count":   4,
		"decimal": 2.5,
		"flag":    true,
		"name":    "Ada",
	})
	executor := NewExecutor(registry)
	var results []ScriptResult
	script := ScriptItem{
		ID: "lua-helpers", Language: "lua", OutputVar: "lua-output",
		Code: `print(vars.GetString("name"), vars.GetInt("count"), vars.GetNumber("decimal"), vars.GetFloat("decimal"), vars.GetBool("flag"))
vars:Set("dot-set", vars.Get("name"))
vars.Set("colon-set", vars:GetString("name"))
vars.Set("all-name", vars.GetAll().name)
vars.Set("fraction", 2.5)
vars:Set("array", {1, "two"})`,
	}
	if failed := executor.executeScriptNode(context.Background(), script, &results); failed {
		t.Fatalf("Lua script failed: %#v", results)
	}
	if got := registry.GetVarString("lua-output"); !strings.Contains(got, "Ada") {
		t.Fatalf("Lua output variable = %q", got)
	}
	if registry.GetVarString("dot-set") != "Ada" || registry.GetVarString("colon-set") != "Ada" ||
		registry.GetVarString("all-name") != "Ada" || registry.GetVarFloat("fraction") != 2.5 {
		t.Fatalf("Lua variables were not stored correctly: %#v", registry.CopyVariables())
	}

	results = nil
	if failed := executor.executeScriptNode(context.Background(), ScriptItem{
		ID: "lua-error", Language: "lua", Code: `error("expected failure")`,
	}, &results); !failed || len(results) != 1 || results[0].ReturnCode == 0 {
		t.Fatalf("Lua error result = %#v", results)
	}
}

func TestExecuteHTMLTemplateNodePaths(t *testing.T) {
	registry := NewRegistry()
	registry.SetVar("name", "<Ada>")
	executor := NewExecutor(registry)

	var results []ScriptResult
	if failed := executor.executeHtmlTemplateNode(context.Background(), HtmlTemplateElement{
		ID: "inline", Content: `<p>{{.name}}</p>`, OutputVar: "rendered",
	}, &results); failed {
		t.Fatalf("inline template failed: %#v", results)
	}
	if got := registry.GetVarString("rendered"); got != "<p>&lt;Ada&gt;</p>" {
		t.Fatalf("rendered HTML = %q", got)
	}

	templatePath := filepath.Join(t.TempDir(), "template.html")
	if err := os.WriteFile(templatePath, []byte(`<b>{{.name}}</b>`), 0600); err != nil {
		t.Fatal(err)
	}
	results = nil
	if failed := executor.executeHtmlTemplateNode(context.Background(), HtmlTemplateElement{
		ID: "file", File: templatePath, Var: "from_file", Mode: "summary",
	}, &results); failed {
		t.Fatalf("file template failed: %#v", results)
	}
	if got := registry.GetVarString("from_file"); got != "<b>&lt;Ada&gt;</b>" {
		t.Fatalf("file template output = %q", got)
	}
	if !strings.Contains(results[0].ResultsString, "Successfully rendered template") {
		t.Fatalf("summary result = %q", results[0].ResultsString)
	}

	for _, elem := range []HtmlTemplateElement{
		{ID: "missing-file", File: filepath.Join(t.TempDir(), "missing.html")},
		{ID: "bad-template", Content: `{{`},
		{ID: "execution-error", Content: `{{call .name}}`},
	} {
		results = nil
		if failed := executor.executeHtmlTemplateNode(context.Background(), elem, &results); !failed {
			t.Errorf("template %q unexpectedly succeeded", elem.ID)
		}
		if len(results) != 1 || results[0].ReturnCode == 0 {
			t.Errorf("template %q result = %#v, want error result", elem.ID, results)
		}
	}
}

func TestExecuteHTTPClientNode(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("X-Test"); got != "header-value" {
			t.Errorf("X-Test = %q", got)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("reading request body: %v", err)
		}
		if string(body) != "hello Ada" {
			t.Errorf("request body = %q", body)
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(" response "))
	}))
	defer server.Close()

	registry := NewRegistry()
	registry.SetVar("name", "Ada")
	executor := NewExecutor(registry)
	var results []ScriptResult
	if failed := executor.executeHTTPClientNode(context.Background(), HTTPClientElement{
		ID: "request", URI: server.URL, Method: http.MethodPost,
		BodyContent: "hello {{name}}", Headers: "X-Test: header-value",
		OutputVar: "response", StatusCodeVar: "status",
	}, &results); failed {
		t.Fatalf("HTTP request failed: %#v", results)
	}
	if got := registry.GetVarString("response"); got != "response" || registry.GetVarInt("status") != http.StatusCreated {
		t.Fatalf("HTTP output variables = %q, %d", got, registry.GetVarInt("status"))
	}
	if len(results) != 1 || results[0].ReturnCode != 0 {
		t.Fatalf("HTTP result = %#v", results)
	}

	for _, elem := range []HTTPClientElement{
		{ID: "bad-url", URI: "://invalid"},
		{ID: "cancelled", URI: server.URL},
	} {
		ctx := context.Background()
		if elem.ID == "cancelled" {
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			ctx = cancelled
		}
		results = nil
		if failed := executor.executeHTTPClientNode(ctx, elem, &results); !failed {
			t.Errorf("HTTP node %q unexpectedly succeeded", elem.ID)
		}
		if len(results) != 1 || results[0].ReturnCode == 0 {
			t.Errorf("HTTP node %q result = %#v, want error result", elem.ID, results)
		}
	}
}

func TestValidateASTNodeKinds(t *testing.T) {
	invalid := []PipelineNode{
		{Kind: NodeKV, KV: &KVElement{ID: "duplicate", Op: "", Code: "  "}},
		{Kind: NodeKVBulk, KVBulk: &KVBulkElement{ID: "bulk", DBName: "missing", TargetDB: "also-missing"}},
		{Kind: NodeAssert, Assert: &AssertElement{ID: "duplicate", FailureNodes: []PipelineNode{
			{Kind: NodeScript, Script: &ScriptItem{ID: "nested"}},
		}}},
		{Kind: NodeSQL, SQL: &SQLElement{ID: "duplicate", DBName: "missing"}},
		{Kind: NodeSQLBulk, SQLBulk: &SQLBulkElement{ID: "sql-bulk", DBName: "missing", TargetDB: "also-missing"}},
		{Kind: NodeHTTPClient, HTTPClient: &HTTPClientElement{ID: "duplicate"}},
		{Kind: NodeScript, Script: &ScriptItem{ID: "duplicate"}},
		{Kind: NodeParallel, MaxThreads: -1},
		{Kind: NodeIf},
		{Kind: NodeForEach, GroupID: "foreach", ForEachScript: &ScriptItem{DBName: "missing"}},
		{Kind: NodeWhile, GroupID: "while"},
		{Kind: NodeFileSave, FileSave: &FileSaveElement{ID: "save"}},
		{Kind: NodeFileRead, FileRead: &FileReadElement{ID: "read"}},
		{Kind: NodeExcelRead, ExcelRead: &ExcelReadElement{ID: "excel-read"}},
		{Kind: NodeExcelWrite, ExcelWrite: &ExcelWriteElement{ID: "excel-write", DBName: "missing"}},
		{Kind: NodeTemplate, Template: &TemplateElement{ID: "template"}},
		{Kind: NodeHtmlTemplate, HtmlTemplate: &HtmlTemplateElement{ID: "html"}},
		{Kind: NodeXMLXPath, XmlXPath: &XmlXPathElement{ID: "xml"}},
		{Kind: NodeJSONPath, JsonPath: &JsonPathElement{ID: "json"}},
		{Kind: NodeYAMLPath, YamlPath: &YamlPathElement{ID: "yaml"}},
		{Kind: NodeGroup, Transaction: true, Session: true},
	}
	if err := ValidateAST(nil, invalid, nil); err == nil {
		t.Fatal("ValidateAST() accepted invalid node definitions")
	}

	valid := []PipelineNode{
		{Kind: NodeKV, KV: &KVElement{ID: "kv", DBName: "db", Op: "get"}},
		{Kind: NodeKVBulk, KVBulk: &KVBulkElement{ID: "kv-bulk", DBName: "db", TargetDB: "db", Code: "scan"}},
		{Kind: NodeAssert, Assert: &AssertElement{ID: "assert", Var: "value"}},
		{Kind: NodeSQL, SQL: &SQLElement{ID: "sql", DBName: "db", Code: "select 1"}},
		{Kind: NodeSQLBulk, SQLBulk: &SQLBulkElement{ID: "sql-bulk", DBName: "db", TargetDB: "db", TargetTable: "target", Code: "select 1"}},
		{Kind: NodeHTTPClient, HTTPClient: &HTTPClientElement{ID: "http", URI: "https://example.test"}},
		{Kind: NodeScript, Script: &ScriptItem{ID: "script", Code: "run"}},
		{Kind: NodeParallel, MaxThreads: 2, Children: []PipelineNode{{Kind: NodeScript, Script: &ScriptItem{Code: "run"}}}},
		{Kind: NodeIf, IfVar: "value", Children: []PipelineNode{{Kind: NodeScript, Script: &ScriptItem{Code: "run"}}}},
		{Kind: NodeForEach, GroupID: "foreach", ForEachScript: &ScriptItem{DBName: "db"}, Children: []PipelineNode{{Kind: NodeScript, Script: &ScriptItem{Code: "run"}}}},
		{Kind: NodeWhile, GroupID: "while", IfVar: "value", Children: []PipelineNode{{Kind: NodeScript, Script: &ScriptItem{Code: "run"}}}},
		{Kind: NodeFileSave, FileSave: &FileSaveElement{ID: "save", File: "file"}},
		{Kind: NodeFileRead, FileRead: &FileReadElement{ID: "read", File: "file", Var: "output"}},
		{Kind: NodeExcelRead, ExcelRead: &ExcelReadElement{ID: "excel-read", File: "file"}},
		{Kind: NodeExcelWrite, ExcelWrite: &ExcelWriteElement{ID: "excel-write", File: "file", DBName: "db"}},
		{Kind: NodeTemplate, Template: &TemplateElement{ID: "template", Content: "text"}},
		{Kind: NodeHtmlTemplate, HtmlTemplate: &HtmlTemplateElement{ID: "html", Content: "<p>text</p>"}},
		{Kind: NodeXMLXPath, XmlXPath: &XmlXPathElement{ID: "xml", File: "file", XPath: "/root"}},
		{Kind: NodeJSONPath, JsonPath: &JsonPathElement{ID: "json", Var: "value", Path: "$.name"}},
		{Kind: NodeYAMLPath, YamlPath: &YamlPathElement{ID: "yaml", File: "file", Path: "$.name"}},
		{Kind: NodeGroup, Transaction: true, Session: true, DBName: "db"},
	}
	if err := ValidateAST(nil, valid, []DatabaseConfig{{Name: "db"}}); err != nil {
		t.Fatalf("ValidateAST() rejected valid node definitions: %v", err)
	}
}
