package flow

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func TestFileSaveAndRead(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "flow_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	filePath := filepath.Join(tmpDir, "sub", "test_file.txt")

	xmlConfig := []byte(`<?xml version="1.0" encoding="UTF-8"?>
	<pipeline>
		<variables>
			<variable name="my_content" type="string" value="Hello, from Variable!" />
			<variable name="file_path" type="string" value="` + strings.ReplaceAll(filePath, `\`, `/`) + `" />
		</variables>
		
		<file_save id="save_1" file="{{file_path}}" var="my_content" />
		<file_read id="read_1" file="{{file_path}}" output_var="read_content" />
		
		<file_save id="save_2" file="{{file_path}}" append="true">
			Additional Content
		</file_save>
		<file_read id="read_2" file="{{file_path}}" output_var="read_content_appended" />
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
	_, err = executor.Execute(context.Background(), nodes)
	if err != nil {
		t.Fatalf("execution failed: %v", err)
	}

	// Verify read_content
	readContentVal := registry.GetVar("read_content")
	if readContentVal == nil {
		t.Fatal("read_content variable is nil")
	}
	if readContentVal.(string) != "Hello, from Variable!" {
		t.Errorf("expected 'Hello, from Variable!', got %v", readContentVal)
	}

	// Verify read_content_appended
	readContentAppendedVal := registry.GetVar("read_content_appended")
	if readContentAppendedVal == nil {
		t.Fatal("read_content_appended variable is nil")
	}
	if readContentAppendedVal.(string) != "Hello, from Variable!Additional Content" {
		t.Errorf("expected 'Hello, from Variable!Additional Content', got %v", readContentAppendedVal)
	}
}

func TestTemplate(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "flow_test_tmpl")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	tmplFilePath := filepath.Join(tmpDir, "template.txt")
	err = os.WriteFile(tmplFilePath, []byte("File tmpl: hello {{.name}}"), 0644)
	if err != nil {
		t.Fatalf("failed to write template file: %v", err)
	}

	xmlConfig := []byte(`<?xml version="1.0" encoding="UTF-8"?>
	<pipeline>
		<variables>
			<variable name="name" type="string" value="World" />
			<variable name="tmpl_file" type="string" value="` + strings.ReplaceAll(tmplFilePath, `\`, `/`) + `" />
		</variables>

		<template id="tmpl_inline" output_var="res_inline">
			Inline tmpl: hello {{.name}}
		</template>

		<template id="tmpl_file_node" file="{{tmpl_file}}" output_var="res_file" />
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

	resInline := registry.GetVar("res_inline")
	if resInline == nil || resInline.(string) != "Inline tmpl: hello World" {
		t.Errorf("expected 'Inline tmpl: hello World', got %v", resInline)
	}

	resFile := registry.GetVar("res_file")
	if resFile == nil || resFile.(string) != "File tmpl: hello World" {
		t.Errorf("expected 'File tmpl: hello World', got %v", resFile)
	}
}

func TestDatabasePoolConfigParsing(t *testing.T) {
	xmlConfig := []byte(`<?xml version="1.0" encoding="UTF-8"?>
	<pipeline>
		<databases>
			<database name="pool_db" driver="sqlite" connection_string="file::memory:?cache=shared" max_open_conns="42" max_idle_conns="7" conn_max_lifetime_seconds="90" workload="oltp" />
		</databases>
	</pipeline>`)

	cfg, err := ParseXMLConfig(xmlConfig)
	if err != nil {
		t.Fatalf("failed to parse XML: %v", err)
	}
	if len(cfg.Databases) != 1 {
		t.Fatalf("expected 1 database config, got %d", len(cfg.Databases))
	}

	dbCfg := cfg.Databases[0]
	if dbCfg.MaxOpenConns != 42 || dbCfg.MaxIdleConns != 7 {
		t.Fatalf("unexpected pool values: %+v", dbCfg)
	}
	if dbCfg.ConnMaxLifetime != 90*time.Second {
		t.Fatalf("expected 90s connection lifetime, got %s", dbCfg.ConnMaxLifetime)
	}
	if dbCfg.Workload != "oltp" {
		t.Fatalf("expected workload oltp, got %q", dbCfg.Workload)
	}
}

func TestExcelReadAndWrite(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "flow_test_excel")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	excelFilePath := filepath.Join(tmpDir, "output.xlsx")

	xmlConfig := []byte(`<?xml version="1.0" encoding="UTF-8"?>
	<pipeline>
		<databases>
			<database name="excel_test_db" driver="sqlite" connection_string="file::memory:?cache=shared" />
		</databases>
		<variables>
			<variable name="excel_path" type="string" value="` + strings.ReplaceAll(excelFilePath, `\`, `/`) + `" />
		</variables>
		
		<scripts>
			<script id="setup_db" language="sql" db="excel_test_db">
				CREATE TABLE users (id INTEGER, name TEXT, role TEXT);
				INSERT INTO users (id, name, role) VALUES (1, 'Alice', 'Admin');
				INSERT INTO users (id, name, role) VALUES (2, 'Bob', 'User');
			</script>
		</scripts>

		<excel_write id="write_excel" file="{{excel_path}}" db="excel_test_db" sheet="UsersList">
			SELECT id, name, role FROM users ORDER BY id ASC
		</excel_write>

		<excel_read id="read_excel" file="{{excel_path}}" sheet="UsersList" header="true" output_var="excel_json" />
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

	excelJson := registry.GetVar("excel_json")
	if excelJson == nil {
		t.Fatal("excel_json variable is nil")
	}

	var records []map[string]string
	if err := json.Unmarshal([]byte(excelJson.(string)), &records); err != nil {
		t.Fatalf("failed to unmarshal excel JSON: %v", err)
	}

	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}

	if records[0]["name"] != "Alice" || records[0]["role"] != "Admin" || records[0]["id"] != "1" {
		t.Errorf("unexpected Alice record: %v", records[0])
	}

	if records[1]["name"] != "Bob" || records[1]["role"] != "User" || records[1]["id"] != "2" {
		t.Errorf("unexpected Bob record: %v", records[1])
	}
}

func TestXMLXPath(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "flow_test_xml")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	xmlFilePath := filepath.Join(tmpDir, "data.xml")
	xmlData := []byte(`<root><item id="1"><name>Apple</name></item><item id="2"><name>Banana</name></item></root>`)
	err = os.WriteFile(xmlFilePath, xmlData, 0644)
	if err != nil {
		t.Fatalf("failed to write XML file: %v", err)
	}

	xmlConfig := []byte(`<?xml version="1.0" encoding="UTF-8"?>
	<pipeline>
		<variables>
			<variable name="xml_path" type="string" value="` + strings.ReplaceAll(xmlFilePath, `\`, `/`) + `" />
			<variable name="xml_content" type="string" value="&lt;root&gt;&lt;item id='3'&gt;&lt;name&gt;Cherry&lt;/name&gt;&lt;/item&gt;&lt;/root&gt;" />
		</variables>

		<!-- Test reading XML from file and extracting text mode (default) using attribute xpath -->
		<xml_xpath id="xpath_file" file="{{xml_path}}" xpath="//item/name" output_var="names_text" />

		<!-- Test reading XML from file and extracting xml mode using attribute xpath -->
		<xml_xpath id="xpath_file_xml" file="{{xml_path}}" xpath="//item/name" mode="xml" output_var="names_xml" />

		<!-- Test reading XML from variable and extracting json_array mode using xpath from body content -->
		<xml_xpath id="xpath_var_json" var="xml_content" mode="json_array" output_var="names_json">
			//item/name
		</xml_xpath>
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

	// Verify default / text mode
	namesText := registry.GetVar("names_text")
	if namesText == nil {
		t.Fatal("names_text variable is nil")
	}
	if namesText.(string) != "Apple\nBanana" {
		t.Errorf("expected 'Apple\\nBanana', got %q", namesText)
	}

	// Verify xml mode
	namesXML := registry.GetVar("names_xml")
	if namesXML == nil {
		t.Fatal("names_xml variable is nil")
	}
	// Verify that namesXML contains the raw tags
	expectedXML := "<name>Apple</name>\n<name>Banana</name>"
	// Clean string differences just in case of formatting variations
	cleanNamesXML := strings.ReplaceAll(namesXML.(string), "\r", "")
	if cleanNamesXML != expectedXML {
		t.Errorf("expected xml:\n%q\ngot:\n%q", expectedXML, cleanNamesXML)
	}

	// Verify json_array mode
	namesJSON := registry.GetVar("names_json")
	if namesJSON == nil {
		t.Fatal("names_json variable is nil")
	}
	var jsonArray []string
	if err := json.Unmarshal([]byte(namesJSON.(string)), &jsonArray); err != nil {
		t.Fatalf("failed to unmarshal JSON: %v", err)
	}
	if len(jsonArray) != 1 || jsonArray[0] != "Cherry" {
		t.Errorf("expected json array with ['Cherry'], got %v", jsonArray)
	}
}

func TestJSONPath(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "flow_test_json")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	jsonFilePath := filepath.Join(tmpDir, "data.json")
	jsonData := []byte(`{"store": {"book": [{"title": "Sayings of the Century", "price": 8.95}, {"title": "Sword of Honour", "price": 12.99}]}}`)
	err = os.WriteFile(jsonFilePath, jsonData, 0644)
	if err != nil {
		t.Fatalf("failed to write JSON file: %v", err)
	}

	xmlConfig := []byte(`<?xml version="1.0" encoding="UTF-8"?>
	<pipeline>
		<variables>
			<variable name="json_path" type="string" value="` + strings.ReplaceAll(jsonFilePath, `\`, `/`) + `" />
			<variable name="json_content" type="string" value='{"store": {"bicycle": {"color": "red", "price": 19.95}}}' />
		</variables>

		<!-- Test reading JSON from file and extracting using attribute jsonpath, default (value) mode -->
		<json_path id="jp_file" file="{{json_path}}" jsonpath="$.store.book[*].title" output_var="titles_text" />

		<!-- Test reading JSON from file and extracting as json_array -->
		<json_path id="jp_file_array" file="{{json_path}}" jsonpath="$.store.book[*].price" mode="json_array" output_var="prices_json" />

		<!-- Test reading JSON from variable and extracting from inner body content using json mode -->
		<json_path id="jp_var_json" var="json_content" mode="json" output_var="bicycle_price">
			$.store.bicycle.price
		</json_path>
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

	// Verify default / value mode (text join)
	titlesText := registry.GetVar("titles_text")
	if titlesText == nil {
		t.Fatal("titles_text variable is nil")
	}
	if titlesText.(string) != "Sayings of the Century\nSword of Honour" {
		t.Errorf("expected joined titles, got %q", titlesText)
	}

	// Verify json_array mode
	pricesJson := registry.GetVar("prices_json")
	if pricesJson == nil {
		t.Fatal("prices_json variable is nil")
	}
	var prices []float64
	if err := json.Unmarshal([]byte(pricesJson.(string)), &prices); err != nil {
		t.Fatalf("failed to unmarshal prices JSON: %v", err)
	}
	if len(prices) != 2 || prices[0] != 8.95 || prices[1] != 12.99 {
		t.Errorf("unexpected prices array: %v", prices)
	}

	// Verify json mode for single value
	bicyclePrice := registry.GetVar("bicycle_price")
	if bicyclePrice == nil {
		t.Fatal("bicycle_price variable is nil")
	}
	if bicyclePrice.(string) != "19.95" {
		t.Errorf("expected '19.95', got %q", bicyclePrice)
	}
}

func TestYAMLPath(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "flow_test_yaml")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	yamlFilePath := filepath.Join(tmpDir, "data.yaml")
	yamlData := []byte(`
store:
  book:
    - title: "Sayings of the Century"
      price: 8.95
    - title: "Sword of Honour"
      price: 12.99
`)
	err = os.WriteFile(yamlFilePath, yamlData, 0644)
	if err != nil {
		t.Fatalf("failed to write YAML file: %v", err)
	}

	xmlConfig := []byte(`<?xml version="1.0" encoding="UTF-8"?>
	<pipeline>
		<variables>
			<variable name="yaml_path" type="string" value="` + strings.ReplaceAll(yamlFilePath, `\`, `/`) + `" />
			<variable name="yaml_content" type="string" value="store:&#10;  bicycle:&#10;    color: red&#10;    price: 19.95" />
		</variables>

		<!-- Test reading YAML from file and extracting using attribute yamlpath, default (value) mode -->
		<yaml_path id="yp_file" file="{{yaml_path}}" yamlpath="$.store.book[*].title" output_var="titles_text" />

		<!-- Test reading YAML from file and extracting as json_array -->
		<yaml_path id="yp_file_array" file="{{yaml_path}}" yamlpath="$.store.book[*].price" mode="json_array" output_var="prices_json" />

		<!-- Test reading YAML from variable and extracting using yaml mode -->
		<yaml_path id="yp_var_yaml" var="yaml_content" mode="yaml" output_var="bicycle_yaml">
			$.store.bicycle
		</yaml_path>
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

	// Verify default / value mode (text join)
	titlesText := registry.GetVar("titles_text")
	if titlesText == nil {
		t.Fatal("titles_text variable is nil")
	}
	if titlesText.(string) != "Sayings of the Century\nSword of Honour" {
		t.Errorf("expected joined titles, got %q", titlesText)
	}

	// Verify json_array mode
	pricesJson := registry.GetVar("prices_json")
	if pricesJson == nil {
		t.Fatal("prices_json variable is nil")
	}
	t.Logf("pricesJson raw output: %q", pricesJson.(string))
	var prices []float64
	if err := json.Unmarshal([]byte(pricesJson.(string)), &prices); err != nil {
		t.Fatalf("failed to unmarshal prices JSON: %v", err)
	}
	if len(prices) != 2 || prices[0] != 8.95 || prices[1] != 12.99 {
		t.Errorf("unexpected prices array: %v", prices)
	}

	// Verify yaml mode
	bicycleYaml := registry.GetVar("bicycle_yaml")
	if bicycleYaml == nil {
		t.Fatal("bicycle_yaml variable is nil")
	}
	t.Logf("bicycle_yaml raw output: %q", bicycleYaml.(string))
	var bMap map[string]interface{}
	// Since we got a YAML array representation for multiple nodes, let's unmarshal and check
	var yamlArr []map[string]interface{}
	if err := yaml.Unmarshal([]byte(bicycleYaml.(string)), &yamlArr); err != nil {
		t.Fatalf("failed to unmarshal bicycle YAML: %v", err)
	}
	if len(yamlArr) != 1 {
		t.Fatalf("expected 1 element in YAML array, got %v", yamlArr)
	}
	bMap = yamlArr[0]
	if bMap["color"] != "red" || bMap["price"] != 19.95 {
		t.Errorf("expected bicycle details, got %v", bMap)
	}
}

func TestExcelMultiTabs(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "flow_test_excel_tabs")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	excelFilePath := filepath.Join(tmpDir, "report.xlsx")

	xmlConfig := []byte(`<?xml version="1.0" encoding="UTF-8"?>
	<pipeline>
		<databases>
			<database name="excel_test_db" driver="sqlite" connection_string="file::memory:?cache=shared" />
		</databases>
		<variables>
			<variable name="excel_path" type="string" value="` + strings.ReplaceAll(excelFilePath, `\`, `/`) + `" />
		</variables>
		
		<scripts>
			<script id="setup_db" language="sql" db="excel_test_db">
				CREATE TABLE users (id INTEGER, name TEXT, role TEXT);
				INSERT INTO users (id, name, role) VALUES (1, 'Alice', 'Admin');
				INSERT INTO users (id, name, role) VALUES (2, 'Bob', 'User');

				CREATE TABLE products (sku TEXT, name TEXT, price REAL);
				INSERT INTO products (sku, name, price) VALUES ('P001', 'Widget A', 10.99);
				INSERT INTO products (sku, name, price) VALUES ('P002', 'Gadget B', 20.49);
			</script>
		</scripts>

		<!-- Write Tab 1: UsersList -->
		<excel_write id="write_users" file="{{excel_path}}" db="excel_test_db" sheet="UsersList">
			SELECT id, name, role FROM users ORDER BY id ASC
		</excel_write>

		<!-- Write Tab 2: ProductsList -->
		<excel_write id="write_products" file="{{excel_path}}" db="excel_test_db" sheet="ProductsList">
			SELECT sku, name, price FROM products ORDER BY sku ASC
		</excel_write>

		<!-- Read both tabs to verify -->
		<excel_read id="read_users" file="{{excel_path}}" sheet="UsersList" header="true" output_var="users_json" />
		<excel_read id="read_products" file="{{excel_path}}" sheet="ProductsList" header="true" output_var="products_json" />
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

	// Verify users_json
	usersJson := registry.GetVar("users_json")
	if usersJson == nil {
		t.Fatal("users_json variable is nil")
	}
	var users []map[string]string
	if err := json.Unmarshal([]byte(usersJson.(string)), &users); err != nil {
		t.Fatalf("failed to unmarshal users JSON: %v", err)
	}
	if len(users) != 2 {
		t.Fatalf("expected 2 user records, got %d", len(users))
	}
	if users[0]["name"] != "Alice" || users[1]["name"] != "Bob" {
		t.Errorf("unexpected users data: %v", users)
	}

	// Verify products_json
	productsJson := registry.GetVar("products_json")
	if productsJson == nil {
		t.Fatal("products_json variable is nil")
	}
	var products []map[string]string
	if err := json.Unmarshal([]byte(productsJson.(string)), &products); err != nil {
		t.Fatalf("failed to unmarshal products JSON: %v", err)
	}
	if len(products) != 2 {
		t.Fatalf("expected 2 product records, got %d", len(products))
	}
	if products[0]["name"] != "Widget A" || products[1]["name"] != "Gadget B" {
		t.Errorf("unexpected products data: %v", products)
	}
}
