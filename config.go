// Package flow provides a high-performance, modular, and embeddable data pipeline
// orchestration and stream ETL library for Go. It allows developers to programmatically
// load, validate, and execute complex pipeline AST nodes (such as loops, parallel batches,
// and dynamic SQL/Go scripts) from XML configuration files.
package flow

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

// VariableConfig represents an individual environment variable loaded from XML.
type VariableConfig struct {
	Name  string // Name of the variable
	Type  string // Type of the variable (e.g. string, int, bool, float)
	Value string // Value of the variable as a raw string
}

// DatabaseConfig represents a database connection setup defined in the XML.
type DatabaseConfig struct {
	Name             string // Unique identifier for the database
	Driver           string // Database driver name (e.g. postgres, mysql, sqlite)
	ConnectionString string // Driver-specific connection string
}

// ScriptItem represents an executable script payload (either SQL or Go) with metadata.
type ScriptItem struct {
	ID               string // Unique identifier of the script
	Language         string // Language identifier (sql or go)
	DBName           string // Target database identifier for SQL queries
	TargetDB         string // Destination database identifier for streaming ETL
	TargetTable      string // Destination table name for streaming ETL
	BatchSize        int    // Maximum rows loaded per batch
	VarName          string // Input environment variable to pull script code from dynamically
	OutputVar        string // Environment variable to store the command's outputs or logs into
	Code             string // Inner script text/payload
	Tablock          bool   // Acquire table lock for minimal logging on SQL Server
	CheckConstraints bool   // Evaluate constraints during MSSQL bulk insert
	FireTriggers     bool   // Execute target table triggers during MSSQL bulk insert
	KeepNulls        bool   // Preserve explicit NULL values during MSSQL bulk insert
}

// NodeKind represents the structural type of a PipelineNode.
type NodeKind int

const (
	// NodeScript represents a leaf script execution step.
	NodeScript NodeKind = iota
	// NodeGroup represents a simple sequence container of nodes.
	NodeGroup
	// NodeIf represents a conditional branching sequence.
	NodeIf
	// NodeForEach represents an iterative driver loop.
	NodeForEach
	// NodeParallel represents a concurrent block container.
	NodeParallel
	// NodeWhile represents a condition-controlled iteration loop.
	NodeWhile
	// NodeHTTPClient represents an HTTP client execution step.
	NodeHTTPClient // Added NodeHTTPClient enum
	// NodeTemplate represents a template inclusion step.
	NodeTemplate // New enum item
	// NodeFileSave represents a file save operation step.
	NodeFileSave // New enum item for file save operation
	// NodeFileRead represents a file read operation step.
	NodeFileRead   // New enum item for file read operation
	NodeExcelRead  // New enum item for Excel read operation
	NodeExcelWrite // New enum item for Excel write operation
	NodeXMLXPath   // New enum item for XML XPath extraction
	NodeJSONPath   // New enum item for JSON path extraction
	NodeYAMLPath   // New enum item for YAML path extraction
	NodeSQL        // New enum item for standard SQL execution
	NodeSQLBulk    // New enum item for bulk SQL execution
	NodeAssert      // New enum item for assert operation
)

// PipelineNode is an AST node in the pipeline execution tree.
type PipelineNode struct {
	Kind          NodeKind           // Struct/flow type of the node
	MaxThreads    int                // Concurrency limit (only used for NodeParallel)
	MaxIterations int                // Infinite loop safety limit (only used for NodeWhile)
	Script        *ScriptItem        // Leaf script item payload (only used for NodeScript)
	HTTPClient    *HTTPClientElement // Added HTTP payload
	GroupID       string             // Structural/group name or ID
	IfVar         string             // Condition driver variable name
	IfEquals      string             // Expected variable value to match
	ForEachScript *ScriptItem        // Iterator driver script config (only used for NodeForEach)
	Children      []PipelineNode     // List of sequential child execution steps
	ElseNodes     []PipelineNode     // Else branching steps (only used for NodeIf)
	Transaction   bool               // Start transaction for this group
	DBName        string             // Database name for the transaction
	Template      *TemplateElement   // New payload field for template inclusion step
	FileSave      *FileSaveElement   // New payload field for file save operation
	FileRead      *FileReadElement   // New payload field for file read operation
	ExcelRead     *ExcelReadElement  // New payload field for Excel read operation
	ExcelWrite    *ExcelWriteElement // New payload field for Excel write operation
	XmlXPath      *XmlXPathElement   // New payload field for XML XPath extraction
	JsonPath      *JsonPathElement   // New payload field for JSON path extraction
	YamlPath      *YamlPathElement   // New payload field for YAML path extraction
	Assert    *AssertElement 	 // New enum item for assert operation
}

type AssertElement struct {
    ID          string         `xml:"id,attr"`
    Var         string         `xml:"var,attr"`
    Equals      string         `xml:"equals,attr"`
    Value       string         `xml:"value,attr"`
    Operator    string         `xml:"operator,attr"`
    Message     string         `xml:"message,attr"`
    OnFailure   string         `xml:"on_failure,attr"` // "halt", "warn", "continue", "set_var"
    FailVar     string         `xml:"fail_var,attr"`
    FailVal     string         `xml:"fail_val,attr"`
    FailureNodes []PipelineNode // Nodes inside <on_failure> block
}
type YamlPathElement struct {
	ID        string `xml:"id,attr"`
	File      string `xml:"file,attr"`
	Var       string `xml:"var,attr"`
	Path      string `xml:"path,attr"`
	YAMLPath  string `xml:"yamlpath,attr"`
	Content   string `xml:",chardata"` // Captures inner element body text
	Mode      string `xml:"mode,attr"` // "value", "json", "json_array", "yaml"
	OutputVar string `xml:"output_var,attr"`
	OutVar    string `xml:"out_var,attr"`
}
type JsonPathElement struct {
	ID        string `xml:"id,attr"`
	File      string `xml:"file,attr"`
	Var       string `xml:"var,attr"`
	Path      string `xml:"path,attr"`
	JSONPath  string `xml:"jsonpath,attr"`
	Content   string `xml:",chardata"` // Captures inner element body text
	Mode      string `xml:"mode,attr"` // "value", "json", "json_array"
	OutputVar string `xml:"output_var,attr"`
	OutVar    string `xml:"out_var,attr"`
}
type XmlXPathElement struct {
	ID        string `xml:"id,attr"`
	File      string `xml:"file,attr"`
	Var       string `xml:"var,attr"`
	XPath     string `xml:"xpath,attr"`
	Content   string `xml:",chardata"` // Captures inner element body text
	Mode      string `xml:"mode,attr"` // "text", "xml", "json_array"
	OutputVar string `xml:"output_var,attr"`
}
type TemplateElement struct {
	ID        string `xml:"id,attr"`
	Name      string `xml:"name,attr"`
	File      string `xml:"file,attr"`
	Engine    string `xml:"engine,attr"`
	OutputVar string `xml:"output_var,attr"`
	Var       string `xml:"var,attr"`
	Content   string `xml:",chardata"`
}

type FileSaveElement struct {
	ID       string `xml:"id,attr"`
	File     string `xml:"file,attr"`
	Path     string `xml:"path,attr"`
	Filename string `xml:"filename,attr"`
	Var      string `xml:"var,attr"`
	Variable string `xml:"variable,attr"`
	Append   *bool  `xml:"append,attr"`
	Content  string `xml:",chardata"`
}
type FileReadElement struct {
	ID             string `xml:"id,attr"`
	File           string `xml:"file,attr"`
	Path           string `xml:"path,attr"`
	Filename       string `xml:"filename,attr"`
	Var            string `xml:"var,attr"`
	Variable       string `xml:"variable,attr"`
	OutputVar      string `xml:"output_var,attr"`
	OutputVariable string `xml:"output_variable,attr"`
	OutVar         string `xml:"out_var,attr"`
}
type ExcelReadElement struct {
	ID        string `xml:"id,attr"`
	File      string `xml:"file,attr"`
	Sheet     string `xml:"sheet,attr"`
	Header    *bool  `xml:"header,attr"`
	Var       string `xml:"var,attr"`
	OutputVar string `xml:"output_var,attr"`
}
type ExcelWriteElement struct {
	ID     string `xml:"id,attr"`
	File   string `xml:"file,attr"`
	Sheet  string `xml:"sheet,attr"`
	DBName string `xml:"db,attr"`
	Var    string `xml:"var,attr"`
	Query  string `xml:",chardata"`
}

func (f *FileSaveElement) GetFilePath() string {
	if f.File != "" {
		return f.File
	}
	if f.Path != "" {
		return f.Path
	}
	return f.Filename
}

func (f *FileSaveElement) GetInputVar() string {
	if f.Var != "" {
		return f.Var
	}
	return f.Variable
}

func (t *TemplateElement) GetOutputVar() string {
	if t.OutputVar != "" {
		return t.OutputVar
	}
	return t.Var
}
func (e *ExcelReadElement) GetOutputVar() string {
	if e.OutputVar != "" {
		return e.OutputVar
	}
	return e.Var
}

// ValidateXSD invokes 'xmllint' to validate the XML file against the given XSD schema.
func ValidateXSD(xmlPath string, xsdPath string) error {
	if _, err := os.Stat(xsdPath); os.IsNotExist(err) {
		return fmt.Errorf("XSD schema file not found at path: %s", xsdPath)
	}

	if _, err := exec.LookPath("xmllint"); err != nil {
		return fmt.Errorf("'xmllint' executable not found in PATH. Please install libxml2-utils / xmllint to enable XSD validation")
	}

	cmd := exec.Command("xmllint", "--schema", xsdPath, "--noout", xmlPath)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		errStr := strings.TrimSpace(stderr.String())
		if errStr == "" {
			errStr = err.Error()
		}
		return fmt.Errorf("XSD Schema Validation Failure:\n%s", errStr)
	}

	return nil
}

// PipelineConfig encapsulates the complete parsed AST structure.


// ParseXMLConfig parses XML pipeline config definitions into separate Preflight and Flow ASTs.
func ParseXMLConfig(xmlData []byte) (PipelineConfig, error) {
	decoder := xml.NewDecoder(bytes.NewReader(xmlData))
	var cfg PipelineConfig
	scriptIndex := 1

	for {
		tok, err := decoder.Token()
		if err == io.EOF {
			return cfg, nil
		}
		if err != nil {
			return cfg, err
		}

		if se, ok := tok.(xml.StartElement); ok {
			elemName := strings.ToLower(se.Name.Local)
			if elemName == "variable" {
				var vCfg VariableConfig
				for _, attr := range se.Attr {
					if strings.EqualFold(attr.Name.Local, "name") {
						vCfg.Name = attr.Value
					} else if strings.EqualFold(attr.Name.Local, "type") {
						vCfg.Type = strings.ToLower(attr.Value)
					} else if strings.EqualFold(attr.Name.Local, "value") {
						vCfg.Value = attr.Value
					}
				}
				if vCfg.Name != "" {
					cfg.Variables = append(cfg.Variables, vCfg)
				}
			} else if elemName == "database" {
				var dbCfg DatabaseConfig
				for _, attr := range se.Attr {
					if strings.EqualFold(attr.Name.Local, "name") {
						dbCfg.Name = attr.Value
					} else if strings.EqualFold(attr.Name.Local, "driver") || strings.EqualFold(attr.Name.Local, "type") {
						dbCfg.Driver = strings.ToLower(attr.Value)
					} else if strings.EqualFold(attr.Name.Local, "connection_string") {
						dbCfg.ConnectionString = attr.Value
					}
				}
				if dbCfg.Driver == "" {
					dbCfg.Driver = "sqlserver"
				}
				if dbCfg.Name != "" && dbCfg.ConnectionString != "" {
					cfg.Databases = append(cfg.Databases, dbCfg)
				}
			} else if elemName == "preflight" {
				// Parse child nodes inside <preflight> into PreflightNodes
				pNodes, err := parseChildrenUntil(decoder, "preflight", &scriptIndex)
				if err != nil {
					return cfg, err
				}
				cfg.PreflightNodes = append(cfg.PreflightNodes, pNodes...)
			} else if elemName == "flow" || elemName == "scripts" || elemName == "config" || elemName == "variables" || elemName == "databases" || elemName == "pipeline" {
				continue
			} else {
				node, err := parseNodeElement(decoder, se, &scriptIndex)
				if err != nil {
					return cfg, err
				}
				if node != nil {
					cfg.FlowNodes = append(cfg.FlowNodes, *node)
				}
			}
		}
	}

	return cfg, nil
}

func parseNodeElement(decoder *xml.Decoder, se xml.StartElement, scriptIndex *int) (*PipelineNode, error) {
	elemName := strings.ToLower(se.Name.Local)

	switch elemName {
	// In config.go -> parseNodeElement switch elemName
	case "assert":
    var elem AssertElement
    for _, attr := range se.Attr {
        switch strings.ToLower(attr.Name.Local) {
        case "id": elem.ID = attr.Value
        case "var": elem.Var = attr.Value
        case "equals": elem.Equals = attr.Value
        case "value": elem.Value = attr.Value
        case "operator": elem.Operator = attr.Value
        case "message": elem.Message = attr.Value
        case "on_failure": elem.OnFailure = attr.Value
        case "fail_var": elem.FailVar = attr.Value
        case "fail_val": elem.FailVal = attr.Value
        }
    }
    if elem.ID == "" {
        elem.ID = fmt.Sprintf("assert_%d", *scriptIndex)
        (*scriptIndex)++
    }

    // Parse inner tags (such as <on_failure>)
    for {
        tok, err := decoder.Token()
        if err == io.EOF || err != nil { break }

        if end, ok := tok.(xml.EndElement); ok && strings.EqualFold(end.Name.Local, "assert") {
            break
        }

        if start, ok := tok.(xml.StartElement); ok {
            if strings.EqualFold(start.Name.Local, "on_failure") {
                nodes, err := parseChildrenUntil(decoder, "on_failure", scriptIndex)
                if err != nil { return nil, err }
                elem.FailureNodes = nodes
            }
        }
    }

    return &PipelineNode{Kind: NodeAssert, Assert: &elem}, nil
	case "sql":
		s := ScriptItem{Language: "sql", Tablock: true}
		for _, attr := range se.Attr {
			switch strings.ToLower(attr.Name.Local) {
			case "id":
				s.ID = attr.Value
			case "db", "database":
				s.DBName = attr.Value
			case "var", "variable":
				s.VarName = attr.Value
			case "output_var", "out_var", "output_variable":
				s.OutputVar = attr.Value
			}
		}
		if s.ID == "" {
			s.ID = fmt.Sprintf("sql_%d", *scriptIndex)
			(*scriptIndex)++
		}
		var content string
		if err := decoder.DecodeElement(&content, &se); err != nil {
			return nil, err
		}
		s.Code = strings.TrimSpace(content)
		return &PipelineNode{Kind: NodeSQL, Script: &s}, nil

	case "sql_bulk", "sql-bulk", "SQL_BULK":
		s := ScriptItem{Language: "sql", Tablock: true}
		for _, attr := range se.Attr {
			switch strings.ToLower(attr.Name.Local) {
			case "id":
				s.ID = attr.Value
			case "db", "database":
				s.DBName = attr.Value
			case "target_db", "target_database":
				s.TargetDB = attr.Value
			case "target_table":
				s.TargetTable = attr.Value
			case "batch_size":
				if b, err := strconv.Atoi(attr.Value); err == nil {
					s.BatchSize = b
				}
			case "var", "variable":
				s.VarName = attr.Value
			case "output_var", "out_var":
				s.OutputVar = attr.Value
			case "tablock":
				if b, err := strconv.ParseBool(attr.Value); err == nil {
					s.Tablock = b
				}
			case "check_constraints":
				if b, err := strconv.ParseBool(attr.Value); err == nil {
					s.CheckConstraints = b
				}
			case "fire_triggers":
				if b, err := strconv.ParseBool(attr.Value); err == nil {
					s.FireTriggers = b
				}
			case "keep_nulls":
				if b, err := strconv.ParseBool(attr.Value); err == nil {
					s.KeepNulls = b
				}
			}
		}
		if s.ID == "" {
			s.ID = fmt.Sprintf("sql_bulk_%d", *scriptIndex)
			(*scriptIndex)++
		}
		var content string
		if err := decoder.DecodeElement(&content, &se); err != nil {
			return nil, err
		}
		s.Code = strings.TrimSpace(content)
		return &PipelineNode{Kind: NodeSQLBulk, Script: &s}, nil
	case "yaml_path", "yaml-path", "YAML_PATH":
		var elem YamlPathElement
		if err := decoder.DecodeElement(&elem, &se); err != nil {
			return nil, err
		}
		if elem.ID == "" {
			elem.ID = fmt.Sprintf("yaml_path_%d", *scriptIndex)
			(*scriptIndex)++
		}
		return &PipelineNode{Kind: NodeYAMLPath, YamlPath: &elem}, nil
	case "json_path", "json-path", "JSON_PATH":
		var elem JsonPathElement
		if err := decoder.DecodeElement(&elem, &se); err != nil {
			return nil, err
		}
		if elem.ID == "" {
			elem.ID = fmt.Sprintf("json_path_%d", *scriptIndex)
			(*scriptIndex)++
		}
		return &PipelineNode{Kind: NodeJSONPath, JsonPath: &elem}, nil
	case "xml_xpath", "xml-xpath":
		var elem XmlXPathElement
		if err := decoder.DecodeElement(&elem, &se); err != nil {
			return nil, err
		}
		return &PipelineNode{Kind: NodeXMLXPath, XmlXPath: &elem}, nil
	case "excel_read", "excel-read", "EXCEL_READ":
		var elem ExcelReadElement
		if err := decoder.DecodeElement(&elem, &se); err != nil {
			return nil, err
		}
		if elem.ID == "" {
			elem.ID = fmt.Sprintf("excel_read_%d", *scriptIndex)
			(*scriptIndex)++
		}
		return &PipelineNode{Kind: NodeExcelRead, ExcelRead: &elem}, nil

	case "excel_write", "excel-write", "EXCEL_WRITE":
		var elem ExcelWriteElement
		if err := decoder.DecodeElement(&elem, &se); err != nil {
			return nil, err
		}
		if elem.ID == "" {
			elem.ID = fmt.Sprintf("excel_write_%d", *scriptIndex)
			(*scriptIndex)++
		}
		return &PipelineNode{Kind: NodeExcelWrite, ExcelWrite: &elem}, nil
	case "file_save", "file-save", "FILE_SAVE":
		var elem FileSaveElement
		if err := decoder.DecodeElement(&elem, &se); err != nil {
			return nil, err
		}
		if elem.ID == "" {
			elem.ID = fmt.Sprintf("file_save_%d", *scriptIndex)
			(*scriptIndex)++
		}
		return &PipelineNode{
			Kind:     NodeFileSave,
			FileSave: &elem,
		}, nil
	case "file_read", "file-read", "FILE_READ":
		var elem FileReadElement
		if err := decoder.DecodeElement(&elem, &se); err != nil {
			return nil, err
		}
		if elem.ID == "" {
			elem.ID = fmt.Sprintf("file_read_%d", *scriptIndex)
			(*scriptIndex)++
		}
		return &PipelineNode{
			Kind:     NodeFileRead,
			FileRead: &elem,
		}, nil
	case "template":
		var elem TemplateElement
		if err := decoder.DecodeElement(&elem, &se); err != nil {
			return nil, err
		}
		if elem.ID == "" {
			elem.ID = fmt.Sprintf("template_%d", *scriptIndex)
			(*scriptIndex)++
		}
		return &PipelineNode{
			Kind:     NodeTemplate,
			Template: &elem,
		}, nil
	case "http_client", "http-client":
		var elem HTTPClientElement
		if err := decoder.DecodeElement(&elem, &se); err != nil {
			return nil, err
		}
		if elem.ID == "" {
			elem.ID = fmt.Sprintf("http_%d", *scriptIndex)
			(*scriptIndex)++
		}
		return &PipelineNode{
			Kind:       NodeHTTPClient,
			HTTPClient: &elem,
		}, nil
	case "script":
		lang, scriptID, dbName, targetDB, targetTable, varName, outputVar := "", "", "", "", "", "", ""
		batchSize := 0
		tablock := true // Default TABLOCK to true for high performance bulk copy
		checkConstraints := false
		fireTriggers := false
		keepNulls := false

		for _, attr := range se.Attr {
			attrName := strings.ToLower(attr.Name.Local)
			switch attrName {
			case "language", "lang":
				lang = strings.ToLower(attr.Value)
			case "id":
				scriptID = attr.Value
			case "db", "database":
				dbName = attr.Value
			case "target_db", "target_database":
				targetDB = attr.Value
			case "target_table":
				targetTable = attr.Value
			case "batch_size":
				if b, err := strconv.Atoi(attr.Value); err == nil {
					batchSize = b
				}
			case "variable", "var":
				varName = attr.Value
			case "output_var", "output_variable", "out_var":
				outputVar = attr.Value
			case "tablock":
				if b, err := strconv.ParseBool(attr.Value); err == nil {
					tablock = b
				}
			case "check_constraints":
				if b, err := strconv.ParseBool(attr.Value); err == nil {
					checkConstraints = b
				}
			case "fire_triggers":
				if b, err := strconv.ParseBool(attr.Value); err == nil {
					fireTriggers = b
				}
			case "keep_nulls":
				if b, err := strconv.ParseBool(attr.Value); err == nil {
					keepNulls = b
				}
			}
		}

		if lang == "go" || lang == "sql" || lang == "shell" || lang == "cmd" ||
			lang == "powershell" || lang == "pwsh" || lang == "bash" ||
			lang == "git-bash" || lang == "gitbash" || lang == "zsh" ||
			lang == "ksh" || lang == "csh" || lang == "tcsh" || lang == "dash" ||
			lang == "fish" || lang == "sh" || lang == "dotnet-script" || lang == "csx" {
			if scriptID == "" {
				scriptID = fmt.Sprintf("script_%d", *scriptIndex)
				(*scriptIndex)++
			}
			var content string
			if err := decoder.DecodeElement(&content, &se); err != nil {
				return nil, err
			}
			return &PipelineNode{
				Kind: NodeScript,
				Script: &ScriptItem{
					ID:               scriptID,
					Language:         lang,
					DBName:           dbName,
					TargetDB:         targetDB,
					TargetTable:      targetTable,
					BatchSize:        batchSize,
					VarName:          varName,
					OutputVar:        outputVar,
					Code:             strings.TrimSpace(content),
					Tablock:          tablock,
					CheckConstraints: checkConstraints,
					FireTriggers:     fireTriggers,
					KeepNulls:        keepNulls,
				},
			}, nil
		}

	case "group":
		var groupID, ifVar, ifEquals, condition, dbName string
		var transaction bool
		for _, attr := range se.Attr {
			attrName := strings.ToLower(attr.Name.Local)
			switch attrName {
			case "id":
				groupID = attr.Value
			case "if_var", "var":
				ifVar = attr.Value
			case "if_val", "if_equals", "equals", "value":
				ifEquals = attr.Value
			case "condition", "cond":
				condition = attr.Value
			case "transaction":
				if b, err := strconv.ParseBool(attr.Value); err == nil {
					transaction = b
				}
			case "db", "database":
				dbName = attr.Value
			}
		}
		if condition != "" && ifVar == "" {
			ifVar = condition
		}

		children, err := parseChildrenUntil(decoder, "group", scriptIndex)
		if err != nil {
			return nil, err
		}

		return &PipelineNode{
			Kind:        NodeGroup,
			GroupID:     groupID,
			IfVar:       ifVar,
			IfEquals:    ifEquals,
			Children:    children,
			Transaction: transaction,
			DBName:      dbName,
		}, nil

	case "parallel":
		maxThreads := 0
		for _, attr := range se.Attr {
			attrName := strings.ToLower(attr.Name.Local)
			if attrName == "max_threads" || attrName == "threads" || attrName == "concurrency" {
				if t, err := strconv.Atoi(attr.Value); err == nil {
					maxThreads = t
				}
			}
		}

		children, err := parseChildrenUntil(decoder, "parallel", scriptIndex)
		if err != nil {
			return nil, err
		}

		return &PipelineNode{
			Kind:       NodeParallel,
			MaxThreads: maxThreads,
			Children:   children,
		}, nil

	case "if":
		var ifVar, ifEquals, condition string
		for _, attr := range se.Attr {
			attrName := strings.ToLower(attr.Name.Local)
			switch attrName {
			case "var", "if_var":
				ifVar = attr.Value
			case "equals", "val", "value", "if_val", "if_equals":
				ifEquals = attr.Value
			case "condition", "cond":
				condition = attr.Value
			}
		}
		if condition != "" && ifVar == "" {
			ifVar = condition
		}

		thenNodes, elseNodes, err := parseIfChildren(decoder, scriptIndex)
		if err != nil {
			return nil, err
		}

		return &PipelineNode{
			Kind:      NodeIf,
			IfVar:     ifVar,
			IfEquals:  ifEquals,
			Children:  thenNodes,
			ElseNodes: elseNodes,
		}, nil

	case "foreach", "loop":
		var foreachID, lang, dbName, varName string
		for _, attr := range se.Attr {
			attrName := strings.ToLower(attr.Name.Local)
			switch attrName {
			case "id":
				foreachID = attr.Value
			case "language", "lang":
				lang = strings.ToLower(attr.Value)
			case "db", "database":
				dbName = attr.Value
			case "variable", "var":
				varName = attr.Value
			}
		}

		if lang == "" {
			lang = "sql"
		}
		if foreachID == "" {
			foreachID = fmt.Sprintf("foreach_%d", *scriptIndex)
			(*scriptIndex)++
		}

		driverCode, children, err := parseForEachBody(decoder, elemName, scriptIndex)
		if err != nil {
			return nil, err
		}
		driverScript := &ScriptItem{
			ID:       fmt.Sprintf("%s_driver", foreachID),
			Language: lang,
			DBName:   dbName,
			VarName:  varName,
			Code:     driverCode,
		}

		return &PipelineNode{
			Kind:          NodeForEach,
			GroupID:       foreachID,
			ForEachScript: driverScript,
			Children:      children,
		}, nil

	case "while":
		var whileID, ifVar, ifEquals, condition string
		maxIterations := 1000

		for _, attr := range se.Attr {
			attrName := strings.ToLower(attr.Name.Local)
			switch attrName {
			case "id":
				whileID = attr.Value
			case "var", "if_var":
				ifVar = attr.Value
			case "equals", "val", "value", "if_val", "if_equals":
				ifEquals = attr.Value
			case "condition", "cond":
				condition = attr.Value
			case "max_iterations", "max_loops":
				if m, err := strconv.Atoi(attr.Value); err == nil && m > 0 {
					maxIterations = m
				}
			}
		}

		if condition != "" && ifVar == "" {
			ifVar = condition
		}

		if whileID == "" {
			whileID = fmt.Sprintf("while_%d", *scriptIndex)
			(*scriptIndex)++
		}

		children, err := parseChildrenUntil(decoder, "while", scriptIndex)
		if err != nil {
			return nil, err
		}

		return &PipelineNode{
			Kind:          NodeWhile,
			GroupID:       whileID,
			IfVar:         ifVar,
			IfEquals:      ifEquals,
			MaxIterations: maxIterations,
			Children:      children,
		}, nil
	}

	return nil, nil
}

func parseForEachBody(decoder *xml.Decoder, closingTag string, scriptIndex *int) (string, []PipelineNode, error) {
	var nodes []PipelineNode
	var codeBuilder strings.Builder

	for {
		tok, err := decoder.Token()
		if err == io.EOF {
			return "", nil, fmt.Errorf("unexpected EOF waiting for </%s>", closingTag)
		}
		if err != nil {
			return "", nil, err
		}

		switch t := tok.(type) {
		case xml.EndElement:
			if strings.EqualFold(t.Name.Local, closingTag) {
				return strings.TrimSpace(codeBuilder.String()), nodes, nil
			}
		case xml.CharData:
			codeBuilder.Write([]byte(t))
		case xml.StartElement:
			node, err := parseNodeElement(decoder, t, scriptIndex)
			if err != nil {
				return "", nil, err
			}
			if node != nil {
				nodes = append(nodes, *node)
			}
		}
	}
}

func parseChildrenUntil(decoder *xml.Decoder, closingTag string, scriptIndex *int) ([]PipelineNode, error) {
	var nodes []PipelineNode
	for {
		tok, err := decoder.Token()
		if err == io.EOF {
			return nil, fmt.Errorf("unexpected EOF waiting for </%s>", closingTag)
		}
		if err != nil {
			return nil, err
		}

		switch t := tok.(type) {
		case xml.EndElement:
			if strings.EqualFold(t.Name.Local, closingTag) {
				return nodes, nil
			}
		case xml.StartElement:
			node, err := parseNodeElement(decoder, t, scriptIndex)
			if err != nil {
				return nil, err
			}
			if node != nil {
				nodes = append(nodes, *node)
			}
		}
	}
}

func parseIfChildren(decoder *xml.Decoder, scriptIndex *int) ([]PipelineNode, []PipelineNode, error) {
	var thenNodes []PipelineNode
	var elseNodes []PipelineNode

	for {
		tok, err := decoder.Token()
		if err == io.EOF {
			return nil, nil, fmt.Errorf("unexpected EOF inside <if>")
		}
		if err != nil {
			return nil, nil, err
		}

		switch t := tok.(type) {
		case xml.EndElement:
			if strings.EqualFold(t.Name.Local, "if") {
				return thenNodes, elseNodes, nil
			}
		case xml.StartElement:
			elemName := strings.ToLower(t.Name.Local)
			if elemName == "then" {
				nodes, err := parseChildrenUntil(decoder, "then", scriptIndex)
				if err != nil {
					return nil, nil, err
				}
				thenNodes = append(thenNodes, nodes...)
			} else if elemName == "else" {
				nodes, err := parseChildrenUntil(decoder, "else", scriptIndex)
				if err != nil {
					return nil, nil, err
				}
				elseNodes = append(elseNodes, nodes...)
			} else {
				node, err := parseNodeElement(decoder, t, scriptIndex)
				if err != nil {
					return nil, nil, err
				}
				if node != nil {
					thenNodes = append(thenNodes, *node)
				}
			}
		}
	}
}
func (f *FileReadElement) GetFilePath() string {
	if f.File != "" {
		return f.File
	}
	if f.Path != "" {
		return f.Path
	}
	return f.Filename
}

func (f *FileReadElement) GetOutputVar() string {
	for _, v := range []string{f.Var, f.Variable, f.OutputVar, f.OutputVariable, f.OutVar} {
		if v != "" {
			return v
		}
	}
	return ""
}
func (x *XmlXPathElement) GetXPath() string {
	if x.XPath != "" {
		return x.XPath
	}
	return strings.TrimSpace(x.Content)
}
func (j *JsonPathElement) GetJSONPath() string {
	if j.Path != "" {
		return j.Path
	}
	if j.JSONPath != "" {
		return j.JSONPath
	}
	return strings.TrimSpace(j.Content)
}

func (j *JsonPathElement) GetOutputVar() string {
	if j.OutputVar != "" {
		return j.OutputVar
	}
	return j.OutVar
}
func (y *YamlPathElement) GetYAMLPath() string {
	if y.Path != "" {
		return y.Path
	}
	if y.YAMLPath != "" {
		return y.YAMLPath
	}
	return strings.TrimSpace(y.Content)
}

func (y *YamlPathElement) GetOutputVar() string {
	if y.OutputVar != "" {
		return y.OutputVar
	}
	return y.OutVar
}
// PipelineConfig encapsulates the complete parsed AST structure.
type PipelineConfig struct {
	Variables      []VariableConfig
	Databases      []DatabaseConfig
	PreflightNodes []PipelineNode
	FlowNodes      []PipelineNode
}

 