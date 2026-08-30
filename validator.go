package flow

import (
	"fmt"
	"strings"
)

// ValidateAST verifies script IDs, database names, and structural rules across preflight and flow ASTs.
func ValidateAST(preflightNodes []PipelineNode, flowNodes []PipelineNode, registeredDBs []DatabaseConfig) error {
	var errs []string
	knownIDs := make(map[string]bool)

	definedDBs := make(map[string]bool)
	for _, db := range registeredDBs {
		definedDBs[db.Name] = true
	}

	var inspect func(nodes []PipelineNode)
	inspect = func(nodes []PipelineNode) {
		for _, node := range nodes {
			switch node.Kind {
			case NodeAssert:
				a := node.Assert
				if a != nil {
					if a.ID != "" {
						if knownIDs[a.ID] {
							errs = append(errs, fmt.Sprintf("duplicate ID found: '%s'", a.ID))
						}
						knownIDs[a.ID] = true
					}
					if a.Var == "" {
						errs = append(errs, fmt.Sprintf("assert node '%s' is missing required 'var' attribute", a.ID))
					}
					if len(a.FailureNodes) > 0 {
						inspect(a.FailureNodes)
					}
				}
			case NodeSQL, NodeSQLBulk:
				s := node.Script
				if s != nil {
					if s.ID != "" {
						if knownIDs[s.ID] {
							errs = append(errs, fmt.Sprintf("duplicate script ID found: '%s'", s.ID))
						}
						knownIDs[s.ID] = true
					}
					if s.DBName != "" && !definedDBs[s.DBName] {
						errs = append(errs, fmt.Sprintf("sql script '%s' references unregistered database '%s'", s.ID, s.DBName))
					}
					if node.Kind == NodeSQLBulk {
						if s.TargetTable == "" {
							errs = append(errs, fmt.Sprintf("sql-bulk node '%s' is missing 'target_table' attribute", s.ID))
						}
						if s.TargetDB != "" && !definedDBs[s.TargetDB] {
							errs = append(errs, fmt.Sprintf("sql-bulk '%s' target_db references unregistered database '%s'", s.ID, s.TargetDB))
						}
					}
					if strings.TrimSpace(s.Code) == "" && s.VarName == "" {
						errs = append(errs, fmt.Sprintf("sql script '%s' has an empty body and no driver variable", s.ID))
					}
				}
			case NodeHTTPClient:
				h := node.HTTPClient
				if h.ID != "" {
					if knownIDs[h.ID] {
						errs = append(errs, fmt.Sprintf("duplicate script/HTTP ID found: '%s'", h.ID))
					}
					knownIDs[h.ID] = true
				}
				if h.URI == "" && h.URL == "" {
					errs = append(errs, fmt.Sprintf("HTTP_CLIENT '%s' is missing 'uri' or 'url' attribute", h.ID))
				}
			case NodeScript:
				s := node.Script
				if s.ID != "" {
					if knownIDs[s.ID] {
						errs = append(errs, fmt.Sprintf("duplicate script ID found: '%s'", s.ID))
					}
					knownIDs[s.ID] = true
				}

				if s.Language == "sql" && s.DBName != "" {
					if !definedDBs[s.DBName] {
						errs = append(errs, fmt.Sprintf("script '%s' references unregistered database '%s'", s.ID, s.DBName))
					}
				}
				if s.TargetDB != "" && !definedDBs[s.TargetDB] {
					errs = append(errs, fmt.Sprintf("script '%s' target_db references unregistered database '%s'", s.ID, s.TargetDB))
				}

				if strings.TrimSpace(s.Code) == "" && s.VarName == "" {
					errs = append(errs, fmt.Sprintf("script '%s' has an empty body and no driver variable", s.ID))
				}

			case NodeParallel:
				if node.MaxThreads < 0 {
					errs = append(errs, fmt.Sprintf("parallel block has invalid max_threads: %d", node.MaxThreads))
				}
				if len(node.Children) == 0 {
					errs = append(errs, "parallel block contains no child scripts")
				}
				inspect(node.Children)

			case NodeIf:
				if node.IfVar == "" {
					errs = append(errs, "<if> condition tag is missing a target variable name")
				}
				if len(node.Children) == 0 && len(node.ElseNodes) == 0 {
					errs = append(errs, "<if> block contains neither <then> nor <else> child nodes")
				}
				inspect(node.Children)
				inspect(node.ElseNodes)

			case NodeForEach:
				if node.ForEachScript != nil && node.ForEachScript.DBName != "" {
					if !definedDBs[node.ForEachScript.DBName] {
						errs = append(errs, fmt.Sprintf("foreach '%s' driver query references unregistered database '%s'", node.GroupID, node.ForEachScript.DBName))
					}
				}
				inspect(node.Children)

			case NodeWhile:
				if node.IfVar == "" {
					errs = append(errs, fmt.Sprintf("<while> loop '%s' is missing condition/var attributes", node.GroupID))
				}
				if len(node.Children) == 0 {
					errs = append(errs, fmt.Sprintf("<while> loop '%s' contains no child nodes", node.GroupID))
				}
				inspect(node.Children)

			case NodeGroup:
				inspect(node.Children)
			}
		}
	}

	// Inspect both preflight and flow node trees
	inspect(preflightNodes)
	inspect(flowNodes)

	if len(errs) > 0 {
		return fmt.Errorf("XML semantic validation failed:\n - %s", strings.Join(errs, "\n - "))
	}
	return nil
}
