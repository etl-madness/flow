package flow

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	htmltemplate "html/template"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"text/template"
	"time"

	"github.com/antchfx/xmlquery"
	"github.com/spyzhov/ajson"
	"github.com/traefik/yaegi/interp"
	"github.com/traefik/yaegi/stdlib"
	"github.com/xuri/excelize/v2"
	"gopkg.in/yaml.v3"
)

// ScriptResult represents the complete outcome of a single executed script or loop block.
type ScriptResult struct {
	ScriptID      string `json:"script_id"`          // Unique script identifier
	ReturnCode    any    `json:"return_code"`        // 0 on success, or error details on failure
	ResultsString string `json:"results_string"`     // Output logs, driver queries, or execution results
	Duration      string `json:"duration,omitempty"` // Cumulative execution time
}

// Executor orchestrates recursive pipeline AST node executions.
type Executor struct {
	registry    *Registry
	resultsMu   sync.Mutex
	sinksMu     sync.RWMutex
	eventSinks  []EventSink
	verbose     atomic.Bool
	goPath      string
	activeTxs   map[string]*sql.Tx // Track active transactions per database
	interpHook  func(*interp.Options)
	interpMu    sync.Mutex
	interpCache map[string]*interp.Interpreter
}

// NewExecutor creates and returns a new Executor configured with the provided Registry.
func NewExecutor(r *Registry) *Executor {
	return &Executor{
		registry:    r,
		interpCache: make(map[string]*interp.Interpreter),
	}
}
func (e *Executor) SetGoPath(goPath string) {
	e.goPath = goPath
}

// SetVerbose sets whether execution start and finish events should be printed to the console.
func (e *Executor) SetVerbose(verbose bool) {
	e.verbose.Store(verbose)
}

// SetEventSink replaces all event sinks with sink. A nil sink disables event emission.
func (e *Executor) SetEventSink(sink EventSink) {
	e.SetEventSinks(sink)
}

// SetEventSinks replaces the event sinks used by subsequent executions.
func (e *Executor) SetEventSinks(sinks ...EventSink) {
	e.sinksMu.Lock()
	defer e.sinksMu.Unlock()
	e.eventSinks = append([]EventSink(nil), sinks...)
}

// SetInterpHook registers a callback to customize Yaegi interpreter options.
func (e *Executor) SetInterpHook(hook func(*interp.Options)) {
	e.interpHook = hook
}

func (e *Executor) getGoInterpreter(ctx context.Context, script ScriptItem, opts interp.Options) (*interp.Interpreter, error) {
	cacheKey := script.ID
	if cacheKey == "" {
		hash := sha256.Sum256([]byte(script.Code))
		cacheKey = hex.EncodeToString(hash[:])
	}
	cacheKey = fmt.Sprintf("%s|%s|%s", cacheKey, e.goPath, opts.GoPath)

	e.interpMu.Lock()
	if e.interpCache == nil {
		e.interpCache = make(map[string]*interp.Interpreter)
	}
	_, _ = e.interpCache[cacheKey]
	e.interpMu.Unlock()

	interpInstance := interp.New(opts)
	if err := interpInstance.Use(stdlib.Symbols); err != nil {
		return nil, fmt.Errorf("failed to load stdlib symbols: %w", err)
	}

	dbExports := map[string]reflect.Value{
		"Get": reflect.ValueOf(e.registry.GetDB),
		"StreamETL": reflect.ValueOf(func(srcDB, query, dstDB, targetTable string, opts ETLOptions) (int64, error) {
			return StreamETL(ctx, e.registry, srcDB, query, dstDB, targetTable, opts)
		}),
	}

	varsExports := map[string]reflect.Value{
		"Get":         reflect.ValueOf(e.registry.GetVar),
		"GetString":   reflect.ValueOf(e.registry.GetVarString),
		"GetInt":      reflect.ValueOf(e.registry.GetVarInt),
		"GetBool":     reflect.ValueOf(e.registry.GetVarBool),
		"GetFloat":    reflect.ValueOf(e.registry.GetVarFloat),
		"GetTime":     reflect.ValueOf(e.registry.GetVarTime),
		"GetDateTime": reflect.ValueOf(e.registry.GetVarTime),
	}

	if err := interpInstance.Use(interp.Exports{
		"host/db":        dbExports,
		"host/vars":      varsExports,
		"host/db/db":     dbExports,
		"host/vars/vars": varsExports,
	}); err != nil {
		return nil, fmt.Errorf("failed to export packages: %w", err)
	}

	return interpInstance, nil
}

// getActiveTx returns the active transaction for the specified database if one exists.
func (e *Executor) getActiveTx(dbName string) *sql.Tx {
	if e.activeTxs == nil {
		return nil
	}
	return e.activeTxs[dbName]
}

// Execute triggers sequential or parallel tree evaluation for a slice of PipelineNodes.
func (e *Executor) Execute(ctx context.Context, nodes []PipelineNode) ([]ScriptResult, error) {
	_, results, err := e.executeRun(ctx, nodes)
	return results, err
}

// ExecuteRun executes nodes and returns structured run and node lifecycle results.
func (e *Executor) ExecuteRun(ctx context.Context, nodes []PipelineNode) (RunResult, error) {
	run, _, err := e.executeRun(ctx, nodes)
	return run, err
}

func (e *Executor) executeRun(ctx context.Context, nodes []PipelineNode) (RunResult, []ScriptResult, error) {
	var results []ScriptResult
	e.sinksMu.RLock()
	sinks := append([]EventSink(nil), e.eventSinks...)
	e.sinksMu.RUnlock()
	collector := newRunCollector(sinks)
	collector.emit(ctx, ExecutionEvent{Type: EventRunStarted, Status: RunStatusSucceeded})
	ctx = context.WithValue(ctx, executionScopeKey{}, &executionScope{collector: collector})
	hasErr := e.executeNodes(ctx, nodes, &results)
	run := collector.finish(ctx, hasErr)
	if hasErr {
		return run, results, fmt.Errorf("pipeline execution encountered errors")
	}
	return run, results, nil
}

func (e *Executor) appendResult(results *[]ScriptResult, res ScriptResult) {
	e.resultsMu.Lock()
	defer e.resultsMu.Unlock()
	*results = append(*results, res)
}

func (e *Executor) evalCondition(varName string, expectedVal string) bool {
	varName = strings.TrimSpace(varName)
	expectedVal = strings.TrimSpace(expectedVal)

	if expectedVal == "" && (strings.Contains(varName, "==") || strings.Contains(varName, "!=")) {
		if strings.Contains(varName, "==") {
			parts := strings.SplitN(varName, "==", 2)
			varName = strings.TrimSpace(parts[0])
			expectedVal = strings.TrimSpace(parts[1])
		} else if strings.Contains(varName, "!=") {
			parts := strings.SplitN(varName, "!=", 2)
			vName := strings.TrimSpace(parts[0])
			eVal := strings.TrimSpace(parts[1])
			return !e.evalCondition(vName, eVal)
		}
	}

	val := e.registry.GetVar(varName)
	if val == nil {
		return false
	}

	if expectedVal != "" {
		expectedVal = strings.Trim(expectedVal, "'\"")
		actualStr := strings.TrimSpace(fmt.Sprintf("%v", val))
		return strings.EqualFold(actualStr, expectedVal)
	}

	switch v := val.(type) {
	case bool:
		return v
	case int:
		return v != 0
	case float64:
		return v != 0.0
	case string:
		s := strings.ToLower(strings.TrimSpace(v))
		return s == "true" || s == "1" || s == "yes" || s == "y"
	default:
		return false
	}
}

func (e *Executor) executeSQLScript(ctx context.Context, dbName string, queryStr string) (resultsString string, rawOutput string, err error) {
	if dbName == "" {
		return "", "", fmt.Errorf("missing 'db' attribute on <script language=\"sql\"> tag")
	}

	variables := e.registry.CopyVariables()
	for name, val := range variables {
		placeholder := fmt.Sprintf("{{%s}}", name)
		queryStr = strings.ReplaceAll(queryStr, placeholder, fmt.Sprintf("%v", val))
	}
	trimmedQuery := strings.TrimSpace(queryStr)
	if strings.HasPrefix(trimmedQuery, "<![CDATA[") {
		trimmedQuery = strings.TrimPrefix(trimmedQuery, "<![CDATA[")
		trimmedQuery = strings.TrimSuffix(trimmedQuery, "]]>")
		trimmedQuery = strings.TrimSpace(trimmedQuery)
	}
	trimmedQuery = strings.ToUpper(trimmedQuery)
	hasReturning := strings.Contains(trimmedQuery, "RETURNING") ||
		strings.Contains(trimmedQuery, "OUTPUT") ||
		strings.Contains(trimmedQuery, "@@ROWCOUNT") ||
		(strings.Contains(trimmedQuery, ";") && strings.Contains(trimmedQuery, "SELECT"))
	isDML := (strings.HasPrefix(trimmedQuery, "INSERT") ||
		strings.HasPrefix(trimmedQuery, "UPDATE") ||
		strings.HasPrefix(trimmedQuery, "DELETE")) &&
		!hasReturning
	// --- Execute DML statements with ExecContext ---
	if isDML {
		var res sql.Result
		var execErr error

		if tx := e.getActiveTx(dbName); tx != nil {
			res, execErr = tx.ExecContext(ctx, queryStr)
		} else {
			dbConn, err := e.registry.GetDB(dbName)
			if err != nil {
				return "", "", err
			}
			res, execErr = dbConn.ExecContext(ctx, queryStr)
		}
		if execErr != nil {
			return "", "", execErr
		}

		rowsAffected, err := res.RowsAffected()
		if err != nil {
			rowsAffected = 0
		}

		logOutput := fmt.Sprintf("(%d row(s) affected)\n", rowsAffected)
		rawOutput = fmt.Sprintf("%d", rowsAffected)
		return logOutput, rawOutput, nil
	}
	var rows *sql.Rows
	var queryErr error
	if tx := e.getActiveTx(dbName); tx != nil {
		rows, queryErr = tx.QueryContext(ctx, queryStr)
	} else {
		dbConn, err := e.registry.GetDB(dbName)
		if err != nil {
			return "", "", err
		}
		rows, queryErr = dbConn.QueryContext(ctx, queryStr)
	}
	if queryErr != nil {
		return "", "", queryErr
	}
	defer rows.Close()

	var dataBuf bytes.Buffer
	rowCount := 0
	var lastRowStrs []string
	var cols []string

	for {
		curCols, err := rows.Columns()
		if err == nil && len(curCols) > 0 {
			cols = curCols
			dataBuf.Reset()
			dataBuf.WriteString(strings.Join(cols, "\t") + "\n")
			rowCount = 0
			lastRowStrs = nil
		}

		for rows.Next() {
			rowCount++
			vals := make([]interface{}, len(cols))
			valPtrs := make([]interface{}, len(cols))
			for i := range vals {
				valPtrs[i] = &vals[i]
			}

			if err := rows.Scan(valPtrs...); err != nil {
				return dataBuf.String(), "", fmt.Errorf("row scan error: %w", err)
			}

			rowStrs := make([]string, len(cols))
			for i, v := range vals {
				if v == nil {
					rowStrs[i] = "NULL"
				} else if b, ok := v.([]byte); ok {
					rowStrs[i] = string(b)
				} else {
					rowStrs[i] = fmt.Sprintf("%v", v)
				}
			}
			lastRowStrs = rowStrs
			dataBuf.WriteString(strings.Join(rowStrs, "\t") + "\n")
		}

		if err := rows.Err(); err != nil {
			return dataBuf.String(), "", err
		}

		if !rows.NextResultSet() {
			break
		}
	}

	if rowCount == 1 && len(lastRowStrs) == 1 {
		rawOutput = strings.TrimSpace(lastRowStrs[0])
	} else {
		rawOutput = strings.TrimSpace(dataBuf.String())
	}

	var logBuf bytes.Buffer
	if hasReturning && rowCount == 1 {
		displayCount := rowCount
		if n, err := strconv.Atoi(rawOutput); err == nil {
			displayCount = n
		}
		logBuf.WriteString(fmt.Sprintf("\n(%d row(s) returned)\n", displayCount))
	} else {
		logBuf.WriteString(dataBuf.String())
		logBuf.WriteString(fmt.Sprintf("\n(%d row(s) returned)\n", rowCount))
	}

	return logBuf.String(), rawOutput, nil
}

func (e *Executor) storeScriptOutput(outputVar string, output string) {
	e.registry.SetVar("LAST_OUTPUT", output)
	if outputVar != "" {
		e.registry.SetVar(outputVar, output)
	}
}
func (e *Executor) executeScriptNode(ctx context.Context, script ScriptItem, results *[]ScriptResult) bool {
	startTime := time.Now()
	if e.verbose.Load() {
		if script.Language == "sql" {
			if script.DBName != "" && script.TargetTable != "" {
				fmt.Printf("Starting execution of script %q on database %q and target table %q", script.ID, script.DBName, script.TargetTable)
			} else if script.DBName != "" {
				fmt.Printf("Starting execution of script %q on database %q", script.ID, script.DBName)
			} else {
				fmt.Printf("Starting execution of script %q", script.ID)
			}
		} else {
			fmt.Printf("Starting execution of script %q", script.ID)
		}
	}
	codeToEval := script.Code

	if script.VarName != "" {
		val := e.registry.GetVar(script.VarName)
		if val != nil {
			strVal := strings.TrimSpace(fmt.Sprintf("%v", val))
			if strVal != "" && strVal != "<nil>" {
				codeToEval = strVal
			}
		}
	}

	res := ScriptResult{ScriptID: script.ID}

	appendWithDuration := func(r ScriptResult) {
		duration := time.Since(startTime)
		r.Duration = duration.String()
		if e.verbose.Load() {
			if r.ReturnCode != nil && r.ReturnCode != 0 && r.ReturnCode != "0" {
				fmt.Printf("Finished execution of script %q with error: %v (duration: %s)", script.ID, r.ReturnCode, r.Duration)
			} else {
				fmt.Printf("Finished execution of script %q (duration: %s)", script.ID, r.Duration)
			}
		}
		e.appendResult(results, r)
	}

	if script.Language == "sql" {
		if script.TargetTable != "" {
			targetDB := script.TargetDB
			if targetDB == "" {
				targetDB = script.DBName
			}

			targetTable := script.TargetTable
			variables := e.registry.CopyVariables()
			for name, val := range variables {
				placeholder := fmt.Sprintf("{{%s}}", name)
				targetTable = strings.ReplaceAll(targetTable, placeholder, fmt.Sprintf("%v", val))
				targetDB = strings.ReplaceAll(targetDB, placeholder, fmt.Sprintf("%v", val))
			}

			opts := ETLOptions{
				BatchSize:        script.BatchSize,
				Tablock:          script.Tablock,
				CheckConstraints: script.CheckConstraints,
				FireTriggers:     script.FireTriggers,
				KeepNulls:        script.KeepNulls,
			}

			copied, err := StreamETL(ctx, e.registry, script.DBName, codeToEval, targetDB, targetTable, opts)
			if err != nil {
				res.ReturnCode = err.Error()
				appendWithDuration(res)
				return true
			}
			res.ReturnCode = 0
			res.ResultsString = fmt.Sprintf("Streamed %d row(s) directly to %s.%s", copied, targetDB, targetTable)
			e.storeScriptOutput(script.OutputVar, fmt.Sprintf("%d", copied))
			appendWithDuration(res)
		} else {
			logOutput, rawOutput, err := e.executeSQLScript(ctx, script.DBName, codeToEval)
			res.ResultsString = logOutput
			if err != nil {
				res.ReturnCode = err.Error()
				appendWithDuration(res)
				return true
			}
			res.ReturnCode = 0
			e.storeScriptOutput(script.OutputVar, rawOutput)
			appendWithDuration(res)
		}

	} else if script.Language == "go" {
		var outBuf bytes.Buffer
		opts := interp.Options{
			GoPath: e.goPath,
			Stdout: &outBuf,
			Stderr: &outBuf,
		}
		if e.interpHook != nil {
			e.interpHook(&opts)
		}

		i, err := e.getGoInterpreter(ctx, script, opts)
		if err != nil {
			res.ReturnCode = 1
			res.ResultsString = err.Error()
			appendWithDuration(res)
			return true
		}

		_, err = i.Eval(codeToEval)
		if err != nil {
			res.ReturnCode = err.Error()
			res.ResultsString = outBuf.String()
			appendWithDuration(res)
			return true
		}

		res.ReturnCode = 0
		res.ResultsString = outBuf.String()
		e.storeScriptOutput(script.OutputVar, strings.TrimSpace(outBuf.String()))
		appendWithDuration(res)

	} else if script.Language == "dotnet-script" || script.Language == "csx" {
		tmpFile, err := os.CreateTemp("", "flow_script_*.csx")
		if err != nil {
			res.ReturnCode = 1
			res.ResultsString = fmt.Sprintf("failed to create temp file: %v", err)
			appendWithDuration(res)
			return true
		}
		tempPath := tmpFile.Name()
		defer os.Remove(tempPath)

		if _, err := tmpFile.WriteString(codeToEval); err != nil {
			tmpFile.Close()
			res.ReturnCode = 1
			res.ResultsString = fmt.Sprintf("failed to write script content: %v", err)
			appendWithDuration(res)
			return true
		}
		tmpFile.Close()

		variables := e.registry.CopyVariables()
		envVars := os.Environ()
		for name, val := range variables {
			envVars = append(envVars, fmt.Sprintf("%s=%v", name, val))
		}

		var cmd *exec.Cmd
		if _, err := exec.LookPath("dotnet-script"); err == nil {
			cmd = exec.CommandContext(ctx, "dotnet-script", tempPath)
		} else {
			cmd = exec.CommandContext(ctx, "dotnet", "script", tempPath)
		}
		cmd.Env = envVars

		outBytes, err := cmd.CombinedOutput()
		outStr := string(outBytes)

		if err != nil {
			res.ReturnCode = err.Error()
			res.ResultsString = outStr
			appendWithDuration(res)
			return true
		}

		res.ReturnCode = 0
		res.ResultsString = outStr
		e.storeScriptOutput(script.OutputVar, strings.TrimSpace(outStr))
		appendWithDuration(res)

	} else if isShellLanguage(script.Language) {
		variables := e.registry.CopyVariables()
		envVars := os.Environ()
		for name, val := range variables {
			envVars = append(envVars, fmt.Sprintf("%s=%v", name, val))
			// Still support simple {{Var}} templates if needed
			placeholder := fmt.Sprintf("{{%s}}", name)
			codeToEval = strings.ReplaceAll(codeToEval, placeholder, fmt.Sprintf("%v", val))
		}

		var cmd *exec.Cmd
		switch script.Language {
		case "pwsh":
			cmd = exec.CommandContext(ctx, "pwsh", "-NoProfile", "-NonInteractive", "-Command", codeToEval)
		case "powershell":
			cmd = exec.CommandContext(ctx, "powershell", "-NoProfile", "-NonInteractive", "-Command", codeToEval)
		case "bash", "zsh", "ksh", "csh", "tcsh", "dash", "fish", "sh":
			cmd = exec.CommandContext(ctx, script.Language, "-c", codeToEval)
		case "git-bash", "gitbash":
			bashExec := "bash"
			if _, err := exec.LookPath("git-bash"); err == nil {
				bashExec = "git-bash"
			} else if _, err := exec.LookPath("bash"); err == nil {
				bashExec = "bash"
			} else if _, err := os.Stat(`C:\Program Files\Git\bin\bash.exe`); err == nil {
				bashExec = `C:\Program Files\Git\bin\bash.exe`
			}
			cmd = exec.CommandContext(ctx, bashExec, "-c", codeToEval)
		case "cmd":
			cmd = exec.CommandContext(ctx, "cmd", "/C", codeToEval)
		default: // "shell" fallback based on OS / environment
			if userShell := os.Getenv("SHELL"); userShell != "" {
				cmd = exec.CommandContext(ctx, userShell, "-c", codeToEval)
			} else {
				switch runtime.GOOS {
				case "windows":
					cmd = exec.CommandContext(ctx, "cmd", "/C", codeToEval)
				case "darwin": // macOS
					cmd = exec.CommandContext(ctx, "zsh", "-c", codeToEval)
				case "freebsd":
					cmd = exec.CommandContext(ctx, "sh", "-c", codeToEval)
				default: // linux, openbsd, netbsd
					cmd = exec.CommandContext(ctx, "sh", "-c", codeToEval)
				}
			}
		}
		cmd.Env = envVars

		outBytes, err := cmd.CombinedOutput()
		outStr := string(outBytes)

		if err != nil {
			res.ReturnCode = err.Error()
			res.ResultsString = outStr
			appendWithDuration(res)
			return true
		}

		res.ReturnCode = 0
		res.ResultsString = outStr
		e.storeScriptOutput(script.OutputVar, strings.TrimSpace(outStr))
		appendWithDuration(res)
	}

	return false
}
func (e *Executor) executeForEachNode(ctx context.Context, node PipelineNode, results *[]ScriptResult) bool {
	startTime := time.Now()
	script := node.ForEachScript
	if script == nil {
		return false
	}

	codeToEval := script.Code
	if script.VarName != "" {
		val := e.registry.GetVar(script.VarName)
		if val != nil {
			strVal := strings.TrimSpace(fmt.Sprintf("%v", val))
			if strVal != "" && strVal != "<nil>" {
				codeToEval = strVal
			}
		}
	}

	appendWithDuration := func(r ScriptResult) {
		r.Duration = time.Since(startTime).String()
		e.appendResult(results, r)
	}

	if script.Language == "sql" {
		variables := e.registry.CopyVariables()
		for name, val := range variables {
			placeholder := fmt.Sprintf("{{%s}}", name)
			codeToEval = strings.ReplaceAll(codeToEval, placeholder, fmt.Sprintf("%v", val))
		}

		var rows *sql.Rows
		var queryErr error
		if tx := e.getActiveTx(script.DBName); tx != nil {
			rows, queryErr = tx.QueryContext(ctx, codeToEval)
		} else {
			dbConn, err := e.registry.GetDB(script.DBName)
			if err != nil {
				res := ScriptResult{ScriptID: script.ID, ReturnCode: err.Error()}
				appendWithDuration(res)
				return true
			}
			rows, queryErr = dbConn.QueryContext(ctx, codeToEval)
		}

		if queryErr != nil {
			res := ScriptResult{ScriptID: script.ID, ReturnCode: queryErr.Error()}
			appendWithDuration(res)
			return true
		}
		defer rows.Close()

		cols, err := rows.Columns()
		if err != nil {
			res := ScriptResult{ScriptID: script.ID, ReturnCode: err.Error()}
			appendWithDuration(res)
			return true
		}

		loopIdx := 0
		for rows.Next() {
			select {
			case <-ctx.Done():
				res := ScriptResult{ScriptID: script.ID, ReturnCode: ctx.Err().Error()}
				appendWithDuration(res)
				return true
			default:
			}

			vals := make([]interface{}, len(cols))
			valPtrs := make([]interface{}, len(cols))
			for i := range vals {
				valPtrs[i] = &vals[i]
			}

			if err := rows.Scan(valPtrs...); err != nil {
				res := ScriptResult{ScriptID: script.ID, ReturnCode: err.Error()}
				appendWithDuration(res)
				return true
			}

			e.registry.SetVar("LOOP_INDEX", loopIdx)
			for i, col := range cols {
				var strVal string
				if vals[i] == nil {
					strVal = "NULL"
				} else if b, ok := vals[i].([]byte); ok {
					strVal = string(b)
				} else {
					strVal = fmt.Sprintf("%v", vals[i])
				}
				e.registry.SetVar(col, strVal)
				e.registry.SetVar(strings.ToLower(col), strVal)
				e.registry.SetVar(strings.ToUpper(col), strVal)
			}

			if hasErr := e.executeNodes(ctx, node.Children, results); hasErr {
				return true
			}

			loopIdx++
		}

		if err := rows.Err(); err != nil {
			res := ScriptResult{ScriptID: script.ID, ReturnCode: err.Error()}
			appendWithDuration(res)
			return true
		}
		summary := ScriptResult{
			ScriptID:   script.ID,
			ReturnCode: 0,
			ResultsString: fmt.Sprintf(
				"foreach '%s' loop driver returned %d row(s) and executed %d iteration(s).", node.GroupID, loopIdx, loopIdx),
		}
		appendWithDuration(summary)
	}
	return false
}

func (e *Executor) executeWhileNode(ctx context.Context, node PipelineNode, results *[]ScriptResult) bool {
	iterations := 0
	maxLimit := node.MaxIterations
	if maxLimit <= 0 {
		maxLimit = 1000
	}

	for e.evalCondition(node.IfVar, node.IfEquals) {
		select {
		case <-ctx.Done():
			res := ScriptResult{
				ScriptID:   node.GroupID,
				ReturnCode: ctx.Err().Error(),
			}
			e.appendResult(results, res)
			return true
		default:
		}

		if iterations >= maxLimit {
			res := ScriptResult{
				ScriptID:   node.GroupID,
				ReturnCode: fmt.Sprintf("Exceeded maximum iteration limit (%d)", maxLimit),
			}
			e.appendResult(results, res)
			return true
		}

		e.registry.SetVar("WHILE_INDEX", iterations)

		if hasErr := e.executeNodes(ctx, node.Children, results); hasErr {
			return true
		}

		iterations++
	}

	return false
}
func (e *Executor) executeParallelNode(ctx context.Context, node PipelineNode, results *[]ScriptResult) bool {
	maxThreads := node.MaxThreads
	if maxThreads <= 0 {
		maxThreads = 4
	}

	sem := make(chan struct{}, maxThreads)
	var wg sync.WaitGroup
	var hasErr atomic.Bool

	// Track worker registries to collect variables after execution
	workerRegistries := make([]*Registry, len(node.Children))

	for i, child := range node.Children {
		wg.Add(1)
		sem <- struct{}{}

		// Create worker-isolated Executor with cloned variables
		workerReg := e.registry.Snapshot()
		workerRegistries[i] = workerReg

		// Inject worker-specific context variable
		workerReg.SetVar("_THREAD_ID", i)

		workerExec := &Executor{
			registry: workerReg,
			goPath:   e.goPath,
		}
		workerExec.verbose.Store(e.verbose.Load())

		go func(childNode PipelineNode, exec *Executor) {
			defer wg.Done()
			defer func() { <-sem }()

			if hasErr.Load() || ctx.Err() != nil {
				return
			}

			var localResults []ScriptResult
			childErr := exec.executeNodes(ctx, []PipelineNode{childNode}, &localResults)

			e.resultsMu.Lock()
			*results = append(*results, localResults...)
			e.resultsMu.Unlock()

			if childErr {
				hasErr.Store(true)
			}
		}(child, workerExec)
	}

	wg.Wait()

	// Merge variables from worker threads back into the main pipeline registry using conflict resolution
	mutationCounts := make(map[string]int)
	for _, wReg := range workerRegistries {
		if wReg == nil {
			continue
		}
		wReg.varMu.RLock()
		for k := range wReg.dirtyVars {
			mutationCounts[k]++
		}
		wReg.varMu.RUnlock()
	}

	for i, wReg := range workerRegistries {
		if wReg == nil {
			continue
		}
		wReg.varMu.RLock()
		for k := range wReg.dirtyVars {
			val := wReg.varRegistry[k]
			if mutationCounts[k] > 1 {
				// Collision detected! Apply namespace
				scopedKey := fmt.Sprintf("WORKER_%d_%s", i, k)
				e.registry.SetVar(scopedKey, val)
			} else {
				// Safe merge
				e.registry.SetVar(k, val)
			}
		}
		wReg.varMu.RUnlock()
	}

	return hasErr.Load() || ctx.Err() != nil
}

func (e *Executor) executeNodes(ctx context.Context, nodes []PipelineNode, results *[]ScriptResult) bool {
	for index, node := range nodes {
		select {
		case <-ctx.Done():
			return true
		default:
		}

		nodeCtx := ctx
		var executionID string
		if scope := scopeFromContext(ctx); scope != nil {
			nodeKind, nodeID := nodeIdentity(node)
			path := fmt.Sprintf("%s[%d]", nodeKind, index)
			if scope.path != "" {
				path = fmt.Sprintf("%s/%s", scope.path, path)
			}
			nodeCtx, executionID = scope.collector.startNode(ctx, scope.parentExecutionID, nodeID, nodeKind, path)
		}

		var nodeResults []ScriptResult
		var hasErr bool

		switch node.Kind {
		case NodeAssert:
			if node.Assert != nil {
				hasErr = e.executeAssertNode(nodeCtx, *node.Assert, &nodeResults)
			}
		case NodeSQL, NodeSQLBulk:
			if node.Script != nil {
				hasErr = e.executeScriptNode(nodeCtx, *node.Script, &nodeResults)
			}
		case NodeYAMLPath:
			hasErr = e.executeYAMLPathNode(nodeCtx, *node.YamlPath, &nodeResults)
		case NodeJSONPath:
			hasErr = e.executeJSONPathNode(nodeCtx, *node.JsonPath, &nodeResults)
		case NodeExcelRead:
			hasErr = e.executeExcelReadNode(nodeCtx, *node.ExcelRead, &nodeResults)
		case NodeExcelWrite:
			hasErr = e.executeExcelWriteNode(nodeCtx, *node.ExcelWrite, &nodeResults)
		case NodeFileRead:
			hasErr = e.executeFileReadNode(nodeCtx, *node.FileRead, &nodeResults)
		case NodeFileSave:
			hasErr = e.executeFileSaveNode(nodeCtx, *node.FileSave, &nodeResults)
		case NodeTemplate:
			hasErr = e.executeTemplateNode(nodeCtx, *node.Template, &nodeResults)
		case NodeHtmlTemplate:
			hasErr = e.executeHtmlTemplateNode(nodeCtx, *node.HtmlTemplate, &nodeResults)
		case NodeXMLXPath:
			hasErr = e.executeXMLXPathNode(nodeCtx, *node.XmlXPath, &nodeResults)
		case NodeHTTPClient:
			hasErr = e.executeHTTPClientNode(nodeCtx, *node.HTTPClient, &nodeResults)
		case NodeScript:
			hasErr = e.executeScriptNode(nodeCtx, *node.Script, &nodeResults)

		case NodeGroup:
			if node.IfVar != "" {
				if !e.evalCondition(node.IfVar, node.IfEquals) {
					break
				}
			}
			if node.Transaction {
				if node.DBName == "" {
					res := ScriptResult{
						ScriptID:   node.GroupID,
						ReturnCode: "transaction group is missing 'db' or 'database' attribute",
					}
					e.appendResult(&nodeResults, res)
					hasErr = true
					break
				}
				dbConn, err := e.registry.GetDB(node.DBName)
				if err != nil {
					res := ScriptResult{
						ScriptID:   node.GroupID,
						ReturnCode: fmt.Sprintf("failed to get db connection for transaction: %v", err),
					}
					e.appendResult(&nodeResults, res)
					hasErr = true
					break
				}
				tx, err := dbConn.BeginTx(ctx, nil)
				if err != nil {
					res := ScriptResult{
						ScriptID:   node.GroupID,
						ReturnCode: fmt.Sprintf("failed to begin transaction: %v", err),
					}
					e.appendResult(&nodeResults, res)
					hasErr = true
					break
				}

				if e.activeTxs == nil {
					e.activeTxs = make(map[string]*sql.Tx)
				}
				e.activeTxs[node.DBName] = tx

				hasErr = e.executeNodes(nodeCtx, node.Children, &nodeResults)

				delete(e.activeTxs, node.DBName)

				if hasErr {
					tx.Rollback()
					break
				}

				if err := tx.Commit(); err != nil {
					res := ScriptResult{
						ScriptID:   node.GroupID,
						ReturnCode: fmt.Sprintf("failed to commit transaction: %v", err),
					}
					e.appendResult(&nodeResults, res)
					hasErr = true
					break
				}
			} else {
				hasErr = e.executeNodes(nodeCtx, node.Children, &nodeResults)
			}

		case NodeParallel:
			hasErr = e.executeParallelNode(nodeCtx, node, &nodeResults)

		case NodeIf:
			condPassed := e.evalCondition(node.IfVar, node.IfEquals)
			var target []PipelineNode
			if condPassed {
				target = node.Children
			} else {
				target = node.ElseNodes
			}
			hasErr = e.executeNodes(nodeCtx, target, &nodeResults)

		case NodeForEach:
			hasErr = e.executeForEachNode(nodeCtx, node, &nodeResults)

		case NodeWhile:
			hasErr = e.executeWhileNode(nodeCtx, node, &nodeResults)
		}

		for _, result := range nodeResults {
			e.appendResult(results, result)
		}
		if scope := scopeFromContext(ctx); scope != nil {
			scope.collector.finishNode(nodeCtx, executionID, hasErr, nodeResultFor(node, nodeResults))
		}
		if hasErr {
			return true
		}
	}
	return false
}

// Place at the bottom of executor.go
func isShellLanguage(lang string) bool {
	switch lang {
	case "shell", "cmd", "powershell", "pwsh", "bash", "git-bash", "gitbash",
		"zsh", "ksh", "csh", "tcsh", "dash", "fish", "sh":
		return true
	default:
		return false
	}
}

var placeholderRegex = regexp.MustCompile(`\{\{\s*([a-zA-Z0-9_]+)\s*\}\}`)

func (e *Executor) executeHTTPClientNode(ctx context.Context, elem HTTPClientElement, results *[]ScriptResult) bool {
	startTime := time.Now()
	res := ScriptResult{ScriptID: elem.ID}

	if e.verbose.Load() {
		fmt.Printf("Starting execution of HTTP_CLIENT %q\n", elem.ID)
	}

	variables := e.registry.CopyVariables()

	// 1. Interpolate attribute placeholders
	elem.URI = interpolateVars(elem.URI, variables)
	elem.URL = interpolateVars(elem.URL, variables)
	elem.Headers = interpolateVars(elem.Headers, variables)
	elem.ContentType = interpolateVars(elem.ContentType, variables)

	// 2. Resolve POST/Request Payload (data attribute or element body text)
	rawBody := elem.Data
	if rawBody == "" {
		rawBody = strings.TrimSpace(elem.BodyContent)
	}

	if rawBody != "" {
		// Convert {{VarName}} to standard Go template {{.VarName}}
		templateText := placeholderRegex.ReplaceAllString(rawBody, "{{.$1}}")

		tmpl, err := template.New("http_body").Option("missingkey=zero").Parse(templateText)
		if err == nil {
			var buf bytes.Buffer
			if err := tmpl.Execute(&buf, variables); err == nil {
				elem.Data = buf.String()
			} else {
				elem.Data = interpolateVars(rawBody, variables)
			}
		} else {
			elem.Data = interpolateVars(rawBody, variables)
		}
	}

	// 3. Construct HTTP Client and Request
	client, req, err := BuildClientAndRequest(elem)
	if err != nil {
		res.ReturnCode = err.Error()
		res.Duration = time.Since(startTime).String()
		e.appendResult(results, res)
		return true
	}

	// 4. Send Request
	resp, err := client.Do(req.WithContext(ctx))
	if err != nil {
		res.ReturnCode = err.Error()
		res.Duration = time.Since(startTime).String()
		e.appendResult(results, res)
		return true
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		res.ReturnCode = fmt.Sprintf("failed to read response body: %v", err)
		res.Duration = time.Since(startTime).String()
		e.appendResult(results, res)
		return true
	}

	bodyStr := string(bodyBytes)
	res.ReturnCode = 0
	res.ResultsString = fmt.Sprintf("HTTP %d - %s", resp.StatusCode, http.StatusText(resp.StatusCode))
	res.Duration = time.Since(startTime).String()

	// 5. Store Output Variables
	e.storeScriptOutput(elem.GetOutputVariable(), strings.TrimSpace(bodyStr))
	if statusVar := elem.GetStatusCodeVariable(); statusVar != "" {
		e.registry.SetVar(statusVar, resp.StatusCode)
	}

	if e.verbose.Load() {
		fmt.Printf("Finished execution of HTTP_CLIENT %q (duration: %s)", elem.ID, res.Duration)
	}

	e.appendResult(results, res)
	return false
}

// Helper to replace {{VarName}} placeholders directly
func interpolateVars(input string, variables map[string]interface{}) string {
	if input == "" {
		return ""
	}
	for name, val := range variables {
		placeholder := fmt.Sprintf("{{%s}}", name)
		input = strings.ReplaceAll(input, placeholder, fmt.Sprintf("%v", val))
	}
	return input
}
func (e *Executor) executeTemplateNode(ctx context.Context, elem TemplateElement, results *[]ScriptResult) bool {
	startTime := time.Now()
	res := ScriptResult{ScriptID: elem.ID}

	variables := e.registry.CopyVariables()
	var templateText string
	if elem.File != "" {
		filePath := interpolateVars(elem.File, variables)
		fileBytes, err := os.ReadFile(filePath)
		if err != nil {
			res.ReturnCode = fmt.Sprintf("failed to read template file %s: %v", filePath, err)
			res.Duration = time.Since(startTime).String()
			e.appendResult(results, res)
			return true
		}
		templateText = string(fileBytes)
	} else {
		templateText = strings.TrimSpace(elem.Content)
	}

	tmplName := elem.Name
	if tmplName == "" {
		tmplName = elem.ID
	}

	tmpl, err := template.New(tmplName).Option("missingkey=zero").Parse(templateText)
	if err != nil {
		res.ReturnCode = fmt.Sprintf("failed to parse template: %v", err)
		res.Duration = time.Since(startTime).String()
		e.appendResult(results, res)
		return true
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, variables); err != nil {
		res.ReturnCode = fmt.Sprintf("failed to execute template: %v", err)
		res.Duration = time.Since(startTime).String()
		e.appendResult(results, res)
		return true
	}

	outputStr := buf.String()
	res.ReturnCode = 0
	if elem.Mode == "summary" {
		res.ResultsString = fmt.Sprintf("Successfully rendered template %q (%d bytes)", tmplName, len(outputStr))
	} else {
		res.ResultsString = fmt.Sprintf("%s", outputStr)
	}
	res.Duration = time.Since(startTime).String()

	e.storeScriptOutput(elem.GetOutputVar(), outputStr)
	e.appendResult(results, res)
	return false
}
func (e *Executor) executeHtmlTemplateNode(ctx context.Context, elem HtmlTemplateElement, results *[]ScriptResult) bool {
	startTime := time.Now()
	res := ScriptResult{ScriptID: elem.ID}

	variables := e.registry.CopyVariables()
	var templateText string
	if elem.File != "" {
		filePath := interpolateVars(elem.File, variables)
		fileBytes, err := os.ReadFile(filePath)
		if err != nil {
			res.ReturnCode = fmt.Sprintf("failed to read template file %s: %v", filePath, err)
			res.Duration = time.Since(startTime).String()
			e.appendResult(results, res)
			return true
		}
		templateText = string(fileBytes)
	} else {
		templateText = strings.TrimSpace(elem.Content)
	}

	tmplName := elem.Name
	if tmplName == "" {
		tmplName = elem.ID
	}

	tmpl, err := htmltemplate.New(tmplName).Option("missingkey=zero").Parse(templateText)
	if err != nil {
		res.ReturnCode = fmt.Sprintf("failed to parse template: %v", err)
		res.Duration = time.Since(startTime).String()
		e.appendResult(results, res)
		return true
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, variables); err != nil {
		res.ReturnCode = fmt.Sprintf("failed to execute template: %v", err)
		res.Duration = time.Since(startTime).String()
		e.appendResult(results, res)
		return true
	}

	outputStr := buf.String()
	res.ReturnCode = 0
	if elem.Mode == "summary" {
		res.ResultsString = fmt.Sprintf("Successfully rendered template %q (%d bytes)", tmplName, len(outputStr))
	} else {
		res.ResultsString = fmt.Sprintf("%s", outputStr)
	}
	res.Duration = time.Since(startTime).String()

	e.storeScriptOutput(elem.GetOutputVar(), outputStr)
	e.appendResult(results, res)
	return false
}
func (e *Executor) executeFileSaveNode(ctx context.Context, elem FileSaveElement, results *[]ScriptResult) bool {
	startTime := time.Now()
	res := ScriptResult{ScriptID: elem.ID}

	variables := e.registry.CopyVariables()
	filePath := interpolateVars(elem.GetFilePath(), variables)
	if filePath == "" {
		res.ReturnCode = "missing target file path attribute (file, path, or filename)"
		res.Duration = time.Since(startTime).String()
		e.appendResult(results, res)
		return true
	}

	// 1. Determine content source (either from a variable or inline body text)
	var contentToWrite string
	inputVar := elem.GetInputVar()
	if inputVar != "" {
		val := e.registry.GetVar(inputVar)
		if val != nil {
			contentToWrite = fmt.Sprintf("%v", val)
		}
	} else {
		contentToWrite = interpolateVars(strings.TrimSpace(elem.Content), variables)
	}

	// 2. Automatically create parent directories if needed
	dir := filepath.Dir(filePath)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			res.ReturnCode = fmt.Sprintf("failed to create directory structure %s: %v", dir, err)
			res.Duration = time.Since(startTime).String()
			e.appendResult(results, res)
			return true
		}
	}

	// 3. Open file in append or overwrite mode
	flags := os.O_WRONLY | os.O_CREATE
	if elem.Append != nil && *elem.Append {
		flags |= os.O_APPEND
	} else {
		flags |= os.O_TRUNC
	}

	file, err := os.OpenFile(filePath, flags, 0644)
	if err != nil {
		res.ReturnCode = fmt.Sprintf("failed to open destination file %s: %v", filePath, err)
		res.Duration = time.Since(startTime).String()
		e.appendResult(results, res)
		return true
	}
	defer file.Close()

	n, err := file.WriteString(contentToWrite)
	if err != nil {
		res.ReturnCode = fmt.Sprintf("failed to write file contents to %s: %v", filePath, err)
		res.Duration = time.Since(startTime).String()
		e.appendResult(results, res)
		return true
	}

	res.ReturnCode = 0
	res.ResultsString = fmt.Sprintf("Wrote %d bytes to %s", n, filePath)
	res.Duration = time.Since(startTime).String()

	if e.verbose.Load() {
		fmt.Printf("Finished execution of FILE_SAVE %q (wrote %d bytes to %s)", elem.ID, n, filePath)
	}

	e.appendResult(results, res)
	return false
}
func (e *Executor) executeFileReadNode(ctx context.Context, elem FileReadElement, results *[]ScriptResult) bool {
	startTime := time.Now()
	res := ScriptResult{ScriptID: elem.ID}

	variables := e.registry.CopyVariables()
	filePath := interpolateVars(elem.GetFilePath(), variables)
	if filePath == "" {
		res.ReturnCode = "missing target file path attribute (file, path, or filename)"
		res.Duration = time.Since(startTime).String()
		e.appendResult(results, res)
		return true
	}

	outVar := elem.GetOutputVar()
	if outVar == "" {
		res.ReturnCode = "missing output variable attribute (var, variable, output_var, out_var)"
		res.Duration = time.Since(startTime).String()
		e.appendResult(results, res)
		return true
	}

	// Read file contents from disk
	fileBytes, err := os.ReadFile(filePath)
	if err != nil {
		res.ReturnCode = fmt.Sprintf("failed to read file %s: %v", filePath, err)
		res.Duration = time.Since(startTime).String()
		e.appendResult(results, res)
		return true
	}

	contentStr := string(fileBytes)

	// Save to pipeline variables
	e.storeScriptOutput(outVar, contentStr)

	res.ReturnCode = 0
	res.ResultsString = fmt.Sprintf("Successfully read %d bytes from %s into variable %q", len(fileBytes), filePath, outVar)
	res.Duration = time.Since(startTime).String()

	if e.verbose.Load() {
		fmt.Printf("Finished execution of FILE_READ %q (read %d bytes from %s)", elem.ID, len(fileBytes), filePath)
	}

	e.appendResult(results, res)
	return false
}
func (e *Executor) executeExcelReadNode(ctx context.Context, elem ExcelReadElement, results *[]ScriptResult) bool {
	startTime := time.Now()
	res := ScriptResult{ScriptID: elem.ID}

	variables := e.registry.CopyVariables()
	filePath := interpolateVars(elem.File, variables)

	f, err := excelize.OpenFile(filePath)
	if err != nil {
		res.ReturnCode = fmt.Sprintf("failed to open excel file %s: %v", filePath, err)
		res.Duration = time.Since(startTime).String()
		e.appendResult(results, res)
		return true
	}
	defer f.Close()

	sheet := elem.Sheet
	if sheet == "" {
		sheet = f.GetSheetName(0)
	}

	rows, err := f.GetRows(sheet)
	if err != nil {
		res.ReturnCode = fmt.Sprintf("failed to read sheet %s: %v", sheet, err)
		res.Duration = time.Since(startTime).String()
		e.appendResult(results, res)
		return true
	}

	hasHeader := elem.Header == nil || *elem.Header
	var records []map[string]string

	if hasHeader && len(rows) > 0 {
		headers := rows[0]
		for _, row := range rows[1:] {
			record := make(map[string]string)
			for i, colCell := range row {
				if i < len(headers) {
					record[headers[i]] = colCell
				}
			}
			records = append(records, record)
		}
	}

	jsonBytes, err := json.Marshal(records)
	if err != nil {
		res.ReturnCode = fmt.Sprintf("failed to serialize excel data to JSON: %v", err)
		res.Duration = time.Since(startTime).String()
		e.appendResult(results, res)
		return true
	}

	outVar := elem.GetOutputVar()
	e.storeScriptOutput(outVar, string(jsonBytes))

	res.ReturnCode = 0
	res.ResultsString = fmt.Sprintf("Read %d row(s) from sheet %q into %q", len(records), sheet, outVar)
	res.Duration = time.Since(startTime).String()
	e.appendResult(results, res)
	return false
}
func (e *Executor) executeExcelWriteNode(ctx context.Context, elem ExcelWriteElement, results *[]ScriptResult) bool {
	startTime := time.Now()
	res := ScriptResult{ScriptID: elem.ID}

	variables := e.registry.CopyVariables()
	filePath := interpolateVars(elem.File, variables)
	sheet := elem.Sheet
	if sheet == "" {
		sheet = "Sheet1"
	}

	var f *excelize.File
	var err error

	// 1. Open existing file if present, otherwise create a new workbook
	if _, statErr := os.Stat(filePath); statErr == nil {
		f, err = excelize.OpenFile(filePath)
		if err != nil {
			res.ReturnCode = fmt.Sprintf("failed to open existing excel file %s: %v", filePath, err)
			res.Duration = time.Since(startTime).String()
			e.appendResult(results, res)
			return true
		}
	} else {
		f = excelize.NewFile()
	}
	defer f.Close()

	// 2. Create the sheet if it doesn't exist, or select it if it does
	sheetIdx, _ := f.GetSheetIndex(sheet)
	if sheetIdx == -1 {
		sheetIdx, _ = f.NewSheet(sheet)
	}
	f.SetActiveSheet(sheetIdx)

	var rowCount int

	if elem.DBName != "" && elem.Query != "" {
		dbConn, err := e.registry.GetDB(elem.DBName)
		if err != nil {
			res.ReturnCode = fmt.Sprintf("db connection error: %v", err)
			res.Duration = time.Since(startTime).String()
			e.appendResult(results, res)
			return true
		}

		queryStr := interpolateVars(strings.TrimSpace(elem.Query), variables)
		rows, err := dbConn.QueryContext(ctx, queryStr)
		if err != nil {
			res.ReturnCode = fmt.Sprintf("query error: %v", err)
			res.Duration = time.Since(startTime).String()
			e.appendResult(results, res)
			return true
		}
		defer rows.Close()

		cols, _ := rows.Columns()
		for i, col := range cols {
			cell, _ := excelize.CoordinatesToCellName(i+1, 1)
			f.SetCellValue(sheet, cell, col)
		}

		rowIdx := 2
		for rows.Next() {
			vals := make([]interface{}, len(cols))
			valPtrs := make([]interface{}, len(cols))
			for i := range vals {
				valPtrs[i] = &vals[i]
			}
			if err := rows.Scan(valPtrs...); err != nil {
				res.ReturnCode = fmt.Sprintf("scan error: %v", err)
				res.Duration = time.Since(startTime).String()
				e.appendResult(results, res)
				return true
			}

			for i, val := range vals {
				cell, _ := excelize.CoordinatesToCellName(i+1, rowIdx)
				if b, ok := val.([]byte); ok {
					f.SetCellValue(sheet, cell, string(b))
				} else {
					f.SetCellValue(sheet, cell, val)
				}
			}
			rowIdx++
		}
		rowCount = rowIdx - 2
	}

	if dir := filepath.Dir(filePath); dir != "" && dir != "." {
		os.MkdirAll(dir, 0755)
	}

	if err := f.SaveAs(filePath); err != nil {
		res.ReturnCode = fmt.Sprintf("failed to save excel file: %v", err)
		res.Duration = time.Since(startTime).String()
		e.appendResult(results, res)
		return true
	}

	res.ReturnCode = 0
	res.ResultsString = fmt.Sprintf("Wrote %d row(s) to Excel file %s (sheet: %s)", rowCount, filePath, sheet)
	res.Duration = time.Since(startTime).String()
	e.appendResult(results, res)
	return false
}

func (e *Executor) executeXMLXPathNode(ctx context.Context, elem XmlXPathElement, results *[]ScriptResult) bool {
	startTime := time.Now()
	res := ScriptResult{ScriptID: elem.ID}

	variables := e.registry.CopyVariables()

	// 1. Resolve XPath from attribute or body content
	xpathQuery := interpolateVars(elem.GetXPath(), variables)
	if xpathQuery == "" {
		res.ReturnCode = "missing XPath expression: specify 'xpath' attribute or inner body text"
		res.Duration = time.Since(startTime).String()
		e.appendResult(results, res)
		return true
	}

	// 2. Determine XML Source
	var rawXML string
	if elem.File != "" {
		filePath := interpolateVars(elem.File, variables)
		fileBytes, err := os.ReadFile(filePath)
		if err != nil {
			res.ReturnCode = fmt.Sprintf("failed to read xml file: %v", err)
			res.Duration = time.Since(startTime).String()
			e.appendResult(results, res)
			return true
		}
		rawXML = string(fileBytes)
	} else if elem.Var != "" {
		rawXML = e.registry.GetVarString(elem.Var)
	}

	// 3. Parse XML & Execute Query
	doc, err := xmlquery.Parse(strings.NewReader(rawXML))
	if err != nil {
		res.ReturnCode = fmt.Sprintf("xml parse error: %v", err)
		res.Duration = time.Since(startTime).String()
		e.appendResult(results, res)
		return true
	}

	nodes, err := xmlquery.QueryAll(doc, xpathQuery)
	if err != nil {
		res.ReturnCode = fmt.Sprintf("invalid xpath query %q: %v", xpathQuery, err)
		res.Duration = time.Since(startTime).String()
		e.appendResult(results, res)
		return true
	}

	// 4. Format Results
	var resultsArray []string
	for _, n := range nodes {
		if elem.Mode == "xml" {
			resultsArray = append(resultsArray, n.OutputXML(true))
		} else {
			resultsArray = append(resultsArray, n.InnerText())
		}
	}

	var finalOutput string
	if elem.Mode == "json_array" {
		jsonBytes, _ := json.Marshal(resultsArray)
		finalOutput = string(jsonBytes)
	} else {
		finalOutput = strings.Join(resultsArray, "\n")
	}

	e.storeScriptOutput(elem.OutputVar, finalOutput)

	res.ReturnCode = 0
	res.ResultsString = fmt.Sprintf("XPath query matched %d node(s)", len(nodes))
	res.Duration = time.Since(startTime).String()
	e.appendResult(results, res)
	return false
}
func (e *Executor) executeJSONPathNode(ctx context.Context, elem JsonPathElement, results *[]ScriptResult) bool {
	startTime := time.Now()
	res := ScriptResult{ScriptID: elem.ID}

	variables := e.registry.CopyVariables()

	// 1. Resolve JSONPath expression from attribute or inner body text
	jsonPathQuery := interpolateVars(elem.GetJSONPath(), variables)
	if jsonPathQuery == "" {
		res.ReturnCode = "missing JSONPath expression: specify 'path' attribute or inner body text"
		res.Duration = time.Since(startTime).String()
		e.appendResult(results, res)
		return true
	}

	outVar := elem.GetOutputVar()
	if outVar == "" {
		res.ReturnCode = "missing output variable attribute (output_var or out_var)"
		res.Duration = time.Since(startTime).String()
		e.appendResult(results, res)
		return true
	}

	// 2. Determine JSON source content (file or variable)
	var rawJSON string
	if elem.File != "" {
		filePath := interpolateVars(elem.File, variables)
		fileBytes, err := os.ReadFile(filePath)
		if err != nil {
			res.ReturnCode = fmt.Sprintf("failed to read json file %s: %v", filePath, err)
			res.Duration = time.Since(startTime).String()
			e.appendResult(results, res)
			return true
		}
		rawJSON = string(fileBytes)
	} else if elem.Var != "" {
		rawJSON = e.registry.GetVarString(elem.Var)
	}

	if strings.TrimSpace(rawJSON) == "" {
		res.ReturnCode = "source JSON string is empty"
		res.Duration = time.Since(startTime).String()
		e.appendResult(results, res)
		return true
	}

	// 3. Evaluate JSONPath query
	nodes, err := ajson.JSONPath([]byte(rawJSON), jsonPathQuery)
	if err != nil {
		res.ReturnCode = fmt.Sprintf("invalid JSONPath query %q: %v", jsonPathQuery, err)
		res.Duration = time.Since(startTime).String()
		e.appendResult(results, res)
		return true
	}

	// 4. Format outputs based on mode
	var finalOutput string
	mode := strings.ToLower(elem.Mode)

	if mode == "json_array" || mode == "json" {
		var values []interface{}
		for _, node := range nodes {
			nodeBytes, err := ajson.Marshal(node)
			var val interface{}
			if err == nil {
				_ = json.Unmarshal(nodeBytes, &val)
			} else {
				val, _ = node.Value()
			}
			values = append(values, val)
		}
		if mode == "json" && len(values) == 1 {
			jsonBytes, _ := json.Marshal(values[0])
			finalOutput = string(jsonBytes)
		} else {
			jsonBytes, _ := json.Marshal(values)
			finalOutput = string(jsonBytes)
		}
	} else { // Default "value" mode: scalar string or multiline text
		var resultsArray []string
		for _, node := range nodes {
			if node.IsString() {
				s, _ := node.GetString()
				resultsArray = append(resultsArray, s)
			} else {
				resultsArray = append(resultsArray, node.String())
			}
		}
		finalOutput = strings.Join(resultsArray, "\n")
	}

	e.storeScriptOutput(outVar, finalOutput)

	res.ReturnCode = 0
	res.ResultsString = fmt.Sprintf("JSONPath query %q matched %d node(s)", jsonPathQuery, len(nodes))
	res.Duration = time.Since(startTime).String()
	e.appendResult(results, res)
	return false
}
func convertYAMLMap(i interface{}) interface{} {
	switch x := i.(type) {
	case map[interface{}]interface{}:
		m2 := make(map[string]interface{})
		for k, v := range x {
			m2[fmt.Sprintf("%v", k)] = convertYAMLMap(v)
		}
		return m2
	case map[string]interface{}:
		m2 := make(map[string]interface{})
		for k, v := range x {
			m2[k] = convertYAMLMap(v)
		}
		return m2
	case []interface{}:
		for idx, v := range x {
			x[idx] = convertYAMLMap(v)
		}
	}
	return i
}
func (e *Executor) executeYAMLPathNode(ctx context.Context, elem YamlPathElement, results *[]ScriptResult) bool {
	startTime := time.Now()
	res := ScriptResult{ScriptID: elem.ID}

	variables := e.registry.CopyVariables()

	// 1. Resolve path query from attribute or inner body text
	pathQuery := interpolateVars(elem.GetYAMLPath(), variables)
	if pathQuery == "" {
		res.ReturnCode = "missing path expression: specify 'path' attribute or inner body text"
		res.Duration = time.Since(startTime).String()
		e.appendResult(results, res)
		return true
	}

	outVar := elem.GetOutputVar()
	if outVar == "" {
		res.ReturnCode = "missing output variable attribute (output_var or out_var)"
		res.Duration = time.Since(startTime).String()
		e.appendResult(results, res)
		return true
	}

	// 2. Load source YAML content
	var rawYAML string
	if elem.File != "" {
		filePath := interpolateVars(elem.File, variables)
		fileBytes, err := os.ReadFile(filePath)
		if err != nil {
			res.ReturnCode = fmt.Sprintf("failed to read yaml file %s: %v", filePath, err)
			res.Duration = time.Since(startTime).String()
			e.appendResult(results, res)
			return true
		}
		rawYAML = string(fileBytes)
	} else if elem.Var != "" {
		rawYAML = e.registry.GetVarString(elem.Var)
	}

	if strings.TrimSpace(rawYAML) == "" {
		res.ReturnCode = "source YAML string is empty"
		res.Duration = time.Since(startTime).String()
		e.appendResult(results, res)
		return true
	}

	// 3. Convert YAML into normalized JSON
	var yamlObj interface{}
	if err := yaml.Unmarshal([]byte(rawYAML), &yamlObj); err != nil {
		res.ReturnCode = fmt.Sprintf("yaml unmarshal error: %v", err)
		res.Duration = time.Since(startTime).String()
		e.appendResult(results, res)
		return true
	}

	normalizedObj := convertYAMLMap(yamlObj)
	jsonBytes, err := json.Marshal(normalizedObj)
	if err != nil {
		res.ReturnCode = fmt.Sprintf("failed to convert YAML to JSON: %v", err)
		res.Duration = time.Since(startTime).String()
		e.appendResult(results, res)
		return true
	}

	// 4. Evaluate path against converted JSON
	nodes, err := ajson.JSONPath(jsonBytes, pathQuery)
	if err != nil {
		res.ReturnCode = fmt.Sprintf("invalid path query %q: %v", pathQuery, err)
		res.Duration = time.Since(startTime).String()
		e.appendResult(results, res)
		return true
	}

	// 5. Format output based on mode
	var finalOutput string
	mode := strings.ToLower(elem.Mode)

	if mode == "json_array" || mode == "json" {
		var values []interface{}
		for _, node := range nodes {
			nodeBytes, err := ajson.Marshal(node)
			var val interface{}
			if err == nil {
				_ = json.Unmarshal(nodeBytes, &val)
			} else {
				val, _ = node.Value()
			}
			values = append(values, val)
		}
		if mode == "json" && len(values) == 1 {
			outB, _ := json.Marshal(values[0])
			finalOutput = string(outB)
		} else {
			outB, _ := json.Marshal(values)
			finalOutput = string(outB)
		}
	} else if mode == "yaml" {
		var values []interface{}
		for _, node := range nodes {
			nodeBytes, err := ajson.Marshal(node)
			var val interface{}
			if err == nil {
				_ = json.Unmarshal(nodeBytes, &val)
			} else {
				val, _ = node.Value()
			}
			values = append(values, val)
		}
		yBytes, _ := yaml.Marshal(values)
		finalOutput = string(yBytes)
	} else { // Default "value" mode
		var resultsArray []string
		for _, node := range nodes {
			if node.IsString() {
				s, _ := node.GetString()
				resultsArray = append(resultsArray, s)
			} else {
				resultsArray = append(resultsArray, node.String())
			}
		}
		finalOutput = strings.Join(resultsArray, "\n")
	}

	e.storeScriptOutput(outVar, finalOutput)

	res.ReturnCode = 0
	res.ResultsString = fmt.Sprintf("YAML path query %q matched %d node(s)", pathQuery, len(nodes))
	res.Duration = time.Since(startTime).String()
	e.appendResult(results, res)
	return false
}
func (e *Executor) executeAssertNode(ctx context.Context, elem AssertElement, results *[]ScriptResult) bool {
	startTime := time.Now()
	res := ScriptResult{ScriptID: elem.ID}

	expectedVal := elem.Equals
	if expectedVal == "" {
		expectedVal = elem.Value
	}

	// 1. Evaluate Condition using engine's evalCondition[cite: 4]
	passed := e.evalCondition(elem.Var, expectedVal)

	if !passed {
		errMsg := elem.Message
		if errMsg == "" {
			errMsg = fmt.Sprintf("Assertion failed for variable %q (expected: %q)", elem.Var, expectedVal)
		}

		// 2. Action A: Set a Failure Flag Variable if specified
		if elem.FailVar != "" {
			valToSet := elem.FailVal
			if valToSet == "" {
				valToSet = "true"
			}
			e.registry.SetVar(elem.FailVar, valToSet)
		}

		// 3. Action B: Execute nested <on_failure> nodes (e.g., Slack alert, SQL cleanup)
		if len(elem.FailureNodes) > 0 {
			if e.verbose.Load() {
				fmt.Printf("Assertion %q failed. Executing fallback <on_failure> block...", elem.ID)
			}
			// Execute nested child nodes recursively[cite: 4]
			_ = e.executeNodes(ctx, elem.FailureNodes, results)
		}

		actionStr := strings.ToLower(elem.OnFailure)
		res.Duration = time.Since(startTime).String()

		// 4. Action C: Determine whether to Halt or Continue
		switch actionStr {
		case "warn", "continue":
			// Log as a warning but DO NOT halt execution (return false)[cite: 4]
			res.ReturnCode = 0
			res.ResultsString = fmt.Sprintf("WARNING: %s (Pipeline continuing)", errMsg)
			e.appendResult(results, res)
			return false

		case "halt", "":
			fallthrough
		default:
			// Default Fail-Fast behavior: halt pipeline (return true)[cite: 4]
			res.ReturnCode = errMsg
			res.ResultsString = fmt.Sprintf("CRITICAL ASSERTION FAILURE: %s", errMsg)
			e.appendResult(results, res)
			return true
		}
	}

	// Assertion Passed
	res.ReturnCode = 0
	res.ResultsString = fmt.Sprintf("Assertion passed on variable %q", elem.Var)
	res.Duration = time.Since(startTime).String()
	e.appendResult(results, res)
	return false
}
