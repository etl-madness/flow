package flow

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
)

type RunStatus string

const (
	RunStatusSucceeded RunStatus = "succeeded"
	RunStatusFailed    RunStatus = "failed"
	RunStatusCanceled  RunStatus = "canceled"
)

var sensitiveValuePattern = regexp.MustCompile(`(?i)(password|pwd|token|secret|authorization)\s*([=:])\s*([^\s,;]+)`)

type ErrorClass string

const (
	ErrorClassCanceled      ErrorClass = "canceled"
	ErrorClassConfiguration ErrorClass = "configuration"
	ErrorClassValidation    ErrorClass = "validation"
	ErrorClassDatabase      ErrorClass = "database"
	ErrorClassHTTP          ErrorClass = "http"
	ErrorClassFileSystem    ErrorClass = "filesystem"
	ErrorClassScript        ErrorClass = "script"
	ErrorClassTemplate      ErrorClass = "template"
	ErrorClassDataFormat    ErrorClass = "data_format"
	ErrorClassInternal      ErrorClass = "internal"
	ErrorClassUnknown       ErrorClass = "unknown"
)

type RowCounts struct {
	Read     int64 `json:"read,omitempty"`
	Written  int64 `json:"written,omitempty"`
	Affected int64 `json:"affected,omitempty"`
}

type AttemptResult struct {
	Attempt      int        `json:"attempt"`
	StartedAt    time.Time  `json:"started_at"`
	FinishedAt   time.Time  `json:"finished_at"`
	Status       RunStatus  `json:"status"`
	ErrorClass   ErrorClass `json:"error_class,omitempty"`
	ErrorMessage string     `json:"error_message,omitempty"`
	RowCounts    RowCounts  `json:"row_counts,omitempty"`
}

type NodeResult struct {
	ExecutionID       string          `json:"execution_id"`
	ParentExecutionID string          `json:"parent_execution_id,omitempty"`
	NodeID            string          `json:"node_id,omitempty"`
	NodeKind          string          `json:"node_kind"`
	NodePath          string          `json:"node_path"`
	StartedAt         time.Time       `json:"started_at"`
	FinishedAt        time.Time       `json:"finished_at"`
	Status            RunStatus       `json:"status"`
	Attempts          []AttemptResult `json:"attempts"`
	RowCounts         RowCounts       `json:"row_counts,omitempty"`
	ErrorClass        ErrorClass      `json:"error_class,omitempty"`
	ErrorMessage      string          `json:"error_message,omitempty"`
}

type RunResult struct {
	RunID        string       `json:"run_id"`
	StartedAt    time.Time    `json:"started_at"`
	FinishedAt   time.Time    `json:"finished_at"`
	Status       RunStatus    `json:"status"`
	ErrorClass   ErrorClass   `json:"error_class,omitempty"`
	ErrorMessage string       `json:"error_message,omitempty"`
	Nodes        []NodeResult `json:"nodes"`
}

type EventType string

const (
	EventRunStarted      EventType = "run.started"
	EventRunFinished     EventType = "run.finished"
	EventNodeStarted     EventType = "node.started"
	EventNodeFinished    EventType = "node.finished"
	EventAttemptStarted  EventType = "attempt.started"
	EventAttemptFinished EventType = "attempt.finished"
)

type ExecutionEvent struct {
	Sequence          uint64     `json:"sequence"`
	Type              EventType  `json:"type"`
	OccurredAt        time.Time  `json:"occurred_at"`
	RunID             string     `json:"run_id"`
	ExecutionID       string     `json:"execution_id,omitempty"`
	ParentExecutionID string     `json:"parent_execution_id,omitempty"`
	NodeID            string     `json:"node_id,omitempty"`
	NodeKind          string     `json:"node_kind,omitempty"`
	NodePath          string     `json:"node_path,omitempty"`
	Attempt           int        `json:"attempt,omitempty"`
	Status            RunStatus  `json:"status,omitempty"`
	RowCounts         RowCounts  `json:"row_counts,omitempty"`
	ErrorClass        ErrorClass `json:"error_class,omitempty"`
	ErrorMessage      string     `json:"error_message,omitempty"`
}

// EventSink receives structured lifecycle events. Sink failures never change pipeline execution.
type EventSink interface {
	Emit(context.Context, ExecutionEvent) error
}

// JSONLineSink writes one JSON-encoded event per line to Writer.
type JSONLineSink struct {
	Writer io.Writer
	mu     sync.Mutex
}

func (s *JSONLineSink) Emit(_ context.Context, event ExecutionEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.NewEncoder(s.Writer).Encode(event)
}

// ClassifyError returns a stable category for an execution error.
func ClassifyError(err error) ErrorClass {
	if err == nil {
		return ""
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return ErrorClassCanceled
	}
	var pathErr *os.PathError
	if errors.As(err, &pathErr) {
		return ErrorClassFileSystem
	}
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return ErrorClassHTTP
	}
	return classifyErrorMessage(err.Error())
}

func classifyErrorMessage(message string) ErrorClass {
	message = strings.ToLower(message)
	switch {
	case strings.Contains(message, "context canceled"), strings.Contains(message, "deadline exceeded"):
		return ErrorClassCanceled
	case strings.Contains(message, "assertion"), strings.Contains(message, "validation"):
		return ErrorClassValidation
	case strings.Contains(message, "database"), strings.Contains(message, "sql "), strings.Contains(message, "query error"), strings.Contains(message, "transaction"):
		return ErrorClassDatabase
	case strings.Contains(message, "http"), strings.Contains(message, "url"):
		return ErrorClassHTTP
	case strings.Contains(message, "file"), strings.Contains(message, "directory"):
		return ErrorClassFileSystem
	case strings.Contains(message, "template"):
		return ErrorClassTemplate
	case strings.Contains(message, "json"), strings.Contains(message, "yaml"), strings.Contains(message, "xml"), strings.Contains(message, "excel"):
		return ErrorClassDataFormat
	case strings.Contains(message, "script"), strings.Contains(message, "interpreter"), strings.Contains(message, "command"):
		return ErrorClassScript
	default:
		return ErrorClassUnknown
	}
}

type executionScope struct {
	collector         *runCollector
	parentExecutionID string
	path              string
}

type executionScopeKey struct{}

func scopeFromContext(ctx context.Context) *executionScope {
	scope, _ := ctx.Value(executionScopeKey{}).(*executionScope)
	return scope
}

type runCollector struct {
	mu       sync.Mutex
	run      RunResult
	sequence uint64
	sinks    []EventSink
}

func newRunCollector(sinks []EventSink) *runCollector {
	startedAt := time.Now().UTC()
	return &runCollector{run: RunResult{RunID: newExecutionID(), StartedAt: startedAt}, sinks: sinks}
}

func (c *runCollector) emit(ctx context.Context, event ExecutionEvent) {
	c.mu.Lock()
	c.sequence++
	event.Sequence = c.sequence
	event.OccurredAt = time.Now().UTC()
	event.RunID = c.run.RunID
	sinks := append([]EventSink(nil), c.sinks...)
	c.mu.Unlock()

	for _, sink := range sinks {
		if sink != nil {
			_ = sink.Emit(ctx, event)
		}
	}
}

func (c *runCollector) startNode(ctx context.Context, parentID, nodeID, nodeKind, path string) (context.Context, string) {
	startedAt := time.Now().UTC()
	executionID := newExecutionID()
	node := NodeResult{
		ExecutionID: executionID, ParentExecutionID: parentID, NodeID: nodeID, NodeKind: nodeKind, NodePath: path,
		StartedAt: startedAt, Attempts: []AttemptResult{{Attempt: 1, StartedAt: startedAt}},
	}

	c.mu.Lock()
	c.run.Nodes = append(c.run.Nodes, node)
	c.mu.Unlock()

	event := eventFromNode(EventNodeStarted, node)
	c.emit(ctx, event)
	c.emit(ctx, eventFromNode(EventAttemptStarted, node))
	return context.WithValue(ctx, executionScopeKey{}, &executionScope{collector: c, parentExecutionID: executionID, path: path}), executionID
}

func (c *runCollector) finishNode(ctx context.Context, executionID string, hasError bool, result ScriptResult) {
	finishedAt := time.Now().UTC()
	c.mu.Lock()
	for index := range c.run.Nodes {
		node := &c.run.Nodes[index]
		if node.ExecutionID != executionID {
			continue
		}
		node.FinishedAt = finishedAt
		node.Status = RunStatusSucceeded
		if err := ctx.Err(); err != nil {
			node.Status = RunStatusCanceled
			node.ErrorMessage = err.Error()
			node.ErrorClass = ClassifyError(err)
		} else if hasError {
			node.Status = RunStatusFailed
			node.ErrorMessage = redactErrorMessage(scriptResultError(result))
			if node.ErrorMessage == "" {
				node.ErrorMessage = "pipeline node failed"
			}
			node.ErrorClass = classifyErrorMessage(node.ErrorMessage)
		}
		node.RowCounts = rowCountsFromResult(result)
		if len(node.Attempts) > 0 {
			attempt := &node.Attempts[len(node.Attempts)-1]
			attempt.FinishedAt = finishedAt
			attempt.Status = node.Status
			attempt.ErrorClass = node.ErrorClass
			attempt.ErrorMessage = node.ErrorMessage
		}
		finished := *node
		c.mu.Unlock()
		c.emit(ctx, eventFromNode(EventAttemptFinished, finished))
		c.emit(ctx, eventFromNode(EventNodeFinished, finished))
		return
	}
	c.mu.Unlock()
}

func (c *runCollector) finish(ctx context.Context, hasError bool) RunResult {
	c.mu.Lock()
	c.run.FinishedAt = time.Now().UTC()
	c.run.Status = RunStatusSucceeded
	if hasError {
		c.run.Status = RunStatusFailed
		c.run.ErrorMessage = "pipeline execution encountered errors"
		c.run.ErrorClass = ErrorClassUnknown
		for _, node := range c.run.Nodes {
			if node.Status == RunStatusFailed {
				c.run.ErrorMessage = node.ErrorMessage
				c.run.ErrorClass = node.ErrorClass
				break
			}
		}
	}
	if err := ctx.Err(); err != nil {
		c.run.Status = RunStatusCanceled
		c.run.ErrorMessage = err.Error()
		c.run.ErrorClass = ClassifyError(err)
	}
	run := c.run
	c.mu.Unlock()
	c.emit(ctx, ExecutionEvent{Type: EventRunFinished, Status: run.Status, ErrorClass: run.ErrorClass, ErrorMessage: run.ErrorMessage})
	return run
}

func eventFromNode(eventType EventType, node NodeResult) ExecutionEvent {
	event := ExecutionEvent{Type: eventType, ExecutionID: node.ExecutionID, ParentExecutionID: node.ParentExecutionID, NodeID: node.NodeID, NodeKind: node.NodeKind, NodePath: node.NodePath, Status: node.Status, RowCounts: node.RowCounts, ErrorClass: node.ErrorClass, ErrorMessage: node.ErrorMessage}
	if len(node.Attempts) > 0 {
		event.Attempt = node.Attempts[len(node.Attempts)-1].Attempt
	}
	return event
}

func newExecutionID() string {
	bytes := make([]byte, 16)
	if _, err := cryptorand.Read(bytes); err != nil {
		return time.Now().UTC().Format("20060102150405.000000000")
	}
	return hex.EncodeToString(bytes)
}

func scriptResultError(result ScriptResult) string {
	if result.ReturnCode == nil || result.ReturnCode == 0 || result.ReturnCode == "0" {
		return ""
	}
	if message, ok := result.ReturnCode.(string); ok {
		return message
	}
	return "pipeline node failed"
}

func redactErrorMessage(message string) string {
	return sensitiveValuePattern.ReplaceAllString(message, "$1$2[REDACTED]")
}

func nodeIdentity(node PipelineNode) (string, string) {
	switch node.Kind {
	case NodeAssert:
		return "assert", node.Assert.ID
	case NodeSQL:
		return "sql", node.Script.ID
	case NodeSQLBulk:
		return "sql_bulk", node.Script.ID
	case NodeScript:
		return "script", node.Script.ID
	case NodeHTTPClient:
		return "http_client", node.HTTPClient.ID
	case NodeTemplate:
		return "template", node.Template.ID
	case NodeHtmlTemplate:
		return "template_html", node.HtmlTemplate.ID
	case NodeFileSave:
		return "file_save", node.FileSave.ID
	case NodeFileRead:
		return "file_read", node.FileRead.ID
	case NodeExcelRead:
		return "excel_read", node.ExcelRead.ID
	case NodeExcelWrite:
		return "excel_write", node.ExcelWrite.ID
	case NodeXMLXPath:
		return "xml_xpath", node.XmlXPath.ID
	case NodeJSONPath:
		return "json_path", node.JsonPath.ID
	case NodeYAMLPath:
		return "yaml_path", node.YamlPath.ID
	case NodeGroup:
		return "group", node.GroupID
	case NodeParallel:
		return "parallel", node.GroupID
	case NodeIf:
		return "if", node.GroupID
	case NodeForEach:
		return "foreach", node.GroupID
	case NodeWhile:
		return "while", node.GroupID
	default:
		return "unknown", ""
	}
}

func nodeResultFor(node PipelineNode, results []ScriptResult) ScriptResult {
	_, nodeID := nodeIdentity(node)
	for index := len(results) - 1; index >= 0; index-- {
		if nodeID == "" || results[index].ScriptID == nodeID {
			return results[index]
		}
	}
	return ScriptResult{}
}

func rowCountsFromResult(result ScriptResult) RowCounts {
	var count int64
	if _, err := fmt.Sscanf(result.ResultsString, "Streamed %d row(s)", &count); err == nil {
		return RowCounts{Read: count, Written: count}
	}
	if _, err := fmt.Sscanf(result.ResultsString, "(%d row(s) affected)", &count); err == nil {
		return RowCounts{Affected: count}
	}
	if _, err := fmt.Sscanf(result.ResultsString, "(%d row(s) returned)", &count); err == nil {
		return RowCounts{Read: count}
	}
	return RowCounts{}
}
