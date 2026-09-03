package flow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
)

type recordingEventSink struct {
	mu     sync.Mutex
	events []ExecutionEvent
	err    error
}

func (s *recordingEventSink) Emit(_ context.Context, event ExecutionEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, event)
	return s.err
}

func (s *recordingEventSink) Events() []ExecutionEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]ExecutionEvent(nil), s.events...)
}

func TestExecuteRunRecordsNestedNodeLifecycle(t *testing.T) {
	registry := NewRegistry()
	registry.SetVar("environment", "test")
	sink := &recordingEventSink{}
	executor := NewExecutor(registry)
	executor.SetEventSink(sink)

	run, err := executor.ExecuteRun(context.Background(), []PipelineNode{{
		Kind:    NodeGroup,
		GroupID: "setup",
		Children: []PipelineNode{{
			Kind: NodeAssert,
			Assert: &AssertElement{
				ID:     "environment_check",
				Var:    "environment",
				Equals: "test",
			},
		}},
	}})
	if err != nil {
		t.Fatalf("ExecuteRun() error = %v", err)
	}
	if run.RunID == "" || run.StartedAt.IsZero() || run.FinishedAt.IsZero() {
		t.Fatalf("run identity or timestamps were not populated: %+v", run)
	}
	if run.Status != RunStatusSucceeded {
		t.Fatalf("run status = %q, want %q", run.Status, RunStatusSucceeded)
	}
	if len(run.Nodes) != 2 {
		t.Fatalf("node count = %d, want 2", len(run.Nodes))
	}

	group := run.Nodes[0]
	assertion := run.Nodes[1]
	if group.NodeKind != "group" || group.NodeID != "setup" || group.Status != RunStatusSucceeded {
		t.Errorf("unexpected group result: %+v", group)
	}
	if assertion.ParentExecutionID != group.ExecutionID || assertion.NodePath != "group[0]/assert[0]" {
		t.Errorf("unexpected assertion relationship: %+v", assertion)
	}
	if len(assertion.Attempts) != 1 || assertion.Attempts[0].Status != RunStatusSucceeded {
		t.Errorf("unexpected assertion attempts: %+v", assertion.Attempts)
	}

	events := sink.Events()
	if len(events) != 10 {
		t.Fatalf("event count = %d, want 10", len(events))
	}
	for index, event := range events {
		if event.Sequence != uint64(index+1) {
			t.Errorf("event %d sequence = %d, want %d", index, event.Sequence, index+1)
		}
	}
	if events[0].Type != EventRunStarted || events[len(events)-1].Type != EventRunFinished {
		t.Errorf("unexpected run event boundaries: first=%q last=%q", events[0].Type, events[len(events)-1].Type)
	}
}

func TestExecuteRunClassifiesFailuresAndIsolatesSinkErrors(t *testing.T) {
	registry := NewRegistry()
	registry.SetVar("count", "5")
	sink := &recordingEventSink{err: errors.New("log destination unavailable")}
	executor := NewExecutor(registry)
	executor.SetEventSink(sink)

	run, err := executor.ExecuteRun(context.Background(), []PipelineNode{{
		Kind: NodeAssert,
		Assert: &AssertElement{
			ID:        "count_check",
			Var:       "count",
			Equals:    "10",
			OnFailure: "halt",
		},
	}})
	if err == nil {
		t.Fatal("ExecuteRun() error = nil, want pipeline failure")
	}
	if run.Status != RunStatusFailed || run.ErrorClass != ErrorClassValidation {
		t.Errorf("unexpected failed run: %+v", run)
	}
	if len(run.Nodes) != 1 || run.Nodes[0].Status != RunStatusFailed {
		t.Errorf("unexpected failed node results: %+v", run.Nodes)
	}
	if len(sink.Events()) == 0 {
		t.Error("expected events even when the sink returns errors")
	}
}

func TestJSONLineSinkWritesEventAndRedactsSensitiveErrors(t *testing.T) {
	var output bytes.Buffer
	sink := &JSONLineSink{Writer: &output}
	if err := sink.Emit(context.Background(), ExecutionEvent{Type: EventNodeFinished}); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}

	var event ExecutionEvent
	if err := json.Unmarshal(output.Bytes(), &event); err != nil {
		t.Fatalf("JSONLineSink output is not valid JSON: %v", err)
	}
	if event.Type != EventNodeFinished {
		t.Errorf("event type = %q, want %q", event.Type, EventNodeFinished)
	}
	if message := redactErrorMessage("connection failed: password=super-secret"); message != "connection failed: password=[REDACTED]" {
		t.Errorf("redacted message = %q", message)
	}
}
