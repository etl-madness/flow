package flow

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
)

type bulkTestConnector struct {
	stage string
}

func (c *bulkTestConnector) Connect(context.Context) (driver.Conn, error) {
	return &bulkTestConn{stage: c.stage}, nil
}

func (c *bulkTestConnector) Driver() driver.Driver { return bulkTestDriver{} }

type bulkTestDriver struct{}

func (bulkTestDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("use connector")
}

type bulkTestConn struct {
	stage string
}

func (c *bulkTestConn) Prepare(string) (driver.Stmt, error) {
	if c.stage == "prepare" {
		return nil, errors.New("prepare failed")
	}
	return &bulkTestStmt{stage: c.stage}, nil
}

func (c *bulkTestConn) Close() error { return nil }

func (c *bulkTestConn) Begin() (driver.Tx, error) {
	if c.stage == "begin" {
		return nil, errors.New("begin failed")
	}
	return &bulkTestTx{stage: c.stage}, nil
}

type bulkTestStmt struct {
	stage string
}

func (s *bulkTestStmt) Close() error  { return nil }
func (s *bulkTestStmt) NumInput() int { return -1 }
func (s *bulkTestStmt) Exec(args []driver.Value) (driver.Result, error) {
	if s.stage == "row" && len(args) > 0 {
		return nil, errors.New("row failed")
	}
	if s.stage == "finalize" && len(args) == 0 {
		return nil, errors.New("finalize failed")
	}
	return driver.RowsAffected(1), nil
}
func (s *bulkTestStmt) Query([]driver.Value) (driver.Rows, error) {
	return nil, errors.New("unexpected query")
}

type bulkTestTx struct {
	stage string
}

func (tx *bulkTestTx) Commit() error {
	if tx.stage == "commit" {
		return errors.New("commit failed")
	}
	return nil
}

func (tx *bulkTestTx) Rollback() error { return nil }

func TestFlushMSSQLBatchTransactionPaths(t *testing.T) {
	for _, tc := range []struct {
		stage string
		want  string
	}{
		{stage: "success"},
		{stage: "begin", want: "failed to begin transaction"},
		{stage: "prepare", want: "failed to prepare bulk insert"},
		{stage: "row", want: "bulk copy row execution error"},
		{stage: "finalize", want: "failed to finalize bulk insert"},
		{stage: "commit", want: "commit failed"},
	} {
		t.Run(tc.stage, func(t *testing.T) {
			db := sql.OpenDB(&bulkTestConnector{stage: tc.stage})
			defer db.Close()
			err := flushMSSQLBatch(context.Background(), db, "items", []string{"name"},
				[][]interface{}{{"one"}, {"two"}}, ETLOptions{
					Tablock: true, CheckConstraints: true, FireTriggers: true, KeepNulls: true,
				})
			if tc.want == "" && err != nil {
				t.Fatalf("flushMSSQLBatch() error = %v", err)
			}
			if tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)) {
				t.Fatalf("flushMSSQLBatch() error = %v, want %q", err, tc.want)
			}
		})
	}

	db := sql.OpenDB(&bulkTestConnector{})
	defer db.Close()
	if err := flushMSSQLBatch(context.Background(), db, "items", nil, nil, ETLOptions{}); err != nil {
		t.Fatalf("empty batch error = %v", err)
	}
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := flushMSSQLBatch(context.Background(), db, "items", []string{"name"}, [][]interface{}{{"one"}}, ETLOptions{DstConn: conn}); err != nil {
		t.Fatalf("pinned connection batch error = %v", err)
	}
	tx, err := conn.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := flushMSSQLBatch(context.Background(), db, "items", []string{"name"}, [][]interface{}{{"one"}}, ETLOptions{DstTx: tx}); err != nil {
		t.Fatalf("existing transaction batch error = %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
}
