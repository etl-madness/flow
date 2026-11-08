package flow

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
)

func TestStreamETLPinnedConnectionAndTransaction(t *testing.T) {
	for _, mode := range []string{"connection", "transaction"} {
		t.Run(mode, func(t *testing.T) {
			registry := NewRegistry()
			if err := registry.InitDatabases([]DatabaseConfig{
				{Name: "db", Driver: "sqlite", ConnectionString: filepath.Join(t.TempDir(), "etl.db")},
			}); err != nil {
				t.Fatal(err)
			}

			defer registry.CloseDatabases()
			db, err := registry.GetDB("db")
			if err != nil {
				t.Fatal(err)
			}
			for _, query := range []string{
				"CREATE TABLE source (value BLOB)",
				"CREATE TABLE target (value BLOB)",
				"INSERT INTO source VALUES (x'6869'), (x'ff00'), (NULL)",
			} {
				if _, err := db.Exec(query); err != nil {
					t.Fatal(err)
				}
			}

			conn, err := db.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			opts := ETLOptions{BatchSize: 2, SrcConn: conn, DstConn: conn}
			var tx *sql.Tx
			if mode == "transaction" {
				tx, err = conn.BeginTx(context.Background(), nil)
				if err != nil {
					t.Fatal(err)
				}
				opts.SrcConn, opts.DstConn = nil, nil
				opts.SrcTx, opts.DstTx = tx, tx
			}
			count, err := StreamETL(context.Background(), registry, "db", "SELECT value FROM source ORDER BY rowid", "db", "target", opts)
			if err != nil || count != 3 {
				t.Fatalf("StreamETL() = %d, %v; want 3 rows", count, err)
			}
			if tx != nil {
				if err := tx.Commit(); err != nil {
					t.Fatal(err)
				}
			}
			var text, binary string
			var nulls int
			if err := conn.QueryRowContext(context.Background(), "SELECT hex(value) FROM target WHERE rowid = 1").Scan(&text); err != nil {
				t.Fatal(err)
			}
			if err := conn.QueryRowContext(context.Background(), "SELECT hex(value) FROM target WHERE rowid = 2").Scan(&binary); err != nil {
				t.Fatal(err)
			}
			if err := conn.QueryRowContext(context.Background(), "SELECT count(*) FROM target WHERE value IS NULL").Scan(&nulls); err != nil {
				t.Fatal(err)
			}
			if text != "6869" || binary != "FF00" || nulls != 1 {
				t.Fatalf("copied values = %q, %q, %d nulls", text, binary, nulls)
			}
		})
	}
}
