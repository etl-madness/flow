package flow

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestInitDatabasesCleansUpOnFailure(t *testing.T) {
	dir := t.TempDir()
	registry := NewRegistry()
	configs := []DatabaseConfig{
		{Name: "sqlite", Driver: "sqlite", ConnectionString: filepath.Join(dir, "test.db")},
		{Name: "bolt", Driver: "bolt", ConnectionString: filepath.Join(dir, "bolt", "test.db")},
		{Name: "badger", Driver: "badger", ConnectionString: filepath.Join(dir, "badger")},
		{Name: "bad", Driver: "not-a-driver", ConnectionString: "invalid"},
	}
	err := registry.InitDatabases(configs)
	if err == nil || !strings.Contains(err.Error(), "failed to open database") {
		t.Fatalf("InitDatabases() error = %v", err)
	}
	if _, err := registry.GetDB("sqlite"); err == nil {
		t.Error("SQL database retained after failure")
	}
	if _, err := registry.GetBoltDB("bolt"); err == nil {
		t.Error("Bolt database retained after failure")
	}
	if _, err := registry.GetBadgerDB("badger"); err == nil {
		t.Error("Badger database retained after failure")
	}
	if err := registry.InitDatabases(configs[:3]); err != nil {
		t.Fatalf("databases could not be reopened after cleanup: %v", err)
	}
	registry.CloseDatabases()
}

func TestInitDatabasesInvalidInputs(t *testing.T) {
	dir := t.TempDir()
	blockingFile := filepath.Join(dir, "file")
	if err := os.WriteFile(blockingFile, []byte("file"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		cfg  DatabaseConfig
		want string
	}{
		{"bolt directory", DatabaseConfig{Name: "bolt", Driver: "bolt", ConnectionString: filepath.Join(blockingFile, "db")}, "failed to create directory"},
		{"badger directory", DatabaseConfig{Name: "badger", Driver: "badger", ConnectionString: filepath.Join(blockingFile, "db")}, "failed to create directory"},
		{"invalid redis URL", DatabaseConfig{Name: "redis", Driver: "redis", ConnectionString: "redis://localhost:notaport"}, "invalid redis connection url"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			registry := NewRegistry()
			defer registry.CloseDatabases()
			err := registry.InitDatabases([]DatabaseConfig{tc.cfg})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("InitDatabases() error = %v, want %q", err, tc.want)
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := NewRegistry().InitDatabasesWithContext(ctx, []DatabaseConfig{{Name: "db", Driver: "sqlite"}}); err != context.Canceled {
		t.Fatalf("canceled initialization error = %v", err)
	}
}
