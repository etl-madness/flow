package flow

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
)

func TestResolveConfinedPath(t *testing.T) {
	workDir := t.TempDir()

	// Empty workDir preserves backward compatibility
	got, err := ResolveConfinedPath("", "../outside.txt")
	if err != nil || got != "../outside.txt" {
		t.Fatalf("empty workDir: got (%q, %v), want (%q, nil)", got, err, "../outside.txt")
	}

	// Relative path inside workDir succeeds
	got, err = ResolveConfinedPath(workDir, "sub/dir/out.txt")
	if err != nil {
		t.Fatalf("relative path failed: %v", err)
	}
	if !strings.HasSuffix(filepath.ToSlash(got), "/sub/dir/out.txt") {
		t.Fatalf("unexpected resolved path: %q", got)
	}

	// Traversal escape fails
	if _, err := ResolveConfinedPath(workDir, "../escape.txt"); err == nil {
		t.Fatal("expected error for ../escape.txt")
	}

	// Absolute path outside workDir fails
	outsideAbs := filepath.Join(filepath.Dir(workDir), "outside.txt")
	if _, err := ResolveConfinedPath(workDir, outsideAbs); err == nil {
		t.Fatalf("expected error for outside absolute path %q", outsideAbs)
	}

	// Absolute path inside workDir succeeds
	insideAbs := filepath.Join(workDir, "inside.txt")
	got, err = ResolveConfinedPath(workDir, insideAbs)
	if err != nil {
		t.Fatalf("inside absolute path failed: %v", err)
	}
	if filepath.Base(got) != "inside.txt" {
		t.Fatalf("unexpected inside path: %q", got)
	}

	// Empty path and NUL byte fail
	if _, err := ResolveConfinedPath(workDir, ""); err == nil {
		t.Fatal("expected error for empty path")
	}
	if _, err := ResolveConfinedPath(workDir, "bad\x00name.txt"); err == nil {
		t.Fatal("expected error for NUL byte path")
	}

	// Current directory "." resolves to workDir
	if gotDot, err := ResolveConfinedPath(workDir, "."); err != nil || gotDot == "" {
		t.Fatalf("expected '.' to resolve cleanly: got (%q, %v)", gotDot, err)
	}

	// Symlink escape check (skip if OS/privileges disallow creating symlinks)
	outsideDir := t.TempDir()
	linkPath := filepath.Join(workDir, "escape_link")
	if symErr := os.Symlink(outsideDir, linkPath); symErr == nil {
		if _, err := ResolveConfinedPath(workDir, filepath.Join("escape_link", "secret.txt")); err == nil {
			t.Fatal("expected error for symlink escape")
		}
	}
}

func TestResolveLocalDBDSN(t *testing.T) {
	workDir := t.TempDir()

	// Empty workDir passes through unchanged
	if got, err := resolveLocalDBDSN("", "sqlite", "../raw.db"); err != nil || got != "../raw.db" {
		t.Fatalf("empty workDir: got (%q, %v)", got, err)
	}

	// Network drivers pass through unchanged
	if got, err := resolveLocalDBDSN(workDir, "postgres", "postgres://user:pass@host/db"); err != nil || got != "postgres://user:pass@host/db" {
		t.Fatalf("postgres passthrough: got (%q, %v)", got, err)
	}

	// SQLite in-memory variants pass through unchanged
	for _, memDSN := range []string{"", ":memory:", "file::memory:?cache=shared", "file::memory:"} {
		if got, err := resolveLocalDBDSN(workDir, "sqlite", memDSN); err != nil || got != memDSN {
			t.Fatalf("sqlite memory DSN %q: got (%q, %v)", memDSN, got, err)
		}
	}

	// SQLite relative file & file: URI with query params confined inside workDir
	gotSQLite, err := resolveLocalDBDSN(workDir, "sqlite", "db/local.sqlite?cache=shared")
	if err != nil || !strings.Contains(filepath.ToSlash(gotSQLite), "/db/local.sqlite?cache=shared") {
		t.Fatalf("sqlite relative: got (%q, %v)", gotSQLite, err)
	}
	gotFileURI, err := resolveLocalDBDSN(workDir, "sqlite3", "file:db2/app.db?mode=rwc")
	if err != nil || !strings.HasPrefix(gotFileURI, "file:") || !strings.HasSuffix(filepath.ToSlash(gotFileURI), "/db2/app.db?mode=rwc") {
		t.Fatalf("sqlite file URI: got (%q, %v)", gotFileURI, err)
	}
	if _, err := resolveLocalDBDSN(workDir, "sqlite", "../escape.db"); err == nil {
		t.Fatal("expected error for sqlite escape")
	}

	// bbolt and badger confined inside workDir
	if got, err := resolveLocalDBDSN(workDir, "bbolt", "kv/state.bolt"); err != nil || !strings.HasSuffix(filepath.ToSlash(got), "/kv/state.bolt") {
		t.Fatalf("bbolt relative: got (%q, %v)", got, err)
	}
	if _, err := resolveLocalDBDSN(workDir, "bolt", "../escape.bolt"); err == nil {
		t.Fatal("expected error for bbolt escape")
	}
	if got, err := resolveLocalDBDSN(workDir, "badger", "kv/badger_dir"); err != nil || !strings.HasSuffix(filepath.ToSlash(got), "/kv/badger_dir") {
		t.Fatalf("badger relative: got (%q, %v)", got, err)
	}
	if _, err := resolveLocalDBDSN(workDir, "badgerdb", "../escape_badger"); err == nil {
		t.Fatal("expected error for badger escape")
	}
}

func TestApplyWorkDirEnv(t *testing.T) {
	base := []string{"PATH=/usr/bin", "TMPDIR=/old", "TEMP=/old", "TMP=/old", "FLOW_JOB_DIR=/old"}
	if got := applyWorkDirEnv(base, ""); len(got) != len(base) {
		t.Fatalf("empty workDir should return env unchanged")
	}
	got := applyWorkDirEnv(base, "/sandbox/job1")
	joined := strings.Join(got, "\n")
	for _, want := range []string{"PATH=/usr/bin", "TMPDIR=/sandbox/job1", "TEMP=/sandbox/job1", "TMP=/sandbox/job1", "FLOW_JOB_DIR=/sandbox/job1"} {
		if !strings.Contains(joined, want) {
			t.Errorf("expected %q in env, got:\n%s", want, joined)
		}
	}
	if strings.Contains(joined, "/old") {
		t.Errorf("expected old temp vars to be replaced, got:\n%s", joined)
	}
}

func TestRegistryWorkDirDatabases(t *testing.T) {
	workDir := t.TempDir()
	reg := NewRegistry()
	reg.SetWorkDir(workDir)

	if reg.WorkDir() != workDir {
		t.Fatalf("WorkDir() = %q, want %q", reg.WorkDir(), workDir)
	}

	// Valid confined sqlite, bbolt, and badger databases
	err := reg.InitDatabases([]DatabaseConfig{
		{Name: "sq", Driver: "sqlite", ConnectionString: "dbs/app.db"},
		{Name: "bb", Driver: "bbolt", ConnectionString: "dbs/kv.bolt"},
		{Name: "bg", Driver: "badger", ConnectionString: "dbs/badger"},
	})
	if err != nil {
		t.Fatalf("InitDatabases with confined paths failed: %v", err)
	}
	reg.CloseDatabases()

	for _, rel := range []string{"dbs/kv.bolt", "dbs/badger"} {
		if _, statErr := os.Stat(filepath.Join(workDir, filepath.FromSlash(rel))); statErr != nil {
			t.Fatalf("expected %s inside workDir: %v", rel, statErr)
		}
	}

	// Escape attempts must fail for sqlite, bbolt, and badger
	for _, tc := range []DatabaseConfig{
		{Name: "bad_sq", Driver: "sqlite", ConnectionString: "../escape.db"},
		{Name: "bad_bb", Driver: "bbolt", ConnectionString: "../escape.bolt"},
		{Name: "bad_bg", Driver: "badger", ConnectionString: "../escape_badger"},
	} {
		if err := reg.InitDatabases([]DatabaseConfig{tc}); err == nil {
			t.Errorf("expected InitDatabases to reject escape for %s", tc.Driver)
		}
	}
}

func TestExecutorWorkDirAllNodeEscapes(t *testing.T) {
	workDir := t.TempDir()
	reg := NewRegistry()
	exec := NewExecutor(reg)
	exec.SetWorkDir(workDir)
	if exec.WorkDir() != workDir {
		t.Fatalf("Executor.WorkDir() = %q, want %q", exec.WorkDir(), workDir)
	}

	pipelines := []struct {
		name string
		xml  string
	}{
		{"script_file", `<flow><script id="s1" language="go" file_path="../escape.go"/></flow>`},
		{"template", `<flow><template id="t1" file="../escape.tmpl" output_var="o"/></flow>`},
		{"file_read", `<flow><file_read id="fr1" file="../escape.txt" var="o"/></flow>`},
		{"excel_read", `<flow><excel_read id="er1" file="../escape.xlsx" output_var="o"/></flow>`},
		{"excel_write", `<flow><excel_write id="ew1" file="../escape.xlsx"/></flow>`},
		{"xml_xpath", `<flow><xml_xpath id="x1" file="../escape.xml" xpath="//a" output_var="o"/></flow>`},
		{"json_path", `<flow><json_path id="j1" file="../escape.json" path="$.a" output_var="o"/></flow>`},
		{"yaml_path", `<flow><yaml_path id="y1" file="../escape.yaml" path="$.a" output_var="o"/></flow>`},
	}

	for _, tc := range pipelines {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := ParseXMLConfig([]byte(`<?xml version="1.0" encoding="UTF-8"?>` + tc.xml))
			if err != nil {
				t.Fatalf("ParseXMLConfig(%s): %v", tc.name, err)
			}
			results, _ := exec.Execute(context.Background(), cfg.FlowNodes)
			if len(results) == 0 || results[0].ReturnCode == 0 {
				t.Fatalf("%s: expected escape to fail, got results=%+v", tc.name, results)
			}
		})
	}
}

func TestExecutorWorkDirFileNodesAndShellConfinement(t *testing.T) {
	workDir := t.TempDir()

	shellLang := "sh"
	shellCmd := `mkdir -p scripts_out && echo "from-shell" > scripts_out/hello.txt && pwd`
	if runtime.GOOS == "windows" {
		shellLang = "powershell"
		shellCmd = `New-Item -ItemType Directory -Force -Path scripts_out | Out-Null; Set-Content -Path scripts_out\hello.txt -Value "from-shell" -NoNewline; (Get-Location).Path`
	}

	xmlDoc := `<?xml version="1.0" encoding="UTF-8"?>
<flow>
  <file_save id="save1" file="nested/data.txt">hello-confined</file_save>
  <file_read id="read1" file="nested/data.txt" var="read_back"/>
  <script id="sh1" language="` + shellLang + `" output_var="sh_cwd">` + shellCmd + `</script>
  <file_read id="read_shell" file="scripts_out/hello.txt" var="shell_file"/>
</flow>`

	cfg, err := ParseXMLConfig([]byte(xmlDoc))
	if err != nil {
		t.Fatalf("ParseXMLConfig: %v", err)
	}

	reg := NewRegistry()
	reg.SetWorkDir(workDir)
	exec := NewExecutor(reg)

	results, err := exec.Execute(context.Background(), cfg.FlowNodes)
	if err != nil {
		t.Fatalf("Execute failed: %v (results=%+v)", err, results)
	}
	for _, r := range results {
		if r.ReturnCode != 0 {
			t.Fatalf("node %s failed: rc=%v out=%s", r.ScriptID, r.ReturnCode, r.ResultsString)
		}
	}

	if got := reg.GetVarString("read_back"); got != "hello-confined" {
		t.Fatalf("read_back = %q, want %q", got, "hello-confined")
	}
	if got := strings.TrimSpace(reg.GetVarString("shell_file")); got != "from-shell" {
		t.Fatalf("shell_file = %q, want %q", got, "from-shell")
	}

	// Verify file actually exists inside workDir
	if _, err := os.Stat(filepath.Join(workDir, "nested", "data.txt")); err != nil {
		t.Fatalf("expected nested/data.txt in workDir: %v", err)
	}
	if _, err := os.Stat(filepath.Join(workDir, "scripts_out", "hello.txt")); err != nil {
		t.Fatalf("expected scripts_out/hello.txt in workDir: %v", err)
	}
}

func TestExecutorWorkDirRejectsEscape(t *testing.T) {
	workDir := t.TempDir()

	xmlDoc := `<?xml version="1.0" encoding="UTF-8"?>
<flow>
  <file_save id="escape_save" file="../escaped.txt">bad</file_save>
</flow>`

	cfg, err := ParseXMLConfig([]byte(xmlDoc))
	if err != nil {
		t.Fatalf("ParseXMLConfig: %v", err)
	}

	reg := NewRegistry()
	exec := NewExecutor(reg)
	exec.SetWorkDir(workDir)

	results, _ := exec.Execute(context.Background(), cfg.FlowNodes)
	if len(results) == 0 || results[0].ReturnCode == 0 {
		t.Fatalf("expected escape attempt to fail, got results=%+v", results)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(workDir), "escaped.txt")); !os.IsNotExist(err) {
		t.Fatalf("escaped.txt should not have been created outside workDir")
	}
}

func TestConcurrentExecutorsIsolatedWorkDirs(t *testing.T) {
	const workers = 4
	var wg sync.WaitGroup
	errs := make(chan error, workers)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			wd := t.TempDir()
			xmlDoc := `<?xml version="1.0" encoding="UTF-8"?>
<flow>
  <file_save id="w" file="out.txt">worker-{{val}}</file_save>
  <file_read id="r" file="out.txt" var="res"/>
</flow>`
			cfg, err := ParseXMLConfig([]byte(xmlDoc))
			if err != nil {
				errs <- err
				return
			}
			reg := NewRegistry()
			reg.SetVar("val", idx)
			reg.SetWorkDir(wd)
			exec := NewExecutor(reg)
			results, err := exec.Execute(context.Background(), cfg.FlowNodes)
			if err != nil {
				errs <- err
				return
			}
			for _, r := range results {
				if r.ReturnCode != 0 {
					errs <- os.ErrInvalid
					return
				}
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent worker failed: %v", err)
	}
}
