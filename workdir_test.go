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
