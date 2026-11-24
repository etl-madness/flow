package flow

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// ResolveConfinedPath resolves rawPath relative to workDir and enforces strict
// confinement within workDir (rejecting ".." escapes, symlink escapes, and
// absolute paths outside workDir). When workDir is empty, rawPath is returned
// unchanged for backward compatibility.
func ResolveConfinedPath(workDir, rawPath string) (string, error) {
	trimmedWorkDir := strings.TrimSpace(workDir)
	trimmedPath := strings.TrimSpace(rawPath)
	if trimmedWorkDir == "" {
		return trimmedPath, nil
	}
	if trimmedPath == "" {
		return "", fmt.Errorf("file path cannot be empty")
	}
	if strings.ContainsRune(trimmedPath, '\x00') {
		return "", fmt.Errorf("invalid file path containing NUL byte")
	}

	absWorkDir, err := filepath.Abs(trimmedWorkDir)
	if err != nil {
		return "", fmt.Errorf("failed resolving working directory %q: %w", trimmedWorkDir, err)
	}
	if evalWorkDir, evalErr := filepath.EvalSymlinks(absWorkDir); evalErr == nil {
		absWorkDir = evalWorkDir
	}
	absWorkDir = filepath.Clean(absWorkDir)

	var candidate string
	if filepath.IsAbs(trimmedPath) {
		candidate = filepath.Clean(trimmedPath)
		if evalCand, evalErr := filepath.EvalSymlinks(candidate); evalErr == nil {
			candidate = filepath.Clean(evalCand)
		}
	} else {
		candidate = filepath.Join(absWorkDir, trimmedPath)
	}

	if !isPathWithinDir(absWorkDir, candidate) {
		return "", fmt.Errorf("path %q escapes job working directory", rawPath)
	}

	// Verify the deepest existing ancestor does not symlink outside absWorkDir.
	checkPath := candidate
	for {
		if _, statErr := os.Lstat(checkPath); statErr == nil {
			if evalPath, evalErr := filepath.EvalSymlinks(checkPath); evalErr == nil {
				if !isPathWithinDir(absWorkDir, filepath.Clean(evalPath)) {
					return "", fmt.Errorf("path %q resolves via symlink outside job working directory", rawPath)
				}
			}
			break
		}
		parent := filepath.Dir(checkPath)
		if parent == checkPath {
			break
		}
		checkPath = parent
	}

	return candidate, nil
}

func isPathWithinDir(baseDir, target string) bool {
	rel, err := filepath.Rel(baseDir, target)
	if err != nil {
		return false
	}
	if rel == "." {
		return true
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		return false
	}
	return true
}

// resolveLocalDBDSN confines local file-backed database/KV paths (sqlite, bbolt, badger)
// inside workDir when workDir is non-empty.
func resolveLocalDBDSN(workDir, driverName, connStr string) (string, error) {
	if strings.TrimSpace(workDir) == "" {
		return connStr, nil
	}
	trimmed := strings.TrimSpace(connStr)
	lowerDriver := strings.ToLower(strings.TrimSpace(driverName))

	switch lowerDriver {
	case "bbolt", "bolt", "badger", "badgerdb":
		return ResolveConfinedPath(workDir, trimmed)
	case "sqlite", "sqlite3":
		if trimmed == "" || strings.EqualFold(trimmed, ":memory:") {
			return trimmed, nil
		}
		lowerConn := strings.ToLower(trimmed)
		if strings.HasPrefix(lowerConn, "file::memory:") {
			return trimmed, nil
		}
		hasFilePrefix := strings.HasPrefix(lowerConn, "file:")
		pathPart := trimmed
		queryPart := ""
		if hasFilePrefix {
			pathPart = trimmed[len("file:"):]
		}
		if qIdx := strings.IndexByte(pathPart, '?'); qIdx >= 0 {
			queryPart = pathPart[qIdx:]
			pathPart = pathPart[:qIdx]
		}
		if strings.EqualFold(strings.TrimSpace(pathPart), ":memory:") || strings.TrimSpace(pathPart) == "" {
			return trimmed, nil
		}
		resolved, err := ResolveConfinedPath(workDir, pathPart)
		if err != nil {
			return "", err
		}
		if dir := filepath.Dir(resolved); dir != "" && dir != "." {
			if mkErr := os.MkdirAll(dir, 0750); mkErr != nil {
				return "", fmt.Errorf("failed creating directory for sqlite db: %w", mkErr)
			}
		}
		if hasFilePrefix {
			return "file:" + resolved + queryPart, nil
		}
		return resolved + queryPart, nil
	default:
		return connStr, nil
	}
}

// applyWorkDirEnv updates TMPDIR, TEMP, TMP, and FLOW_JOB_DIR in envVars when workDir is set.
func applyWorkDirEnv(envVars []string, workDir string) []string {
	if strings.TrimSpace(workDir) == "" {
		return envVars
	}
	filtered := make([]string, 0, len(envVars)+4)
	for _, entry := range envVars {
		upper := strings.ToUpper(entry)
		if strings.HasPrefix(upper, "TMPDIR=") ||
			strings.HasPrefix(upper, "TEMP=") ||
			strings.HasPrefix(upper, "TMP=") ||
			strings.HasPrefix(upper, "FLOW_JOB_DIR=") {
			continue
		}
		filtered = append(filtered, entry)
	}
	filtered = append(filtered,
		"TMPDIR="+workDir,
		"TEMP="+workDir,
		"TMP="+workDir,
		"FLOW_JOB_DIR="+workDir,
	)
	return filtered
}
