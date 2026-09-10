# Security Audit Report

## Summary
The security audit identified several critical and high-severity vulnerabilities related to injection and path traversal. The system is highly susceptible to SQL, Command, and Path injection due to extensive use of string interpolation for dynamic inputs.

## Findings

### 1. SQL Injection
- **Critical**: `etl.go:160` uses `fmt.Sprintf` to construct an `INSERT` statement using `targetTable` and `colList`. While values are parameterized, the table and column names are not, allowing for structural SQL injection if these values are sourced from untrusted input.
- **High**: `executor.go:236` and `executor.go:733` perform direct string replacement of `{{VarName}}` placeholders in SQL queries. This allows any pipeline variable to be used as raw SQL, enabling full SQL injection if variables are user-controlled.
- **High**: `executor.go:1557` uses `interpolateVars` for queries used in Excel writes, leading to the same injection risk.

### 2. Command Injection
- **Critical**: `executor.go:653-670` executes shell commands (pwsh, powershell, bash, cmd, etc.) using `codeToEval`. This code is subject to variable interpolation (`executor.go:647`), allowing arbitrary command execution via pipeline variables.
- **Critical**: `executor.go:619-622` executes `dotnet-script` or `dotnet script` with a temporary file path. While the path is generated, the environment variables passed to the process are interpolated, which could be exploited depending on the shell environment.

### 3. Path Traversal
- **High**: `executor.go:1231`, `executor.go:1285`, `executor.go:1336`, `executor.go:1408`, `executor.go:1454`, and `executor.go:1516` all use `interpolateVars` to resolve file paths. There is no validation or sanitization of the resulting paths, allowing for arbitrary file read/write (Path Traversal) via pipeline variables.

### 4. Secret Exposure
- **Medium**: Multiple documentation files (`docs/database.md`, `docs/kv.md`, etc.) contain hardcoded passwords and secrets in examples. While these are examples, they set a poor precedent.
- **Low**: `observability.go` implements a redaction pattern for sensitive values in logs, which is a good practice, but the system still allows secrets to be stored in plain text within the `Registry`.

## Recommendations
- Use parameterized queries for all SQL operations, including structural elements where possible (or use a strict allow-list).
- Avoid executing shell commands with interpolated strings. Use fixed arguments or strict validation.
- Sanitize all file paths using `filepath.Clean` and validate them against a base directory (chroot-like behavior).
- Ensure all secrets are handled via a secure vault rather than pipeline variables.
