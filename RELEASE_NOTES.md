# Release Notes: Flow Engine Enhancements 🌊

We are pleased to introduce two major architectural enhancements to the Flow Pipeline Engine: **Comprehensive `context.Context` Propagation** and **Thread-Isolated Parallel Variable Merging with Conflict Resolution**.

---

## 🚀 Key Enhancements

### 1. Robust `context.Context` Propagation
All execution boundaries and resource calls are now fully context-aware:
- **Database Contextualization:** Replaced standard queries, transactions, and inserts with their `Context` equivalents (`QueryContext`, `ExecContext`, `BeginTx`, `PrepareContext`). Database operations are now immediately aborted and rolled back if a cancellation or timeout is tripped.
- **Subprocess Cancellation:** Custom scripts (`dotnet-script`, `powershell`, Unix shells) are now spawned with `exec.CommandContext(...)`, guaranteeing immediate termination of child OS processes upon timeout or parent cancellation.
- **Embedded Script Control:** Injected the active pipeline `ctx` directly into the dynamic Go (Yaegi) interpreter exported closures.
- **Cooperative Loop Terminations:** Added select-drain cooperative cancellation checks to `<foreach>`, `<while>`, and heterogeneous database bulk streaming copies (`StreamETL`) to eradicate goroutine leaks.

### 2. Thread-Isolated Parallel Variable Merging & Conflict Resolution
Isolated executions inside `<parallel>` queues are now robustly tracked and merged:
- **Dirty Mutation Tracking:** Introduced a thread-safe `dirtyVars` tracking map inside the environment `Registry`. 
- **Variable Mutation Isolation:** Only variables explicitly changed or added during a parallel worker's execution are registered as mutated. Stale variables from snapshot snapshots are safely discarded rather than overriding changes from concurrent tasks.
- **Automatic Namespacing:** If multiple parallel branches modify the exact same variable, a conflict resolution mechanism automatically namespaces them inside the parent registry as `WORKER_<id>_<variable_name>`. Non-colliding keys merge directly back into the parent registry.
- **Thread ID Injection:** A thread-specific `_THREAD_ID` variable is injected into each parallel worker's snapshot.

---

## 🛠️ API & Configuration Updates

- `Execute` signature updated:
  ```go
  func (e *Executor) Execute(ctx context.Context, nodes []PipelineNode) ([]ScriptResult, error)
  ```
- `StreamETL` signature updated:
  ```go
  func StreamETL(ctx context.Context, r *Registry, srcDBName, queryStr, dstDBName, targetTable string, opts ETLOptions) error
  ```

---

## 🧪 Verification

These enhancements are covered by a suite of tests inside [`executor_test.go`](file:///c:/Users/U00001/source/repos/etl-madness/flow/executor_test.go), verifying:
1. Graceful termination under active context cancellation (`TestExecutorContextCancellation`).
2. Concurrent parallel variable isolation, non-colliding variable merging, and correct `WORKER_<id>_<key>` namespacing on key collisions (`TestParallelVariableIsolationAndNamespacing`).
