package flow

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	_ "github.com/go-sql-driver/mysql"  // MySQL driver
	_ "github.com/lib/pq"               // PostgreSQL driver
	_ "github.com/microsoft/go-mssqldb" // MSSQL driver
	_ "github.com/sijms/go-ora/v2"      // Pure Go Oracle driver
	goBolt "go.etcd.io/bbolt"
	clientv3 "go.etcd.io/etcd/client/v3"
	_ "modernc.org/sqlite" // Pure Go SQLite driver
)

// DBHandle encapsulates an active sql.DB connection pool along with its driver name.
type DBHandle struct {
	Conn   *sql.DB // Connection pool handle
	Driver string  // Database driver name (e.g. "sqlite", "mysql")
}

// Registry is a thread-safe container that manages active database connection pools
// and dynamic pipeline environment variables.
type Registry struct {
	dbRegistry     map[string]DBHandle
	boltRegistry   map[string]*goBolt.DB       // Track active BBolt instances
	badgerRegistry map[string]*badger.DB       // Track active BadgerDB instances
	etcdRegistry   map[string]*clientv3.Client // Track active etcd client connections
	dbMu           sync.RWMutex
	varRegistry    map[string]interface{}
	varMu          sync.RWMutex
	dirtyVars      map[string]struct{} // Tracks keys set after snapshotting
}

// NewRegistry instantiates and returns an empty Registry context.
func NewRegistry() *Registry {
	return &Registry{
		dbRegistry:     make(map[string]DBHandle),
		boltRegistry:   make(map[string]*goBolt.DB),
		badgerRegistry: make(map[string]*badger.DB),
		etcdRegistry:   make(map[string]*clientv3.Client),
		varRegistry:    make(map[string]interface{}),
		dirtyVars:      make(map[string]struct{}),
	}
}

// SetVar sets an environment variable value in a thread-safe manner.
func (r *Registry) SetVar(name string, value interface{}) {
	r.varMu.Lock()
	defer r.varMu.Unlock()
	r.varRegistry[name] = value
	if r.dirtyVars != nil {
		r.dirtyVars[name] = struct{}{}
	}
}

// GetVar retrieves an environment variable's raw interface value in a thread-safe manner.
func (r *Registry) GetVar(name string) interface{} {
	r.varMu.RLock()
	defer r.varMu.RUnlock()
	return r.varRegistry[name]
}

// GetVarString retrieves a variable and returns its value formatted as a string.
func (r *Registry) GetVarString(name string) string {
	val := r.GetVar(name)
	if val == nil {
		return ""
	}
	if v, ok := val.(string); ok {
		return v
	}
	return fmt.Sprintf("%v", val)
}

// GetVarInt retrieves a variable and returns its value as an integer (parsing strings if necessary).
func (r *Registry) GetVarInt(name string) int {
	val := r.GetVar(name)
	if val == nil {
		return 0
	}
	if v, ok := val.(int); ok {
		return v
	}
	if str, ok := val.(string); ok {
		if i, err := strconv.Atoi(str); err == nil {
			return i
		}
	}
	return 0
}

func (r *Registry) GetVarBool(name string) bool {
	val := r.GetVar(name)
	if val == nil {
		return false
	}
	if v, ok := val.(bool); ok {
		return v
	}
	if str, ok := val.(string); ok {
		if b, err := strconv.ParseBool(str); err == nil {
			return b
		}
	}
	return false
}

func (r *Registry) GetVarFloat(name string) float64 {
	val := r.GetVar(name)
	if val == nil {
		return 0.0
	}
	if v, ok := val.(float64); ok {
		return v
	}
	if str, ok := val.(string); ok {
		if f, err := r.parseFloat(str); err == nil {
			return f
		}
	}
	return 0.0
}

func (r *Registry) parseFloat(val string) (float64, error) {
	return strconv.ParseFloat(val, 64)
}

// GetDB returns the direct sql.DB pointer for the requested database name, if registered.
func (r *Registry) GetDB(name string) (*sql.DB, error) {
	r.dbMu.RLock()
	defer r.dbMu.RUnlock()

	handle, ok := r.dbRegistry[name]
	if !ok {
		return nil, fmt.Errorf("database connection '%s' not registered", name)
	}
	return handle.Conn, nil
}

// GetDBHandle returns the DBHandle wrapper (containing sql.DB and Driver name) for the database.
func (r *Registry) GetDBHandle(name string) (DBHandle, error) {
	r.dbMu.RLock()
	defer r.dbMu.RUnlock()

	handle, ok := r.dbRegistry[name]
	if !ok {
		return DBHandle{}, fmt.Errorf("database connection '%s' not registered", name)
	}
	return handle, nil
}

// InitVariables registers and parses multiple environment variables based on type configuration.
func (r *Registry) InitVariables(configs []VariableConfig) error {
	r.varMu.Lock()
	defer r.varMu.Unlock()

	for _, cfg := range configs {
		switch cfg.Type {
		case "int", "integer":
			val, err := strconv.Atoi(cfg.Value)
			if err != nil {
				return fmt.Errorf("invalid int value '%s' for variable '%s'", cfg.Value, cfg.Name)
			}
			r.varRegistry[cfg.Name] = val
		case "bool", "boolean":
			val, err := strconv.ParseBool(cfg.Value)
			if err != nil {
				return fmt.Errorf("invalid bool value '%s' for variable '%s'", cfg.Value, cfg.Name)
			}
			r.varRegistry[cfg.Name] = val
		case "float", "double", "float64":
			val, err := r.parseFloat(cfg.Value)
			if err != nil {
				return fmt.Errorf("invalid float value '%s' for variable '%s'", cfg.Value, cfg.Name)
			}
			r.varRegistry[cfg.Name] = val
		default:
			r.varRegistry[cfg.Name] = cfg.Value
		}
	}
	return nil
}

const (
	defaultDBMaxOpenConns = 25
	defaultDBMaxIdleConns = 10
	defaultDBConnMaxLife  = 5 * time.Minute
)

func applyDatabasePoolSettings(dbConn *sql.DB, cfg DatabaseConfig) {
	maxOpenConns := cfg.MaxOpenConns
	if maxOpenConns <= 0 {
		maxOpenConns = defaultDBMaxOpenConns
	}

	maxIdleConns := cfg.MaxIdleConns
	if maxIdleConns <= 0 {
		maxIdleConns = defaultDBMaxIdleConns
	}
	if maxIdleConns > maxOpenConns {
		maxIdleConns = maxOpenConns
	}

	connMaxLifetime := cfg.ConnMaxLifetime
	if connMaxLifetime <= 0 {
		connMaxLifetime = defaultDBConnMaxLife
	}

	dbConn.SetMaxOpenConns(maxOpenConns)
	dbConn.SetMaxIdleConns(maxIdleConns)
	dbConn.SetConnMaxLifetime(connMaxLifetime)
}

// InitDatabases opens connection pools for all supplied DatabaseConfigs with variable interpolation in connection strings.
func (r *Registry) InitDatabases(configs []DatabaseConfig) error {
	r.dbMu.Lock()
	defer r.dbMu.Unlock()

	r.varMu.RLock()
	defer r.varMu.RUnlock()

	for _, cfg := range configs {
		connStr := cfg.ConnectionString

		for name, val := range r.varRegistry {
			placeholder := fmt.Sprintf("{{%s}}", name)
			connStr = strings.ReplaceAll(connStr, placeholder, fmt.Sprintf("%v", val))
		}

		driverName := strings.ToLower(cfg.Driver)

		if driverName == "bbolt" || driverName == "bolt" {
			// Ensure path directory exists
			dir := filepath.Dir(connStr)
			if dir != "" && dir != "." {
				_ = os.MkdirAll(dir, 0755)
			}

			db, err := goBolt.Open(connStr, 0600, &goBolt.Options{Timeout: 2 * time.Second})
			if err != nil {
				return fmt.Errorf("failed to open bbolt database '%s' at path '%s': %w", cfg.Name, connStr, err)
			}
			r.boltRegistry[cfg.Name] = db
			continue
		}
		if driverName == "badger" || driverName == "badgerdb" {
			// Badger stores data inside a directory
			if err := os.MkdirAll(connStr, 0755); err != nil {
				return fmt.Errorf("failed to create directory for badger db '%s': %w", cfg.Name, err)
			}

			// Open BadgerDB with standard options (suppress verbose logger)
			opts := badger.DefaultOptions(connStr).WithLogger(nil)
			db, err := badger.Open(opts)
			if err != nil {
				return fmt.Errorf("failed to open badger database '%s' at path '%s': %w", cfg.Name, connStr, err)
			}
			r.badgerRegistry[cfg.Name] = db
			continue
		}
		if driverName == "etcd" || driverName == "etcdv3" {
			endpoints := strings.Split(connStr, ",")
			for i, ep := range endpoints {
				endpoints[i] = strings.TrimSpace(ep)
			}
			cli, err := clientv3.New(clientv3.Config{
				Endpoints:   endpoints,
				DialTimeout: 5 * time.Second,
			})
			if err != nil {
				return fmt.Errorf("failed to connect to etcd database '%s' at '%s': %w", cfg.Name, connStr, err)
			}
			r.etcdRegistry[cfg.Name] = cli
			continue
		}
		if driverName == "" {
			driverName = "sqlserver"
		}

		dbConn, err := sql.Open(driverName, connStr)
		if err != nil {
			return fmt.Errorf("failed to open database '%s' (%s): %w", cfg.Name, driverName, err)
		}

		applyDatabasePoolSettings(dbConn, cfg)

		r.dbRegistry[cfg.Name] = DBHandle{
			Conn:   dbConn,
			Driver: driverName,
		}
	}
	return nil
}

// CloseDatabases closes all open database connections tracked inside the registry and removes them.
func (r *Registry) CloseDatabases() {
	r.dbMu.Lock()
	defer r.dbMu.Unlock()
	for name, db := range r.boltRegistry {
		db.Close()
		delete(r.boltRegistry, name)
	}

	for name, db := range r.badgerRegistry {
		db.Close()
		delete(r.badgerRegistry, name)
	}
	for name, cli := range r.etcdRegistry {
		_ = cli.Close()
		delete(r.etcdRegistry, name)
	}
	for name, handle := range r.dbRegistry {
		handle.Conn.Close()
		delete(r.dbRegistry, name)
	}
}
func (r *Registry) GetBadgerDB(name string) (*badger.DB, error) {
	r.dbMu.RLock()
	defer r.dbMu.RUnlock()
	db, ok := r.badgerRegistry[name]
	if !ok {
		return nil, fmt.Errorf("badger database connection '%s' not registered", name)
	}
	return db, nil
}

func (r *Registry) GetBoltDB(name string) (*goBolt.DB, error) {
	r.dbMu.RLock()
	defer r.dbMu.RUnlock()
	db, ok := r.boltRegistry[name]
	if !ok {
		return nil, fmt.Errorf("bbolt database connection '%s' not registered", name)
	}
	return db, nil
}

// CopyVariables creates and returns a thread-safe snapshot map of all current environment variables.
func (r *Registry) CopyVariables() map[string]interface{} {
	r.varMu.RLock()
	defer r.varMu.RUnlock()
	copyMap := make(map[string]interface{})
	for k, v := range r.varRegistry {
		copyMap[k] = v
	}
	return copyMap
}

// GetVarTime retrieves a variable and returns its value as time.Time (parsing string dates if necessary).
func (r *Registry) GetVarTime(name string) time.Time {
	val := r.GetVar(name)
	if val == nil {
		return time.Time{}
	}
	if t, ok := val.(time.Time); ok {
		return t
	}
	if str, ok := val.(string); ok {
		str = strings.TrimSpace(str)
		if str == "" {
			return time.Time{}
		}

		// Common SQL, ISO, and standard date/time layouts
		layouts := []string{
			"2006-01-02 15:04:05",
			"2006-01-02 15:04:05.999",
			"2006-01-02 15:04:05.999999",
			time.RFC3339,
			"2006-01-02T15:04:05",
			"2006-01-02",
		}

		for _, layout := range layouts {
			if t, err := time.Parse(layout, str); err == nil {
				return t
			}
		}
	}
	return time.Time{}
}

// MergeVariables copies variable key-value pairs into the parent registry.
func (r *Registry) MergeVariables(src map[string]interface{}) {
	r.varMu.Lock()
	defer r.varMu.Unlock()
	for k, v := range src {
		r.varRegistry[k] = v
	}
}

// Snapshot returns a new Registry instance with isolated variable storage
// while sharing the underlying database connection handles.
func (r *Registry) Snapshot() *Registry {
	r.dbMu.RLock()
	r.varMu.RLock()
	defer r.dbMu.RUnlock()
	defer r.varMu.RUnlock()

	varsCopy := make(map[string]interface{}, len(r.varRegistry))
	for k, v := range r.varRegistry {
		varsCopy[k] = v
	}

	return &Registry{
		dbRegistry:     r.dbRegistry,
		boltRegistry:   r.boltRegistry,   // FIX: Preserve bbolt handles
		badgerRegistry: r.badgerRegistry, // FIX: Preserve BadgerDB handles
		etcdRegistry:   r.etcdRegistry, // Share etcd gRPC connection pool across threads
		varRegistry:    varsCopy,
		dirtyVars:      make(map[string]struct{}),
	}
}
func (r *Registry) GetEtcdDB(name string) (*clientv3.Client, error) {
	r.dbMu.RLock()
	defer r.dbMu.RUnlock()
	cli, ok := r.etcdRegistry[name]
	if !ok {
		return nil, fmt.Errorf("etcd database connection '%s' not registered", name)
	}
	return cli, nil
}
