package flow

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/grpc"
)

// --- Mock In-Memory etcd KV Server ---

type mockEtcdServer struct {
	etcdserverpb.UnimplementedKVServer
	mu   sync.RWMutex
	data map[string]string
}

func newMockEtcdServer() *mockEtcdServer {
	return &mockEtcdServer{
		data: make(map[string]string),
	}
}

func (s *mockEtcdServer) Put(ctx context.Context, req *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[string(req.Key)] = string(req.Value)
	return &etcdserverpb.PutResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: 1},
	}, nil
}

func (s *mockEtcdServer) Range(ctx context.Context, req *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	reqKey := string(req.Key)
	var kvs []*mvccpb.KeyValue

	if len(req.RangeEnd) > 0 {
		// Prefix scan: rangeEnd is prefix incremented or \x00
		rangeEnd := string(req.RangeEnd)
		for k, v := range s.data {
			if rangeEnd == "\x00" {
				if k >= reqKey {
					kvs = append(kvs, &mvccpb.KeyValue{
						Key:   []byte(k),
						Value: []byte(v),
					})
				}
			} else {
				if k >= reqKey && k < rangeEnd {
					kvs = append(kvs, &mvccpb.KeyValue{
						Key:   []byte(k),
						Value: []byte(v),
					})
				}
			}
		}
	} else {
		if val, ok := s.data[reqKey]; ok {
			kvs = append(kvs, &mvccpb.KeyValue{
				Key:   []byte(reqKey),
				Value: []byte(val),
			})
		}
	}

	return &etcdserverpb.RangeResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: 1},
		Kvs:    kvs,
		Count:  int64(len(kvs)),
	}, nil
}

func (s *mockEtcdServer) DeleteRange(ctx context.Context, req *etcdserverpb.DeleteRangeRequest) (*etcdserverpb.DeleteRangeResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	reqKey := string(req.Key)
	var deleted int64

	if len(req.RangeEnd) > 0 {
		rangeEnd := string(req.RangeEnd)
		for k := range s.data {
			if rangeEnd == "\x00" {
				if k >= reqKey {
					delete(s.data, k)
					deleted++
				}
			} else {
				if k >= reqKey && k < rangeEnd {
					delete(s.data, k)
					deleted++
				}
			}
		}
	} else {
		if _, ok := s.data[reqKey]; ok {
			delete(s.data, reqKey)
			deleted = 1
		}
	}

	return &etcdserverpb.DeleteRangeResponse{
		Header:  &etcdserverpb.ResponseHeader{Revision: 1},
		Deleted: deleted,
	}, nil
}

func (s *mockEtcdServer) Txn(ctx context.Context, req *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var responses []*etcdserverpb.ResponseOp
	for _, successOp := range req.Success {
		if putReq := successOp.GetRequestPut(); putReq != nil {
			s.data[string(putReq.Key)] = string(putReq.Value)
			responses = append(responses, &etcdserverpb.ResponseOp{
				Response: &etcdserverpb.ResponseOp_ResponsePut{
					ResponsePut: &etcdserverpb.PutResponse{
						Header: &etcdserverpb.ResponseHeader{Revision: 1},
					},
				},
			})
		}
	}
	return &etcdserverpb.TxnResponse{
		Header:    &etcdserverpb.ResponseHeader{Revision: 1},
		Succeeded: true,
		Responses: responses,
	}, nil
}

func startMockEtcdServer(t *testing.T) (string, func()) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen for mock etcd: %v", err)
	}

	grpcServer := grpc.NewServer()
	srv := newMockEtcdServer()
	etcdserverpb.RegisterKVServer(grpcServer, srv)

	go func() {
		_ = grpcServer.Serve(lis)
	}()

	cleanup := func() {
		grpcServer.Stop()
		_ = lis.Close()
	}

	return "http://" + lis.Addr().String(), cleanup
}

// --- Mock In-Memory Redis Server ---

type mockRedisServer struct {
	listener net.Listener
	mu       sync.RWMutex
	data     map[string]string
	stopChan chan struct{}
}

func startMockRedisServer(t *testing.T) (string, func()) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen for mock redis: %v", err)
	}

	s := &mockRedisServer{
		listener: lis,
		data:     make(map[string]string),
		stopChan: make(chan struct{}),
	}

	go s.acceptConnections()

	cleanup := func() {
		close(s.stopChan)
		_ = s.listener.Close()
	}

	return lis.Addr().String(), cleanup
}

func (s *mockRedisServer) acceptConnections() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.stopChan:
				return
			default:
				return
			}
		}
		go s.handleConnection(conn)
	}
}

func (s *mockRedisServer) handleConnection(conn net.Conn) {
	defer conn.Close()

	buf := make([]byte, 65536)
	var pending []byte

	for {
		n, err := conn.Read(buf)
		if err != nil {
			return
		}
		pending = append(pending, buf[:n]...)

		for len(pending) > 0 {
			commands, consumed, ok := parseRESPCommands(&pending)
			if !ok {
				break
			}
			pending = pending[consumed:]

			for _, args := range commands {
				if len(args) == 0 {
					continue
				}
				cmd := strings.ToUpper(args[0])
				switch cmd {
				case "HELLO":
					_, _ = conn.Write([]byte("-NOPROTO sorry this protocol version is not supported\r\n"))
				case "PING":
					_, _ = conn.Write([]byte("+PONG\r\n"))
				case "SET":
					if len(args) >= 3 {
						s.mu.Lock()
						s.data[args[1]] = args[2]
						s.mu.Unlock()
						_, _ = conn.Write([]byte("+OK\r\n"))
					} else {
						_, _ = conn.Write([]byte("-ERR wrong number of arguments for 'set' command\r\n"))
					}
				case "GET":
					if len(args) >= 2 {
						s.mu.RLock()
						val, found := s.data[args[1]]
						s.mu.RUnlock()
						if found {
							_, _ = conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(val), val)))
						} else {
							_, _ = conn.Write([]byte("$-1\r\n"))
						}
					} else {
						_, _ = conn.Write([]byte("-ERR wrong number of arguments for 'get' command\r\n"))
					}
				case "DEL":
					if len(args) >= 2 {
						deleted := 0
						s.mu.Lock()
						for _, k := range args[1:] {
							if _, found := s.data[k]; found {
								delete(s.data, k)
								deleted++
							}
						}
						s.mu.Unlock()
						_, _ = conn.Write([]byte(fmt.Sprintf(":%d\r\n", deleted)))
					} else {
						_, _ = conn.Write([]byte("-ERR wrong number of arguments for 'del' command\r\n"))
					}
				case "SCAN":
					matchPattern := ""
					for i := 2; i < len(args); i++ {
						if strings.ToUpper(args[i]) == "MATCH" && i+1 < len(args) {
							matchPattern = args[i+1]
							i++
						}
					}

					prefix := strings.TrimSuffix(matchPattern, "*")
					var matchedKeys []string
					s.mu.RLock()
					for k := range s.data {
						if prefix == "" || strings.HasPrefix(k, prefix) {
							matchedKeys = append(matchedKeys, k)
						}
					}
					s.mu.RUnlock()

					var resp strings.Builder
					resp.WriteString("*2\r\n$1\r\n0\r\n")
					resp.WriteString(fmt.Sprintf("*%d\r\n", len(matchedKeys)))
					for _, k := range matchedKeys {
						resp.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(k), k))
					}
					_, _ = conn.Write([]byte(resp.String()))
				default:
					_, _ = conn.Write([]byte("+OK\r\n"))
				}
			}
		}
	}
}

func parseRESPCommands(data *[]byte) ([][]string, int, bool) {
	bytes := *data
	if len(bytes) == 0 {
		return nil, 0, false
	}

	var commands [][]string
	idx := 0

	for idx < len(bytes) {
		if bytes[idx] == '*' {
			lineEnd := strings.Index(string(bytes[idx:]), "\r\n")
			if lineEnd == -1 {
				return commands, idx, len(commands) > 0
			}
			numElements := 0
			fmt.Sscanf(string(bytes[idx+1:idx+lineEnd]), "%d", &numElements)
			cur := idx + lineEnd + 2

			var args []string
			parseSuccess := true
			for i := 0; i < numElements; i++ {
				if cur >= len(bytes) || bytes[cur] != '$' {
					parseSuccess = false
					break
				}
				bulkEnd := strings.Index(string(bytes[cur:]), "\r\n")
				if bulkEnd == -1 {
					parseSuccess = false
					break
				}
				strLen := 0
				fmt.Sscanf(string(bytes[cur+1:cur+bulkEnd]), "%d", &strLen)
				cur += bulkEnd + 2

				if cur+strLen+2 > len(bytes) {
					parseSuccess = false
					break
				}
				args = append(args, string(bytes[cur:cur+strLen]))
				cur += strLen + 2
			}

			if !parseSuccess {
				return commands, idx, len(commands) > 0
			}
			commands = append(commands, args)
			idx = cur
		} else {
			lineEnd := strings.Index(string(bytes[idx:]), "\r\n")
			if lineEnd == -1 {
				return commands, idx, len(commands) > 0
			}
			line := strings.TrimSpace(string(bytes[idx : idx+lineEnd]))
			idx += lineEnd + 2
			if len(line) > 0 {
				commands = append(commands, strings.Fields(line))
			}
		}
	}

	return commands, idx, true
}

// --- Unit Tests for BBolt ---

func TestBoltKVOperations(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "flow_test_bolt")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test_bolt.db")

	xmlConfig := []byte(fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
	<pipeline>
		<databases>
			<database name="bolt_store" driver="bbolt" connection_string="%s" />
			<database name="src_sql" driver="sqlite" connection_string="file::memory:?cache=shared" />
		</databases>

		<scripts>
			<sql id="setup_sql" db="src_sql">
				CREATE TABLE kv_export (key TEXT, val TEXT);
				INSERT INTO kv_export VALUES ('bulk_key1', 'bulk_val1');
				INSERT INTO kv_export VALUES ('bulk_key2', 'bulk_val2');
			</sql>
		</scripts>

		<flow>
			<!-- 1. Attribute-based PUT -->
			<kv id="bolt_put" db="bolt_store" bucket="users" op="put" key="user:101" value="Alice" />

			<!-- 2. Attribute-based GET -->
			<kv id="bolt_get" db="bolt_store" bucket="users" op="get" key="user:101" output_var="alice_val" />

			<!-- 3. Inline DSL SET -->
			<kv id="bolt_inline_set" db="bolt_store" bucket="users">
				set user:102 Bob
			</kv>

			<!-- 4. Inline DSL GET -->
			<kv id="bolt_inline_get" db="bolt_store" bucket="users" output_var="bob_val">
				get user:102
			</kv>

			<!-- 5. Scan operation -->
			<kv id="bolt_scan" db="bolt_store" bucket="users" op="scan" key="user:" output_var="users_scan" />

			<!-- 6. Delete operation -->
			<kv id="bolt_del" db="bolt_store" bucket="users" op="delete" key="user:101" />

			<!-- 7. Verify deletion with GET -->
			<kv id="bolt_get_deleted" db="bolt_store" bucket="users" op="get" key="user:101" output_var="deleted_val" />

			<!-- 8. Bulk ETL from SQL to bbolt -->
			<kv_bulk id="bolt_bulk" db="src_sql" target_db="bolt_store" target_bucket="bulk_bucket" batch_size="10" output_var="copied_count">
				SELECT key, val FROM kv_export
			</kv_bulk>

			<!-- 9. Verify bulk inserted key -->
			<kv id="bolt_get_bulk" db="bolt_store" bucket="bulk_bucket" op="get" key="bulk_key1" output_var="bulk_val1" />
		</flow>
	</pipeline>`, strings.ReplaceAll(dbPath, `\`, `/`)))

	cfg, err := ParseXMLConfig(xmlConfig)
	if err != nil {
		t.Fatalf("failed to parse XML: %v", err)
	}

	registry := NewRegistry()
	if err := registry.InitDatabases(cfg.Databases); err != nil {
		t.Fatalf("failed to init databases: %v", err)
	}
	defer registry.CloseDatabases()

	executor := NewExecutor(registry)
	results, err := executor.Execute(context.Background(), cfg.FlowNodes)
	if err != nil {
		for _, res := range results {
			t.Logf("Result: %s, ReturnCode: %v, Output: %s", res.ScriptID, res.ReturnCode, res.ResultsString)
		}
		t.Fatalf("execution failed: %v", err)
	}

	if val := registry.GetVar("alice_val"); val != "Alice" {
		t.Errorf("expected alice_val to be 'Alice', got '%v'", val)
	}
	if val := registry.GetVar("bob_val"); val != "Bob" {
		t.Errorf("expected bob_val to be 'Bob', got '%v'", val)
	}
	if val := registry.GetVar("users_scan"); val == nil || !strings.Contains(val.(string), "user:101") {
		t.Errorf("expected users_scan to contain user:101, got '%v'", val)
	}
	if val := registry.GetVar("deleted_val"); val != "" {
		t.Errorf("expected deleted_val to be empty string, got '%v'", val)
	}
	if val := registry.GetVar("copied_count"); val != "2" {
		t.Errorf("expected copied_count to be '2', got '%v'", val)
	}
	if val := registry.GetVar("bulk_val1"); val != "bulk_val1" {
		t.Errorf("expected bulk_val1 to be 'bulk_val1', got '%v'", val)
	}
}

// --- Unit Tests for BadgerDB ---

func TestBadgerKVOperations(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "flow_test_badger")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbDir := filepath.Join(tmpDir, "badger_data")

	xmlConfig := []byte(fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
	<pipeline>
		<databases>
			<database name="badger_store" driver="badger" connection_string="%s" />
			<database name="src_sql_badger" driver="sqlite" connection_string="file::memory:?cache=shared" />
		</databases>

		<scripts>
			<sql id="setup_sql" db="src_sql_badger">
				CREATE TABLE kv_export_badger (key TEXT, val TEXT);
				INSERT INTO kv_export_badger VALUES ('b_k1', 'b_v1');
				INSERT INTO kv_export_badger VALUES ('b_k2', 'b_v2');
			</sql>
		</scripts>

		<flow>
			<!-- 1. Attribute-based PUT -->
			<kv id="badger_put" db="badger_store" bucket="cache" op="put" key="session:1" value="sess_data_1" />

			<!-- 2. Attribute-based GET -->
			<kv id="badger_get" db="badger_store" bucket="cache" op="get" key="session:1" output_var="sess_val" />

			<!-- 3. Inline DSL SET -->
			<kv id="badger_inline_set" db="badger_store" bucket="cache">
				set session:2 sess_data_2
			</kv>

			<!-- 4. Inline DSL GET -->
			<kv id="badger_inline_get" db="badger_store" bucket="cache" output_var="sess_val2">
				get session:2
			</kv>

			<!-- 5. Scan operation -->
			<kv id="badger_scan" db="badger_store" bucket="cache" op="scan" key="session:" output_var="sessions_scan" />

			<!-- 6. Delete operation -->
			<kv id="badger_del" db="badger_store" bucket="cache" op="delete" key="session:1" />

			<!-- 7. Verify deletion with GET -->
			<kv id="badger_get_deleted" db="badger_store" bucket="cache" op="get" key="session:1" output_var="deleted_sess" />

			<!-- 8. Bulk ETL from SQL to BadgerDB -->
			<kv_bulk id="badger_bulk" db="src_sql_badger" target_db="badger_store" target_bucket="badger_bulk" batch_size="10" output_var="badger_copied">
				SELECT key, val FROM kv_export_badger
			</kv_bulk>

			<!-- 9. Verify bulk inserted key -->
			<kv id="badger_get_bulk" db="badger_store" bucket="badger_bulk" op="get" key="b_k1" output_var="bulk_bk1" />
		</flow>
	</pipeline>`, strings.ReplaceAll(dbDir, `\`, `/`)))

	cfg, err := ParseXMLConfig(xmlConfig)
	if err != nil {
		t.Fatalf("failed to parse XML: %v", err)
	}

	registry := NewRegistry()
	if err := registry.InitDatabases(cfg.Databases); err != nil {
		t.Fatalf("failed to init databases: %v", err)
	}
	defer registry.CloseDatabases()

	executor := NewExecutor(registry)
	results, err := executor.Execute(context.Background(), cfg.FlowNodes)
	if err != nil {
		for _, res := range results {
			t.Logf("Result: %s, ReturnCode: %v, Output: %s", res.ScriptID, res.ReturnCode, res.ResultsString)
		}
		t.Fatalf("execution failed: %v", err)
	}

	if val := registry.GetVar("sess_val"); val != "sess_data_1" {
		t.Errorf("expected sess_val to be 'sess_data_1', got '%v'", val)
	}
	if val := registry.GetVar("sess_val2"); val != "sess_data_2" {
		t.Errorf("expected sess_val2 to be 'sess_data_2', got '%v'", val)
	}
	if val := registry.GetVar("sessions_scan"); val == nil || !strings.Contains(val.(string), "session:1") {
		t.Errorf("expected sessions_scan to contain session:1, got '%v'", val)
	}
	if val := registry.GetVar("deleted_sess"); val != "" {
		t.Errorf("expected deleted_sess to be empty string, got '%v'", val)
	}
	if val := registry.GetVar("badger_copied"); val != "2" {
		t.Errorf("expected badger_copied to be '2', got '%v'", val)
	}
	if val := registry.GetVar("bulk_bk1"); val != "b_v1" {
		t.Errorf("expected bulk_bk1 to be 'b_v1', got '%v'", val)
	}
}

// --- Unit Tests for Redis ---

func TestRedisKVOperations(t *testing.T) {
	redisAddr, cleanup := startMockRedisServer(t)
	defer cleanup()

	xmlConfig := []byte(fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
	<pipeline>
		<databases>
			<database name="redis_store" driver="redis" connection_string="%s" />
			<database name="src_sql_redis" driver="sqlite" connection_string="file::memory:?cache=shared" />
		</databases>

		<scripts>
			<sql id="setup_sql" db="src_sql_redis">
				CREATE TABLE kv_export_redis (key TEXT, val TEXT);
				INSERT INTO kv_export_redis VALUES ('r_k1', 'r_v1');
				INSERT INTO kv_export_redis VALUES ('r_k2', 'r_v2');
			</sql>
		</scripts>

		<flow>
			<!-- 1. Attribute-based PUT / SET -->
			<kv id="redis_put" db="redis_store" bucket="app" op="put" key="token:abc" value="valid_token" />

			<!-- 2. Attribute-based GET -->
			<kv id="redis_get" db="redis_store" bucket="app" op="get" key="token:abc" output_var="token_val" />

			<!-- 3. Inline DSL SET -->
			<kv id="redis_inline_set" db="redis_store" bucket="app">
				set token:def second_token
			</kv>

			<!-- 4. Inline DSL GET -->
			<kv id="redis_inline_get" db="redis_store" bucket="app" output_var="token_val2">
				get token:def
			</kv>

			<!-- 5. Scan operation -->
			<kv id="redis_scan" db="redis_store" bucket="app" op="scan" key="token:" output_var="tokens_scan" />

			<!-- 6. Delete operation -->
			<kv id="redis_del" db="redis_store" bucket="app" op="del" key="token:abc" />

			<!-- 7. Verify deletion with GET -->
			<kv id="redis_get_deleted" db="redis_store" bucket="app" op="get" key="token:abc" output_var="deleted_token" />

			<!-- 8. Bulk ETL from SQL to Redis -->
			<kv_bulk id="redis_bulk" db="src_sql_redis" target_db="redis_store" target_bucket="redis_bulk" batch_size="10" output_var="redis_copied">
				SELECT key, val FROM kv_export_redis
			</kv_bulk>

			<!-- 9. Verify bulk inserted key -->
			<kv id="redis_get_bulk" db="redis_store" bucket="redis_bulk" op="get" key="r_k1" output_var="bulk_rk1" />
		</flow>
	</pipeline>`, redisAddr))

	cfg, err := ParseXMLConfig(xmlConfig)
	if err != nil {
		t.Fatalf("failed to parse XML: %v", err)
	}

	registry := NewRegistry()
	if err := registry.InitDatabases(cfg.Databases); err != nil {
		t.Fatalf("failed to init databases: %v", err)
	}
	defer registry.CloseDatabases()

	executor := NewExecutor(registry)
	results, err := executor.Execute(context.Background(), cfg.FlowNodes)
	if err != nil {
		for _, res := range results {
			t.Logf("Result: %s, ReturnCode: %v, Output: %s", res.ScriptID, res.ReturnCode, res.ResultsString)
		}
		t.Fatalf("execution failed: %v", err)
	}

	if val := registry.GetVar("token_val"); val != "valid_token" {
		t.Errorf("expected token_val to be 'valid_token', got '%v'", val)
	}
	if val := registry.GetVar("token_val2"); val != "second_token" {
		t.Errorf("expected token_val2 to be 'second_token', got '%v'", val)
	}
	if val := registry.GetVar("tokens_scan"); val == nil || !strings.Contains(val.(string), "token:abc") {
		t.Errorf("expected tokens_scan to contain token:abc, got '%v'", val)
	}
	if val := registry.GetVar("deleted_token"); val != "" {
		t.Errorf("expected deleted_token to be empty string, got '%v'", val)
	}
	if val := registry.GetVar("redis_copied"); val != "2" {
		t.Errorf("expected redis_copied to be '2', got '%v'", val)
	}
	if val := registry.GetVar("bulk_rk1"); val != "r_v1" {
		t.Errorf("expected bulk_rk1 to be 'r_v1', got '%v'", val)
	}
}

// --- Unit Tests for etcd ---

func TestEtcdKVOperations(t *testing.T) {
	etcdAddr, cleanup := startMockEtcdServer(t)
	defer cleanup()

	xmlConfig := []byte(fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
	<pipeline>
		<databases>
			<database name="etcd_cluster" driver="etcd" connection_string="%s" />
			<database name="src_sql_etcd" driver="sqlite" connection_string="file::memory:?cache=shared" />
		</databases>

		<scripts>
			<sql id="setup_sql" db="src_sql_etcd">
				CREATE TABLE kv_export_etcd (key TEXT, val TEXT);
				INSERT INTO kv_export_etcd VALUES ('e_k1', 'e_v1');
				INSERT INTO kv_export_etcd VALUES ('e_k2', 'e_v2');
			</sql>
		</scripts>

		<flow>
			<!-- 1. Attribute-based PUT -->
			<kv id="etcd_put" db="etcd_cluster" bucket="config" op="put" key="max_threads" value="32" />

			<!-- 2. Attribute-based GET -->
			<kv id="etcd_get" db="etcd_cluster" bucket="config" op="get" key="max_threads" output_var="threads_val" />

			<!-- 3. Inline DSL SET -->
			<kv id="etcd_inline_set" db="etcd_cluster" bucket="config">
				set timeout_sec 60
			</kv>

			<!-- 4. Inline DSL GET -->
			<kv id="etcd_inline_get" db="etcd_cluster" bucket="config" output_var="timeout_val">
				get timeout_sec
			</kv>

			<!-- 5. Scan operation -->
			<kv id="etcd_scan" db="etcd_cluster" bucket="config" op="scan" key="max_" output_var="config_scan" />

			<!-- 6. Delete operation -->
			<kv id="etcd_del" db="etcd_cluster" bucket="config" op="del" key="max_threads" />

			<!-- 7. Verify deletion with GET -->
			<kv id="etcd_get_deleted" db="etcd_cluster" bucket="config" op="get" key="max_threads" output_var="deleted_threads" />

			<!-- 8. Bulk ETL from SQL to etcd -->
			<kv_bulk id="etcd_bulk" db="src_sql_etcd" target_db="etcd_cluster" target_bucket="etcd_bulk" batch_size="10" output_var="etcd_copied">
				SELECT key, val FROM kv_export_etcd
			</kv_bulk>

			<!-- 9. Verify bulk inserted key -->
			<kv id="etcd_get_bulk" db="etcd_cluster" bucket="etcd_bulk" op="get" key="e_k1" output_var="bulk_ek1" />
		</flow>
	</pipeline>`, etcdAddr))

	cfg, err := ParseXMLConfig(xmlConfig)
	if err != nil {
		t.Fatalf("failed to parse XML: %v", err)
	}

	registry := NewRegistry()
	if err := registry.InitDatabases(cfg.Databases); err != nil {
		t.Fatalf("failed to init databases: %v", err)
	}
	defer registry.CloseDatabases()

	executor := NewExecutor(registry)
	results, err := executor.Execute(context.Background(), cfg.FlowNodes)
	if err != nil {
		for _, res := range results {
			t.Logf("Result: %s, ReturnCode: %v, Output: %s", res.ScriptID, res.ReturnCode, res.ResultsString)
		}
		t.Fatalf("execution failed: %v", err)
	}

	if val := registry.GetVar("threads_val"); val != "32" {
		t.Errorf("expected threads_val to be '32', got '%v'", val)
	}
	if val := registry.GetVar("timeout_val"); val != "60" {
		t.Errorf("expected timeout_val to be '60', got '%v'", val)
	}
	if val := registry.GetVar("config_scan"); val == nil || !strings.Contains(val.(string), "max_threads") {
		t.Errorf("expected config_scan to contain max_threads, got '%v'", val)
	}
	if val := registry.GetVar("deleted_threads"); val != "" {
		t.Errorf("expected deleted_threads to be empty string, got '%v'", val)
	}
	if val := registry.GetVar("etcd_copied"); val != "2" {
		t.Errorf("expected etcd_copied to be '2', got '%v'", val)
	}
	if val := registry.GetVar("bulk_ek1"); val != "e_v1" {
		t.Errorf("expected bulk_ek1 to be 'e_v1', got '%v'", val)
	}
}
