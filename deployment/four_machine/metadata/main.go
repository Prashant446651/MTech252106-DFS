package main

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

const (
	desiredReplicationFactor = 3
	nodeCheckInterval        = 5 * time.Second
	reconciliationInterval   = 10 * time.Second
	deletionRetryInterval    = 5 * time.Second
	replicationRetryInterval = 5 * time.Second
)

type ReplicaInfo struct {
	Node    string `json:"node"`
	Address string `json:"address"`
	Path    string `json:"path"`
}

type FileMetadata struct {
	Name     string        `json:"name"`
	Size     int64         `json:"size"`
	SHA256   string        `json:"sha256"`
	Replicas []ReplicaInfo `json:"replicas"`
}

type NodeStatus struct {
	Node      string `json:"node"`
	Address   string `json:"address"`
	Status    string `json:"status"`
	LastSeen  string `json:"last_seen,omitempty"`
	LastError string `json:"last_error,omitempty"`
}

type NodeRecord struct {
	Node    string
	Address string
	Status  string
}

type FileRecord struct {
	Name   string
	Size   int64
	SHA256 string
}

type ReplicaCheck struct {
	Replica ReplicaInfo
	Hash    string
	Size    int64
	Err     error
}

type PendingDelete struct {
	FileName string
	Node     string
	Address  string
}

type ReplicationJob struct {
	ID            int64  `json:"id"`
	FileName      string `json:"file_name"`
	SourceNode    string `json:"source_node"`
	SourceAddress string `json:"source_address"`
	TargetNode    string `json:"target_node"`
	TargetAddress string `json:"target_address"`
	Attempts      int    `json:"attempts"`
	LastError     string `json:"last_error"`
	CreatedAt     string `json:"created_at"`
	UpdatedAt     string `json:"updated_at"`
}

type PendingReplicaDelete struct {
	FileName string
	Node     string
	Address  string
}

var (
	db   *sql.DB
	dbMu sync.Mutex

	// Serializes deletion and reconciliation operations.
	fileOperationMu sync.Mutex

	// Prevents two deletion workers from processing the same queue
	// simultaneously.
	deleteQueueMu sync.Mutex
)

func configuredDatabasePath() string {
	path := strings.TrimSpace(os.Getenv("METADATA_DB"))
	if path == "" {
		return "./metadata.db"
	}
	return path
}

func initializeDatabase() error {
	var err error

	db, err = sql.Open("sqlite", configuredDatabasePath())
	if err != nil {
		return err
	}

	db.SetMaxOpenConns(1)

	_, err = db.Exec(`
		PRAGMA foreign_keys = ON;

		CREATE TABLE IF NOT EXISTS files (
			name TEXT PRIMARY KEY,
			size INTEGER NOT NULL,
			sha256 TEXT NOT NULL DEFAULT ''
		);

		CREATE TABLE IF NOT EXISTS replicas (
			file_name TEXT NOT NULL,
			node TEXT NOT NULL,
			address TEXT NOT NULL,
			path TEXT NOT NULL,
			PRIMARY KEY (file_name, node),
			FOREIGN KEY (file_name)
				REFERENCES files(name)
				ON DELETE CASCADE
		);

		CREATE TABLE IF NOT EXISTS nodes (
			node TEXT PRIMARY KEY,
			address TEXT NOT NULL,
			status TEXT NOT NULL DEFAULT 'unknown',
			last_seen TEXT,
			last_error TEXT NOT NULL DEFAULT ''
		);

		CREATE TABLE IF NOT EXISTS deletion_tombstones (
			file_name TEXT NOT NULL,
			node TEXT NOT NULL,
			address TEXT NOT NULL DEFAULT '',
			requested_at TEXT NOT NULL,
			PRIMARY KEY (file_name, node)
		);

		CREATE TABLE IF NOT EXISTS replication_queue (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			file_name TEXT NOT NULL,
			source_node TEXT NOT NULL,
			source_address TEXT NOT NULL,
			target_node TEXT NOT NULL,
			target_address TEXT NOT NULL,
			attempts INTEGER NOT NULL DEFAULT 0,
			last_error TEXT NOT NULL DEFAULT '',
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			UNIQUE(file_name, target_node)
		);

		CREATE TABLE IF NOT EXISTS replica_exclusions (
			file_name TEXT NOT NULL,
			node TEXT NOT NULL,
			reason TEXT NOT NULL DEFAULT 'manually excluded',
			created_at TEXT NOT NULL,
			PRIMARY KEY(file_name, node)
		);

		CREATE TABLE IF NOT EXISTS replica_deletion_queue (
			file_name TEXT NOT NULL,
			node TEXT NOT NULL,
			address TEXT NOT NULL DEFAULT '',
			requested_at TEXT NOT NULL,
			PRIMARY KEY(file_name, node)
		);
	`)
	if err != nil {
		return err
	}

	// Migrate databases created by earlier versions.
	if err := ensureColumn(
		"files", "sha256", "TEXT NOT NULL DEFAULT ''",
	); err != nil {
		return err
	}

	if err := ensureColumn(
		"deletion_tombstones", "address", "TEXT NOT NULL DEFAULT ''",
	); err != nil {
		return err
	}

	// Fill addresses for pending deletion requests created by the
	// earlier tombstone implementation.
	_, err = db.Exec(`
		UPDATE deletion_tombstones
		SET address = COALESCE(
			(SELECT nodes.address
			 FROM nodes
			 WHERE nodes.node = deletion_tombstones.node),
			''
		)
		WHERE address = ''
	`)
	if err != nil {
		return err
	}

	return db.Ping()
}

func ensureColumn(table, column, definition string) error {
	// These identifiers are constants owned by this program.
	rows, err := db.Query("PRAGMA table_info(" + table + ")")
	if err != nil {
		return err
	}

	found := false

	for rows.Next() {
		var cid int
		var name, dataType string
		var notNull, primaryKey int
		var defaultValue interface{}

		if err := rows.Scan(
			&cid, &name, &dataType, &notNull,
			&defaultValue, &primaryKey,
		); err != nil {
			_ = rows.Close()
			return err
		}

		if name == column {
			found = true
		}
	}

	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}

	if found {
		return nil
	}

	_, err = db.Exec(
		"ALTER TABLE " + table + " ADD COLUMN " +
			column + " " + definition,
	)
	return err
}

func validFileName(name string) bool {
	return name != "" &&
		name != "." &&
		name != ".." &&
		!strings.ContainsAny(name, `/\`) &&
		name == strings.TrimSpace(name)
}

func validSHA256(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func baseURL(address string) string {
	address = strings.TrimRight(address, "/")

	if !strings.HasPrefix(address, "http://") &&
		!strings.HasPrefix(address, "https://") {
		address = "http://" + address
	}

	return address
}

// Register a storage node.
func registerNodeHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		http.Error(w, "Use GET or POST",
			http.StatusMethodNotAllowed)
		return
	}

	node := r.URL.Query().Get("node")
	address := r.URL.Query().Get("address")

	if node == "" || address == "" {
		http.Error(w, "Missing node or address",
			http.StatusBadRequest)
		return
	}

	dbMu.Lock()
	defer dbMu.Unlock()

	_, err := db.Exec(`
		INSERT INTO nodes(node, address, status, last_error)
		VALUES(?, ?, 'unknown', '')
		ON CONFLICT(node) DO UPDATE SET
			address = excluded.address,
			status = 'unknown',
			last_error = ''
	`, node, address)

	if err != nil {
		http.Error(w, "Cannot register node",
			http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, "Node registered: %s\n", node)
}

// Normal upload registration. A non-empty SHA-256 records the
// content expected by the reconciliation manager.
func addFileHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		http.Error(w, "Use GET or POST",
			http.StatusMethodNotAllowed)
		return
	}

	q := r.URL.Query()
	name := q.Get("name")
	node := q.Get("node")
	address := q.Get("address")
	path := q.Get("path")
	hash := strings.ToLower(q.Get("sha256"))

	if !validFileName(name) ||
		node == "" || address == "" || path == "" {
		http.Error(w, "Missing or invalid file metadata",
			http.StatusBadRequest)
		return
	}

	size, err := strconv.ParseInt(q.Get("size"), 10, 64)
	if err != nil || size < 0 {
		http.Error(w, "Invalid file size",
			http.StatusBadRequest)
		return
	}

	if hash != "" && !validSHA256(hash) {
		http.Error(w, "Invalid SHA-256 checksum",
			http.StatusBadRequest)
		return
	}

	fileOperationMu.Lock()
	defer fileOperationMu.Unlock()

	dbMu.Lock()
	defer dbMu.Unlock()

	// Do not let an upload recreate a file while deletion is pending.
	var pending int
	err = db.QueryRow(`
		SELECT COUNT(*)
		FROM deletion_tombstones
		WHERE file_name = ?
	`, name).Scan(&pending)
	if err != nil {
		http.Error(w, "Cannot check deletion queue",
			http.StatusInternalServerError)
		return
	}
	if pending > 0 {
		http.Error(w,
			"Deletion is still pending for this filename",
			http.StatusConflict)
		return
	}

	tx, err := db.Begin()
	if err != nil {
		http.Error(w, "Database error",
			http.StatusInternalServerError)
		return
	}
	defer tx.Rollback()

	_, err = tx.Exec(`
		INSERT INTO files(name, size, sha256)
		VALUES(?, ?, ?)
		ON CONFLICT(name) DO UPDATE SET
			size = excluded.size,
			sha256 = CASE
				WHEN excluded.sha256 <> ''
				THEN excluded.sha256
				ELSE files.sha256
			END
	`, name, size, hash)
	if err != nil {
		http.Error(w, "Cannot save file metadata",
			http.StatusInternalServerError)
		return
	}

	_, err = tx.Exec(`
		INSERT INTO nodes(node, address, status, last_error)
		VALUES(?, ?, 'unknown', '')
		ON CONFLICT(node) DO UPDATE SET
			address = excluded.address
	`, node, address)
	if err != nil {
		http.Error(w, "Cannot register node",
			http.StatusInternalServerError)
		return
	}

	_, err = tx.Exec(`
		INSERT INTO replicas(file_name, node, address, path)
		VALUES(?, ?, ?, ?)
		ON CONFLICT(file_name, node) DO UPDATE SET
			address = excluded.address,
			path = excluded.path
	`, name, node, address, path)
	if err != nil {
		http.Error(w, "Cannot save replica metadata",
			http.StatusInternalServerError)
		return
	}

	if err := tx.Commit(); err != nil {
		http.Error(w, "Cannot commit metadata",
			http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, "Metadata registered for %s on %s\n",
		name, node)
}

// Repaired replicas must match the canonical checksum. This endpoint
// must not change the canonical checksum.
func registerReplicaHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		http.Error(w, "Use GET or POST",
			http.StatusMethodNotAllowed)
		return
	}

	q := r.URL.Query()
	name := q.Get("name")
	node := q.Get("node")
	address := q.Get("address")
	path := q.Get("path")
	hash := strings.ToLower(q.Get("sha256"))

	if !validFileName(name) || node == "" || address == "" ||
		path == "" || !validSHA256(hash) {
		http.Error(w, "Missing or invalid replica registration data",
			http.StatusBadRequest)
		return
	}

	size, err := strconv.ParseInt(q.Get("size"), 10, 64)
	if err != nil || size < 0 {
		http.Error(w, "Invalid file size",
			http.StatusBadRequest)
		return
	}

	if excluded, err := isReplicaExcluded(name, node); err != nil {
		http.Error(w, "Cannot check replica exclusion", http.StatusInternalServerError)
		return
	} else if excluded {
		http.Error(w, "Replica is deliberately excluded on this node", http.StatusConflict)
		return
	}

	dbMu.Lock()
	defer dbMu.Unlock()

	var canonicalHash string
	var canonicalSize int64

	err = db.QueryRow(`
		SELECT size, sha256
		FROM files
		WHERE name = ?
	`, name).Scan(&canonicalSize, &canonicalHash)

	if err == sql.ErrNoRows {
		http.Error(w, "Canonical file not found",
			http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, "Cannot read canonical file",
			http.StatusInternalServerError)
		return
	}
	if canonicalHash == "" {
		http.Error(w, "Canonical checksum is not established",
			http.StatusConflict)
		return
	}
	if hash != canonicalHash || size != canonicalSize {
		http.Error(w, "Recovered copy does not match canonical file",
			http.StatusConflict)
		return
	}

	// Reject recovery if the filename is being deleted.
	var pending int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM deletion_tombstones
		WHERE file_name = ?
	`, name).Scan(&pending); err != nil {
		http.Error(w, "Cannot check deletion queue",
			http.StatusInternalServerError)
		return
	}
	if pending > 0 {
		http.Error(w, "Deletion is pending for this filename",
			http.StatusConflict)
		return
	}

	tx, err := db.Begin()
	if err != nil {
		http.Error(w, "Database error",
			http.StatusInternalServerError)
		return
	}
	defer tx.Rollback()

	_, err = tx.Exec(`
		INSERT INTO nodes(node, address, status, last_error)
		VALUES(?, ?, 'unknown', '')
		ON CONFLICT(node) DO UPDATE SET address = excluded.address
	`, node, address)
	if err != nil {
		http.Error(w, "Cannot register node",
			http.StatusInternalServerError)
		return
	}

	_, err = tx.Exec(`
		INSERT INTO replicas(file_name, node, address, path)
		VALUES(?, ?, ?, ?)
		ON CONFLICT(file_name, node) DO UPDATE SET
			address = excluded.address,
			path = excluded.path
	`, name, node, address, path)
	if err != nil {
		http.Error(w, "Cannot register recovered replica",
			http.StatusInternalServerError)
		return
	}

	if err := tx.Commit(); err != nil {
		http.Error(w, "Cannot commit recovered replica",
			http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, "Verified replica registered for %s on %s\n",
		name, node)
}

func getFileHandler(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	if !validFileName(name) {
		http.Error(w, "Missing or invalid file name",
			http.StatusBadRequest)
		return
	}

	var result FileMetadata

	err := db.QueryRow(`
		SELECT name, size, COALESCE(sha256, '')
		FROM files WHERE name = ?
	`, name).Scan(
		&result.Name, &result.Size, &result.SHA256,
	)

	if err == sql.ErrNoRows {
		http.Error(w, "File metadata not found",
			http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, "Database error",
			http.StatusInternalServerError)
		return
	}

	rows, err := db.Query(`
		SELECT node, address, path
		FROM replicas
		WHERE file_name = ?
		ORDER BY node
	`, name)
	if err != nil {
		http.Error(w, "Cannot read replica metadata",
			http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	result.Replicas = []ReplicaInfo{}

	for rows.Next() {
		var replica ReplicaInfo
		if err := rows.Scan(
			&replica.Node, &replica.Address, &replica.Path,
		); err != nil {
			http.Error(w, "Cannot read replica",
				http.StatusInternalServerError)
			return
		}
		result.Replicas = append(result.Replicas, replica)
	}

	if err := rows.Err(); err != nil {
		http.Error(w, "Cannot read replicas",
			http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}

func listFilesHandler(w http.ResponseWriter, r *http.Request) {
	rows, err := db.Query("SELECT name FROM files ORDER BY name")
	if err != nil {
		http.Error(w, "Database error",
			http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	names := []string{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			http.Error(w, "Cannot read file list",
				http.StatusInternalServerError)
			return
		}
		names = append(names, name)
	}

	if err := rows.Err(); err != nil {
		http.Error(w, "Cannot read file list",
			http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(names)
}

func nodesHandler(w http.ResponseWriter, r *http.Request) {
	rows, err := db.Query(`
		SELECT node, address, status,
		       COALESCE(last_seen, ''),
		       COALESCE(last_error, '')
		FROM nodes ORDER BY node
	`)
	if err != nil {
		http.Error(w, "Cannot read node status",
			http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	nodes := []NodeStatus{}
	for rows.Next() {
		var n NodeStatus
		if err := rows.Scan(
			&n.Node, &n.Address, &n.Status,
			&n.LastSeen, &n.LastError,
		); err != nil {
			http.Error(w, "Cannot read node record",
				http.StatusInternalServerError)
			return
		}
		nodes = append(nodes, n)
	}

	if err := rows.Err(); err != nil {
		http.Error(w, "Cannot read nodes",
			http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(nodes)
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	if err := db.Ping(); err != nil {
		http.Error(w, "Metadata database unavailable",
			http.StatusInternalServerError)
		return
	}
	fmt.Fprintln(w, "Metadata server is healthy")
}

func checkNode(address string) error {
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(baseURL(address) + "/health")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("health check returned %s", resp.Status)
	}
	return nil
}

func updateNodeStatus(node, status, errorMessage string) {
	dbMu.Lock()
	defer dbMu.Unlock()

	if status == "online" {
		_, err := db.Exec(`
			UPDATE nodes
			SET status = ?, last_seen = ?, last_error = ''
			WHERE node = ?
		`, status, time.Now().Format(time.RFC3339), node)
		if err != nil {
			log.Printf("Cannot update node %s: %v", node, err)
		}
		return
	}

	_, err := db.Exec(`
		UPDATE nodes SET status = ?, last_error = ?
		WHERE node = ?
	`, status, errorMessage, node)
	if err != nil {
		log.Printf("Cannot update node %s: %v", node, err)
	}
}

func loadNodes() ([]NodeRecord, error) {
	rows, err := db.Query(`
		SELECT node, address, status FROM nodes ORDER BY node
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var nodes []NodeRecord
	for rows.Next() {
		var n NodeRecord
		if err := rows.Scan(&n.Node, &n.Address, &n.Status); err != nil {
			return nil, err
		}
		nodes = append(nodes, n)
	}
	return nodes, rows.Err()
}

func loadFiles() ([]FileRecord, error) {
	rows, err := db.Query(`
		SELECT name, size, COALESCE(sha256, '')
		FROM files ORDER BY name
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var files []FileRecord
	for rows.Next() {
		var f FileRecord
		if err := rows.Scan(&f.Name, &f.Size, &f.SHA256); err != nil {
			return nil, err
		}
		files = append(files, f)
	}
	return files, rows.Err()
}

func loadReplicas(fileName string) ([]ReplicaInfo, error) {
	rows, err := db.Query(`
		SELECT node, address, path
		FROM replicas WHERE file_name = ?
		ORDER BY node
	`, fileName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var replicas []ReplicaInfo
	for rows.Next() {
		var replica ReplicaInfo
		if err := rows.Scan(
			&replica.Node, &replica.Address, &replica.Path,
		); err != nil {
			return nil, err
		}
		replicas = append(replicas, replica)
	}
	return replicas, rows.Err()
}

func hashRemoteFile(address, fileName string) (string, int64, error) {
	endpoint := baseURL(address) +
		"/download?name=" + url.QueryEscape(fileName)

	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Get(endpoint)
	if err != nil {
		return "", 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, resp.Body)
		return "", 0, fmt.Errorf("download returned %s", resp.Status)
	}

	h := sha256.New()
	size, err := io.Copy(h, resp.Body)
	if err != nil {
		return "", 0, err
	}

	return hex.EncodeToString(h.Sum(nil)), size, nil
}

func establishLegacyChecksum(
	fileName string,
	size int64,
	checksum string,
) (string, int64, error) {
	dbMu.Lock()
	_, err := db.Exec(`
		UPDATE files SET sha256 = ?, size = ?
		WHERE name = ? AND (sha256 = '' OR sha256 IS NULL)
	`, checksum, size, fileName)
	dbMu.Unlock()

	if err != nil {
		return "", 0, err
	}

	var currentHash string
	var currentSize int64
	err = db.QueryRow(`
		SELECT COALESCE(sha256, ''), size
		FROM files WHERE name = ?
	`, fileName).Scan(&currentHash, &currentSize)

	return currentHash, currentSize, err
}

func copyReplica(
	fileName string,
	source ReplicaInfo,
	target NodeRecord,
) error {
	client := &http.Client{Timeout: 5 * time.Minute}

	sourceURL := baseURL(source.Address) +
		"/download?name=" + url.QueryEscape(fileName)

	sourceResp, err := client.Get(sourceURL)
	if err != nil {
		return fmt.Errorf("cannot download from %s: %w",
			source.Node, err)
	}
	defer sourceResp.Body.Close()

	if sourceResp.StatusCode < 200 ||
		sourceResp.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, sourceResp.Body)
		return fmt.Errorf("source %s returned %s",
			source.Node, sourceResp.Status)
	}

	targetURL := baseURL(target.Address) +
		"/upload?name=" + url.QueryEscape(fileName) +
		"&recovery=1"

	req, err := http.NewRequest(
		http.MethodPost, targetURL, sourceResp.Body,
	)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")

	targetResp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("cannot upload to %s: %w",
			target.Node, err)
	}
	defer targetResp.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(targetResp.Body, 4096))
	if targetResp.StatusCode < 200 ||
		targetResp.StatusCode >= 300 {
		return fmt.Errorf("target %s returned HTTP %d: %s",
			target.Node, targetResp.StatusCode,
			strings.TrimSpace(string(body)))
	}

	return nil
}

func deleteRemoteReplica(
	fileName string,
	replica ReplicaInfo,
) error {
	endpoint := baseURL(replica.Address) +
		"/delete?name=" + url.QueryEscape(fileName)

	req, err := http.NewRequest(http.MethodDelete, endpoint, nil)
	if err != nil {
		return err
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	if resp.StatusCode == http.StatusNotFound {
		return nil
	}

	return fmt.Errorf("delete returned HTTP %d: %s",
		resp.StatusCode, strings.TrimSpace(string(body)))
}

func sortedReplicaValues(
	valid map[string]ReplicaInfo,
) []ReplicaInfo {
	result := make([]ReplicaInfo, 0, len(valid))
	for _, replica := range valid {
		result = append(result, replica)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Node < result[j].Node
	})
	return result
}

// Reconcile one active file. The shared operation lock prevents a
// deletion request from racing with a new replica being created.
func reconcileOneFile(
	requestedFile FileRecord,
	nodes []NodeRecord,
	nodeByName map[string]NodeRecord,
) {
	fileOperationMu.Lock()
	defer fileOperationMu.Unlock()

	var file FileRecord
	err := db.QueryRow(`
		SELECT name, size, COALESCE(sha256, '')
		FROM files WHERE name = ?
	`, requestedFile.Name).Scan(
		&file.Name, &file.Size, &file.SHA256,
	)
	if err == sql.ErrNoRows {
		// It was deleted after the list of files was loaded.
		return
	}
	if err != nil {
		log.Printf("Reconcile %s: cannot load file: %v",
			requestedFile.Name, err)
		return
	}

	replicas, err := loadReplicas(file.Name)
	if err != nil {
		log.Printf("Reconcile %s: cannot load replicas: %v",
			file.Name, err)
		return
	}
	excludedNodes, err := loadExcludedNodes(file.Name)
	if err != nil {
		log.Printf("Reconcile %s: cannot load replica exclusions: %v", file.Name, err)
		return
	}
	if len(replicas) == 0 {
		// A file with no registered replicas cannot be safely reconstructed.
		return
	}

	checks := make(map[string]ReplicaCheck)

	for _, replica := range replicas {
		if excludedNodes[replica.Node] {
			continue
		}
		node, exists := nodeByName[replica.Node]
		if !exists || node.Status != "online" {
			checks[replica.Node] = ReplicaCheck{
				Replica: replica,
				Err:     fmt.Errorf("node is not online"),
			}
			continue
		}

		replica.Address = node.Address
		hash, size, hashErr := hashRemoteFile(
			node.Address, file.Name,
		)
		checks[replica.Node] = ReplicaCheck{
			Replica: replica,
			Hash:    hash,
			Size:    size,
			Err:     hashErr,
		}
	}

	canonicalHash := file.SHA256

	// Older files have no checksum. Establish one only when a strict
	// majority of all recorded replica locations agree.
	if canonicalHash == "" {
		counts := make(map[string]int)
		sizes := make(map[string]int64)

		for _, check := range checks {
			if check.Err != nil {
				continue
			}
			counts[check.Hash]++
			sizes[check.Hash] = check.Size
		}

		bestHash := ""
		bestCount := 0
		tie := false

		for hash, count := range counts {
			if count > bestCount {
				bestHash = hash
				bestCount = count
				tie = false
			} else if count == bestCount {
				tie = true
			}
		}

		if bestHash == "" || tie ||
			bestCount*2 <= len(replicas) {
			log.Printf(
				"Reconcile %s: legacy checksum missing and no clear majority; "+
					"not repairing or deleting copies",
				file.Name,
			)
			return
		}

		currentHash, _, err := establishLegacyChecksum(
			file.Name, sizes[bestHash], bestHash,
		)
		if err != nil || currentHash == "" {
			log.Printf("Reconcile %s: cannot establish checksum: %v",
				file.Name, err)
			return
		}
		canonicalHash = currentHash
	}

	// Only healthy copies matching the canonical hash can be sources.
	valid := make(map[string]ReplicaInfo)
	for node, check := range checks {
		if check.Err == nil && check.Hash == canonicalHash {
			valid[node] = check.Replica
		}
	}

	if len(valid) == 0 {
		log.Printf(
			"Reconcile %s: no online replica matches the canonical checksum; "+
				"refusing to guess",
			file.Name,
		)
		return
	}

	// Repair registered copies that are missing or stale.
	for _, replica := range replicas {
		if excludedNodes[replica.Node] {
			continue
		}
		if _, ok := valid[replica.Node]; ok {
			continue
		}

		target, exists := nodeByName[replica.Node]
		if !exists || target.Status != "online" {
			continue
		}

		sources := sortedReplicaValues(valid)
		if len(sources) == 0 {
			break
		}

		log.Printf("Reconcile %s: repairing replica on %s",
			file.Name, target.Node)

		if err := copyReplica(file.Name, sources[0], target); err != nil {
			log.Printf("Reconcile %s: repair on %s failed: %v",
				file.Name, target.Node, err)
			continue
		}

		hash, size, err := hashRemoteFile(target.Address, file.Name)
		if err != nil || hash != canonicalHash || size != file.Size {
			log.Printf("Reconcile %s: verification failed on %s",
				file.Name, target.Node)
			continue
		}

		valid[target.Node] = ReplicaInfo{
			Node:    target.Node,
			Address: target.Address,
		}
	}

	usedNodes := make(map[string]bool)
	for _, replica := range replicas {
		usedNodes[replica.Node] = true
	}
	// A deliberate exclusion is a permanent placement constraint until an
	// administrator explicitly removes it; never select that node as a target.
	for node := range excludedNodes {
		usedNodes[node] = true
	}

	// Create missing replicas if unused, healthy nodes are available.
	for len(valid) < desiredReplicationFactor {
		sources := sortedReplicaValues(valid)
		if len(sources) == 0 {
			break
		}

		var target *NodeRecord
		for i := range nodes {
			if nodes[i].Status == "online" &&
				!usedNodes[nodes[i].Node] {
				target = &nodes[i]
				break
			}
		}

		if target == nil {
			log.Printf(
				"Reconcile %s: %d verified copies; no unused healthy node",
				file.Name, len(valid),
			)
			break
		}

		usedNodes[target.Node] = true

		log.Printf("Reconcile %s: creating replacement on %s",
			file.Name, target.Node)

		if err := copyReplica(file.Name, sources[0], *target); err != nil {
			log.Printf("Reconcile %s: copy to %s failed: %v",
				file.Name, target.Node, err)
			continue
		}

		hash, size, err := hashRemoteFile(target.Address, file.Name)
		if err != nil || hash != canonicalHash || size != file.Size {
			log.Printf("Reconcile %s: verification failed on %s",
				file.Name, target.Node)
			continue
		}

		valid[target.Node] = ReplicaInfo{
			Node:    target.Node,
			Address: target.Address,
		}
	}

	// Remove excess copies only after at least RF verified copies exist.
	verified := sortedReplicaValues(valid)
	if len(verified) <= desiredReplicationFactor {
		return
	}

	for _, extra := range verified[desiredReplicationFactor:] {
		log.Printf("Reconcile %s: removing excess copy from %s",
			file.Name, extra.Node)

		if err := deleteRemoteReplica(file.Name, extra); err != nil {
			log.Printf("Reconcile %s: cannot delete copy on %s: %v",
				file.Name, extra.Node, err)
			continue
		}

		dbMu.Lock()
		_, err := db.Exec(
			"DELETE FROM replicas WHERE file_name = ? AND node = ?",
			file.Name, extra.Node,
		)
		dbMu.Unlock()

		if err != nil {
			log.Printf("Reconcile %s: cannot remove replica record for %s: %v",
				file.Name, extra.Node, err)
			continue
		}

		log.Printf("Reconcile %s: removed excess copy on %s",
			file.Name, extra.Node)
	}
}

func nodeMonitorLoop() {
	log.Printf("Node monitor started; interval %s", nodeCheckInterval)

	for {
		nodes, err := loadNodes()
		if err != nil {
			log.Printf("Node monitor: cannot load nodes: %v", err)
		} else {
			for _, node := range nodes {
				if err := checkNode(node.Address); err != nil {
					updateNodeStatus(node.Node, "offline", err.Error())
				} else {
					updateNodeStatus(node.Node, "online", "")
				}
			}
		}

		time.Sleep(nodeCheckInterval)
	}
}

func reconciliationLoop() {
	log.Printf(
		"Reconciler started; interval %s; target RF=%d",
		reconciliationInterval,
		desiredReplicationFactor,
	)

	for {
		time.Sleep(reconciliationInterval)

		nodes, err := loadNodes()
		if err != nil {
			log.Printf("Reconciler: cannot load nodes: %v", err)
			continue
		}

		nodeByName := make(map[string]NodeRecord)
		for _, node := range nodes {
			nodeByName[node.Node] = node
		}

		files, err := loadFiles()
		if err != nil {
			log.Printf("Reconciler: cannot load files: %v", err)
			continue
		}

		for _, file := range files {
			reconcileOneFile(file, nodes, nodeByName)
		}
	}
}

// Record a whole-DFS deletion before attempting physical removal.
// Active file metadata is removed in the same SQLite transaction.
func requestWholeFileDelete(name string) error {
	fileOperationMu.Lock()
	defer fileOperationMu.Unlock()

	dbMu.Lock()
	defer dbMu.Unlock()

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	requestedAt := time.Now().Format(time.RFC3339)

	// Queue this filename on every registered node, so even an
	// unregistered stray copy on a known node can be removed.
	_, err = tx.Exec(`
		INSERT OR IGNORE INTO deletion_tombstones
			(file_name, node, address, requested_at)
		SELECT ?, node, address, ?
		FROM nodes
	`, name, requestedAt)
	if err != nil {
		return err
	}

	// Also include any replica location missing from the nodes table.
	_, err = tx.Exec(`
		INSERT OR IGNORE INTO deletion_tombstones
			(file_name, node, address, requested_at)
		SELECT ?, r.node,
		       COALESCE(NULLIF(n.address, ''), r.address), ?
		FROM replicas AS r
		LEFT JOIN nodes AS n ON n.node = r.node
		WHERE r.file_name = ?
	`, name, requestedAt, name)
	if err != nil {
		return err
	}

	// Update addresses of old pending tombstones where possible.
	_, err = tx.Exec(`
		UPDATE deletion_tombstones
		SET address = COALESCE(
			(SELECT nodes.address
			 FROM nodes
			 WHERE nodes.node = deletion_tombstones.node),
			address
		)
		WHERE file_name = ? AND address = ''
	`, name)
	if err != nil {
		return err
	}

	// Whole-file deletion supersedes selected-replica deletion and replication
	// jobs for this filename. Clear exclusions so a later upload starts clean.
	if _, err = tx.Exec(`DELETE FROM replication_queue WHERE file_name = ?`, name); err != nil {
		return err
	}
	if _, err = tx.Exec(`DELETE FROM replica_exclusions WHERE file_name = ?`, name); err != nil {
		return err
	}
	if _, err = tx.Exec(`DELETE FROM replica_deletion_queue WHERE file_name = ?`, name); err != nil {
		return err
	}

	// Deleting this row also removes replicas via the foreign key,
	// while tombstones remain until the physical deletions are done.
	_, err = tx.Exec(
		"DELETE FROM files WHERE name = ?", name,
	)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func pendingDeletionCount(name string) (int, error) {
	var count int

	err := db.QueryRow(`
		SELECT COUNT(*)
		FROM deletion_tombstones
		WHERE file_name = ?
	`, name).Scan(&count)

	return count, err
}

func processPendingDeletions() {
	deleteQueueMu.Lock()
	defer deleteQueueMu.Unlock()

	rows, err := db.Query(`
		SELECT file_name, node, COALESCE(address, '')
		FROM deletion_tombstones
		ORDER BY file_name, node
	`)
	if err != nil {
		log.Printf("Delete queue: cannot read requests: %v", err)
		return
	}

	var pending []PendingDelete

	for rows.Next() {
		var item PendingDelete
		if err := rows.Scan(
			&item.FileName, &item.Node, &item.Address,
		); err != nil {
			log.Printf("Delete queue: cannot read row: %v", err)
			_ = rows.Close()
			return
		}
		pending = append(pending, item)
	}

	if err := rows.Err(); err != nil {
		log.Printf("Delete queue: row error: %v", err)
		_ = rows.Close()
		return
	}
	_ = rows.Close()

	client := &http.Client{Timeout: 3 * time.Second}

	for _, item := range pending {
		if item.Address == "" {
			log.Printf(
				"Delete pending for %s on %s: no address available",
				item.FileName, item.Node,
			)
			continue
		}

		endpoint := baseURL(item.Address) +
			"/delete?name=" + url.QueryEscape(item.FileName)

		req, err := http.NewRequest(http.MethodDelete, endpoint, nil)
		if err != nil {
			log.Printf("Cannot form delete request for %s on %s: %v",
				item.FileName, item.Node, err)
			continue
		}

		resp, err := client.Do(req)
		if err != nil {
			log.Printf("Delete pending for %s on %s: %v",
				item.FileName, item.Node, err)
			continue
		}

		status := resp.StatusCode
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		_ = resp.Body.Close()

		// 404 means the requested file is already absent on that node.
		if (status >= 200 && status < 300) ||
			status == http.StatusNotFound {
			dbMu.Lock()
			_, err = db.Exec(`
				DELETE FROM deletion_tombstones
				WHERE file_name = ? AND node = ?
			`, item.FileName, item.Node)
			dbMu.Unlock()

			if err != nil {
				log.Printf("Cannot clear deletion request for %s on %s: %v",
					item.FileName, item.Node, err)
				continue
			}

			log.Printf("Delete confirmed: %s on %s",
				item.FileName, item.Node)
		} else {
			log.Printf(
				"Delete pending for %s on %s: HTTP %d: %s",
				item.FileName, item.Node, status,
				strings.TrimSpace(string(body)),
			)
		}
	}
}

func deletionRetryLoop() {
	log.Println("Deletion retry worker started")

	for {
		processPendingDeletions()
		time.Sleep(deletionRetryInterval)
	}
}

// DELETE /delete?name=filename
func deleteFileHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "Use DELETE",
			http.StatusMethodNotAllowed)
		return
	}

	name := r.URL.Query().Get("name")
	if !validFileName(name) {
		http.Error(w, "Invalid file name",
			http.StatusBadRequest)
		return
	}

	if err := requestWholeFileDelete(name); err != nil {
		http.Error(w, "Could not record deletion: "+err.Error(),
			http.StatusInternalServerError)
		return
	}

	// Attempt immediately; the worker retries anything that remains.
	processPendingDeletions()

	remaining, err := pendingDeletionCount(name)
	if err != nil {
		http.Error(w, "Deletion recorded but status lookup failed",
			http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")

	if remaining == 0 {
		fmt.Fprintf(w,
			"Deletion confirmed on all registered nodes for: %s\n",
			name)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	fmt.Fprintf(w,
		"Deletion queued for %s; %d node deletion(s) pending. "+
			"The server will retry automatically.\n",
		name, remaining,
	)
}

func main() {
	if err := initializeDatabase(); err != nil {
		log.Fatal("Database initialization failed: ", err)
	}
	defer db.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("/add", addFileHandler)
	mux.HandleFunc("/get", getFileHandler)
	mux.HandleFunc("/list", listFilesHandler)
	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/register-node", registerNodeHandler)
	mux.HandleFunc("/register-replica", registerReplicaHandler)
	mux.HandleFunc("/nodes", nodesHandler)
	mux.HandleFunc("/delete", deleteFileHandler)
	mux.HandleFunc("/queue-replication", queueReplicationHandler)
	mux.HandleFunc("/replication-queue", replicationQueueHandler)
	mux.HandleFunc("/queue-status", replicationQueueHandler)
	mux.HandleFunc("/delete-replica", deleteSelectedReplicaHandler)
	mux.HandleFunc("/replica-exclusions", replicaExclusionsHandler)

	go nodeMonitorLoop()
	go reconciliationLoop()
	go deletionRetryLoop()
	go replicationRetryLoop()
	go selectedReplicaDeletionRetryLoop()

	port := os.Getenv("METADATA_PORT")
	if port == "" {
		port = "9000"
	}

	log.Println("Metadata Server listening on port", port)
	log.Println("SQLite database:", configuredDatabasePath())

	log.Fatal(http.ListenAndServe(":"+port, mux))
}

// normalizeAddress makes node addresses comparable whether they are stored as
// localhost:8002 or supplied as http://localhost:8002/.
func normalizeAddress(address string) string {
	return strings.TrimRight(baseURL(strings.TrimSpace(address)), "/")
}

func isReplicaExcluded(fileName, node string) (bool, error) {
	var count int
	err := db.QueryRow(`SELECT COUNT(*) FROM replica_exclusions WHERE file_name = ? AND node = ?`, fileName, node).Scan(&count)
	return count > 0, err
}

func loadExcludedNodes(fileName string) (map[string]bool, error) {
	rows, err := db.Query(`SELECT node FROM replica_exclusions WHERE file_name = ?`, fileName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make(map[string]bool)
	for rows.Next() {
		var node string
		if err := rows.Scan(&node); err != nil {
			return nil, err
		}
		result[node] = true
	}
	return result, rows.Err()
}

// queueReplicationHandler durably records a replication job. The target address
// is resolved to a registered node so jobs remain associated with stable node IDs.
func queueReplicationHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		http.Error(w, "Use GET or POST", http.StatusMethodNotAllowed)
		return
	}
	q := r.URL.Query()
	name := q.Get("name")
	sourceNode := q.Get("source_node")
	sourceAddress := q.Get("source_address")
	targetAddress := q.Get("target_address")
	if !validFileName(name) || sourceNode == "" || sourceAddress == "" || targetAddress == "" {
		http.Error(w, "Missing or invalid replication job fields", http.StatusBadRequest)
		return
	}
	if normalizeAddress(sourceAddress) == normalizeAddress(targetAddress) {
		http.Error(w, "Source and target must be different nodes", http.StatusBadRequest)
		return
	}

	var exists int
	if err := db.QueryRow(`SELECT COUNT(*) FROM files WHERE name = ?`, name).Scan(&exists); err != nil {
		http.Error(w, "Cannot verify file metadata", http.StatusInternalServerError)
		return
	}
	if exists == 0 {
		http.Error(w, "File metadata not found", http.StatusNotFound)
		return
	}

	nodes, err := loadNodes()
	if err != nil {
		http.Error(w, "Cannot read registered nodes", http.StatusInternalServerError)
		return
	}
	targetNode := ""
	canonicalTargetAddress := targetAddress
	for _, node := range nodes {
		if normalizeAddress(node.Address) == normalizeAddress(targetAddress) {
			targetNode = node.Node
			canonicalTargetAddress = node.Address
			break
		}
	}
	if targetNode == "" {
		http.Error(w, "Target address is not a registered storage node", http.StatusNotFound)
		return
	}
	if targetNode == sourceNode {
		http.Error(w, "Source and target must be different nodes", http.StatusBadRequest)
		return
	}
	if excluded, err := isReplicaExcluded(name, targetNode); err != nil {
		http.Error(w, "Cannot check replica exclusion", http.StatusInternalServerError)
		return
	} else if excluded {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		fmt.Fprintf(w, "Replication skipped: %s is deliberately excluded for %s\n", targetNode, name)
		return
	}

	now := time.Now().Format(time.RFC3339)
	dbMu.Lock()
	_, err = db.Exec(`
		INSERT INTO replication_queue
			(file_name, source_node, source_address, target_node, target_address, attempts, last_error, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, 0, '', ?, ?)
		ON CONFLICT(file_name, target_node) DO UPDATE SET
			source_node = excluded.source_node,
			source_address = excluded.source_address,
			target_address = excluded.target_address,
			last_error = '',
			updated_at = excluded.updated_at
	`, name, sourceNode, sourceAddress, targetNode, canonicalTargetAddress, now, now)
	dbMu.Unlock()
	if err != nil {
		http.Error(w, "Cannot persist replication job", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusAccepted)
	fmt.Fprintf(w, "Replication job queued for %s on %s\n", name, targetNode)
}

func replicationQueueHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Use GET", http.StatusMethodNotAllowed)
		return
	}
	rows, err := db.Query(`
		SELECT id, file_name, source_node, source_address, target_node, target_address,
		       attempts, last_error, created_at, updated_at
		FROM replication_queue ORDER BY created_at, id
	`)
	if err != nil {
		http.Error(w, "Cannot read replication queue", http.StatusInternalServerError)
		return
	}
	defer rows.Close()
	jobs := []ReplicationJob{}
	for rows.Next() {
		var job ReplicationJob
		if err := rows.Scan(&job.ID, &job.FileName, &job.SourceNode, &job.SourceAddress,
			&job.TargetNode, &job.TargetAddress, &job.Attempts, &job.LastError,
			&job.CreatedAt, &job.UpdatedAt); err != nil {
			http.Error(w, "Cannot read replication job", http.StatusInternalServerError)
			return
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		http.Error(w, "Cannot finish reading replication queue", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{"pending": len(jobs), "jobs": jobs})
}

func updateReplicationJobError(id int64, message string) {
	dbMu.Lock()
	defer dbMu.Unlock()
	_, err := db.Exec(`UPDATE replication_queue SET attempts = attempts + 1, last_error = ?, updated_at = ? WHERE id = ?`, message, time.Now().Format(time.RFC3339), id)
	if err != nil {
		log.Printf("Cannot update replication job %d: %v", id, err)
	}
}

func deleteReplicationJob(id int64) {
	dbMu.Lock()
	defer dbMu.Unlock()
	if _, err := db.Exec(`DELETE FROM replication_queue WHERE id = ?`, id); err != nil {
		log.Printf("Cannot remove completed replication job %d: %v", id, err)
	}
}

func processOneReplicationJob(job ReplicationJob) {
	fileOperationMu.Lock()
	defer fileOperationMu.Unlock()

	if excluded, err := isReplicaExcluded(job.FileName, job.TargetNode); err != nil {
		updateReplicationJobError(job.ID, "cannot check exclusion: "+err.Error())
		return
	} else if excluded {
		deleteReplicationJob(job.ID)
		return
	}

	var file FileRecord
	err := db.QueryRow(`SELECT name, size, COALESCE(sha256, '') FROM files WHERE name = ?`, job.FileName).Scan(&file.Name, &file.Size, &file.SHA256)
	if err == sql.ErrNoRows {
		deleteReplicationJob(job.ID)
		return
	}
	if err != nil {
		updateReplicationJobError(job.ID, "cannot load file metadata: "+err.Error())
		return
	}
	if file.SHA256 == "" {
		updateReplicationJobError(job.ID, "canonical SHA-256 checksum is not established yet")
		return
	}

	nodes, err := loadNodes()
	if err != nil {
		updateReplicationJobError(job.ID, "cannot load nodes: "+err.Error())
		return
	}
	var target NodeRecord
	nodeByName := make(map[string]NodeRecord, len(nodes))
	for _, node := range nodes {
		nodeByName[node.Node] = node
		if node.Node == job.TargetNode {
			target = node
		}
	}
	if target.Node == "" {
		updateReplicationJobError(job.ID, "target node is no longer registered")
		return
	}
	if target.Status != "online" {
		updateReplicationJobError(job.ID, "target node is offline")
		return
	}

	// If a correct copy already exists, the queued job is complete.
	targetHash, targetSize, targetErr := hashRemoteFile(target.Address, file.Name)
	if targetErr == nil && targetHash == file.SHA256 && targetSize == file.Size {
		deleteReplicationJob(job.ID)
		return
	}

	// Choose any registered, healthy replica that matches the canonical checksum.
	// This lets a job continue even if its originally selected source goes offline.
	replicas, err := loadReplicas(file.Name)
	if err != nil {
		updateReplicationJobError(job.ID, "cannot load source replicas: "+err.Error())
		return
	}
	excludedNodes, err := loadExcludedNodes(file.Name)
	if err != nil {
		updateReplicationJobError(job.ID, "cannot load replica exclusions: "+err.Error())
		return
	}
	sources := make([]ReplicaInfo, 0, len(replicas))
	targetAlreadyRegistered := false
	for _, replica := range replicas {
		if replica.Node == target.Node {
			targetAlreadyRegistered = true
		}
		if replica.Node == target.Node || excludedNodes[replica.Node] {
			continue
		}
		node, ok := nodeByName[replica.Node]
		if !ok || node.Status != "online" {
			continue
		}
		hash, size, hashErr := hashRemoteFile(node.Address, file.Name)
		if hashErr == nil && hash == file.SHA256 && size == file.Size {
			sources = append(sources, ReplicaInfo{Node: node.Node, Address: node.Address, Path: replica.Path})
		}
	}
	if len(sources) == 0 {
		updateReplicationJobError(job.ID, "no online replica matches the canonical checksum")
		return
	}

	// There may be four registered storage nodes even though the configured
	// replication factor is three. Do not create an extra copy for a target
	// that is not already recorded as a replica when enough verified copies
	// already exist. If this target has a registered but corrupt/missing copy,
	// continue so the existing replica can be repaired.
	if !targetAlreadyRegistered && len(sources) >= desiredReplicationFactor {
		deleteReplicationJob(job.ID)
		log.Printf("Skipping extra replication for %s to %s: already have %d verified copies (target RF=%d)",
			file.Name, target.Node, len(sources), desiredReplicationFactor)
		return
	}

	sort.Slice(sources, func(i, j int) bool { return sources[i].Node < sources[j].Node })
	if err := copyReplica(file.Name, sources[0], target); err != nil {
		updateReplicationJobError(job.ID, err.Error())
		return
	}

	verifiedHash, verifiedSize, err := hashRemoteFile(target.Address, file.Name)
	if err != nil || verifiedHash != file.SHA256 || verifiedSize != file.Size {
		if err == nil {
			err = fmt.Errorf("target checksum/size mismatch after copy")
		}
		updateReplicationJobError(job.ID, "target verification failed: "+err.Error())
		return
	}
	deleteReplicationJob(job.ID)
	log.Printf("Replication complete: %s from %s to %s", file.Name, sources[0].Node, target.Node)
}

func processReplicationJobs() {
	rows, err := db.Query(`SELECT id, file_name, source_node, source_address, target_node, target_address, attempts, last_error, created_at, updated_at FROM replication_queue ORDER BY created_at, id`)
	if err != nil {
		log.Printf("Replication queue: cannot read jobs: %v", err)
		return
	}
	var jobs []ReplicationJob
	for rows.Next() {
		var job ReplicationJob
		if err := rows.Scan(&job.ID, &job.FileName, &job.SourceNode, &job.SourceAddress, &job.TargetNode, &job.TargetAddress, &job.Attempts, &job.LastError, &job.CreatedAt, &job.UpdatedAt); err != nil {
			log.Printf("Replication queue: cannot read job: %v", err)
			_ = rows.Close()
			return
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		log.Printf("Replication queue: row error: %v", err)
		_ = rows.Close()
		return
	}
	_ = rows.Close()
	for _, job := range jobs {
		processOneReplicationJob(job)
	}
}

func replicationRetryLoop() {
	log.Printf("Replication retry worker started; interval %s", replicationRetryInterval)
	for {
		processReplicationJobs()
		time.Sleep(replicationRetryInterval)
	}
}

// DELETE /delete-replica?name=filename&node=node2 removes only the selected
// replica, persists an exclusion, and retries physical deletion while the node
// is offline. It deliberately does not remove the canonical file metadata.
func deleteSelectedReplicaHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete && r.Method != http.MethodPost {
		http.Error(w, "Use DELETE or POST", http.StatusMethodNotAllowed)
		return
	}
	name := r.URL.Query().Get("name")
	nodeName := r.URL.Query().Get("node")
	if !validFileName(name) || nodeName == "" {
		http.Error(w, "Missing or invalid file name or node", http.StatusBadRequest)
		return
	}

	fileOperationMu.Lock()
	dbMu.Lock()
	var fileCount int
	err := db.QueryRow(`SELECT COUNT(*) FROM files WHERE name = ?`, name).Scan(&fileCount)
	if err != nil {
		dbMu.Unlock()
		fileOperationMu.Unlock()
		http.Error(w, "Cannot check file metadata", http.StatusInternalServerError)
		return
	}
	if fileCount == 0 {
		dbMu.Unlock()
		fileOperationMu.Unlock()
		http.Error(w, "File metadata not found", http.StatusNotFound)
		return
	}
	var address string
	err = db.QueryRow(`SELECT address FROM nodes WHERE node = ?`, nodeName).Scan(&address)
	if err != nil && err != sql.ErrNoRows {
		dbMu.Unlock()
		fileOperationMu.Unlock()
		http.Error(w, "Cannot read node metadata", http.StatusInternalServerError)
		return
	}
	var replicaAddress string
	err = db.QueryRow(`SELECT address FROM replicas WHERE file_name = ? AND node = ?`, name, nodeName).Scan(&replicaAddress)
	if err != nil && err != sql.ErrNoRows {
		dbMu.Unlock()
		fileOperationMu.Unlock()
		http.Error(w, "Cannot read replica metadata", http.StatusInternalServerError)
		return
	}
	if replicaAddress != "" {
		address = replicaAddress
	}
	if address == "" {
		dbMu.Unlock()
		fileOperationMu.Unlock()
		http.Error(w, "Node is not registered and has no known replica address", http.StatusNotFound)
		return
	}
	now := time.Now().Format(time.RFC3339)
	tx, err := db.Begin()
	if err != nil {
		dbMu.Unlock()
		fileOperationMu.Unlock()
		http.Error(w, "Cannot begin exclusion transaction", http.StatusInternalServerError)
		return
	}
	_, err = tx.Exec(`INSERT INTO replica_exclusions(file_name, node, reason, created_at) VALUES(?, ?, 'manually selected for deletion', ?) ON CONFLICT(file_name, node) DO UPDATE SET reason = excluded.reason, created_at = excluded.created_at`, name, nodeName, now)
	if err == nil {
		_, err = tx.Exec(`INSERT INTO replica_deletion_queue(file_name, node, address, requested_at) VALUES(?, ?, ?, ?) ON CONFLICT(file_name, node) DO UPDATE SET address = excluded.address, requested_at = excluded.requested_at`, name, nodeName, address, now)
	}
	if err == nil {
		_, err = tx.Exec(`DELETE FROM replicas WHERE file_name = ? AND node = ?`, name, nodeName)
	}
	if err == nil {
		_, err = tx.Exec(`DELETE FROM replication_queue WHERE file_name = ? AND target_node = ?`, name, nodeName)
	}
	if err == nil {
		err = tx.Commit()
	} else {
		_ = tx.Rollback()
	}
	dbMu.Unlock()
	fileOperationMu.Unlock()
	if err != nil {
		http.Error(w, "Could not record selected replica deletion: "+err.Error(), http.StatusInternalServerError)
		return
	}

	processPendingReplicaDeletions()
	var remaining int
	if err := db.QueryRow(`SELECT COUNT(*) FROM replica_deletion_queue WHERE file_name = ? AND node = ?`, name, nodeName).Scan(&remaining); err != nil {
		http.Error(w, "Deletion recorded but status lookup failed", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	if remaining == 0 {
		fmt.Fprintf(w, "Replica permanently removed from %s for %s; file metadata and other replicas remain.\n", nodeName, name)
		return
	}
	w.WriteHeader(http.StatusAccepted)
	fmt.Fprintf(w, "Replica exclusion recorded for %s on %s; physical deletion is queued and will retry automatically.\n", name, nodeName)
}

func processPendingReplicaDeletions() {
	rows, err := db.Query(`SELECT file_name, node, address FROM replica_deletion_queue ORDER BY requested_at`)
	if err != nil {
		log.Printf("Selected replica deletion queue: cannot read queue: %v", err)
		return
	}
	var pending []PendingReplicaDelete
	for rows.Next() {
		var item PendingReplicaDelete
		if err := rows.Scan(&item.FileName, &item.Node, &item.Address); err != nil {
			log.Printf("Selected replica deletion queue: cannot read row: %v", err)
			_ = rows.Close()
			return
		}
		pending = append(pending, item)
	}
	if err := rows.Err(); err != nil {
		log.Printf("Selected replica deletion queue: row error: %v", err)
		_ = rows.Close()
		return
	}
	_ = rows.Close()
	for _, item := range pending {
		replica := ReplicaInfo{Node: item.Node, Address: item.Address}
		if err := deleteRemoteReplica(item.FileName, replica); err != nil {
			log.Printf("Selected replica deletion pending for %s on %s: %v", item.FileName, item.Node, err)
			continue
		}
		dbMu.Lock()
		_, err := db.Exec(`DELETE FROM replica_deletion_queue WHERE file_name = ? AND node = ?`, item.FileName, item.Node)
		dbMu.Unlock()
		if err != nil {
			log.Printf("Cannot clear selected replica deletion for %s on %s: %v", item.FileName, item.Node, err)
		} else {
			log.Printf("Selected replica deletion confirmed: %s on %s", item.FileName, item.Node)
		}
	}
}

func selectedReplicaDeletionRetryLoop() {
	log.Println("Selected replica deletion retry worker started")
	for {
		processPendingReplicaDeletions()
		time.Sleep(deletionRetryInterval)
	}
}

func replicaExclusionsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Use GET", http.StatusMethodNotAllowed)
		return
	}
	name := r.URL.Query().Get("name")
	if name != "" && !validFileName(name) {
		http.Error(w, "Invalid file name", http.StatusBadRequest)
		return
	}
	query := `SELECT file_name, node, reason, created_at FROM replica_exclusions`
	args := []interface{}{}
	if name != "" {
		query += ` WHERE file_name = ?`
		args = append(args, name)
	}
	query += ` ORDER BY file_name, node`
	rows, err := db.Query(query, args...)
	if err != nil {
		http.Error(w, "Cannot read replica exclusions", http.StatusInternalServerError)
		return
	}
	defer rows.Close()
	type Exclusion struct {
		FileName  string `json:"file_name"`
		Node      string `json:"node"`
		Reason    string `json:"reason"`
		CreatedAt string `json:"created_at"`
	}
	items := []Exclusion{}
	for rows.Next() {
		var item Exclusion
		if err := rows.Scan(&item.FileName, &item.Node, &item.Reason, &item.CreatedAt); err != nil {
			http.Error(w, "Cannot read replica exclusion", http.StatusInternalServerError)
			return
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		http.Error(w, "Cannot finish reading replica exclusions", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(items)
}
