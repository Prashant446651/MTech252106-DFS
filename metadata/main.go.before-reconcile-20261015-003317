package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

type ReplicaInfo struct {
	Node    string `json:"node"`
	Address string `json:"address"`
	Path    string `json:"path"`
}

type FileMetadata struct {
	Name     string        `json:"name"`
	Size     int64         `json:"size"`
	Replicas []ReplicaInfo `json:"replicas"`
}

type NodeStatus struct {
	Node      string `json:"node"`
	Address   string `json:"address"`
	Status    string `json:"status"`
	LastSeen  string `json:"last_seen,omitempty"`
	LastError string `json:"last_error,omitempty"`
}

type RegisteredNode struct {
	Node    string
	Address string
}

var (
	db   *sql.DB
	dbMu sync.Mutex
)

func initializeDatabase() error {
	var err error

	db, err = sql.Open("sqlite", "./metadata.db")
	if err != nil {
		return err
	}

	db.SetMaxOpenConns(1)

	_, err = db.Exec(`
		PRAGMA foreign_keys = ON;

		CREATE TABLE IF NOT EXISTS files (
			name TEXT PRIMARY KEY,
			size INTEGER NOT NULL
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
	`)
	if err != nil {
		return err
	}

	return db.Ping()
}

// Register a storage node when it starts.
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
			address = excluded.address
	`, node, address)
	if err != nil {
		http.Error(w, "Cannot register node",
			http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, "Node registered: %s\n", node)
}

// Register or update file and replica metadata.
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
	sizeText := q.Get("size")

	if name == "" || node == "" || address == "" || path == "" {
		http.Error(w, "Missing name, node, address or path",
			http.StatusBadRequest)
		return
	}

	size, err := strconv.ParseInt(sizeText, 10, 64)
	if err != nil || size < 0 {
		http.Error(w, "Invalid file size",
			http.StatusBadRequest)
		return
	}

	dbMu.Lock()
	defer dbMu.Unlock()

	tx, err := db.Begin()
	if err != nil {
		http.Error(w, "Database error",
			http.StatusInternalServerError)
		return
	}
	defer tx.Rollback()

	_, err = tx.Exec(`
		INSERT INTO files(name, size)
		VALUES(?, ?)
		ON CONFLICT(name) DO UPDATE SET
			size = excluded.size
	`, name, size)
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
		http.Error(w, "Cannot register storage node",
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

func getFileHandler(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	if name == "" {
		http.Error(w, "Missing file name",
			http.StatusBadRequest)
		return
	}

	var result FileMetadata

	err := db.QueryRow(
		"SELECT name, size FROM files WHERE name = ?", name,
	).Scan(&result.Name, &result.Size)

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
			&replica.Node,
			&replica.Address,
			&replica.Path,
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
	json.NewEncoder(w).Encode(result)
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
	json.NewEncoder(w).Encode(names)
}

// Return all registered nodes and their latest health status.
func nodesHandler(w http.ResponseWriter, r *http.Request) {
	rows, err := db.Query(`
		SELECT node, address, status,
		       COALESCE(last_seen, ''),
		       COALESCE(last_error, '')
		FROM nodes
		ORDER BY node
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
			&n.Node,
			&n.Address,
			&n.Status,
			&n.LastSeen,
			&n.LastError,
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
	json.NewEncoder(w).Encode(nodes)
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	if err := db.Ping(); err != nil {
		http.Error(w, "Metadata database unavailable",
			http.StatusInternalServerError)
		return
	}

	fmt.Fprintln(w, "Metadata server is healthy")
}

// Check a storage node's /health endpoint.
func checkNode(address string) error {
	endpoint := strings.TrimRight(address, "/")

	if !strings.HasPrefix(endpoint, "http://") &&
		!strings.HasPrefix(endpoint, "https://") {
		endpoint = "http://" + endpoint
	}

	endpoint += "/health"

	client := &http.Client{
		Timeout: 2 * time.Second,
	}

	resp, err := client.Get(endpoint)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Drain the response so the HTTP connection can be reused.
	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("health check returned %s", resp.Status)
	}

	return nil
}

// Save the latest health-check result in SQLite.
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
			log.Printf("Cannot update status for %s: %v",
				node, err)
		}
		return
	}

	_, err := db.Exec(`
		UPDATE nodes
		SET status = ?, last_error = ?
		WHERE node = ?
	`, status, errorMessage, node)

	if err != nil {
		log.Printf("Cannot update status for %s: %v",
			node, err)
	}
}

// Check every registered node.
func checkNodesOnce() {
	rows, err := db.Query(`
		SELECT node, address FROM nodes ORDER BY node
	`)
	if err != nil {
		log.Printf("Cannot load registered nodes: %v", err)
		return
	}

	var registered []RegisteredNode

	for rows.Next() {
		var n RegisteredNode
		if err := rows.Scan(&n.Node, &n.Address); err != nil {
			log.Printf("Cannot read node: %v", err)
			_ = rows.Close()
			return
		}
		registered = append(registered, n)
	}

	if err := rows.Err(); err != nil {
		log.Printf("Error reading registered nodes: %v", err)
	}
	_ = rows.Close()

	for _, n := range registered {
		err := checkNode(n.Address)
		if err != nil {
			updateNodeStatus(n.Node, "offline", err.Error())
		} else {
			updateNodeStatus(n.Node, "online", "")
		}
	}
}

func nodeMonitorLoop() {
	log.Println("Node monitor started; check interval: 5 seconds")

	for {
		checkNodesOnce()
		time.Sleep(5 * time.Second)
	}
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
	mux.HandleFunc("/nodes", nodesHandler)

	go nodeMonitorLoop()

	port := os.Getenv("METADATA_PORT")
	if port == "" {
		port = "9000"
	}

	log.Println("Metadata Server listening on port", port)
	log.Println("SQLite database: ./metadata.db")

	log.Fatal(http.ListenAndServe(":"+port, mux))
}
