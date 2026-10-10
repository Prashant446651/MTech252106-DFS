package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"

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

	// SQLite can safely handle our small project workload.
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
	`)
	if err != nil {
		return err
	}

	return db.Ping()
}

// Add or update metadata when a storage node uploads a file.
// Compatible with the Storage Node's GET /add request.
func addFileHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		http.Error(w, "Use GET or POST", http.StatusMethodNotAllowed)
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
		http.Error(w, "Invalid file size", http.StatusBadRequest)
		return
	}

	dbMu.Lock()
	defer dbMu.Unlock()

	tx, err := db.Begin()
	if err != nil {
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}
	defer tx.Rollback()

	_, err = tx.Exec(`
		INSERT INTO files(name, size)
		VALUES(?, ?)
		ON CONFLICT(name) DO UPDATE SET size = excluded.size
	`, name, size)
	if err != nil {
		http.Error(w, "Cannot save file metadata",
			http.StatusInternalServerError)
		return
	}

	// A file can have one replica per node.
	// Re-uploading on the same node updates that node's record.
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

	fmt.Fprintf(w, "Metadata registered for %s on %s\n", name, node)
}

func getFileHandler(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	if name == "" {
		http.Error(w, "Missing file name", http.StatusBadRequest)
		return
	}

	var result FileMetadata

	err := db.QueryRow(
		"SELECT name, size FROM files WHERE name = ?", name,
	).Scan(&result.Name, &result.Size)

	if err == sql.ErrNoRows {
		http.Error(w, "File metadata not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, "Database error", http.StatusInternalServerError)
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
	json.NewEncoder(w).Encode(result)
}

func listFilesHandler(w http.ResponseWriter, r *http.Request) {
	rows, err := db.Query("SELECT name FROM files ORDER BY name")
	if err != nil {
		http.Error(w, "Database error", http.StatusInternalServerError)
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

func healthHandler(w http.ResponseWriter, r *http.Request) {
	if err := db.Ping(); err != nil {
		http.Error(w, "Metadata database unavailable",
			http.StatusInternalServerError)
		return
	}
	fmt.Fprintln(w, "Metadata server is healthy")
}

func main() {
	if err := initializeDatabase(); err != nil {
		log.Fatal("Database initialization failed: ", err)
	}
	defer db.Close()

	http.HandleFunc("/add", addFileHandler)
	http.HandleFunc("/get", getFileHandler)
	http.HandleFunc("/list", listFilesHandler)
	http.HandleFunc("/health", healthHandler)

	port := os.Getenv("METADATA_PORT")
	if port == "" {
		port = "9000"
	}

	log.Println("Metadata Server listening on port", port)
	log.Println("SQLite database: ./metadata.db")

	log.Fatal(http.ListenAndServe(":"+port, nil))
}
