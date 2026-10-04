package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
)

type FileMetadata struct {
	Name     string   `json:"name"`
	Size     int64    `json:"size"`
	Replicas []string `json:"replicas"`
}

var metadata = make(map[string]FileMetadata)

var mutex sync.Mutex

func addFileHandler(w http.ResponseWriter, r *http.Request) {

	fileName := r.URL.Query().Get("name")
	node := r.URL.Query().Get("node")
	size := r.URL.Query().Get("size")

	if fileName == "" || node == "" {
		http.Error(w, "File name or node is missing", http.StatusBadRequest)
		return
	}

	var fileSize int64

	fmt.Sscanf(size, "%d", &fileSize)

	mutex.Lock()

	metadata[fileName] = FileMetadata{
		Name:     fileName,
		Size:     fileSize,
		Replicas: []string{node},
	}

	mutex.Unlock()

	fmt.Fprintf(w, "Metadata added for file: %s\n", fileName)
}

func getFileHandler(w http.ResponseWriter, r *http.Request) {

	fileName := r.URL.Query().Get("name")

	if fileName == "" {
		http.Error(w, "File name is missing", http.StatusBadRequest)
		return
	}

	mutex.Lock()

	file, exists := metadata[fileName]

	mutex.Unlock()

	if !exists {
		http.Error(w, "File metadata not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	json.NewEncoder(w).Encode(file)
}

func listFilesHandler(w http.ResponseWriter, r *http.Request) {

	mutex.Lock()

	defer mutex.Unlock()

	w.Header().Set("Content-Type", "application/json")

	json.NewEncoder(w).Encode(metadata)
}

func healthHandler(w http.ResponseWriter, r *http.Request) {

	fmt.Fprintln(w, "Metadata server is healthy")
}

func main() {

	http.HandleFunc("/add", addFileHandler)
	http.HandleFunc("/get", getFileHandler)
	http.HandleFunc("/list", listFilesHandler)
	http.HandleFunc("/health", healthHandler)

	fmt.Println("Metadata Server")
	fmt.Println("Port: 9000")

	err := http.ListenAndServe(":9000", nil)

	if err != nil {
		fmt.Println("Server error:", err)
	}
}
