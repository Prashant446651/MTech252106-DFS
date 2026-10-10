package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

var storageDir string
var port string
var nodeName string
var metadataURL string

func validFileName(name string) bool {
	return name != "" &&
		filepath.Base(name) == name &&
		name != "." &&
		name != ".." &&
		!strings.ContainsAny(name, `/\`)
}

func registerMetadata(fileName string, fileSize int64) error {
	absolutePath, err := filepath.Abs(
		filepath.Join(storageDir, fileName),
	)
	if err != nil {
		return err
	}

	params := url.Values{}
	params.Set("name", fileName)
	params.Set("node", nodeName)
	params.Set("address", "localhost:"+port)
	params.Set("path", absolutePath)
	params.Set("size", strconv.FormatInt(fileSize, 10))

	resp, err := http.Get(metadataURL + "/add?" + params.Encode())
	if err != nil {
		return fmt.Errorf("metadata server unavailable: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("metadata registration failed: %s",
			strings.TrimSpace(string(body)))
	}

	return nil
}

func uploadHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Use POST to upload", http.StatusMethodNotAllowed)
		return
	}

	fileName := r.URL.Query().Get("name")
	if !validFileName(fileName) {
		http.Error(w, "Invalid file name", http.StatusBadRequest)
		return
	}

	if err := os.MkdirAll(storageDir, 0755); err != nil {
		http.Error(w, "Cannot create storage directory",
			http.StatusInternalServerError)
		return
	}

	filePath := filepath.Join(storageDir, fileName)
	file, err := os.Create(filePath)
	if err != nil {
		http.Error(w, "Cannot create file: "+err.Error(),
			http.StatusInternalServerError)
		return
	}

	fileSize, copyErr := io.Copy(file, r.Body)
	closeErr := file.Close()

	if copyErr != nil {
		http.Error(w, "Cannot save file: "+copyErr.Error(),
			http.StatusInternalServerError)
		return
	}
	if closeErr != nil {
		http.Error(w, "Cannot close file: "+closeErr.Error(),
			http.StatusInternalServerError)
		return
	}

	// Register the file after it has been saved.
	if err := registerMetadata(fileName, fileSize); err != nil {
		http.Error(w,
			"File saved, but metadata registration failed: "+err.Error(),
			http.StatusBadGateway)
		return
	}

	fmt.Fprintf(w, "File uploaded and metadata registered: %s\n", fileName)
}

func downloadHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Use GET to download", http.StatusMethodNotAllowed)
		return
	}

	fileName := r.URL.Query().Get("name")
	if !validFileName(fileName) {
		http.Error(w, "Invalid file name", http.StatusBadRequest)
		return
	}

	filePath := filepath.Join(storageDir, fileName)
	info, err := os.Stat(filePath)
	if err != nil || !info.Mode().IsRegular() {
		http.Error(w, "File not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Disposition",
		`attachment; filename="`+fileName+`"`)
	http.ServeFile(w, r, filePath)
}

func deleteHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "Use DELETE to delete", http.StatusMethodNotAllowed)
		return
	}

	fileName := r.URL.Query().Get("name")
	if !validFileName(fileName) {
		http.Error(w, "Invalid file name", http.StatusBadRequest)
		return
	}

	filePath := filepath.Join(storageDir, fileName)
	if err := os.Remove(filePath); err != nil {
		http.Error(w, "Cannot delete file: "+err.Error(),
			http.StatusNotFound)
		return
	}

	// Metadata cleanup is not yet implemented.
	fmt.Fprintf(w, "File deleted from %s: %s\n", nodeName, fileName)
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Storage node %s is healthy\n", nodeName)
}

func main() {
	port = "8001"
	nodeName = "node1"
	metadataURL = "http://localhost:9000"

	for i := 1; i < len(os.Args); i++ {
		if i+1 >= len(os.Args) {
			log.Fatal("Missing value for option: ", os.Args[i])
		}

		switch os.Args[i] {
		case "--port":
			port = os.Args[i+1]
			i++
		case "--dir":
			storageDir = os.Args[i+1]
			i++
		case "--node":
			nodeName = os.Args[i+1]
			i++
		case "--metadata":
			metadataURL = strings.TrimRight(os.Args[i+1], "/")
			i++
		default:
			log.Fatal("Unknown option: ", os.Args[i])
		}
	}

	if storageDir == "" {
		storageDir = filepath.Join("./data", nodeName)
	}

	if err := os.MkdirAll(storageDir, 0755); err != nil {
		log.Fatal("Cannot create storage directory: ", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/upload", uploadHandler)
	mux.HandleFunc("/download", downloadHandler)
	mux.HandleFunc("/delete", deleteHandler)
	mux.HandleFunc("/health", healthHandler)

	log.Println("Storage Node:", nodeName)
	log.Println("Port:", port)
	log.Println("Storage directory:", storageDir)
	log.Println("Metadata Server:", metadataURL)

	log.Fatal(http.ListenAndServe(":"+port, mux))
}
