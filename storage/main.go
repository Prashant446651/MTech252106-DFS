package main

import (
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
)

var storageDir string
var port string

func uploadHandler(w http.ResponseWriter, r *http.Request) {

	fileName := r.URL.Query().Get("name")

	if fileName == "" {
		http.Error(w, "File name is missing", http.StatusBadRequest)
		return
	}

	filePath := filepath.Join(storageDir, fileName)

	file, err := os.Create(filePath)
	if err != nil {
		http.Error(w, "Cannot create file: "+err.Error(), http.StatusInternalServerError)
		return
	}

	defer file.Close()

	_, err = io.Copy(file, r.Body)
	if err != nil {
		http.Error(w, "Cannot save file: "+err.Error(), http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, "File uploaded successfully: %s\n", fileName)
}

func downloadHandler(w http.ResponseWriter, r *http.Request) {

	fileName := r.URL.Query().Get("name")

	if fileName == "" {
		http.Error(w, "File name is missing", http.StatusBadRequest)
		return
	}

	filePath := filepath.Join(storageDir, fileName)

	if _, err := os.Stat(filePath); err != nil {
		http.Error(w, "File not found", http.StatusNotFound)
		return
	}

	http.ServeFile(w, r, filePath)
}

func deleteHandler(w http.ResponseWriter, r *http.Request) {

	fileName := r.URL.Query().Get("name")

	if fileName == "" {
		http.Error(w, "File name is missing", http.StatusBadRequest)
		return
	}

	filePath := filepath.Join(storageDir, fileName)

	if _, err := os.Stat(filePath); err != nil {
		http.Error(w, "File not found", http.StatusNotFound)
		return
	}

	err := os.Remove(filePath)
	if err != nil {
		http.Error(w, "Cannot delete file: "+err.Error(), http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, "File deleted successfully: %s\n", fileName)
}

func healthHandler(w http.ResponseWriter, r *http.Request) {

	fmt.Fprintln(w, "Storage node is healthy")
}

func main() {

	flag.StringVar(&port, "port", "8001", "Port for storage node")
	flag.StringVar(&storageDir, "dir", "./data/node1", "Storage directory")

	flag.Parse()

	err := os.MkdirAll(storageDir, 0755)
	if err != nil {
		fmt.Println("Cannot create storage directory:", err)
		return
	}

	http.HandleFunc("/upload", uploadHandler)
	http.HandleFunc("/download", downloadHandler)
	http.HandleFunc("/delete", deleteHandler)
	http.HandleFunc("/health", healthHandler)

	fmt.Println("Storage Node")
	fmt.Println("Port:", port)
	fmt.Println("Storage directory:", storageDir)

	err = http.ListenAndServe(":"+port, nil)

	if err != nil {
		fmt.Println("Server error:", err)
	}
}
