package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

var storageDir string
var port string
var nodeName string
var metadataURL string
var peerURLs []string

func validFileName(name string) bool {
	return name != "" && filepath.Base(name) == name && name != "." && name != ".." && !strings.ContainsAny(name, `/\\`)
}

func hashFile(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	h := sha256.New()
	if _, err := io.Copy(h, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func metadataGET(endpoint string, params url.Values) error {
	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Get(strings.TrimRight(metadataURL, "/") + endpoint + "?" + params.Encode())
	if err != nil {
		return fmt.Errorf("metadata server unavailable: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("metadata request returned HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

func registerMetadata(fileName string, fileSize int64) error {
	filePath := filepath.Join(storageDir, fileName)
	absolutePath, err := filepath.Abs(filePath)
	if err != nil {
		return err
	}
	checksum, err := hashFile(filePath)
	if err != nil {
		return fmt.Errorf("cannot calculate file checksum: %w", err)
	}
	params := url.Values{}
	params.Set("name", fileName)
	params.Set("node", nodeName)
	params.Set("address", "localhost:"+port)
	params.Set("path", absolutePath)
	params.Set("size", strconv.FormatInt(fileSize, 10))
	params.Set("sha256", checksum)
	return metadataGET("/add", params)
}

// Register a recovered copy without changing the canonical file checksum.
func registerRecoveredReplica(fileName string, fileSize int64) error {
	filePath := filepath.Join(storageDir, fileName)
	absolutePath, err := filepath.Abs(filePath)
	if err != nil {
		return err
	}
	checksum, err := hashFile(filePath)
	if err != nil {
		return fmt.Errorf("cannot calculate recovered file checksum: %w", err)
	}
	params := url.Values{}
	params.Set("name", fileName)
	params.Set("node", nodeName)
	params.Set("address", "localhost:"+port)
	params.Set("path", absolutePath)
	params.Set("size", strconv.FormatInt(fileSize, 10))
	params.Set("sha256", checksum)
	return metadataGET("/register-replica", params)
}

func registerNodeWithMetadata() error {
	params := url.Values{}
	params.Set("node", nodeName)
	params.Set("address", "localhost:"+port)
	return metadataGET("/register-node", params)
}

// Keep peer-to-peer copy support available, but mark it as a recovery upload so
// the target does not fan the upload back out to its own peers.
func replicateToPeer(peerURL, fileName, filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()
	endpoint := strings.TrimRight(peerURL, "/") + "/upload?name=" + url.QueryEscape(fileName) + "&recovery=1"
	req, err := http.NewRequest(http.MethodPost, endpoint, file)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("cannot contact peer: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("peer returned HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

// Persist one replication job in metadata. The metadata server performs the
// verified copy and retries the job while the target or source is unavailable.
func queueReplication(peerURL, fileName string) error {
	params := url.Values{}
	params.Set("name", fileName)
	params.Set("source_node", nodeName)
	params.Set("source_address", "localhost:"+port)
	params.Set("target_address", strings.TrimRight(peerURL, "/"))
	return metadataGET("/queue-replication", params)
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
		http.Error(w, "Cannot create storage directory", http.StatusInternalServerError)
		return
	}

	filePath := filepath.Join(storageDir, fileName)
	// Write to a temporary file first so a failed request cannot leave a
	// partially written file at the final path.
	tempPath := filePath + ".uploading"
	file, err := os.Create(tempPath)
	if err != nil {
		http.Error(w, "Cannot create file: "+err.Error(), http.StatusInternalServerError)
		return
	}
	fileSize, copyErr := io.Copy(file, r.Body)
	closeErr := file.Close()
	if copyErr != nil {
		_ = os.Remove(tempPath)
		http.Error(w, "Cannot save file: "+copyErr.Error(), http.StatusInternalServerError)
		return
	}
	if closeErr != nil {
		_ = os.Remove(tempPath)
		http.Error(w, "Cannot close file: "+closeErr.Error(), http.StatusInternalServerError)
		return
	}
	if err := os.Rename(tempPath, filePath); err != nil {
		_ = os.Remove(tempPath)
		http.Error(w, "Cannot finalize uploaded file: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Recovery uploads register this copy only. They must not change the
	// canonical checksum and must not create a new fan-out of replication jobs.
	if r.URL.Query().Get("recovery") == "1" {
		if err := registerRecoveredReplica(fileName, fileSize); err != nil {
			http.Error(w, "File saved, but recovery registration failed: "+err.Error(), http.StatusBadGateway)
			return
		}
		fmt.Fprintf(w, "Replica recovered on %s: %s\n", nodeName, fileName)
		return
	}

	if err := registerMetadata(fileName, fileSize); err != nil {
		http.Error(w, "File saved locally, but metadata registration failed: "+err.Error(), http.StatusBadGateway)
		return
	}

	queued := 0
	for _, peerURL := range peerURLs {
		if err := queueReplication(peerURL, fileName); err != nil {
			http.Error(w, "File saved and registered, but replication job could not be persisted for "+peerURL+": "+err.Error(), http.StatusBadGateway)
			return
		}
		queued++
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	if queued > 0 {
		w.WriteHeader(http.StatusAccepted)
		fmt.Fprintf(w, "File uploaded on %s: %s. Persisted replication jobs for %d configured peer(s); check /replication-queue on metadata for progress.\n", nodeName, fileName, queued)
		return
	}
	fmt.Fprintf(w, "File uploaded successfully on %s: %s. No peers were configured.\n", nodeName, fileName)
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
	w.Header().Set("Content-Disposition", `attachment; filename="`+fileName+`"`)
	http.ServeFile(w, r, filePath)
}

func deleteHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "Use DELETE", http.StatusMethodNotAllowed)
		return
	}
	fileName := r.URL.Query().Get("name")
	if !validFileName(fileName) {
		http.Error(w, "Invalid file name", http.StatusBadRequest)
		return
	}
	filePath := filepath.Join(storageDir, fileName)
	if err := os.Remove(filePath); err != nil {
		if os.IsNotExist(err) {
			http.Error(w, "File not found", http.StatusNotFound)
			return
		}
		http.Error(w, "Cannot delete file: "+err.Error(), http.StatusInternalServerError)
		return
	}
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
		case "--peer":
			peerURLs = append(peerURLs, strings.TrimRight(os.Args[i+1], "/"))
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
	log.Println("Replication peers:", peerURLs)
	if err := registerNodeWithMetadata(); err != nil {
		log.Printf("WARNING: could not register node: %v", err)
	} else {
		log.Println("Registered with metadata server")
	}
	log.Fatal(http.ListenAndServe(":"+port, mux))
}
