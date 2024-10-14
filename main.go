package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type DataStore struct {
	data map[string]string
	mu   sync.RWMutex
}

var store = DataStore{
	data: make(map[string]string),
}

func parseCSV(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("could not open CSV file: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	if _, err := reader.Read(); err != nil {
		return fmt.Errorf("could not read header row: %v", err)
	}

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("could not read CSV file: %v", err)
		}
		if len(record) < 2 {
			continue
		}
		store.mu.Lock()
		store.data[record[0]] = record[1]
		store.mu.Unlock()
	}
	return nil
}

func predictionHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	file, header, err := r.FormFile("inputFile")
	if err != nil {
		http.Error(w, "Failed to read input file", http.StatusBadRequest)
		return
	}
	defer file.Close()

	filename := strings.TrimSuffix(header.Filename, filepath.Ext(header.Filename))

	store.mu.RLock()
	result, exists := store.data[filename]
	store.mu.RUnlock()
	if !exists {
		http.Error(w, fmt.Sprintf("No prediction found for file: %s", filename), http.StatusNotFound)
		return
	}

	fmt.Fprintf(w, "%s:%s", filename, result)
}

func main() {
	csvFile := "./Classification Results on Face Dataset (1000 images).csv"
	err := parseCSV(csvFile)
	if err != nil {
		log.Fatalf("Failed to parse CSV: %v", err)
	}

	http.HandleFunc("/", predictionHandler)

	fmt.Println("Server started at :8000")
	log.Fatal(http.ListenAndServe(":8000", nil))
}