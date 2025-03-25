package main

import (
	"os"

	"whale_tracker/repository"
)

func main() {

	projectID := os.Getenv("PUBLIC_PROJECT_ID")
	jsonPath := os.Getenv("PUBLIC_JSON_KEY_PATH")
	outputFile := os.Getenv("TX_OUTPUT_FILE")

	println(projectID, jsonPath, outputFile)
}

func saveTransactions(filename string, txs []repository.TxRaw) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	for _, tx := range txs {
		if _, err := f.Write(tx.From[:]); err != nil {
			return err
		}
		if _, err := f.Write(tx.To[:]); err != nil {
			return err
		}
	}
	return nil
}
