package main

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"cloud.google.com/go/storage"
	"github.com/edsrzf/mmap-go"
)

type TxRaw struct {
	From [20]byte
	To   [20]byte
}

func decodeHexAddress(addr string) [20]byte {
	var result [20]byte
	addr = strings.TrimSpace(strings.ToLower(addr))
	addr = strings.TrimPrefix(addr, "0x")

	if len(addr) != 40 {
		log.Printf("Invalid address length: %s", addr)
		return result
	}

	b, err := hex.DecodeString(addr)
	if err != nil {
		log.Printf("Invalid hex string: %s", addr)
		return result
	}

	copy(result[:], b)
	return result
}

func downloadFilesFromGCS(bucket, prefix, destFolder string) ([]string, error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	os.MkdirAll(destFolder, 0755)

	it := client.Bucket(bucket).Objects(ctx, &storage.Query{Prefix: prefix})
	var files []string
	var wg sync.WaitGroup
	var mu sync.Mutex

	for {
		objAttrs, err := it.Next()
		if err != nil {
			break
		}

		wg.Add(1)
		go func(objName string) {
			defer wg.Done()
			rc, err := client.Bucket(bucket).Object(objName).NewReader(ctx)
			if err != nil {
				log.Printf("Error opening file %s: %v", objName, err)
				return
			}
			defer rc.Close()

			localPath := filepath.Join(destFolder, filepath.Base(objName))
			outFile, err := os.Create(localPath)
			if err != nil {
				log.Printf("Error creating file %s: %v", objName, err)
				return
			}
			defer outFile.Close()

			if _, err := io.Copy(outFile, rc); err != nil {
				log.Printf("Error downloading %s: %v", objName, err)
			} else {
				log.Printf("✅ Downloaded %s", objName)
				mu.Lock()
				files = append(files, localPath)
				mu.Unlock()
			}
		}(objAttrs.Name)
	}

	wg.Wait()
	return files, nil
}

func loadTxsFromCSV(csvFile string) ([]TxRaw, error) {
	f, err := os.Open(csvFile)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	data, err := mmap.Map(f, mmap.RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer data.Unmap()

	lines := strings.Split(string(data), "\n")
	txs := make([]TxRaw, 0, len(lines))

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		cols := strings.Split(line, ",")
		if len(cols) != 2 {
			log.Printf("⚠️ 잘못된 CSV 행: %q", line)
			continue
		}
		from := strings.TrimSpace(cols[0])
		to := strings.TrimSpace(cols[1])
		tx := TxRaw{
			From: decodeHexAddress(from),
			To:   decodeHexAddress(to),
		}
		txs = append(txs, tx)
	}
	return txs, nil
}
func saveTxsToBinaryFile(filename string, txs []TxRaw) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	buf := make([]byte, 0, len(txs)*40)
	for _, tx := range txs {
		buf = append(buf, tx.From[:]...)
		buf = append(buf, tx.To[:]...)
	}

	_, err = f.Write(buf)
	return err
}

func saveDepositAddrs(filename string, addrs map[[20]byte]struct{}) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	for addr := range addrs {
		f.Write(addr[:])
	}
	return nil
}

func saveDualTxs(filename string, duals map[[20]byte][][20]byte) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	for deposit, users := range duals {
		binary.Write(f, binary.LittleEndian, uint32(len(users)))
		f.Write(deposit[:])
		for _, u := range users {
			f.Write(u[:])
		}
	}
	return nil
}
func getAllEthTxFiles(dataDir string) ([]string, error) {
	pattern := filepath.Join(dataDir, "eth_tx*.csv")
	return filepath.Glob(pattern)
}

// func main() {
// 	// bucket := "whale_tracker_tx_export"
// 	// prefix := "2023_09_to_12/"
// 	destFolder := "./data"
// 	outputBin := "./output/txs_2023_09_to_12.bin"
// 	depositFile := "./output/deposit_09_to_12.bin"
// 	dualFile := "./output/dual_09_to_12.bin"

// 	// files, err := downloadFilesFromGCS(bucket, prefix, destFolder)
// 	// if err != nil {
// 	// 	log.Fatalf("파일 다운로드 실패: %v", err)
// 	// }

// 	files, err := getAllEthTxFiles(destFolder)
// 	if err != nil {
// 		log.Fatalf("파일 로드 실패: %v", err)
// 	}
// 	var allTxs []TxRaw
// 	for _, file := range files {
// 		txs, err := loadTxsFromCSV(file)
// 		if err != nil {
// 			log.Printf("파일 로드 실패 (%s): %v", file, err)
// 			continue
// 		}
// 		allTxs = append(allTxs, txs...)
// 	}

// 	log.Printf("총 트랜잭션 로드 수: %d", len(allTxs))

// 	// Step 1: save raw
// 	os.MkdirAll("./output", 0755)
// 	if err := saveTxsToBinaryFile(outputBin, allTxs); err != nil {
// 		log.Fatalf("바이너리 저장 실패: %v", err)
// 	}
// 	// Step 2: extract depositSet
// 	depositSet := make(map[[20]byte]struct{})
// 	for _, tx := range allTxs {
// 		if primitives.IsCexAddress(tx.To) {
// 			if !primitives.IsCexAddress(tx.From) {
// 				depositSet[tx.From] = struct{}{}
// 			}
// 		}
// 	}
// 	saveDepositAddrs(depositFile, depositSet)
// 	log.Printf("✅ 디파짓 주소 수: %d", len(depositSet))

// 	duals := make(map[[20]byte][][20]byte)
// 	for _, tx := range allTxs {
// 		if _, ok := depositSet[tx.To]; ok {
// 			_, ok2 := depositSet[tx.From]
// 			if !primitives.IsCexAddress(tx.From) && !ok2 {
// 				duals[tx.To] = append(duals[tx.To], tx.From)
// 			}
// 		}
// 	}
// 	saveDualTxs(dualFile, duals)
// 	log.Printf("✅ 유저 매핑된 deposit 수: %d", len(duals))
// }
