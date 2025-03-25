package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// LoadBloomFilterFromFile loads a BloomFilter from the specified file
func LoadBloomFilterFromFile(filePath string) (*BloomFilter, error) {
	// Check if file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return nil, fmt.Errorf("블룸 필터 파일이 존재하지 않음: %s", filePath)
	}

	// Open file for reading
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("블룸 필터 파일 열기 실패: %w", err)
	}
	defer file.Close()

	// Read header
	var header struct {
		Size          uint64
		HashFunctions uint64
	}

	if err := binary.Read(file, binary.LittleEndian, &header); err != nil {
		return nil, fmt.Errorf("블룸 필터 헤더 읽기 실패: %w", err)
	}

	// Create new filter
	bf := &BloomFilter{
		size:          header.Size,
		hashFunctions: header.HashFunctions,
	}

	// Calculate array size
	arraySize := (header.Size + 63) / 64
	bf.bf = make([]uint64, arraySize)
	numShards := uint64(1024)
	if arraySize > 1000000 { // 매우 큰 블룸필터
		numShards = 4096
	} else if arraySize < 100000 { // 작은 블룸필터
		numShards = 256
	}
	bf.numShards = numShards
	bf.shardMutexes = make([]sync.Mutex, numShards)
	// Read filter data
	if err := binary.Read(file, binary.LittleEndian, bf.bf); err != nil {
		return nil, fmt.Errorf("블룸 필터 데이터 읽기 실패: %w", err)
	}

	fmt.Printf("블룸 필터 로드: 크기 %.2f GB, 해시 함수 %d개\n",
		float64(len(bf.bf)*8)/(1024*1024*1024), bf.hashFunctions)

	return bf, nil
}

// Helper function to load total BloomFilter
func loadTotalBloomFilter(dataDir string) (*BloomFilter, error) {
	filePath := filepath.Join(dataDir, "total.bin")
	bf, err := LoadBloomFilterFromFile(filePath)
	if err != nil {
		// If file doesn't exist, create a new BloomFilter
		fmt.Printf("토탈 블룸 필터 파일 없음, 새로 생성: %v\n", err)
		return NewBloomFilter(4_000_000_000, 0.005), nil
	}
	return bf, nil
}

// Helper function to load multi BloomFilter
func loadMultiBloomFilter(dataDir string) (*BloomFilter, error) {
	filePath := filepath.Join(dataDir, "multi.bin")
	bf, err := LoadBloomFilterFromFile(filePath)
	if err != nil {
		// If file doesn't exist, create a new BloomFilter
		fmt.Printf("멀티 블룸 필터 파일 없음, 새로 생성: %v\n", err)
		return NewBloomFilter(2_000_000_000, 0.005), nil
	}
	return bf, nil
}

// SaveToFile saves a BloomFilter to the specified file
func (bf *BloomFilter) SaveToFile(filePath string) error {
	println("블룸필터-SaveToFile")
	// Create directory if not exists
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("블룸 필터 디렉토리 생성 실패: %w", err)
	}

	// Open file for writing
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("블룸 필터 파일 생성 실패: %w", err)
	}
	defer file.Close()

	// Write header: size and hash functions count
	header := struct {
		Size          uint64
		HashFunctions uint64
	}{
		Size:          bf.size,
		HashFunctions: bf.hashFunctions,
	}

	if err := binary.Write(file, binary.LittleEndian, &header); err != nil {
		return fmt.Errorf("블룸 필터 헤더 쓰기 실패: %w", err)
	}

	// Write filter data
	if err := binary.Write(file, binary.LittleEndian, bf.bf); err != nil {
		return fmt.Errorf("블룸 필터 데이터 쓰기 실패: %w", err)
	}

	return nil
}

// SaveAllBloomFilters saves both BloomFilters to files
func (p *IncrementalUnionFindProcessor) SaveAllBloomFilters() error {
	if err := p.SaveTotalBloomFilter(); err != nil {
		return err
	}
	return p.SaveMultiBloomFilter()
}

// SaveTotalBloomFilter saves the total BloomFilter to a file
func (p *IncrementalUnionFindProcessor) SaveTotalBloomFilter() error {
	filePath := filepath.Join(p.dataDir, "total.bin")
	return p.totalBloomFilter.SaveToFile(filePath)
}

// SaveMultiBloomFilter saves the multi BloomFilter to a file
func (p *IncrementalUnionFindProcessor) SaveMultiBloomFilter() error {
	filePath := filepath.Join(p.dataDir, "multi.bin")
	return p.multiBloomFilter.SaveToFile(filePath)
}
