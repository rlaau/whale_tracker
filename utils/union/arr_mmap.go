package main

import (
	"encoding/binary"
	"fmt"
	"math/bits"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sync"
	"time"
	"unsafe"

	"github.com/edsrzf/mmap-go"
)

const (
	UinonSizeLimit = 120 // 집합 크기 제한
	// 배치 크기 및 설정
	BatchSize            = 200_000_000      // 각 배치당 2억 원소
	TotalBatches         = 10               // 총 10개 배치 (20억 원소)
	ElementSize          = 13               // 원소당 바이트 크기 (parent: 8, rank: 1, size: 1, padding: 3)
	BloomFilterSize      = 10 * 1024 * 1024 // 블룸 필터 크기 (10MB)
	BloomHashFunctions   = 7                // 블룸 필터 해시 함수 수
	ArchiveCheckInterval = 1000000          // 아카이브 확인 간격 (백만 개마다)
)

// 아카이브 정보 - 각 배치의 메타데이터
type ArchiveInfo struct {
	BatchID      int    // 배치 ID
	ElementSize  int    // 원소당 바이트 수
	Count        int    // 원소 수
	ParentFile   string // 부모 배열 파일
	RankFile     string // 랭크 배열 파일
	SizeFile     string // 크기 배열 파일
	SetCountFile string // 집합 정보 파일
	BloomFile    string // 블룸 필터 파일
}

// 메모리 매핑된 Union-Find 자료구조
type MMapUnionFind struct {
	count       int    // 서로소 집합 수
	batchID     int    // 현재 배치 ID
	elementSize int    // 원소당 바이트 수
	numElements int    // 원소 수
	dataDir     string // 데이터 디렉토리

	// 메모리 매핑 파일
	parentMMap mmap.MMap // 부모 배열
	rankMMap   mmap.MMap // 랭크 배열
	sizeMMap   mmap.MMap // 크기 배열

	// 슬라이스 뷰 (직접 접근용)
	parentSlice []uint64 // 부모 배열
	rankSlice   []uint8  // 랭크 배열
	sizeSlice   []uint8  // 크기 배열

	// 아카이브 정보
	bloomFilter []byte // 블룸 필터 (복합 집합 빠른 검색용)

	mu sync.RWMutex // 동시성 제어를 위한 뮤텍스
}

// 메모리 사용량 구조체
type MemoryStats struct {
	Alloc      uint64
	TotalAlloc uint64
	Sys        uint64
	NumGC      uint32
	TimeSince  time.Duration
}

// 메모리 사용량 측정
func getMemoryStats(startTime time.Time) MemoryStats {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return MemoryStats{
		Alloc:      m.Alloc,
		TotalAlloc: m.TotalAlloc,
		Sys:        m.Sys,
		NumGC:      m.NumGC,
		TimeSince:  time.Since(startTime),
	}
}

// 메모리 사용량 출력
func printMemoryUsage(stage string, stats MemoryStats) {
	fmt.Printf("===== %s =====\n", stage)
	fmt.Printf("현재 할당: %.2f GB\n", float64(stats.Alloc)/1024/1024/1024)
	fmt.Printf("총 할당: %.2f GB\n", float64(stats.TotalAlloc)/1024/1024/1024)
	fmt.Printf("시스템 메모리: %.2f GB\n", float64(stats.Sys)/1024/1024/1024)
	fmt.Printf("GC 실행 횟수: %d\n", stats.NumGC)
	fmt.Printf("경과 시간: %v\n", stats.TimeSince)
	fmt.Println()
}

// 대용량 파일 생성
func createLargeFile(fileName string, size int64) (*os.File, error) {
	// 디렉토리 확인 및 생성
	dir := filepath.Dir(fileName)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("디렉토리 생성 실패: %v", err)
	}

	// 파일 생성
	f, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("파일 생성 실패: %v", err)
	}

	fmt.Printf("파일 생성 중: %s (%.2f GB)\n", fileName, float64(size)/1024/1024/1024)

	// 파일 크기 설정
	err = f.Truncate(size)
	if err != nil {
		// truncate 실패 시 sparse 파일로 대체
		fmt.Printf("Truncate 실패, sparse 파일로 시도: %v\n", err)
		_, seekErr := f.Seek(size-1, 0)
		if seekErr != nil {
			f.Close()
			return nil, seekErr
		}
		_, writeErr := f.Write([]byte{0})
		if writeErr != nil {
			f.Close()
			return nil, writeErr
		}
	}

	return f, nil
}

// 새 MMapUnionFind 생성
func NewMMapUnionFind(batchID int, numElements int, dataDir string) (*MMapUnionFind, error) {
	// 결과 구조체 초기화
	uf := &MMapUnionFind{
		count:       numElements,
		batchID:     batchID,
		elementSize: ElementSize,
		numElements: numElements,
		dataDir:     dataDir,
	}

	// 배치 디렉토리 경로
	batchDir := filepath.Join(dataDir, fmt.Sprintf("batch_%d", batchID))

	// 파일 경로
	parentFile := filepath.Join(batchDir, "parent.dat")
	rankFile := filepath.Join(batchDir, "rank.dat")
	sizeFile := filepath.Join(batchDir, "size.dat")
	bloomFile := filepath.Join(batchDir, "bloom.dat")

	// 파일 크기 계산
	parentSize := int64(numElements) * 8 // uint64 배열
	rankSize := int64(numElements)       // uint8 배열
	sizeSize := int64(numElements)       // uint8 배열

	// 1. parent.dat 파일 생성 및 매핑
	fmt.Println("부모 배열 파일 생성 중...")
	fParent, err := createLargeFile(parentFile, parentSize)
	if err != nil {
		return nil, err
	}
	parentMap, err := mmap.Map(fParent, mmap.RDWR, 0)
	if err != nil {
		fParent.Close()
		return nil, err
	}
	fParent.Close()
	uf.parentMMap = parentMap

	// 2. rank.dat 파일 생성 및 매핑
	fmt.Println("랭크 배열 파일 생성 중...")
	fRank, err := createLargeFile(rankFile, rankSize)
	if err != nil {
		uf.parentMMap.Unmap()
		return nil, err
	}
	rankMap, err := mmap.Map(fRank, mmap.RDWR, 0)
	if err != nil {
		fRank.Close()
		uf.parentMMap.Unmap()
		return nil, err
	}
	fRank.Close()
	uf.rankMMap = rankMap

	// 3. size.dat 파일 생성 및 매핑
	fmt.Println("크기 배열 파일 생성 중...")
	fSize, err := createLargeFile(sizeFile, sizeSize)
	if err != nil {
		uf.parentMMap.Unmap()
		uf.rankMMap.Unmap()
		return nil, err
	}
	sizeMap, err := mmap.Map(fSize, mmap.RDWR, 0)
	if err != nil {
		fSize.Close()
		uf.parentMMap.Unmap()
		uf.rankMMap.Unmap()
		return nil, err
	}
	fSize.Close()
	uf.sizeMMap = sizeMap

	// 4. 블룸 필터 파일 생성
	fmt.Println("블룸 필터 파일 생성 중...")
	uf.bloomFilter = make([]byte, BloomFilterSize)
	bloomData, err := os.Create(bloomFile)
	if err != nil {
		uf.parentMMap.Unmap()
		uf.rankMMap.Unmap()
		uf.sizeMMap.Unmap()
		return nil, err
	}
	if _, err := bloomData.Write(uf.bloomFilter); err != nil {
		bloomData.Close()
		uf.parentMMap.Unmap()
		uf.rankMMap.Unmap()
		uf.sizeMMap.Unmap()
		return nil, err
	}
	bloomData.Close()

	// MMap을 Go 슬라이스로 변환
	uf.convertMMapToSlices()

	// 배치 초기화
	fmt.Println("배치 초기화 중...")
	uf.initializeBatch()

	// 집합 수 정보 저장
	setCountFile := filepath.Join(batchDir, "setcount.dat")
	scFile, err := os.Create(setCountFile)
	if err != nil {
		return nil, err
	}
	binary.Write(scFile, binary.LittleEndian, int64(uf.count))
	scFile.Close()

	return uf, nil
}

// MMap을 Go 슬라이스로 변환
func (uf *MMapUnionFind) convertMMapToSlices() {
	// 부모 배열
	hdrParent := (*reflect.SliceHeader)(unsafe.Pointer(&uf.parentSlice))
	hdrParent.Data = uintptr(unsafe.Pointer(&uf.parentMMap[0]))
	hdrParent.Len = uf.numElements
	hdrParent.Cap = uf.numElements

	// 랭크 배열
	hdrRank := (*reflect.SliceHeader)(unsafe.Pointer(&uf.rankSlice))
	hdrRank.Data = uintptr(unsafe.Pointer(&uf.rankMMap[0]))
	hdrRank.Len = uf.numElements
	hdrRank.Cap = uf.numElements

	// 크기 배열
	hdrSize := (*reflect.SliceHeader)(unsafe.Pointer(&uf.sizeSlice))
	hdrSize.Data = uintptr(unsafe.Pointer(&uf.sizeMMap[0]))
	hdrSize.Len = uf.numElements
	hdrSize.Cap = uf.numElements
}

// 배치 초기화 (병렬)
func (uf *MMapUnionFind) initializeBatch() {
	// 병렬 처리 설정
	numWorkers := runtime.NumCPU()
	chunkSize := uf.numElements / numWorkers

	var wg sync.WaitGroup

	start := time.Now()
	fmt.Printf("병렬 초기화 시작: %d개 워커 사용\n", numWorkers)

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)

		go func(workerID int) {
			defer wg.Done()

			startIdx := workerID * chunkSize
			endIdx := (workerID + 1) * chunkSize
			if workerID == numWorkers-1 {
				endIdx = uf.numElements
			}

			for i := startIdx; i < endIdx; i++ {
				uf.parentSlice[i] = uint64(i)
				uf.rankSlice[i] = 0
				uf.sizeSlice[i] = 1

				// 진행 상황 출력
				if (i-startIdx+1)%(chunkSize/10) == 0 {
					fmt.Printf("워커 %d: %d/%d 원소 초기화 완료 (%.1f%%)\n",
						workerID, i-startIdx+1, endIdx-startIdx,
						float64(i-startIdx+1)/float64(endIdx-startIdx)*100)
				}
			}
		}(w)
	}

	wg.Wait()
	fmt.Printf("초기화 완료: %v\n", time.Since(start))
}

// 자원 해제
func (uf *MMapUnionFind) Close(deleteFiles bool) error {
	if err := uf.parentMMap.Unmap(); err != nil {
		return err
	}

	if err := uf.rankMMap.Unmap(); err != nil {
		return err
	}

	if err := uf.sizeMMap.Unmap(); err != nil {
		return err
	}

	if deleteFiles {
		batchDir := filepath.Join(uf.dataDir, fmt.Sprintf("batch_%d", uf.batchID))
		os.RemoveAll(batchDir)
	}

	return nil
}

// FindRoot: 원소의 루트를 찾는 함수 (경로 압축 적용)
func (uf *MMapUnionFind) FindRoot(x uint64) uint64 {
	// 범위 검사
	if int(x) >= uf.numElements {
		return x // 범위 벗어남
	}

	if uf.parentSlice[x] != x {
		uf.parentSlice[x] = uf.FindRoot(uf.parentSlice[x]) // 경로 압축
	}

	return uf.parentSlice[x]
}

// Union: 두 원소를 합치는 함수 (랭크 기반 최적화 적용)
func (uf *MMapUnionFind) Union(x, y uint64) bool {
	// 읽기 잠금 설정
	uf.mu.RLock()

	rootX := uf.FindRoot(x)
	rootY := uf.FindRoot(y)

	// 범위 확인
	if int(rootX) >= uf.numElements || int(rootY) >= uf.numElements {
		uf.mu.RUnlock()
		return false
	}

	// 이미 같은 집합에 속함
	if rootX == rootY {
		uf.mu.RUnlock()
		return false
	}

	// 읽기 잠금 해제
	uf.mu.RUnlock()

	// 쓰기 잠금 설정
	uf.mu.Lock()
	defer uf.mu.Unlock()

	// 랭크 재확인 (동시성 문제 방지)
	rootX = uf.FindRoot(x)
	rootY = uf.FindRoot(y)

	// 랭크에 따라 합치기 (작은 트리를 큰 트리 아래로)
	if uf.rankSlice[rootX] < uf.rankSlice[rootY] {
		uf.parentSlice[rootX] = rootY

		// 집합 크기 업데이트
		newSize := min(uf.sizeSlice[rootX]+uf.sizeSlice[rootY], UinonSizeLimit)
		uf.sizeSlice[rootY] = newSize

		// 블룸 필터에 복합 집합 추가
		if newSize >= 2 {
			uf.addToBloomFilter(rootY)
		}
	} else if uf.rankSlice[rootX] > uf.rankSlice[rootY] {
		uf.parentSlice[rootY] = rootX

		// 집합 크기 업데이트
		newSize := min(uf.sizeSlice[rootX]+uf.sizeSlice[rootY], UinonSizeLimit)
		uf.sizeSlice[rootX] = newSize

		// 블룸 필터에 복합 집합 추가
		if newSize >= 2 {
			uf.addToBloomFilter(rootX)
		}
	} else {
		uf.parentSlice[rootY] = rootX
		uf.rankSlice[rootX]++

		// 집합 크기 업데이트
		newSize := min(uf.sizeSlice[rootX]+uf.sizeSlice[rootY], UinonSizeLimit)
		uf.sizeSlice[rootX] = newSize

		// 블룸 필터에 복합 집합 추가
		if newSize >= 2 {
			uf.addToBloomFilter(rootX)
		}
	}

	// 서로소 집합 수 감소
	uf.count--
	return true
}

// 블룸 필터에 원소 추가
func (uf *MMapUnionFind) addToBloomFilter(x uint64) {
	for i := 0; i < BloomHashFunctions; i++ {
		// i를 시드로 하는 해시 함수
		h := hash(x, uint64(i))
		pos := h % uint64(len(uf.bloomFilter)*8) // 비트 위치
		bytePos := pos / 8
		bitPos := pos % 8

		// 해당 비트 설정
		uf.bloomFilter[bytePos] |= 1 << bitPos
	}
}

// 블룸 필터에서 원소 확인
func checkBloomFilter(bloomFilter []byte, x uint64) bool {
	for i := 0; i < BloomHashFunctions; i++ {
		// i를 시드로 하는 해시 함수
		h := hash(x, uint64(i))
		pos := h % uint64(len(bloomFilter)*8) // 비트 위치
		bytePos := pos / 8
		bitPos := pos % 8

		// 해당 비트가 설정되어 있지 않으면 없음
		if bloomFilter[bytePos]&(1<<bitPos) == 0 {
			return false
		}
	}

	// 모든 해시 함수에서 비트가 설정되어 있으면 있을 수 있음
	return true
}

// FNV-1a 해시 함수 (시드 변형)
func hash(x uint64, seed uint64) uint64 {
	// FNV-1a 해시 상수
	const prime = 1099511628211
	const offset = 14695981039346656037

	// 시드와 값을 XOR
	h := offset ^ seed

	// 8바이트 처리
	for i := 0; i < 8; i++ {
		h ^= (x >> (i * 8)) & 0xFF
		h *= prime
	}

	return h
}

// 로테이션 해싱 (단순하지만 분산 좋음)
func rotHash(x uint64, r int) uint64 {
	return bits.RotateLeft64(x, r) ^ x
}

// 아카이브 로드
func LoadArchive(dataDir string, batchID int) (*ArchiveInfo, error) {
	batchDir := filepath.Join(dataDir, fmt.Sprintf("batch_%d", batchID))
	setCountFile := filepath.Join(batchDir, "setcount.dat")

	// 집합 수 파일 확인
	if _, err := os.Stat(setCountFile); os.IsNotExist(err) {
		return nil, fmt.Errorf("배치 %d의 집합 수 파일이 없음", batchID)
	}

	// 집합 수 읽기
	f, err := os.Open(setCountFile)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var count int64
	binary.Read(f, binary.LittleEndian, &count)

	// 아카이브 정보 반환
	return &ArchiveInfo{
		BatchID:      batchID,
		ElementSize:  ElementSize,
		Count:        int(count),
		ParentFile:   filepath.Join(batchDir, "parent.dat"),
		RankFile:     filepath.Join(batchDir, "rank.dat"),
		SizeFile:     filepath.Join(batchDir, "size.dat"),
		SetCountFile: setCountFile,
		BloomFile:    filepath.Join(batchDir, "bloom.dat"),
	}, nil
}

// 아카이브 블룸 필터 로드
func LoadArchiveBloomFilter(archiveInfo *ArchiveInfo) ([]byte, error) {
	// 블룸 필터 파일 확인
	if _, err := os.Stat(archiveInfo.BloomFile); os.IsNotExist(err) {
		return nil, fmt.Errorf("배치 %d의 블룸 필터 파일이 없음", archiveInfo.BatchID)
	}

	// 블룸 필터 읽기
	bloomData, err := os.ReadFile(archiveInfo.BloomFile)
	if err != nil {
		return nil, err
	}

	return bloomData, nil
}

// 점증적 Union-Find 처리기
type IncrementalUnionFindProcessor struct {
	dataDir      string         // 데이터 디렉토리
	currentUF    *MMapUnionFind // 현재 활성 유니언 파인드
	batchCounter int            // 배치 카운터
	archives     []*ArchiveInfo // 모든 아카이브 정보
	bloomFilters [][]byte       // 각 아카이브의 블룸 필터
	mu           sync.RWMutex   // 동시성 제어를 위한 뮤텍스
	totalSets    int64          // 전체 서로소 집합 수
}

// 새 점증적 처리기 생성
func NewIncrementalProcessor(dataDir string) (*IncrementalUnionFindProcessor, error) {
	// 데이터 디렉토리 생성
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}

	return &IncrementalUnionFindProcessor{
		dataDir:      dataDir,
		batchCounter: 0,
		archives:     make([]*ArchiveInfo, 0),
		bloomFilters: make([][]byte, 0),
		totalSets:    0,
	}, nil
}

// 배치 시작 처리
func (p *IncrementalUnionFindProcessor) StartNewBatch() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// 이전 배치 종료
	if p.currentUF != nil {
		p.totalSets = int64(p.currentUF.count) // 현재 배치의 서로소 집합 수 저장

		// 이전 배치의 정보 저장
		archiveInfo := &ArchiveInfo{
			BatchID:      p.batchCounter - 1,
			ElementSize:  ElementSize,
			Count:        p.currentUF.count,
			ParentFile:   filepath.Join(p.dataDir, fmt.Sprintf("batch_%d", p.batchCounter-1), "parent.dat"),
			RankFile:     filepath.Join(p.dataDir, fmt.Sprintf("batch_%d", p.batchCounter-1), "rank.dat"),
			SizeFile:     filepath.Join(p.dataDir, fmt.Sprintf("batch_%d", p.batchCounter-1), "size.dat"),
			SetCountFile: filepath.Join(p.dataDir, fmt.Sprintf("batch_%d", p.batchCounter-1), "setcount.dat"),
			BloomFile:    filepath.Join(p.dataDir, fmt.Sprintf("batch_%d", p.batchCounter-1), "bloom.dat"),
		}

		p.archives = append(p.archives, archiveInfo)

		// 블룸 필터 저장
		p.bloomFilters = append(p.bloomFilters, p.currentUF.bloomFilter)

		// 이전 배치 자원 해제
		if err := p.currentUF.Close(false); err != nil {
			return err
		}

		// 메모리 정리 강제
		p.currentUF = nil
		runtime.GC()
	}

	// 새 배치 초기화
	uf, err := NewMMapUnionFind(p.batchCounter, BatchSize, p.dataDir)
	if err != nil {
		return err
	}

	p.currentUF = uf
	p.batchCounter++

	return nil
}

// 집합 처리
func (p *IncrementalUnionFindProcessor) ProcessSet(elements []uint64) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.currentUF == nil {
		return fmt.Errorf("배치가 초기화되지 않음")
	}

	// 단일 원소 집합 처리
	if len(elements) <= 1 {
		return nil // 병합 불필요
	}

	// 아카이브 확인 필요 여부 검사 - 블룸 필터 활용
	checkArchives := false
	batchOffset := BatchSize * (p.batchCounter - 1)

	for _, elem := range elements {
		// 현재 배치 범위를 벗어나는 원소가 있는지 확인
		if int(elem) >= batchOffset && int(elem) < batchOffset+BatchSize {
			continue // 현재 배치 범위 내 원소
		}

		// 가장 최근 아카이브만 확인 (요청한 "순서가 있는 병합" 구현)
		if len(p.bloomFilters) > 0 {
			latestBloomFilter := p.bloomFilters[len(p.bloomFilters)-1]

			if checkBloomFilter(latestBloomFilter, elem) {
				//복합 블룸필터로 체크 후, 아카이브 확인 필요 진단
				checkArchives = true
				break
			}
		}

		if checkArchives {
			break
		}
	}

	// 아카이브 확인이 필요하면 최신 아카이브만 참조
	if checkArchives && len(p.archives) > 0 {
		fmt.Println("최신 아카이브 확인: 이전 배치에 복합 집합 참조 가능성")
		// 참고: 실제 구현에서는 여기서 최신 아카이브를 로드하고 참조함
		// 이 예제에서는 간소화를 위해 생략
	}

	// 현재 배치 내 원소들만 있는 경우 간단히 처리
	// 원소 로컬 인덱스로 변환
	localElements := make([]uint64, 0, len(elements))
	for _, elem := range elements {
		// 현재 배치 범위 내 원소만 처리
		if int(elem) >= batchOffset && int(elem) < batchOffset+BatchSize {
			// 로컬 인덱스로 변환
			localElements = append(localElements, elem-uint64(batchOffset))
		}
	}

	// 병합 작업 수행
	if len(localElements) >= 2 {
		baseElem := localElements[0]
		for i := 1; i < len(localElements); i++ {
			p.currentUF.Union(baseElem, localElements[i])
		}
	}

	return nil
}

// 전체 유니언 파인드 상태 계산
func (p *IncrementalUnionFindProcessor) CalculateGlobalState() (int64, map[uint8]int64, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	disjointSets := int64(0)
	sizeDist := make(map[uint8]int64)

	// 1. 현재 배치의 상태 처리
	if p.currentUF != nil {
		disjointSets = int64(p.currentUF.count)

		// 크기 분포 샘플링 (전체의 0.1%)
		sampleInterval := 1000
		for i := 0; i < p.currentUF.numElements; i += sampleInterval {
			root := p.currentUF.FindRoot(uint64(i))
			size := p.currentUF.sizeSlice[root]
			sizeDist[size]++
		}
	}

	// 2. 아카이브 상태 합산
	for _, archive := range p.archives {
		// 서로소 집합 수 합산
		disjointSets += int64(archive.Count)
	}

	// 샘플링된 결과를 전체 비율로 확장
	for size, count := range sizeDist {
		sizeDist[size] = count * 1000 // 샘플링 간격에 비례하여 확장
	}

	return disjointSets, sizeDist, nil
}

// 테스트 배치 생성 (초기화 및 임의 병합)
func generateTestBatch(processor *IncrementalUnionFindProcessor, batchID int) error {
	fmt.Printf("\n===== 배치 %d 처리 시작 =====\n", batchID)
	startTime := time.Now()

	// 배치 시작
	if err := processor.StartNewBatch(); err != nil {
		return err
	}

	// 메모리 사용량 측정
	stats := getMemoryStats(startTime)
	printMemoryUsage("배치 초기화 후", stats)

	// 20%의 원소가 크기 2~4인 집합에 속하도록 설정
	numToMerge := int(float64(BatchSize) * 0.2)
	fmt.Printf("병합할 원소 수: %d (전체의 20%%)\n", numToMerge)

	unionStart := time.Now()

	// 원소들을 크기별로 할당 (2, 3, 4)
	targetSets := make(map[int][]uint64)
	for size := 2; size <= 4; size++ {
		targetSets[size] = make([]uint64, 0, numToMerge/3)
	}

	// 무작위로 원소를 크기별 집합에 할당
	usedElements := make(map[uint64]bool)
	for i := 0; i < numToMerge; i++ {
		// 미사용 원소 선택
		//var elem uint64
		for {
			elem := uint64(rand.Intn(BatchSize) + BatchSize*batchID)
			if !usedElements[elem] {
				usedElements[elem] = true
				targetSets[rand.Intn(3)+2] = append(targetSets[rand.Intn(3)+2], elem)
				break
			}
		}

		// 진행 상황 출력
		if (i+1)%(numToMerge/10) == 0 {
			fmt.Printf("원소 할당 진행률: %d/%d (%.1f%%)\n",
				i+1, numToMerge, float64(i+1)/float64(numToMerge)*100)
		}
	}

	// 병합 작업 수행
	mergedCount := 0
	for size, elems := range targetSets {
		fmt.Printf("크기 %d인 집합 생성 중...\n", size)

		numSets := len(elems) / size
		for i := 0; i < numSets; i++ {
			// 집합 구성
			setElems := make([]uint64, 0, size)
			for j := 0; j < size && i*size+j < len(elems); j++ {
				setElems = append(setElems, elems[i*size+j])
			}

			// 집합 처리
			if err := processor.ProcessSet(setElems); err != nil {
				fmt.Printf("집합 처리 오류: %v\n", err)
				continue
			}

			mergedCount += len(setElems) - 1

			// 진행 상황 출력
			if (i+1)%(numSets/10) == 0 && i > 0 {
				fmt.Printf("크기 %d인 집합 진행률: %d/%d (%.1f%%)\n",
					size, i+1, numSets, float64(i+1)/float64(numSets)*100)
				unionStats := getMemoryStats(startTime)
				printMemoryUsage(fmt.Sprintf("크기 %d 집합 진행중", size), unionStats)
			}
		}
	}

	unionDuration := time.Since(unionStart)
	fmt.Printf("Union 연산 완료: %v (병합 %d개)\n", unionDuration, mergedCount)

	unionStats := getMemoryStats(startTime)
	printMemoryUsage("Union 후", unionStats)

	return nil
}

func MergeArchives(processor *IncrementalUnionFindProcessor) error {
	return mergeArchives(processor)
}

// 아카이브 간 병합 처리
func mergeArchives(processor *IncrementalUnionFindProcessor) error {
	fmt.Println("\n===== 아카이브 간 병합 계산 =====")
	startTime := time.Now()

	// 전체 상태 계산
	disjointSets, sizeDist, err := processor.CalculateGlobalState()
	if err != nil {
		return err
	}

	// 결과 출력
	fmt.Printf("전체 서로소 집합 수: %d\n", disjointSets)
	fmt.Println("집합 크기 분포 (샘플링 기준):")
	for size := uint8(1); size <= UinonSizeLimit; size++ {
		count := sizeDist[size]
		totalPercentage := float64(count) / float64(TotalBatches*BatchSize) * 100
		fmt.Printf("크기 %d인 집합: %d개 (%.2f%%)\n", size, count, totalPercentage)
	}

	mergeDuration := time.Since(startTime)
	fmt.Printf("아카이브 병합 계산 완료: %v\n", mergeDuration)

	return nil
}

// func main() {
// 	// 시작 시간
// 	startTime := time.Now()

// 	// 초기 메모리 상태
// 	initialStats := getMemoryStats(startTime)
// 	printMemoryUsage("초기 상태", initialStats)

// 	// 데이터 디렉토리 설정
// 	dataDir := "unionfind_data"

// 	// 점증적 처리기 생성
// 	processor, err := NewIncrementalProcessor(dataDir)
// 	if err != nil {
// 		fmt.Printf("처리기 생성 실패: %v\n", err)
// 		return
// 	}

// 	// 20억 원소 처리 (10개 배치)
// 	for batchID := 0; batchID < TotalBatches; batchID++ {
// 		if err := generateTestBatch(processor, batchID); err != nil {
// 			fmt.Printf("배치 %d 처리 실패: %v\n", batchID, err)
// 			return
// 		}

// 		// 메모리 정리
// 		runtime.GC()
// 		fmt.Println("가비지 컬렉션 강제 실행")
// 		memStats := getMemoryStats(startTime)
// 		printMemoryUsage(fmt.Sprintf("배치 %d 완료 후", batchID), memStats)
// 	}

// 	// 아카이브 간 병합 계산
// 	if err := mergeArchives(processor); err != nil {
// 		fmt.Printf("아카이브 병합 실패: %v\n", err)
// 		return
// 	}

// 	// 전체 실행 시간 및 최종 메모리 상태
// 	totalDuration := time.Since(startTime)
// 	fmt.Printf("\n전체 실행 시간: %v\n", totalDuration)

// 	finalStats := getMemoryStats(startTime)
// 	printMemoryUsage("최종 상태", finalStats)
// }

// package main

// import (
// 	"encoding/binary"
// 	"fmt"
// 	"math/bits"
// 	"math/rand"
// 	"os"
// 	"path/filepath"
// 	"reflect"
// 	"runtime"
// 	"sync"
// 	"time"
// 	"unsafe"

// 	"github.com/edsrzf/mmap-go"
// )

// const (
// 	// 배치 크기 및 설정
// 	BatchSize            = 200_000_000      // 각 배치당 2억 원소
// 	ElementSize          = 13               // 원소당 바이트 크기 (parent: 8, rank: 1, size: 1, padding: 3)
// 	BloomFilterSize      = 10 * 1024 * 1024 // 블룸 필터 크기 (10MB)
// 	BloomHashFunctions   = 7                // 블룸 필터 해시 함수 수
// 	ArchiveCheckInterval = 1000000          // 아카이브 확인 간격 (백만 개마다)
// )

// // 아카이브 정보 - 각 배치의 메타데이터
// type ArchiveInfo struct {
// 	BatchID      int    // 배치 ID
// 	ElementSize  int    // 원소당 바이트 수
// 	Count        int    // 원소 수
// 	ParentFile   string // 부모 배열 파일
// 	RankFile     string // 랭크 배열 파일
// 	SizeFile     string // 크기 배열 파일
// 	SetCountFile string // 집합 정보 파일
// 	BloomFile    string // 블룸 필터 파일
// }

// // 메모리 매핑된 Union-Find 자료구조
// type MMapUnionFind struct {
// 	count       int    // 서로소 집합 수
// 	batchID     int    // 현재 배치 ID
// 	elementSize int    // 원소당 바이트 수
// 	numElements int    // 원소 수
// 	dataDir     string // 데이터 디렉토리

// 	// 메모리 매핑 파일
// 	parentMMap mmap.MMap // 부모 배열
// 	rankMMap   mmap.MMap // 랭크 배열
// 	sizeMMap   mmap.MMap // 크기 배열

// 	// 슬라이스 뷰 (직접 접근용)
// 	parentSlice []uint64 // 부모 배열
// 	rankSlice   []uint8  // 랭크 배열
// 	sizeSlice   []uint8  // 크기 배열

// 	// 아카이브 정보
// 	archives    []*ArchiveInfo // 이전 아카이브 정보
// 	bloomFilter []byte         // 블룸 필터 (복합 집합 빠른 검색용)

// 	mu sync.RWMutex // 동시성 제어를 위한 뮤텍스
// }

// // 메모리 사용량 구조체
// type MemoryStats struct {
// 	Alloc      uint64
// 	TotalAlloc uint64
// 	Sys        uint64
// 	NumGC      uint32
// 	TimeSince  time.Duration
// }

// // 메모리 사용량 측정
// func getMemoryStats(startTime time.Time) MemoryStats {
// 	var m runtime.MemStats
// 	runtime.ReadMemStats(&m)
// 	return MemoryStats{
// 		Alloc:      m.Alloc,
// 		TotalAlloc: m.TotalAlloc,
// 		Sys:        m.Sys,
// 		NumGC:      m.NumGC,
// 		TimeSince:  time.Since(startTime),
// 	}
// }

// // 메모리 사용량 출력
// func printMemoryUsage(stage string, stats MemoryStats) {
// 	fmt.Printf("===== %s =====\n", stage)
// 	fmt.Printf("현재 할당: %.2f GB\n", float64(stats.Alloc)/1024/1024/1024)
// 	fmt.Printf("총 할당: %.2f GB\n", float64(stats.TotalAlloc)/1024/1024/1024)
// 	fmt.Printf("시스템 메모리: %.2f GB\n", float64(stats.Sys)/1024/1024/1024)
// 	fmt.Printf("GC 실행 횟수: %d\n", stats.NumGC)
// 	fmt.Printf("경과 시간: %v\n", stats.TimeSince)
// 	fmt.Println()
// }

// // 대용량 파일 생성
// func createLargeFile(fileName string, size int64) (*os.File, error) {
// 	// 디렉토리 확인 및 생성
// 	dir := filepath.Dir(fileName)
// 	if err := os.MkdirAll(dir, 0755); err != nil {
// 		return nil, fmt.Errorf("디렉토리 생성 실패: %v", err)
// 	}

// 	// 파일 생성
// 	f, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0644)
// 	if err != nil {
// 		return nil, fmt.Errorf("파일 생성 실패: %v", err)
// 	}

// 	fmt.Printf("파일 생성 중: %s (%.2f GB)\n", fileName, float64(size)/1024/1024/1024)

// 	// 파일 크기 설정
// 	err = f.Truncate(size)
// 	if err != nil {
// 		// truncate 실패 시 sparse 파일로 대체
// 		fmt.Printf("Truncate 실패, sparse 파일로 시도: %v\n", err)
// 		_, seekErr := f.Seek(size-1, 0)
// 		if seekErr != nil {
// 			f.Close()
// 			return nil, seekErr
// 		}
// 		_, writeErr := f.Write([]byte{0})
// 		if writeErr != nil {
// 			f.Close()
// 			return nil, writeErr
// 		}
// 	}

// 	return f, nil
// }

// // 새 MMapUnionFind 생성
// func NewMMapUnionFind(batchID int, numElements int, dataDir string) (*MMapUnionFind, error) {
// 	// 결과 구조체 초기화
// 	uf := &MMapUnionFind{
// 		count:       numElements,
// 		batchID:     batchID,
// 		elementSize: ElementSize,
// 		numElements: numElements,
// 		dataDir:     dataDir,
// 	}

// 	// 배치 디렉토리 경로
// 	batchDir := filepath.Join(dataDir, fmt.Sprintf("batch_%d", batchID))

// 	// 파일 경로
// 	parentFile := filepath.Join(batchDir, "parent.dat")
// 	rankFile := filepath.Join(batchDir, "rank.dat")
// 	sizeFile := filepath.Join(batchDir, "size.dat")
// 	bloomFile := filepath.Join(batchDir, "bloom.dat")

// 	// 파일 크기 계산
// 	parentSize := int64(numElements) * 8 // uint64 배열
// 	rankSize := int64(numElements)       // uint8 배열
// 	sizeSize := int64(numElements)       // uint8 배열

// 	// 1. parent.dat 파일 생성 및 매핑
// 	fmt.Println("부모 배열 파일 생성 중...")
// 	fParent, err := createLargeFile(parentFile, parentSize)
// 	if err != nil {
// 		return nil, err
// 	}
// 	parentMap, err := mmap.Map(fParent, mmap.RDWR, 0)
// 	if err != nil {
// 		fParent.Close()
// 		return nil, err
// 	}
// 	fParent.Close()
// 	uf.parentMMap = parentMap

// 	// 2. rank.dat 파일 생성 및 매핑
// 	fmt.Println("랭크 배열 파일 생성 중...")
// 	fRank, err := createLargeFile(rankFile, rankSize)
// 	if err != nil {
// 		uf.parentMMap.Unmap()
// 		return nil, err
// 	}
// 	rankMap, err := mmap.Map(fRank, mmap.RDWR, 0)
// 	if err != nil {
// 		fRank.Close()
// 		uf.parentMMap.Unmap()
// 		return nil, err
// 	}
// 	fRank.Close()
// 	uf.rankMMap = rankMap

// 	// 3. size.dat 파일 생성 및 매핑
// 	fmt.Println("크기 배열 파일 생성 중...")
// 	fSize, err := createLargeFile(sizeFile, sizeSize)
// 	if err != nil {
// 		uf.parentMMap.Unmap()
// 		uf.rankMMap.Unmap()
// 		return nil, err
// 	}
// 	sizeMap, err := mmap.Map(fSize, mmap.RDWR, 0)
// 	if err != nil {
// 		fSize.Close()
// 		uf.parentMMap.Unmap()
// 		uf.rankMMap.Unmap()
// 		return nil, err
// 	}
// 	fSize.Close()
// 	uf.sizeMMap = sizeMap

// 	// 4. 블룸 필터 파일 생성
// 	fmt.Println("블룸 필터 파일 생성 중...")
// 	uf.bloomFilter = make([]byte, BloomFilterSize)
// 	bloomData, err := os.Create(bloomFile)
// 	if err != nil {
// 		uf.parentMMap.Unmap()
// 		uf.rankMMap.Unmap()
// 		uf.sizeMMap.Unmap()
// 		return nil, err
// 	}
// 	if _, err := bloomData.Write(uf.bloomFilter); err != nil {
// 		bloomData.Close()
// 		uf.parentMMap.Unmap()
// 		uf.rankMMap.Unmap()
// 		uf.sizeMMap.Unmap()
// 		return nil, err
// 	}
// 	bloomData.Close()

// 	// MMap을 Go 슬라이스로 변환
// 	uf.convertMMapToSlices()

// 	// 배치 초기화
// 	fmt.Println("배치 초기화 중...")
// 	uf.initializeBatch()

// 	// 아카이브 정보 저장
// 	archiveInfo := &ArchiveInfo{
// 		BatchID:      batchID,
// 		ElementSize:  ElementSize,
// 		Count:        numElements,
// 		ParentFile:   parentFile,
// 		RankFile:     rankFile,
// 		SizeFile:     sizeFile,
// 		SetCountFile: filepath.Join(batchDir, "setcount.dat"),
// 		BloomFile:    bloomFile,
// 	}

// 	// 집합 수 정보 저장
// 	setCountFile, err := os.Create(archiveInfo.SetCountFile)
// 	if err != nil {
// 		return nil, err
// 	}
// 	binary.Write(setCountFile, binary.LittleEndian, int64(uf.count))
// 	setCountFile.Close()

// 	uf.archives = append(uf.archives, archiveInfo)

// 	return uf, nil
// }

// // MMap을 Go 슬라이스로 변환
// func (uf *MMapUnionFind) convertMMapToSlices() {
// 	// 부모 배열
// 	hdrParent := (*reflect.SliceHeader)(unsafe.Pointer(&uf.parentSlice))
// 	hdrParent.Data = uintptr(unsafe.Pointer(&uf.parentMMap[0]))
// 	hdrParent.Len = uf.numElements
// 	hdrParent.Cap = uf.numElements

// 	// 랭크 배열
// 	hdrRank := (*reflect.SliceHeader)(unsafe.Pointer(&uf.rankSlice))
// 	hdrRank.Data = uintptr(unsafe.Pointer(&uf.rankMMap[0]))
// 	hdrRank.Len = uf.numElements
// 	hdrRank.Cap = uf.numElements

// 	// 크기 배열
// 	hdrSize := (*reflect.SliceHeader)(unsafe.Pointer(&uf.sizeSlice))
// 	hdrSize.Data = uintptr(unsafe.Pointer(&uf.sizeMMap[0]))
// 	hdrSize.Len = uf.numElements
// 	hdrSize.Cap = uf.numElements
// }

// // 배치 초기화 (병렬)
// func (uf *MMapUnionFind) initializeBatch() {
// 	// 병렬 처리 설정
// 	numWorkers := runtime.NumCPU()
// 	chunkSize := uf.numElements / numWorkers

// 	var wg sync.WaitGroup

// 	start := time.Now()
// 	fmt.Printf("병렬 초기화 시작: %d개 워커 사용\n", numWorkers)

// 	for w := 0; w < numWorkers; w++ {
// 		wg.Add(1)

// 		go func(workerID int) {
// 			defer wg.Done()

// 			startIdx := workerID * chunkSize
// 			endIdx := (workerID + 1) * chunkSize
// 			if workerID == numWorkers-1 {
// 				endIdx = uf.numElements
// 			}

// 			for i := startIdx; i < endIdx; i++ {
// 				uf.parentSlice[i] = uint64(i)
// 				uf.rankSlice[i] = 0
// 				uf.sizeSlice[i] = 1

// 				// 진행 상황 출력
// 				if (i-startIdx+1)%(chunkSize/10) == 0 {
// 					fmt.Printf("워커 %d: %d/%d 원소 초기화 완료 (%.1f%%)\n",
// 						workerID, i-startIdx+1, endIdx-startIdx,
// 						float64(i-startIdx+1)/float64(endIdx-startIdx)*100)
// 				}
// 			}
// 		}(w)
// 	}

// 	wg.Wait()
// 	fmt.Printf("초기화 완료: %v\n", time.Since(start))
// }

// // 자원 해제
// func (uf *MMapUnionFind) Close(deleteFiles bool) error {
// 	if err := uf.parentMMap.Unmap(); err != nil {
// 		return err
// 	}

// 	if err := uf.rankMMap.Unmap(); err != nil {
// 		return err
// 	}

// 	if err := uf.sizeMMap.Unmap(); err != nil {
// 		return err
// 	}

// 	if deleteFiles {
// 		batchDir := filepath.Join(uf.dataDir, fmt.Sprintf("batch_%d", uf.batchID))
// 		os.RemoveAll(batchDir)
// 	}

// 	return nil
// }

// // FindRoot: 원소의 루트를 찾는 함수 (경로 압축 적용)
// func (uf *MMapUnionFind) FindRoot(x uint64) uint64 {
// 	// 범위 검사
// 	if int(x) >= uf.numElements {
// 		return x // 범위 벗어남
// 	}

// 	if uf.parentSlice[x] != x {
// 		uf.parentSlice[x] = uf.FindRoot(uf.parentSlice[x]) // 경로 압축
// 	}

// 	return uf.parentSlice[x]
// }

// // Union: 두 원소를 합치는 함수 (랭크 기반 최적화 적용)
// func (uf *MMapUnionFind) Union(x, y uint64) bool {
// 	// 읽기 잠금 설정
// 	uf.mu.RLock()

// 	rootX := uf.FindRoot(x)
// 	rootY := uf.FindRoot(y)

// 	// 범위 확인
// 	if int(rootX) >= uf.numElements || int(rootY) >= uf.numElements {
// 		uf.mu.RUnlock()
// 		return false
// 	}

// 	// 이미 같은 집합에 속함
// 	if rootX == rootY {
// 		uf.mu.RUnlock()
// 		return false
// 	}

// 	// 읽기 잠금 해제
// 	uf.mu.RUnlock()

// 	// 쓰기 잠금 설정
// 	uf.mu.Lock()
// 	defer uf.mu.Unlock()

// 	// 랭크 재확인 (동시성 문제 방지)
// 	rootX = uf.FindRoot(x)
// 	rootY = uf.FindRoot(y)

// 	// 랭크에 따라 합치기 (작은 트리를 큰 트리 아래로)
// 	if uf.rankSlice[rootX] < uf.rankSlice[rootY] {
// 		uf.parentSlice[rootX] = rootY

// 		// 집합 크기 업데이트
// 		newSize := uf.sizeSlice[rootX] + uf.sizeSlice[rootY]
// 		if newSize > 4 {
// 			newSize = 4 // 최대 크기 제한
// 		}
// 		uf.sizeSlice[rootY] = newSize

// 		// 블룸 필터에 복합 집합 추가
// 		if newSize >= 2 {
// 			uf.addToBloomFilter(rootY)
// 		}
// 	} else if uf.rankSlice[rootX] > uf.rankSlice[rootY] {
// 		uf.parentSlice[rootY] = rootX

// 		// 집합 크기 업데이트
// 		newSize := uf.sizeSlice[rootX] + uf.sizeSlice[rootY]
// 		if newSize > 4 {
// 			newSize = 4 // 최대 크기 제한
// 		}
// 		uf.sizeSlice[rootX] = newSize

// 		// 블룸 필터에 복합 집합 추가
// 		if newSize >= 2 {
// 			uf.addToBloomFilter(rootX)
// 		}
// 	} else {
// 		uf.parentSlice[rootY] = rootX
// 		uf.rankSlice[rootX]++

// 		// 집합 크기 업데이트
// 		newSize := uf.sizeSlice[rootX] + uf.sizeSlice[rootY]
// 		if newSize > 4 {
// 			newSize = 4 // 최대 크기 제한
// 		}
// 		uf.sizeSlice[rootX] = newSize

// 		// 블룸 필터에 복합 집합 추가
// 		if newSize >= 2 {
// 			uf.addToBloomFilter(rootX)
// 		}
// 	}

// 	// 서로소 집합 수 감소
// 	uf.count--
// 	return true
// }

// // 블룸 필터에 원소 추가
// func (uf *MMapUnionFind) addToBloomFilter(x uint64) {
// 	for i := 0; i < BloomHashFunctions; i++ {
// 		// i를 시드로 하는 해시 함수
// 		h := hash(x, uint64(i))
// 		pos := h % uint64(len(uf.bloomFilter)*8) // 비트 위치
// 		bytePos := pos / 8
// 		bitPos := pos % 8

// 		// 해당 비트 설정
// 		uf.bloomFilter[bytePos] |= 1 << bitPos
// 	}
// }

// // 블룸 필터에서 원소 확인
// func (uf *MMapUnionFind) checkBloomFilter(x uint64) bool {
// 	for i := 0; i < BloomHashFunctions; i++ {
// 		// i를 시드로 하는 해시 함수
// 		h := hash(x, uint64(i))
// 		pos := h % uint64(len(uf.bloomFilter)*8) // 비트 위치
// 		bytePos := pos / 8
// 		bitPos := pos % 8

// 		// 해당 비트가 설정되어 있지 않으면 없음
// 		if uf.bloomFilter[bytePos]&(1<<bitPos) == 0 {
// 			return false
// 		}
// 	}

// 	// 모든 해시 함수에서 비트가 설정되어 있으면 있을 수 있음
// 	return true
// }

// // FNV-1a 해시 함수 (시드 변형)
// func hash(x uint64, seed uint64) uint64 {
// 	// FNV-1a 해시 상수
// 	const prime = 1099511628211
// 	const offset = 14695981039346656037

// 	// 시드와 값을 XOR
// 	h := offset ^ seed

// 	// 8바이트 처리
// 	for i := 0; i < 8; i++ {
// 		h ^= (x >> (i * 8)) & 0xFF
// 		h *= prime
// 	}

// 	return h
// }

// // 로테이션 해싱 (단순하지만 분산 좋음)
// func rotHash(x uint64, r int) uint64 {
// 	return bits.RotateLeft64(x, r) ^ x
// }

// // 아카이브 로드
// func LoadArchive(dataDir string, batchID int) (*ArchiveInfo, error) {
// 	batchDir := filepath.Join(dataDir, fmt.Sprintf("batch_%d", batchID))
// 	setCountFile := filepath.Join(batchDir, "setcount.dat")

// 	// 집합 수 파일 확인
// 	if _, err := os.Stat(setCountFile); os.IsNotExist(err) {
// 		return nil, fmt.Errorf("배치 %d의 집합 수 파일이 없음", batchID)
// 	}

// 	// 집합 수 읽기
// 	f, err := os.Open(setCountFile)
// 	if err != nil {
// 		return nil, err
// 	}
// 	defer f.Close()

// 	var count int64
// 	binary.Read(f, binary.LittleEndian, &count)

// 	// 아카이브 정보 반환
// 	return &ArchiveInfo{
// 		BatchID:      batchID,
// 		ElementSize:  ElementSize,
// 		Count:        int(count),
// 		ParentFile:   filepath.Join(batchDir, "parent.dat"),
// 		RankFile:     filepath.Join(batchDir, "rank.dat"),
// 		SizeFile:     filepath.Join(batchDir, "size.dat"),
// 		SetCountFile: setCountFile,
// 		BloomFile:    filepath.Join(batchDir, "bloom.dat"),
// 	}, nil
// }

// // 아카이브 블룸 필터 로드
// func LoadArchiveBloomFilter(archiveInfo *ArchiveInfo) ([]byte, error) {
// 	// 블룸 필터 파일 확인
// 	if _, err := os.Stat(archiveInfo.BloomFile); os.IsNotExist(err) {
// 		return nil, fmt.Errorf("배치 %d의 블룸 필터 파일이 없음", archiveInfo.BatchID)
// 	}

// 	// 블룸 필터 읽기
// 	bloomData, err := os.ReadFile(archiveInfo.BloomFile)
// 	if err != nil {
// 		return nil, err
// 	}

// 	return bloomData, nil
// }

// // 점증적 Union-Find 처리기
// type IncrementalUnionFindProcessor struct {
// 	dataDir      string         // 데이터 디렉토리
// 	currentUF    *MMapUnionFind // 현재 활성 유니언 파인드
// 	batchCounter int            // 배치 카운터
// 	archives     []*ArchiveInfo // 모든 아카이브 정보
// 	bloomFilters [][]byte       // 각 아카이브의 블룸 필터
// 	mu           sync.RWMutex   // 동시성 제어를 위한 뮤텍스
// }

// // 새 점증적 처리기 생성
// func NewIncrementalProcessor(dataDir string) (*IncrementalUnionFindProcessor, error) {
// 	// 데이터 디렉토리 생성
// 	if err := os.MkdirAll(dataDir, 0755); err != nil {
// 		return nil, err
// 	}

// 	return &IncrementalUnionFindProcessor{
// 		dataDir:      dataDir,
// 		batchCounter: 0,
// 		archives:     make([]*ArchiveInfo, 0),
// 		bloomFilters: make([][]byte, 0),
// 	}, nil
// }

// // 배치 시작 처리
// func (p *IncrementalUnionFindProcessor) StartNewBatch() error {
// 	p.mu.Lock()
// 	defer p.mu.Unlock()

// 	// 이전 배치 종료
// 	if p.currentUF != nil {
// 		// 이전 배치의 정보 저장
// 		archiveInfo, err := LoadArchive(p.dataDir, p.batchCounter-1)
// 		if err != nil {
// 			return err
// 		}

// 		p.archives = append(p.archives, archiveInfo)

// 		// 블룸 필터 로드
// 		bloomFilter, err := LoadArchiveBloomFilter(archiveInfo)
// 		if err != nil {
// 			return err
// 		}

// 		p.bloomFilters = append(p.bloomFilters, bloomFilter)

// 		// 이전 배치 자원 해제
// 		if err := p.currentUF.Close(false); err != nil {
// 			return err
// 		}
// 	}

// 	// 새 배치 초기화
// 	uf, err := NewMMapUnionFind(p.batchCounter, BatchSize, p.dataDir)
// 	if err != nil {
// 		return err
// 	}

// 	p.currentUF = uf
// 	p.batchCounter++

// 	return nil
// }

// // 집합 처리
// func (p *IncrementalUnionFindProcessor) ProcessSet(elements []uint64) error {
// 	p.mu.RLock()
// 	defer p.mu.RUnlock()

// 	if p.currentUF == nil {
// 		return fmt.Errorf("배치가 초기화되지 않음")
// 	}

// 	// 단일 원소 집합 처리
// 	if len(elements) <= 1 {
// 		return nil // 병합 불필요
// 	}

// 	// 아카이브 확인 필요 여부 검사 - 블룸 필터 활용
// 	checkArchives := false

// 	for _, elem := range elements {
// 		// 현재 배치 범위 내 원소인지 확인
// 		if int(elem) < BatchSize*p.batchCounter && int(elem) >= BatchSize*(p.batchCounter-1) {
// 			continue // 현재 배치 범위 내 원소
// 		}

// 		// 이전 배치에 속하는 원소가 있는지 확인
// 		for _, bloomFilter := range p.bloomFilters {
// 			// 블룸 필터에서 확인
// 			for j := 0; j < BloomHashFunctions; j++ {
// 				h := hash(elem, uint64(j))
// 				pos := h % uint64(len(bloomFilter)*8)
// 				bytePos := pos / 8
// 				bitPos := pos % 8

// 				if bloomFilter[bytePos]&(1<<bitPos) != 0 {
// 					checkArchives = true
// 					break
// 				}
// 			}

// 			if checkArchives {
// 				break
// 			}
// 		}

// 		if checkArchives {
// 			break
// 		}
// 	}

// 	// 아카이브 확인이 필요하면 복잡한 처리 필요
// 	if checkArchives {
// 		fmt.Println("아카이브 확인 필요: 이전 배치에 복합 집합 참조 가능성")
// 		// 아카이브 처리 로직 (향후 구현)
// 		// ...
// 		return nil
// 	}

// 	// 현재 배치 내 원소들만 있는 경우 간단히 처리
// 	baseElem := elements[0]
// 	for i := 1; i < len(elements); i++ {
// 		p.currentUF.Union(baseElem, elements[i])
// 	}

// 	return nil
// }

// // 전체 유니언 파인드 상태 계산
// func (p *IncrementalUnionFindProcessor) CalculateGlobalState() (int, map[uint8]int, error) {
// 	p.mu.RLock()
// 	defer p.mu.RUnlock()

// 	//totalElements := BatchSize * p.batchCounter
// 	disjointSets := 0
// 	sizeDist := make(map[uint8]int)

// 	// 1. 현재 배치의 상태 처리
// 	if p.currentUF != nil {
// 		disjointSets += p.currentUF.count

// 		// 크기 분포 샘플링 (전체의 1%)
// 		sampleInterval := 100
// 		for i := 0; i < p.currentUF.numElements; i += sampleInterval {
// 			root := p.currentUF.FindRoot(uint64(i))
// 			size := p.currentUF.sizeSlice[root]
// 			sizeDist[size]++
// 		}
// 	}

// 	// 2. 아카이브 상태 합산
// 	for _, archive := range p.archives {
// 		disjointSets += archive.Count

// 		// 아카이브의 크기 분포는 현재 샘플링된 비율로 추정
// 		// 실제 구현에서는 아카이브에 크기 분포도 저장할 수 있음
// 	}

// 	return disjointSets, sizeDist, nil
// }

// // 테스트 배치 생성 (초기화 및 임의 병합)
// func generateTestBatch(processor *IncrementalUnionFindProcessor) error {
// 	fmt.Println("테스트 배치 생성 시작...")
// 	startTime := time.Now()

// 	// 배치 시작
// 	if err := processor.StartNewBatch(); err != nil {
// 		return err
// 	}

// 	// 메모리 사용량 측정
// 	stats := getMemoryStats(startTime)
// 	printMemoryUsage("배치 초기화 후", stats)

// 	// 20%의 원소가 크기 2~4인 집합에 속하도록 설정
// 	numToMerge := int(float64(BatchSize) * 0.2)
// 	fmt.Printf("병합할 원소 수: %d (전체의 20%%)\n", numToMerge)

// 	unionStart := time.Now()

// 	// 원소들을 크기별로 할당 (2, 3, 4)
// 	targetSets := make(map[int][]uint64)
// 	for size := 2; size <= 4; size++ {
// 		targetSets[size] = make([]uint64, 0, numToMerge/3)
// 	}

// 	// 무작위로 원소를 크기별 집합에 할당
// 	usedElements := make(map[uint64]bool)
// 	for i := 0; i < numToMerge; i++ {
// 		// 미사용 원소 선택
// 		var elem uint64
// 		for {
// 			elem = uint64(rand.Intn(BatchSize))
// 			if !usedElements[elem] {
// 				usedElements[elem] = true
// 				break
// 			}
// 		}

// 		// 2, 3, 4 중 하나 선택
// 		size := rand.Intn(3) + 2
// 		targetSets[size] = append(targetSets[size], elem)

// 		// 진행 상황 출력
// 		if (i+1)%(numToMerge/10) == 0 {
// 			fmt.Printf("원소 할당 진행률: %d/%d (%.1f%%)\n",
// 				i+1, numToMerge, float64(i+1)/float64(numToMerge)*100)
// 		}
// 	}

// 	// 병합 작업 수행
// 	mergedCount := 0
// 	for size, elems := range targetSets {
// 		fmt.Printf("크기 %d인 집합 생성 중...\n", size)

// 		numSets := len(elems) / size
// 		for i := 0; i < numSets; i++ {
// 			// 집합 구성
// 			setElems := make([]uint64, 0, size)
// 			for j := 0; j < size && i*size+j < len(elems); j++ {
// 				setElems = append(setElems, elems[i*size+j])
// 			}

// 			// 집합 처리
// 			if err := processor.ProcessSet(setElems); err != nil {
// 				fmt.Printf("집합 처리 오류: %v\n", err)
// 				continue
// 			}

// 			mergedCount += len(setElems) - 1

// 			// 진행 상황 출력
// 			if (i+1)%(numSets/10) == 0 && i > 0 {
// 				fmt.Printf("크기 %d인 집합 진행률: %d/%d (%.1f%%)\n",
// 					size, i+1, numSets, float64(i+1)/float64(numSets)*100)
// 				unionStats := getMemoryStats(startTime)
// 				printMemoryUsage(fmt.Sprintf("크기 %d 집합 진행중", size), unionStats)
// 			}
// 		}
// 	}

// 	unionDuration := time.Since(unionStart)
// 	fmt.Printf("Union 연산 완료: %v (병합 %d개)\n", unionDuration, mergedCount)

// 	unionStats := getMemoryStats(startTime)
// 	printMemoryUsage("Union 후", unionStats)

// 	return nil
// }

// // 아카이브 간 병합 처리
// func mergeArchives(processor *IncrementalUnionFindProcessor) error {
// 	fmt.Println("아카이브 간 병합 처리 시작...")
// 	startTime := time.Now()

// 	// 현재는 간단한 구현으로, 전체 상태 계산만 수행
// 	disjointSets, sizeDist, err := processor.CalculateGlobalState()
// 	if err != nil {
// 		return err
// 	}

// 	// 결과 출력
// 	fmt.Printf("전체 서로소 집합 수: %d\n", disjointSets)
// 	fmt.Println("집합 크기 분포 (샘플링 기준):")
// 	for size := uint8(1); size <= 4; size++ {
// 		count := sizeDist[size]
// 		fmt.Printf("크기 %d인 집합: %d개\n", size, count)
// 	}

// 	mergeDuration := time.Since(startTime)
// 	fmt.Printf("아카이브 병합 계산 완료: %v\n", mergeDuration)

// 	return nil
// }

// func main() {
// 	// 시작 시간
// 	startTime := time.Now()

// 	// 초기 메모리 상태
// 	initialStats := getMemoryStats(startTime)
// 	printMemoryUsage("초기 상태", initialStats)

// 	// 데이터 디렉토리 설정
// 	dataDir := "unionfind_data"

// 	// 점증적 처리기 생성
// 	processor, err := NewIncrementalProcessor(dataDir)
// 	if err != nil {
// 		fmt.Printf("처리기 생성 실패: %v\n", err)
// 		return
// 	}

// 	// 배치 1 처리
// 	fmt.Println("\n===== 배치 1 처리 시작 =====")
// 	if err := generateTestBatch(processor); err != nil {
// 		fmt.Printf("배치 1 처리 실패: %v\n", err)
// 		return
// 	}

// 	// 배치 2 처리
// 	fmt.Println("\n===== 배치 2 처리 시작 =====")
// 	if err := generateTestBatch(processor); err != nil {
// 		fmt.Printf("배치 2 처리 실패: %v\n", err)
// 		return
// 	}

// 	// 배치 3 처리
// 	fmt.Println("\n===== 배치 3 처리 시작 =====")
// 	if err := generateTestBatch(processor); err != nil {
// 		fmt.Printf("배치 3 처리 실패: %v\n", err)
// 		return
// 	}

// 	// 아카이브 간 병합 계산
// 	fmt.Println("\n===== 아카이브 간 병합 계산 =====")
// 	if err := mergeArchives(processor); err != nil {
// 		fmt.Printf("아카이브 병합 실패: %v\n", err)
// 		return
// 	}

// 	// 전체 실행 시간 및 최종 메모리 상태
// 	totalDuration := time.Since(startTime)
// 	fmt.Printf("\n전체 실행 시간: %v\n", totalDuration)

// 	finalStats := getMemoryStats(startTime)
// 	printMemoryUsage("최종 상태", finalStats)
// }
