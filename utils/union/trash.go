package main


import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"log"
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

//-------------------------------------------------------
// 상수 및 설정
//-------------------------------------------------------

// 데이터 크기 설정
const (
	TotalElements = 2_000_000_000              // 총 20억 원소
	NumBatches    = 10                         // 전체 배치 수 (10개)
	BatchSize     = TotalElements / NumBatches // 배치당 2억 원소

	// 각 배치를 청크로 분할 (메모리 관리용)
	NumChunksPerBatch = 10                            // 배치당 청크 수
	ChunkSize         = BatchSize / NumChunksPerBatch // 청크당 2천만 원소

	// 테스트 케이스 설정
	PairsPerChunk     = 1_000_000 // 청크당 백만 쌍
	MultiSetsPerChunk = 1_000_000 // 청크당 백만 다중집합

	// 알고리즘 최적화 설정
	MaxDepth        = 16    // 최대 재귀 깊이
	SamplingRatio   = 1000  // 샘플링 비율 (1/1000)
	CommitBatchSize = 10000 // 배치 처리 크기
)

// 파일 경로 템플릿
const (
	DataDir             = "unionfind_data"         // 데이터 디렉토리
	BatchDirTemplate    = "batch_%d"               // 배치 디렉토리
	ParentFileTemplate  = "parent_chunk_%d.dat"    // 부모 정보 파일
	RankFileTemplate    = "rank_chunk_%d.dat"      // 랭크 정보 파일
	RootFileTemplate    = "root_chunk_%d.dat"      // 루트 캐시 파일
	LinkFileTemplate    = "links_chunk_%d.dat"     // 청크 내 링크 파일
	BatchLinkTemplate   = "batch_links_%d_%d.dat"  // 배치 간 링크 파일
	MergeResultTemplate = "merge_result_%d_%d.dat" // 병합 결과 파일
	BatchInfoFile       = "batch_%d_info.dat"      // 배치 정보 파일
	GlobalRootMapFile   = "global_root_map.dat"    // 전역 루트 맵 파일
)

//-------------------------------------------------------
// 자료구조 정의
//-------------------------------------------------------

// MMapUnionFind: 메모리 매핑된 Union-Find 자료구조
type MMapUnionFind struct {
	n          int32 // 원소 수
	baseOffset int32 // 전역 오프셋
	batchID    int   // 배치 ID
	chunkID    int   // 청크 ID

	// 파일 경로
	parentFile string
	rankFile   string
	rootFile   string

	// 메모리 맵
	parentMMap mmap.MMap
	rankMMap   mmap.MMap
	rootMMap   mmap.MMap

	// 슬라이스 뷰 (메모리 맵의 Go 슬라이스 인터페이스)
	parentSlice []int32 // 부모 배열 (4 바이트/원소)
	rankSlice   []int8  // 랭크 배열 (1 바이트/원소)
	rootSlice   []int32 // 루트 캐시 배열 (4 바이트/원소)
}

// Link: 청크 간 또는 배치 간 연결 정보
type Link struct {
	SourceBatch int32 // 소스 배치 ID
	SourceChunk int32 // 소스 청크 ID
	SourceElem  int32 // 소스 원소 ID
	TargetBatch int32 // 대상 배치 ID
	TargetChunk int32 // 대상 청크 ID
	TargetElem  int32 // 대상 원소 ID
}

// BatchInfo: 배치 처리 정보
type BatchInfo struct {
	ID            int   // 배치 ID
	DisjointSets  int   // 서로소 집합 수
	Processed     bool  // 처리 완료 여부
	ElementOffset int64 // 원소 시작 오프셋
	ChunkCount    int   // 청크 수
}

// RootMappingEntry: 루트 매핑 정보
type RootMappingEntry struct {
	OriginalID int32 // 원래 원소 ID
	GlobalRoot int32 // 전역 루트 ID
}

//-------------------------------------------------------
// 유틸리티 함수
//-------------------------------------------------------

// 메모리 사용량 출력
func printMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("메모리 사용량: Alloc=%v MB, Sys=%v MB, NumGC=%v\n",
		m.Alloc/1024/1024, m.Sys/1024/1024, m.NumGC)
}

// createLargeFile: 지정된 크기의 파일 생성
func createLargeFile(fileName string, size int64) (*os.File, error) {
	// 디렉토리 확인 및 생성
	dir := filepath.Dir(fileName)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("디렉토리 생성 실패: %v", err)
	}

	// 파일 생성/오픈
	f, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("파일 생성 실패: %v", err)
	}

	fmt.Printf("파일 생성 중: %s (%.2f MB)\n", fileName, float64(size)/1024/1024)

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

//-------------------------------------------------------
// Union-Find 핵심 구현
//-------------------------------------------------------

// 대용량 파일 생성 함수
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
// NewMMapUnionFind: 새 MMapUnionFind 인스턴스 생성
func NewMMapUnionFind(n int32, batchID, chunkID int) (*MMapUnionFind, error) {
	// 배치 디렉토리 경로
	batchDir := filepath.Join(DataDir, fmt.Sprintf(BatchDirTemplate, batchID))

	// 청크 파일 경로
	parentFile := filepath.Join(batchDir, fmt.Sprintf(ParentFileTemplate, chunkID))
	rankFile := filepath.Join(batchDir, fmt.Sprintf(RankFileTemplate, chunkID))
	rootFile := filepath.Join(batchDir, fmt.Sprintf(RootFileTemplate, chunkID))

	// 전역 오프셋 계산 (원소의 전역 ID 계산용)
	baseOffset := int32(batchID*BatchSize + chunkID*ChunkSize)

	uf := &MMapUnionFind{
		n:          n,
		baseOffset: baseOffset,
		batchID:    batchID,
		chunkID:    chunkID,
		parentFile: parentFile,
		rankFile:   rankFile,
		rootFile:   rootFile,
	}

	fmt.Printf("[배치 %d, 청크 %d] 파일 생성 및 메모리 매핑 시작...\n", batchID, chunkID)

	// 디렉토리 생성
	if err := os.MkdirAll(batchDir, 0755); err != nil {
		return nil, fmt.Errorf("디렉토리 생성 실패: %v", err)
	}

	// 1. parent.dat (4 바이트/원소) - 각 원소의 부모 정보
	parentSize := int64(n) * 4
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

	// 2. rank.dat (1 바이트/원소) - 트리 깊이 정보
	rankSize := int64(n)
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

	// 3. root.dat (4 바이트/원소) - 루트 캐시
	rootSize := int64(n) * 4
	fRoot, err := createLargeFile(rootFile, rootSize)
	if err != nil {
		uf.parentMMap.Unmap()
		uf.rankMMap.Unmap()
		return nil, err
	}
	rootMap, err := mmap.Map(fRoot, mmap.RDWR, 0)
	if err != nil {
		fRoot.Close()
		uf.parentMMap.Unmap()
		uf.rankMMap.Unmap()
		return nil, err
	}
	fRoot.Close()
	uf.rootMMap = rootMap

	// 메모리 맵을 Go 슬라이스로 재해석
	{
		// 1. 부모 슬라이스 설정
		hdrParent := (*reflect.SliceHeader)(unsafe.Pointer(&uf.parentSlice))
		hdrParent.Data = uintptr(unsafe.Pointer(&uf.parentMMap[0]))
		hdrParent.Len = int(parentSize / 4)
		hdrParent.Cap = int(parentSize / 4)

		// 2. 랭크 슬라이스 설정
		hdrRank := (*reflect.SliceHeader)(unsafe.Pointer(&uf.rankSlice))
		hdrRank.Data = uintptr(unsafe.Pointer(&uf.rankMMap[0]))
		hdrRank.Len = int(rankSize)
		hdrRank.Cap = int(rankSize)

		// 3. 루트 슬라이스 설정
		hdrRoot := (*reflect.SliceHeader)(unsafe.Pointer(&uf.rootSlice))
		hdrRoot.Data = uintptr(unsafe.Pointer(&uf.rootMMap[0]))
		hdrRoot.Len = int(rootSize / 4)
		hdrRoot.Cap = int(rootSize / 4)
	}

	// 병렬 초기화
	fmt.Printf("[배치 %d, 청크 %d] 초기화 시작...\n", batchID, chunkID)
	numWorkers := runtime.NumCPU()
	workerChunkSize := n / int32(numWorkers)
	var wg sync.WaitGroup
	start := time.Now()

	// 각 워커가 청크 일부분 초기화
	for w := 0; w < numWorkers; w++ {
		startIdx := int32(w) * workerChunkSize
		endIdx := startIdx + workerChunkSize
		if w == numWorkers-1 {
			endIdx = n // 마지막 워커는 끝까지 처리
		}

		wg.Add(1)
		go func(s, e int32) {
			defer wg.Done()
			// 각 원소 초기화
			for i := s; i < e; i++ {
				uf.parentSlice[i] = i // 자기 자신이 부모
				uf.rankSlice[i] = 0   // 초기 랭크는 0
				uf.rootSlice[i] = -1  // 루트 캐시 초기값 (-1 = 미계산)

				// 진행 상황 주기적 출력
				if (i+1-s)%10000000 == 0 { // 천만 개마다 출력
					fmt.Printf("[배치 %d, 청크 %d] 초기화 진행: %d / %d 원소 처리\n",
						batchID, chunkID, i+1-s, e-s)
				}
			}
		}(startIdx, endIdx)
	}

	wg.Wait()
	fmt.Printf("[배치 %d, 청크 %d] 초기화 완료 (소요 시간: %v)\n",
		batchID, chunkID, time.Since(start))

	return uf, nil
}

// Close: MMapUnionFind 자원 해제
func (uf *MMapUnionFind) Close(deleteFiles bool) error {
	// 메모리 맵 해제
	if err := uf.parentMMap.Unmap(); err != nil {
		return err
	}
	if err := uf.rankMMap.Unmap(); err != nil {
		return err
	}
	if err := uf.rootMMap.Unmap(); err != nil {
		return err
	}

	// 파일 삭제 (옵션)
	if deleteFiles {
		os.Remove(uf.parentFile)
		os.Remove(uf.rankFile)
		os.Remove(uf.rootFile)
	}

	return nil
}

// FindWithDepth: 재귀 깊이 제한이 있는 Find 연산
func (uf *MMapUnionFind) FindWithDepth(x int32, depth int) (int32, bool) {
	// 1. 재귀 깊이 제한 검사
	if depth >= MaxDepth {
		return x, false // 깊이 초과, 실패
	}

	// 2. 인덱스 범위 검사
	if x < 0 || x >= uf.n {
		return x, false // 범위 초과, 실패
	}

	// 3. 루트 캐시 확인
	if uf.rootSlice[x] != -1 {
		return uf.rootSlice[x], true // 캐시된 루트 반환
	}

	// 4. 루트 찾기 (재귀적)
	if uf.parentSlice[x] != x {
		// 부모의 루트를 재귀적으로 찾음
		root, ok := uf.FindWithDepth(uf.parentSlice[x], depth+1)
		if !ok {
			return x, false // 재귀 실패
		}
		// 경로 압축 - 현재 노드의 부모를 루트로 직접 연결
		uf.parentSlice[x] = root
		// 캐시 업데이트
		uf.rootSlice[x] = root
		return root, true
	}

	// 5. 자기 자신이 루트인 경우
	return x, true
}

// Find: 원소의 루트 찾기 (재귀 깊이 초과 시 반복문으로 대체)
func (uf *MMapUnionFind) Find(x int32) int32 {
	// 1. 먼저 재귀적으로 시도
	root, ok := uf.FindWithDepth(x, 0)
	if ok {
		return root
	}

	// 2. 재귀 깊이 초과 시 반복문으로 처리
	current := x
	for {
		parent := uf.parentSlice[current]
		if parent == current {
			break // 루트 찾음
		}
		current = parent
	}

	// 3. 루트 캐시 업데이트 (선택적)
	uf.rootSlice[x] = current

	return current
}

// Union: 두 원소 집합 병합
func (uf *MMapUnionFind) Union(x, y int32) bool {
	// 1. 루트 찾기
	rootX, okX := uf.FindWithDepth(x, 0)
	rootY, okY := uf.FindWithDepth(y, 0)

	// 2. 루트 찾기 실패 검사
	if !okX || !okY {
		return false
	}

	// 3. 이미 같은 집합인 경우
	if rootX == rootY {
		return true
	}

	// 4. Union by Rank 수행 (트리 높이 최소화)
	if uf.rankSlice[rootX] < uf.rankSlice[rootY] {
		// rootY가 더 높은 트리 -> rootX를 rootY 아래로
		uf.parentSlice[rootX] = rootY
	} else if uf.rankSlice[rootX] > uf.rankSlice[rootY] {
		// rootX가 더 높은 트리 -> rootY를 rootX 아래로
		uf.parentSlice[rootY] = rootX
	} else {
		// 랭크가 같으면 rootX를 루트로 하고 랭크 증가
		uf.parentSlice[rootY] = rootX
		uf.rankSlice[rootX]++
	}

	// 5. 루트 캐시 무효화 (병합 후 업데이트 필요)
	uf.rootSlice[x] = -1
	uf.rootSlice[y] = -1

	return true
}

//-------------------------------------------------------
// 청크 처리 함수
//-------------------------------------------------------

// 청크 내 테스트 케이스 처리 및 링크 정보 저장
func processChunkTestCases(uf *MMapUnionFind) (int, int) {
	batchID := uf.batchID
	chunkID := uf.chunkID

	fmt.Printf("[배치 %d, 청크 %d] 테스트 케이스 생성 및 처리 중...\n", batchID, chunkID)
	start := time.Now()

	// 배치 디렉토리
	batchDir := filepath.Join(DataDir, fmt.Sprintf(BatchDirTemplate, batchID))

	// 청크 내 링크 파일 (같은 청크 내 다른 청크와의 연결)
	chunkLinkFile := filepath.Join(batchDir, fmt.Sprintf(LinkFileTemplate, chunkID))
	chunkLinkHandle, err := os.Create(chunkLinkFile)
	if err != nil {
		log.Fatalf("[배치 %d, 청크 %d] 링크 파일 생성 실패: %v", batchID, chunkID, err)
	}
	defer chunkLinkHandle.Close()

	// 배치 간 링크 파일 관리
	batchLinkFiles := make(map[int]*os.File)
	batchLinkWriters := make(map[int]*bufio.Writer)

	// 버퍼링된 writer 생성
	chunkLinkWriter := bufio.NewWriter(chunkLinkHandle)

	// 헤더 공간 (링크 수 기록용)
	binary.Write(chunkLinkWriter, binary.LittleEndian, int64(0)) // 나중에 업데이트

	// 랜덤 생성기 초기화 (시드 분리)
	r := rand.New(rand.NewSource(time.Now().UnixNano() +
		int64(batchID*1000+chunkID)))

	// 1. 두 원소 쌍 처리
	fmt.Printf("[배치 %d, 청크 %d] %d개 두 원소 쌍 처리 중...\n",
		batchID, chunkID, PairsPerChunk)

	// 결과 통계 변수
	pairsSuccess := 0    // 성공적인 Union 연산 수
	pairsFail := 0       // 실패한 Union 연산 수
	crossChunkPairs := 0 // 청크 간 연결 수
	crossBatchPairs := 0 // 배치 간 연결 수

	for i := 0; i < PairsPerChunk; i++ {
		// 진행 상황 출력
		if i > 0 && i%100000 == 0 {
			fmt.Printf("[배치 %d, 청크 %d] 두 원소 쌍 %d/%d 처리 중\n",
				batchID, chunkID, i, PairsPerChunk)
		}

		// 첫 번째 원소는 현재 청크에서 선택
		localA := r.Int31n(ChunkSize)

		// 연결 유형 결정 (청크 내/청크 간/배치 간)
		connectionType := r.Float32()

		// 30% 확률로 다른 청크/배치의 원소 선택
		if connectionType < 0.3 {
			var targetBatch, targetChunk int
			var elemInTarget int32

			// 10% 확률로 이전 배치와 연결 (배치 간 링크)
			if connectionType < 0.1 && batchID > 0 {
				// 이전 배치 중 하나 선택
				targetBatch = r.Intn(batchID)
				targetChunk = r.Intn(NumChunksPerBatch)
				elemInTarget = r.Int31n(ChunkSize)

				// 배치 간 링크 파일 준비
				if _, exists := batchLinkFiles[targetBatch]; !exists {
					batchLinkPath := filepath.Join(batchDir,
						fmt.Sprintf(BatchLinkTemplate, batchID, targetBatch))
					bFile, err := os.Create(batchLinkPath)
					if err != nil {
						log.Printf("배치 링크 파일 생성 실패: %v", err)
						continue
					}
					batchLinkFiles[targetBatch] = bFile
					batchLinkWriters[targetBatch] = bufio.NewWriter(bFile)

					// 헤더 공간 (링크 수)
					binary.Write(batchLinkWriters[targetBatch],
						binary.LittleEndian, int64(0))
				}

				// 배치 간 링크 기록
				writer := batchLinkWriters[targetBatch]
				binary.Write(writer, binary.LittleEndian, int32(batchID))
				binary.Write(writer, binary.LittleEndian, int32(chunkID))
				binary.Write(writer, binary.LittleEndian, localA)
				binary.Write(writer, binary.LittleEndian, int32(targetBatch))
				binary.Write(writer, binary.LittleEndian, int32(targetChunk))
				binary.Write(writer, binary.LittleEndian, elemInTarget)

				crossBatchPairs++

				// 주기적으로 플러시
				if crossBatchPairs%10000 == 0 {
					writer.Flush()
				}

				continue
			} else if connectionType < 0.3 {
				// 20% 확률로 같은 배치 내 다른 청크와 연결 (청크 간 링크)
				// 다른 청크 선택
				targetBatch = batchID
				targetChunk = r.Intn(NumChunksPerBatch)
				// 동일 청크 방지
				if targetChunk == chunkID {
					targetChunk = (targetChunk + 1) % NumChunksPerBatch
				}
				elemInTarget = r.Int31n(ChunkSize)

				// 청크 간 링크 기록
				binary.Write(chunkLinkWriter, binary.LittleEndian, int32(chunkID))
				binary.Write(chunkLinkWriter, binary.LittleEndian, localA)
				binary.Write(chunkLinkWriter, binary.LittleEndian, int32(targetChunk))
				binary.Write(chunkLinkWriter, binary.LittleEndian, elemInTarget)

				crossChunkPairs++

				// 주기적으로 플러시
				if crossChunkPairs%10000 == 0 {
					chunkLinkWriter.Flush()
				}

				continue
			}
		}

		// 70% 확률로 동일 청크 내 원소 간 Union 수행
		localB := r.Int31n(ChunkSize)
		// 동일 원소 방지
		for localA == localB {
			localB = r.Int31n(ChunkSize)
		}

		// Union 수행
		if uf.Union(localA, localB) {
			pairsSuccess++
		} else {
			pairsFail++
		}
	}

	// 2. 다중 원소 집합 처리
	fmt.Printf("[배치 %d, 청크 %d] %d개 다중 원소 집합 처리 중...\n",
		batchID, chunkID, MultiSetsPerChunk)

	// 다중 집합 결과 통계
	multiSuccess := 0    // 성공한 다중 집합 수
	multiFail := 0       // 실패한 다중 집합 수
	crossChunkMulti := 0 // 청크 간 연결된 다중 집합 수
	crossBatchMulti := 0 // 배치 간 연결된 다중 집합 수

	for i := 0; i < MultiSetsPerChunk; i++ {
		// 진행 상황 출력
		if i > 0 && i%100000 == 0 {
			fmt.Printf("[배치 %d, 청크 %d] 다중 원소 집합 %d/%d 처리 중\n",
				batchID, chunkID, i, MultiSetsPerChunk)
		}

		// 3~10 사이의 랜덤한 크기 선택
		size := r.Intn(8) + 3

		// 첫 번째 원소는 현재 청크에서 선택 (기준점)
		localBase := r.Int31n(ChunkSize)

		// 다중 집합의 원소들
		localElements := make([]int32, 0, size-1)
		crossElements := false // 청크 간 또는 배치 간 연결 여부

		for j := 1; j < size; j++ {
			// 연결 유형 결정
			connType := r.Float32()

			// 25% 확률로 다른 청크/배치의 원소 선택
			if connType < 0.25 {
				crossElements = true

				var targetBatch, targetChunk int
				var elemInTarget int32

				// 10% 확률로 이전 배치와 연결
				if connType < 0.1 && batchID > 0 {
					// 이전 배치 중 하나 선택
					targetBatch = r.Intn(batchID)
					targetChunk = r.Intn(NumChunksPerBatch)
					elemInTarget = r.Int31n(ChunkSize)

					// 배치 간 링크 파일 준비
					if _, exists := batchLinkFiles[targetBatch]; !exists {
						batchLinkPath := filepath.Join(batchDir,
							fmt.Sprintf(BatchLinkTemplate, batchID, targetBatch))
						bFile, err := os.Create(batchLinkPath)
						if err != nil {
							log.Printf("배치 링크 파일 생성 실패: %v", err)
							continue
						}
						batchLinkFiles[targetBatch] = bFile
						batchLinkWriters[targetBatch] = bufio.NewWriter(bFile)

						// 헤더 공간 (링크 수)
						binary.Write(batchLinkWriters[targetBatch],
							binary.LittleEndian, int64(0))
					}

					// 배치 간 링크 기록
					writer := batchLinkWriters[targetBatch]
					binary.Write(writer, binary.LittleEndian, int32(batchID))
					binary.Write(writer, binary.LittleEndian, int32(chunkID))
					binary.Write(writer, binary.LittleEndian, localBase)
					binary.Write(writer, binary.LittleEndian, int32(targetBatch))
					binary.Write(writer, binary.LittleEndian, int32(targetChunk))
					binary.Write(writer, binary.LittleEndian, elemInTarget)

					crossBatchMulti++

					// 주기적으로 플러시
					if crossBatchMulti%10000 == 0 {
						writer.Flush()
					}
				} else if connType < 0.25 {
					// 15% 확률로 같은 배치 내 다른 청크와 연결
					// 다른 청크 선택
					targetBatch = batchID
					targetChunk = r.Intn(NumChunksPerBatch)
					// 동일 청크 방지
					if targetChunk == chunkID {
						targetChunk = (targetChunk + 1) % NumChunksPerBatch
					}
					elemInTarget = r.Int31n(ChunkSize)

					// 청크 간 링크 기록
					binary.Write(chunkLinkWriter, binary.LittleEndian, int32(chunkID))
					binary.Write(chunkLinkWriter, binary.LittleEndian, localBase)
					binary.Write(chunkLinkWriter, binary.LittleEndian, int32(targetChunk))
					binary.Write(chunkLinkWriter, binary.LittleEndian, elemInTarget)

					crossChunkMulti++

					// 주기적으로 플러시
					if crossChunkMulti%10000 == 0 {
						chunkLinkWriter.Flush()
					}
				}
			} else {
				// 75% 확률로 같은 청크 내 다른 원소 선택
				localElem := r.Int31n(ChunkSize)

				// 중복 방지
				duplicate := false
				if localElem == localBase {
					duplicate = true
				} else {
					for _, elem := range localElements {
						if elem == localElem {
							duplicate = true
							break
						}
					}
				}

				if !duplicate {
					localElements = append(localElements, localElem)
				}
			}
		}

		// 청크 간 또는 배치 간 연결이 있는 집합은 별도 처리됨
		if crossElements {
			continue
		}

		// 청크 내 원소들에 대해 Union 수행
		success := true
		for _, elem := range localElements {
			if !uf.Union(localBase, elem) {
				success = false
				multiFail++
				break
			}
		}

		if success && len(localElements) > 0 {
			multiSuccess++
		}
	}

	// 총 링크 수 계산
	totalChunkLinks := int64(crossChunkPairs + crossChunkMulti)

	// 파일 플러시 및 링크 수 업데이트
	chunkLinkWriter.Flush()
	chunkLinkHandle.Seek(0, 0)
	binary.Write(chunkLinkHandle, binary.LittleEndian, totalChunkLinks)

	// 배치 간 링크 파일 플러시 및 링크 수 업데이트
	for targetBatch, file := range batchLinkFiles {
		writer := batchLinkWriters[targetBatch]
		writer.Flush()

		// 링크 수 업데이트
		file.Seek(0, 0)
		totalBatchLinks := int64(crossBatchPairs + crossBatchMulti)
		binary.Write(file, binary.LittleEndian, totalBatchLinks)
		file.Close()
	}

	// 결과 출력
	fmt.Printf("[배치 %d, 청크 %d] 처리 결과:\n", batchID, chunkID)
	fmt.Printf("  - 청크 내 Union: 성공 %d, 실패 %d\n", pairsSuccess, pairsFail)
	fmt.Printf("  - 청크 내 집합: 성공 %d, 실패 %d\n", multiSuccess, multiFail)
	fmt.Printf("  - 청크 간 링크: %d개\n", crossChunkPairs+crossChunkMulti)
	fmt.Printf("  - 배치 간 링크: %d개\n", crossBatchPairs+crossBatchMulti)
	fmt.Printf("  - 소요 시간: %v\n", time.Since(start))

	return int(totalChunkLinks), (crossBatchPairs + crossBatchMulti)
}

// 청크 내 서로소 집합 수 계산 (샘플링 방식)
func countChunkDisjointSets(uf *MMapUnionFind) int {
	batchID := uf.batchID
	chunkID := uf.chunkID

	fmt.Printf("[배치 %d, 청크 %d] 서로소 집합 계산 중...\n", batchID, chunkID)
	start := time.Now()

	// 샘플링 간격
	const samplingInterval = 1000

	// 병렬 처리
	numWorkers := runtime.NumCPU()
	chunkSize := uf.n / int32(numWorkers)
	var wg sync.WaitGroup

	// 각 워커가 담당 구간의 루트 계산
	for w := 0; w < numWorkers; w++ {
		startIdx := int32(w) * chunkSize
		endIdx := startIdx + chunkSize
		if w == numWorkers-1 {
			endIdx = uf.n
		}

		wg.Add(1)
		go func(s, e int32) {
			defer wg.Done()
			// 샘플링 간격으로 루트 계산 및 캐싱
			for i := s; i < e; i += samplingInterval {
				root := uf.Find(i)
				uf.rootSlice[i] = root
			}
		}(startIdx, endIdx)
	}

	wg.Wait()

	// 고유 루트 집합 생성
	rootMap := make(map[int32]bool)
	sampleCount := 0

	// 샘플링된 원소들의 루트 수집
	for i := int32(0); i < uf.n; i += samplingInterval {
		if uf.rootSlice[i] != -1 {
			rootMap[uf.rootSlice[i]] = true
			sampleCount++
		}
	}

	// 고유 루트 수 = 서로소 집합 수
	disjointCount := len(rootMap)

	fmt.Printf("[배치 %d, 청크 %d] 서로소 집합 수 (샘플링): %d개 (%d 샘플)\n",
		batchID, chunkID, disjointCount, sampleCount)
	fmt.Printf("[배치 %d, 청크 %d] 서로소 집합 계산 완료. 소요 시간: %v\n",
		batchID, chunkID, time.Since(start))

	return disjointCount
}

// 청크 처리 (초기화, 테스트 케이스 수행, 서로소 집합 계산)
func processChunk(batchID, chunkID int) (int, int, int) {
	chunkStart := time.Now()
	fmt.Printf("======= 배치 %d, 청크 %d 처리 시작 =======\n", batchID, chunkID)

	// 1. 청크 초기화
	uf, err := NewMMapUnionFind(ChunkSize, batchID, chunkID)
	if err != nil {
		log.Fatalf("[배치 %d, 청크 %d] 초기화 실패: %v", batchID, chunkID, err)
	}

	// 2. 테스트 케이스 처리 및 링크 파일 생성
	crossChunkLinks, crossBatchLinks := processChunkTestCases(uf)

	// 3. 서로소 집합 수 계산
	disjointCount := countChunkDisjointSets(uf)

	// 4. 리소스 정리
	if err := uf.Close(false); err != nil {
		log.Printf("[배치 %d, 청크 %d] 리소스 정리 실패: %v", batchID, chunkID, err)
	}

	fmt.Printf("[배치 %d, 청크 %d] 처리 완료. 총 소요 시간: %v\n",
		batchID, chunkID, time.Since(chunkStart))

	return disjointCount, crossChunkLinks, crossBatchLinks
}

//-------------------------------------------------------
// 배치 처리 함수
//-------------------------------------------------------

// 같은 배치 내 청크 간 병합 처리
func mergeBatchChunks(batchID int, chunkDisjointSets []int) int {
	fmt.Printf("======= 배치 %d 청크 간 병합 시작 =======\n", batchID)
	start := time.Now()

	// 청크별 서로소 집합 수 합계
	totalDisjointSets := 0
	for _, count := range chunkDisjointSets {
		totalDisjointSets += count
	}
	fmt.Printf("[배치 %d] 청크별 서로소 집합 합계 (병합 전): %d\n",
		batchID, totalDisjointSets)

	// 메모리 효율적인 병합을 위해 한 번에 한 쌍의 청크만 처리
	totalMergedCount := 0

	// 배치 디렉토리
	batchDir := filepath.Join(DataDir, fmt.Sprintf(BatchDirTemplate, batchID))

	// 각 청크 쌍에 대해 병합 수행
	for i := 0; i < NumChunksPerBatch; i++ {
		for j := i + 1; j < NumChunksPerBatch; j++ {
			pairStart := time.Now()
			fmt.Printf("[배치 %d] 청크 %d - 청크 %d 간 병합 처리 중...\n", batchID, i, j)

			// 링크 파일 경로
			linkFileI := filepath.Join(batchDir, fmt.Sprintf(LinkFileTemplate, i))
			linkFileJ := filepath.Join(batchDir, fmt.Sprintf(LinkFileTemplate, j))

			// 이 청크 쌍의 병합 횟수
			mergedCount := processBatchChunkPair(batchID, i, j, linkFileI, linkFileJ)
			totalMergedCount += mergedCount

			fmt.Printf("[배치 %d] 청크 %d - 청크 %d 간 병합 완료. 병합 수: %d, 소요 시간: %v\n",
				batchID, i, j, mergedCount, time.Since(pairStart))

			// 메모리 정리
			runtime.GC()
			printMemUsage()
		}
	}

	// 최종 서로소 집합 수 계산
	finalDisjointSets := totalDisjointSets - totalMergedCount
	if finalDisjointSets < 0 {
		finalDisjointSets = 1 // 오류 방지
		log.Printf("[배치 %d] 경고: 서로소 집합 수 계산 오류 (음수 방지)", batchID)
	}

	fmt.Printf("[배치 %d] 청크 간 병합 완료. 총 병합 수: %d\n", batchID, totalMergedCount)
	fmt.Printf("[배치 %d] 최종 서로소 집합 수: %d\n", batchID, finalDisjointSets)
	fmt.Printf("[배치 %d] 병합 소요 시간: %v\n", batchID, time.Since(start))

	return finalDisjointSets
}

// 청크 쌍 병합 처리
func processBatchChunkPair(batchID, chunkI, chunkJ int, linkFileI, linkFileJ string) int {
	// 양방향 처리 (I → J, J → I)
	mergedCountIJ := processOneWayLinks(batchID, chunkI, chunkJ, linkFileI)
	mergedCountJI := processOneWayLinks(batchID, chunkJ, chunkI, linkFileJ)

	return mergedCountIJ + mergedCountJI
}

// 한 방향 링크 파일 처리
func processOneWayLinks(batchID, fromChunk, toChunk int, linkFile string) int {
	// 병합 횟수
	mergedCount := 0

	// 링크 파일 열기
	file, err := os.Open(linkFile)
	if err != nil {
		if os.IsNotExist(err) {
			// 파일이 없으면 링크가 없음
			return 0
		}
		log.Printf("[배치 %d] 링크 파일 열기 실패: %v", batchID, err)
		return 0
	}
	defer file.Close()

	// 총 링크 수 읽기
	var totalLinks int64
	binary.Read(file, binary.LittleEndian, &totalLinks)

	if totalLinks == 0 {
		return 0 // 링크 없음
	}

	// 루트 쌍 -> 카운트 맵 초기화
	rootPairCounts := make(map[string]int)

	// 청크 로드
	ufFrom := loadChunkForMerge(batchID, fromChunk)
	if ufFrom == nil {
		log.Printf("[배치 %d] 청크 %d 로드 실패", batchID, fromChunk)
		return 0
	}
	defer ufFrom.Close(false)

	ufTo := loadChunkForMerge(batchID, toChunk)
	if ufTo == nil {
		log.Printf("[배치 %d] 청크 %d 로드 실패", batchID, toChunk)
		return 0
	}
	defer ufTo.Close(false)

	// 링크 처리 시작
	fmt.Printf("[배치 %d] 청크 %d → 청크 %d: %d개 링크 처리 중...\n",
		batchID, fromChunk, toChunk, totalLinks)

	// 링크 읽기 버퍼
	linkBuf := make([]byte, 16) // 4 × 4바이트 필드
	processedLinks := 0

	// 모든 링크 순회
	for {
		// 링크 데이터 읽기
		n, err := file.Read(linkBuf)
		if err == io.EOF || n < 16 {
			break
		}

		// 링크 구조체 파싱
		chunkA := int(binary.LittleEndian.Uint32(linkBuf[0:4]))
		elemA := int32(binary.LittleEndian.Uint32(linkBuf[4:8]))
		chunkB := int(binary.LittleEndian.Uint32(linkBuf[8:12]))
		elemB := int32(binary.LittleEndian.Uint32(linkBuf[12:16]))

		// 방향 확인
		if chunkA == fromChunk && chunkB == toChunk {
			// 각 원소의 루트 찾기
			rootA := ufFrom.Find(elemA)
			rootB := ufTo.Find(elemB)

			// 압축 맵에 추가 (로컬 루트 쌍 -> 카운트)
			key := fmt.Sprintf("%d-%d", rootA, rootB)
			rootPairCounts[key]++

			processedLinks++
			if processedLinks%100000 == 0 {
				fmt.Printf("[배치 %d] 청크 %d → 청크 %d: %d/%d 링크 처리됨\n",
					batchID, fromChunk, toChunk, processedLinks, totalLinks)
			}
		}
	}

	// 압축된 링크 수 = 고유 루트 쌍 수
	compressedCount := len(rootPairCounts)

	// 결과 파일 저장 (배치 디렉토리 내)
	batchDir := filepath.Join(DataDir, fmt.Sprintf(BatchDirTemplate, batchID))
	mergeResultFile := filepath.Join(batchDir,
		fmt.Sprintf(MergeResultTemplate, fromChunk, toChunk))

	resultFile, err := os.Create(mergeResultFile)
	if err != nil {
		log.Printf("[배치 %d] 병합 결과 파일 생성 실패: %v", batchID, err)
		return 0
	}
	defer resultFile.Close()

	// 압축 링크 수 기록
	binary.Write(resultFile, binary.LittleEndian, int32(compressedCount))

	// 압축된 링크 정보 기록
	for key, count := range rootPairCounts {
		var rootA, rootB int32
		fmt.Sscanf(key, "%d-%d", &rootA, &rootB)

		binary.Write(resultFile, binary.LittleEndian, rootA)
		binary.Write(resultFile, binary.LittleEndian, rootB)
		binary.Write(resultFile, binary.LittleEndian, int32(count))

		// 각 고유 루트 쌍은 하나의 병합으로 카운트
		mergedCount++
	}

	fmt.Printf("[배치 %d] 청크 %d → 청크 %d: %d개 링크 처리, %d개 압축 링크 생성\n",
		batchID, fromChunk, toChunk, processedLinks, compressedCount)

	return mergedCount
}

// 병합용 청크 로드
func loadChunkForMerge(batchID, chunkID int) *MMapUnionFind {
	// 청크 로드
	uf, err := NewMMapUnionFind(ChunkSize, batchID, chunkID)
	if err != nil {
		log.Printf("[배치 %d] 청크 %d 로드 실패: %v", batchID, chunkID, err)
		return nil
	}

	return uf
}

// 배치 처리 (초기화, 청크 처리, 청크 간 병합)
func processBatch(batchID int) *BatchInfo {
	batchStart := time.Now()
	fmt.Printf("======= 배치 %d 처리 시작 (원소 %d~%d) =======\n",
		batchID, batchID*BatchSize, (batchID+1)*BatchSize-1)

	// 배치 디렉토리 생성
	batchDir := filepath.Join(DataDir, fmt.Sprintf(BatchDirTemplate, batchID))
	if err := os.MkdirAll(batchDir, 0755); err != nil {
		log.Fatalf("배치 디렉토리 생성 실패: %v", err)
	}

	// 청크별 서로소 집합 수 저장
	chunkDisjointSets := make([]int, NumChunksPerBatch)
	batchLinks := 0 // 배치 간 링크 수

	// 1. 청크 순차 처리
	for i := 0; i < NumChunksPerBatch; i++ {
		// 메모리 정리
		runtime.GC()

		// 청크 처리
		disjointCount, _, crossBatchCount := processChunk(batchID, i)
		chunkDisjointSets[i] = disjointCount
		batchLinks += crossBatchCount

		printMemUsage()
	}

	// 2. 청크 간 병합
	finalDisjointSets := mergeBatchChunks(batchID, chunkDisjointSets)

	// 3. 배치 정보 생성
	batchInfo := &BatchInfo{
		ID:            batchID,
		DisjointSets:  finalDisjointSets,
		Processed:     true,
		ElementOffset: int64(batchID) * BatchSize,
		ChunkCount:    NumChunksPerBatch,
	}

	// 4. 배치 정보 저장
	saveBatchInfo(batchInfo, filepath.Join(batchDir,
		fmt.Sprintf(BatchInfoFile, batchID)))

	fmt.Printf("배치 %d 처리 완료. 서로소 집합 수: %d, 배치 간 링크: %d, 소요 시간: %v\n",
		batchID, finalDisjointSets, batchLinks, time.Since(batchStart))

	return batchInfo
}

// 배치 정보 저장
func saveBatchInfo(info *BatchInfo, filePath string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	binary.Write(file, binary.LittleEndian, int32(info.ID))
	binary.Write(file, binary.LittleEndian, int32(info.DisjointSets))
	binary.Write(file, binary.LittleEndian, int32(info.ChunkCount))
	binary.Write(file, binary.LittleEndian, info.ElementOffset)

	return nil
}

// 배치 정보 로드
func loadBatchInfo(filePath string) (*BatchInfo, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	info := &BatchInfo{Processed: true}

	var id, disjointSets, chunkCount int32
	binary.Read(file, binary.LittleEndian, &id)
	binary.Read(file, binary.LittleEndian, &disjointSets)
	binary.Read(file, binary.LittleEndian, &chunkCount)
	binary.Read(file, binary.LittleEndian, &info.ElementOffset)

	info.ID = int(id)
	info.DisjointSets = int(disjointSets)
	info.ChunkCount = int(chunkCount)

	return info, nil
}

//-------------------------------------------------------
// 배치 간 병합 함수
//-------------------------------------------------------

// 두 배치 간 병합 처리
// 두 배치 간 병합 처리
func mergeTwoBatches(batchA, batchB *BatchInfo) (int, error) {
	if batchA.ID > batchB.ID {
		// 낮은 ID에서 높은 ID 방향으로 병합
		return mergeTwoBatches(batchB, batchA)
	}

	fmt.Printf("======= 배치 %d - 배치 %d 간 병합 시작 =======\n", batchA.ID, batchB.ID)
	start := time.Now()

	// 배치 디렉토리
	batchDirA := filepath.Join(DataDir, fmt.Sprintf(BatchDirTemplate, batchA.ID))
	batchDirB := filepath.Join(DataDir, fmt.Sprintf(BatchDirTemplate, batchB.ID))

	// 총 병합 수
	totalMergeCount := 0

	// 배치 간 링크 파일 (A -> B)
	linkFileAtoB := filepath.Join(batchDirA,
		fmt.Sprintf(BatchLinkTemplate, batchA.ID, batchB.ID))

	// A -> B 방향 링크 처리
	if _, err := os.Stat(linkFileAtoB); !os.IsNotExist(err) {
		mergeCount, err := processBatchLinks(batchA, batchB, linkFileAtoB)
		if err != nil {
			log.Printf("A -> B 링크 처리 오류: %v", err)
		} else {
			totalMergeCount += mergeCount
		}
	}

	// 배치 간 링크 파일 (B -> A)
	linkFileBtoA := filepath.Join(batchDirB,
		fmt.Sprintf(BatchLinkTemplate, batchB.ID, batchA.ID))

	// B -> A 방향 링크 처리
	if _, err := os.Stat(linkFileBtoA); !os.IsNotExist(err) {
		mergeCount, err := processBatchLinks(batchA, batchB, linkFileBtoA)
		if err != nil {
			log.Printf("B -> A 링크 처리 오류: %v", err)
		} else {
			totalMergeCount += mergeCount
		}
	}

	fmt.Printf("배치 %d - 배치 %d 간 병합 완료. 총 병합 수: %d, 소요 시간: %v\n",
		batchA.ID, batchB.ID, totalMergeCount, time.Since(start))

	return totalMergeCount, nil
}

// 배치 간 링크 처리
func processBatchLinks(batchA, batchB *BatchInfo, linkFilePath string) (int, error) {
	// 링크 파일 열기
	file, err := os.Open(linkFilePath)
	if err != nil {
		return 0, fmt.Errorf("링크 파일 열기 실패: %v", err)
	}
	defer file.Close()

	// 총 링크 수 읽기
	var totalLinks int64
	binary.Read(file, binary.LittleEndian, &totalLinks)

	if totalLinks == 0 {
		return 0, nil // 링크 없음
	}

	fmt.Printf("배치 %d - 배치 %d 간 %d개 링크 처리 중...\n",
		batchA.ID, batchB.ID, totalLinks)

	// 병합 카운트
	mergeCount := 0

	// 청크별 압축된 루트 맵 생성
	rootMapsA := make(map[int]map[int32]int32) // 청크ID -> (원소ID -> 루트ID)
	rootMapsB := make(map[int]map[int32]int32)

	// 각 청크의 루트 정보 로드
	for chunkID := 0; chunkID < batchA.ChunkCount; chunkID++ {
		uf := loadChunkForMerge(batchA.ID, chunkID)
		if uf != nil {
			rootMap := make(map[int32]int32)

			// 샘플링하여 루트 맵 구성
			for i := int32(0); i < ChunkSize; i += 100 {
				rootMap[i] = uf.Find(i)
			}

			rootMapsA[chunkID] = rootMap
			uf.Close(false)
		}
	}

	for chunkID := 0; chunkID < batchB.ChunkCount; chunkID++ {
		uf := loadChunkForMerge(batchB.ID, chunkID)
		if uf != nil {
			rootMap := make(map[int32]int32)

			for i := int32(0); i < ChunkSize; i += 100 {
				rootMap[i] = uf.Find(i)
			}

			rootMapsB[chunkID] = rootMap
			uf.Close(false)
		}
	}

	// 중복 체크를 위한 셋
	mergedPairs := make(map[string]bool)

	// 링크 읽기 버퍼
	linkBuf := make([]byte, 24) // 6 × 4바이트 필드
	processedLinks := 0

	// 모든 링크 순회하며 병합 수행
	for {
		n, err := file.Read(linkBuf)
		if err == io.EOF || n < 24 {
			break
		}

		// 링크 파싱
		batchFrom := int(binary.LittleEndian.Uint32(linkBuf[0:4]))
		chunkFrom := int(binary.LittleEndian.Uint32(linkBuf[4:8]))
		elemFrom := int32(binary.LittleEndian.Uint32(linkBuf[8:12]))
		batchTo := int(binary.LittleEndian.Uint32(linkBuf[12:16]))
		chunkTo := int(binary.LittleEndian.Uint32(linkBuf[16:20]))
		elemTo := int32(binary.LittleEndian.Uint32(linkBuf[20:24]))

		// 방향 확인
		if batchFrom == batchB.ID && batchTo == batchA.ID {
			// 루트 찾기
			var rootFrom, rootTo int32

			// 가장 가까운 샘플링된 원소 기반 루트 찾기
			rootFrom = findNearestSampledRoot(rootMapsB[chunkFrom], elemFrom)
			rootTo = findNearestSampledRoot(rootMapsA[chunkTo], elemTo)

			// 전역 ID 계산
			globalRootFrom := batchB.ElementOffset + int64(chunkFrom)*ChunkSize + int64(rootFrom)
			globalRootTo := batchA.ElementOffset + int64(chunkTo)*ChunkSize + int64(rootTo)

			// 중복 병합 방지
			pairKey := fmt.Sprintf("%d-%d", globalRootFrom, globalRootTo)
			if !mergedPairs[pairKey] {
				mergedPairs[pairKey] = true
				mergeCount++
			}
		}

		processedLinks++
		if processedLinks%100000 == 0 {
			fmt.Printf("배치 간 링크 처리 진행: %d/%d\n", processedLinks, totalLinks)
		}
	}

	fmt.Printf("배치 %d - 배치 %d 간 총 %d개 링크 처리됨, %d개 고유 병합\n",
		batchA.ID, batchB.ID, processedLinks, mergeCount)

	return mergeCount, nil
}

// 가장 가까운 샘플링된 원소의 루트 찾기
func findNearestSampledRoot(rootMap map[int32]int32, elem int32) int32 {
	if root, exists := rootMap[elem]; exists {
		return root
	}

	// 가장 가까운 샘플링 원소 찾기
	nearest := (elem / 100) * 100 // 100 단위로 내림
	if root, exists := rootMap[nearest]; exists {
		return root
	}

	// 다음 샘플링 지점 시도
	next := nearest + 100
	if next < ChunkSize {
		if root, exists := rootMap[next]; exists {
			return root
		}
	}

	// 기본값으로 원소 자신 반환
	return elem
}

// 모든 배치 로드
func loadAllBatches() ([]*BatchInfo, error) {
	batches := make([]*BatchInfo, 0, NumBatches)

	for i := 0; i < NumBatches; i++ {
		batchDir := filepath.Join(DataDir, fmt.Sprintf(BatchDirTemplate, i))
		infoPath := filepath.Join(batchDir, fmt.Sprintf(BatchInfoFile, i))

		if _, err := os.Stat(infoPath); os.IsNotExist(err) {
			continue // 아직 처리되지 않은 배치
		}

		info, err := loadBatchInfo(infoPath)
		if err != nil {
			return nil, fmt.Errorf("배치 %d 정보 로드 실패: %v", i, err)
		}

		batches = append(batches, info)
	}

	return batches, nil
}

// 모든 배치 간 병합 수행
func mergeAllBatches(batches []*BatchInfo) (int, error) {
	if len(batches) <= 1 {
		return 0, fmt.Errorf("병합할 배치가 충분하지 않음")
	}

	fmt.Println("======= 모든 배치 간 병합 시작 =======")
	start := time.Now()

	// 초기 서로소 집합 수 합계
	initialDisjointSets := 0
	for _, batch := range batches {
		initialDisjointSets += batch.DisjointSets
		fmt.Printf("배치 %d: 서로소 집합 수 %d\n", batch.ID, batch.DisjointSets)
	}
	fmt.Printf("초기 서로소 집합 수 합계: %d\n", initialDisjointSets)

	// 총 병합 수
	totalMergeCount := 0

	// 각 배치 쌍에 대해 병합 수행
	for i := 0; i < len(batches); i++ {
		for j := i + 1; j < len(batches); j++ {
			mergeCount, err := mergeTwoBatches(batches[i], batches[j])
			if err != nil {
				log.Printf("배치 병합 오류: %v", err)
				continue
			}

			totalMergeCount += mergeCount

			// 주기적 메모리 정리
			runtime.GC()
			printMemUsage()
		}
	}

	// 최종 서로소 집합 수 계산
	finalDisjointSets := initialDisjointSets - totalMergeCount
	if finalDisjointSets < 0 {
		log.Printf("경고: 서로소 집합 수 계산 오류 (음수). 기본값 사용")
		finalDisjointSets = len(batches) // 최소 배치 수만큼은 있어야 함
	}

	fmt.Printf("모든 배치 간 병합 완료. 총 병합 수: %d\n", totalMergeCount)
	fmt.Printf("최종 서로소 집합 수: %d\n", finalDisjointSets)
	fmt.Printf("전체 병합 소요 시간: %v\n", time.Since(start))

	return finalDisjointSets, nil
}

// 점진적 배치 처리 및 통합
func incrementalBatchProcessing() (int, error) {
	fmt.Println("======= 점진적 배치 처리 시작 =======")
	startTime := time.Now()

	// 데이터 디렉토리 생성
	if err := os.MkdirAll(DataDir, 0755); err != nil {
		return 0, fmt.Errorf("데이터 디렉토리 생성 실패: %v", err)
	}

	// 처리된 배치 목록
	processedBatches := make([]*BatchInfo, 0, NumBatches)

	// 각 배치 순차적으로 처리
	for batchID := 0; batchID < NumBatches; batchID++ {
		// 1. 현재 배치 처리
		currentBatch := processBatch(batchID)
		processedBatches = append(processedBatches, currentBatch)

		// 2. 이전 배치들과 병합 (점진적 통합)
		if batchID > 0 {
			fmt.Printf("배치 %d와 이전 배치들과의 병합 시작...\n", batchID)

			for prevID := 0; prevID < batchID; prevID++ {
				mergeTwoBatches(processedBatches[prevID], currentBatch)
			}
		}

		// 메모리 정리
		runtime.GC()
		printMemUsage()
	}

	// 최종 통합 정보 계산
	fmt.Println("모든 배치 처리 및 병합 완료. 최종 통계 계산...")

	finalDisjointSets, err := mergeAllBatches(processedBatches)
	if err != nil {
		return 0, fmt.Errorf("최종 병합 실패: %v", err)
	}

	fmt.Printf("\n======= 최종 결과 =======\n")
	fmt.Printf("총 원소 수: %d\n", TotalElements)
	fmt.Printf("배치 수: %d\n", NumBatches)
	fmt.Printf("최종 서로소 집합 수: %d\n", finalDisjointSets)
	fmt.Printf("총 소요 시간: %v\n", time.Since(startTime))

	return finalDisjointSets, nil
}

// -------------------------------------------------------
// 메인 함수
// -------------------------------------------------------
func main() {
	// 프로그램 시작 시간
	startTime := time.Now()

	// 랜덤 시드 설정
	rand.Seed(time.Now().UnixNano())

	// 초기 메모리 사용량
	fmt.Println("초기 메모리 사용량:")
	printMemUsage()

	// 점진적 배치 처리 수행
	finalDisjointSets, err := incrementalBatchProcessing()
	if err != nil {
		log.Fatalf("점진적 배치 처리 실패: %v", err)
	}

	// 최종 결과 출력
	fmt.Printf("\n======= 최종 결과 요약 =======\n")
	fmt.Printf("총 원소 수: %d\n", TotalElements)
	fmt.Printf("최종 서로소 집합 수: %d\n", finalDisjointSets)
	fmt.Printf("총 소요 시간: %v\n", time.Since(startTime))

	// 최종 메모리 사용량
	fmt.Println("최종 메모리 사용량:")
	printMemUsage()

	fmt.Println("프로그램 정상 종료")
}

package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"reflect"
	"runtime"
	"sync"
	"time"
	"unsafe"

	"github.com/edsrzf/mmap-go"
)

// 최대 재귀 깊이 제한
const maxDepth = 20

// 데이터 크기 설정
const (
	MaxElements       = 2_000_000_00 // 2억 원소
	SingleElementSets = 1_800_000_00 // 1억 8천만 개의 단일 원소 집합
	TwoElementSets    = 100_000_00   // 1천만 개의 2원소 집합
	MultiElementSets  = 100_000_00   // 1천만 개의 다중 원소 집합
)

// 파일 경로 설정
const (
	ParentFilePath = "parent.dat"
	RankFilePath   = "rank.dat"
	RootFilePath   = "root.dat"
)

// MMapUnionFind는 매우 큰 원소 집합을 MMAP로 관리하는 구조체입니다.
type MMapUnionFind struct {
	n          int32
	parentFile string
	rankFile   string
	rootFile   string

	parentMMap mmap.MMap
	rankMMap   mmap.MMap
	rootMMap   mmap.MMap

	parentSlice []int32 // parent 배열 (mmapped)
	rankSlice   []int8  // rank 배열 (mmapped)
	rootSlice   []int32 // 각 인덱스의 최종 루트를 기록하는 배열 (mmapped)
}

// createLargeFile: 주어진 크기로 파일을 생성하거나 Truncate합니다.
func createLargeFile(fileName string, size int64) (*os.File, error) {
	f, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	fmt.Printf("파일 생성 중: %s (%.2f GB)\n", fileName, float64(size)/1024/1024/1024)

	err = f.Truncate(size)
	if err != nil {
		// sparse 파일로 fallback
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

	fmt.Printf("파일 생성 완료: %s\n", fileName)
	return f, nil
}

// NewMMapUnionFind: n개의 원소(0~n-1)를 관리할 parent/rank/root 파일들을 생성하고 MMAP합니다.
func NewMMapUnionFind(n int32, parentFile, rankFile, rootFile string) (*MMapUnionFind, error) {
	uf := &MMapUnionFind{
		n:          n,
		parentFile: parentFile,
		rankFile:   rankFile,
		rootFile:   rootFile,
	}

	fmt.Println("파일 생성 및 메모리 매핑 시작...")

	//--------------------------------
	// 1) parent.dat (n * 4 바이트)
	//--------------------------------
	parentSize := int64(n) * 4
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

	//--------------------------------
	// 2) rank.dat (n * 1 바이트)
	//--------------------------------
	rankSize := int64(n)
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

	//--------------------------------
	// 3) root.dat (n * 4 바이트) - 각 원소의 최종 루트를 기록
	//--------------------------------
	rootSize := int64(n) * 4
	fRoot, err := createLargeFile(rootFile, rootSize)
	if err != nil {
		uf.parentMMap.Unmap()
		uf.rankMMap.Unmap()
		return nil, err
	}
	rootMap, err := mmap.Map(fRoot, mmap.RDWR, 0)
	if err != nil {
		fRoot.Close()
		uf.parentMMap.Unmap()
		uf.rankMMap.Unmap()
		return nil, err
	}
	fRoot.Close()
	uf.rootMMap = rootMap

	//--------------------------------
	// 슬라이스 재해석
	//--------------------------------
	fmt.Println("슬라이스 재해석 시작...")
	{
		hdrParent := (*reflect.SliceHeader)(unsafe.Pointer(&uf.parentSlice))
		hdrParent.Data = uintptr(unsafe.Pointer(&uf.parentMMap[0]))
		hdrParent.Len = int(parentSize / 4)
		hdrParent.Cap = int(parentSize / 4)
	}
	{
		hdrRank := (*reflect.SliceHeader)(unsafe.Pointer(&uf.rankSlice))
		hdrRank.Data = uintptr(unsafe.Pointer(&uf.rankMMap[0]))
		hdrRank.Len = int(rankSize)
		hdrRank.Cap = int(rankSize)
	}
	{
		hdrRoot := (*reflect.SliceHeader)(unsafe.Pointer(&uf.rootSlice))
		hdrRoot.Data = uintptr(unsafe.Pointer(&uf.rootMMap[0]))
		hdrRoot.Len = int(rootSize / 4)
		hdrRoot.Cap = int(rootSize / 4)
	}
	fmt.Println("슬라이스 재해석 완료")

	//--------------------------------
	// 병렬 초기화
	//--------------------------------
	fmt.Println("병렬 초기화 시작...")
	numWorkers := runtime.NumCPU()
	chunkSize := n / int32(numWorkers)
	var wg sync.WaitGroup
	start := time.Now()

	for w := 0; w < numWorkers; w++ {
		startIdx := int32(w) * chunkSize
		endIdx := startIdx + chunkSize
		if w == numWorkers-1 {
			endIdx = n
		}

		wg.Add(1)
		go func(s, e int32) {
			defer wg.Done()
			for i := s; i < e; i++ {
				uf.parentSlice[i] = i
				uf.rankSlice[i] = 0
				uf.rootSlice[i] = -1 // 아직 Find 결과 없음

				if (i+1-s)%10000000 == 0 {
					fmt.Printf("Worker [%d-%d] 초기화 진행: %d / %d 원소 처리\n", s, e, i+1-s, e-s)
				}
			}
		}(startIdx, endIdx)
	}

	wg.Wait()
	fmt.Printf("초기화 완료 (소요 시간: %v)\n", time.Since(start))

	return uf, nil
}

// CloseAndCleanup: MMAP 해제 후, (옵션) 파일 삭제
func (uf *MMapUnionFind) CloseAndCleanup(deleteFiles bool) error {
	if err := uf.parentMMap.Unmap(); err != nil {
		return err
	}

	if err := uf.rankMMap.Unmap(); err != nil {
		return err
	}

	if err := uf.rootMMap.Unmap(); err != nil {
		return err
	}

	if deleteFiles {
		os.Remove(uf.parentFile)
		os.Remove(uf.rankFile)
		os.Remove(uf.rootFile)
	}

	return nil
}

// FindWithDepth: 재귀 깊이가 maxDepth 초과 시 false 반환
func (uf *MMapUnionFind) FindWithDepth(x int32, depth int) (int32, bool) {
	if depth >= maxDepth {
		return x, false
	}

	// 인덱스 범위 체크
	if x < 0 || x >= uf.n {
		return x, false
	}

	// 이미 계산된 루트가 있으면 사용
	if uf.rootSlice[x] != -1 {
		return uf.rootSlice[x], true
	}

	if uf.parentSlice[x] != x {
		root, ok := uf.FindWithDepth(uf.parentSlice[x], depth+1)
		if !ok {
			return x, false
		}
		uf.parentSlice[x] = root
		return root, true
	}

	return x, true
}

// Find: 반복문으로 루트 찾기 (재귀 깊이 초과 시 사용)
func (uf *MMapUnionFind) Find(x int32) int32 {
	root, ok := uf.FindWithDepth(x, 0)
	if ok {
		return root
	}

	// 재귀 깊이 초과 시 반복문으로 처리
	current := x
	for {
		parent := uf.parentSlice[current]
		if parent == current {
			break
		}
		current = parent
	}

	return current
}

// UnionWithDepth: 두 원소 x,y를 병합 (재귀 깊이 초과 시 스킵)
func (uf *MMapUnionFind) UnionWithDepth(x, y int32) bool {
	rootX, okX := uf.FindWithDepth(x, 0)
	rootY, okY := uf.FindWithDepth(y, 0)

	if !okX || !okY {
		return false
	}

	if rootX == rootY {
		return true
	}

	if uf.rankSlice[rootX] < uf.rankSlice[rootY] {
		uf.parentSlice[rootX] = rootY
	} else if uf.rankSlice[rootX] > uf.rankSlice[rootY] {
		uf.parentSlice[rootY] = rootX
	} else {
		uf.parentSlice[rootY] = rootX
		uf.rankSlice[rootX]++
	}

	return true
}

// 테스트 집합 구조체 - 추적 가능한 테스트 케이스
type TestSet struct {
	ID       int32   // 집합 ID
	Elements []int32 // 원소들
}

// 테스트 케이스 생성 함수 - 추적 가능한 테스트 케이스 반환
func generateTestCases(twoElementCount, multiElementCount int) []TestSet {
	fmt.Println("테스트 케이스 생성 중...")
	start := time.Now()

	// 결과 슬라이스
	testSets := make([]TestSet, 0, twoElementCount+multiElementCount)
	setID := int32(0)

	// 2원소 집합 생성
	fmt.Printf("%d개의 2원소 집합 생성 중...\n", twoElementCount)
	for i := 0; i < twoElementCount; i++ {
		if i%1000000 == 0 && i > 0 {
			fmt.Printf("2원소 집합 %d/%d 생성됨\n", i, twoElementCount)
		}

		// 0부터 MaxElements-1 사이의 랜덤한 두 개의 원소 선택
		a := rand.Int31n(int32(MaxElements))
		b := rand.Int31n(int32(MaxElements))
		// 같은 원소가 선택되지 않도록 함
		for a == b {
			b = rand.Int31n(int32(MaxElements))
		}

		testSets = append(testSets, TestSet{
			ID:       setID,
			Elements: []int32{a, b},
		})
		setID++
	}

	// 3~50개 원소 집합 생성
	fmt.Printf("%d개의 다중 원소 집합 생성 중...\n", multiElementCount)
	for i := 0; i < multiElementCount; i++ {
		if i%1000000 == 0 && i > 0 {
			fmt.Printf("다중 원소 집합 %d/%d 생성됨\n", i, multiElementCount)
		}

		// 3~50 사이의 랜덤한 크기 선택
		size := rand.Intn(48) + 3
		elements := make(map[int32]bool)

		// 중복 없이 원소 선택
		for len(elements) < size {
			elements[rand.Int31n(int32(MaxElements))] = true
		}

		// 맵을 슬라이스로 변환
		elementSlice := make([]int32, 0, size)
		for element := range elements {
			elementSlice = append(elementSlice, element)
		}

		testSets = append(testSets, TestSet{
			ID:       setID,
			Elements: elementSlice,
		})
		setID++
	}

	fmt.Printf("테스트 케이스 생성 완료. %d개 생성됨. 소요 시간: %v\n",
		len(testSets), time.Since(start))

	return testSets
}

// 테스트 케이스 처리 함수
func processTestCases(uf *MMapUnionFind, testSets []TestSet) {
	fmt.Printf("%d개 테스트 케이스 처리 시작...\n", len(testSets))
	start := time.Now()

	// 병렬 처리용 고루틴 수
	numWorkers := runtime.NumCPU()
	chunkSize := len(testSets) / numWorkers
	if chunkSize == 0 {
		chunkSize = 1
	}

	var wg sync.WaitGroup
	results := make([]struct {
		success int
		fail    int
	}, numWorkers)

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)

		go func(workerID int) {
			defer wg.Done()

			start := workerID * chunkSize
			end := (workerID + 1) * chunkSize
			if workerID == numWorkers-1 || end > len(testSets) {
				end = len(testSets)
			}

			successCount := 0
			failCount := 0

			for i := start; i < end; i++ {
				set := testSets[i]

				if len(set.Elements) < 2 {
					continue // 원소가 1개 이하면 건너뜀
				}

				// 집합의 첫 번째 원소를 기준으로 나머지를 합침
				base := set.Elements[0]
				setSuccess := true

				for j := 1; j < len(set.Elements); j++ {
					if !uf.UnionWithDepth(base, set.Elements[j]) {
						setSuccess = false
						failCount++
						break
					}
				}

				if setSuccess {
					successCount++
				}

				if (i-start)%10000 == 0 && i > start {
					fmt.Printf("Worker %d: %d/%d 집합 처리됨\n", workerID, i-start, end-start)
				}
			}

			results[workerID].success = successCount
			results[workerID].fail = failCount

			fmt.Printf("Worker %d: 처리 완료. 성공: %d, 실패: %d\n",
				workerID, successCount, failCount)
		}(w)
	}

	wg.Wait()

	// 결과 합산
	totalSuccess := 0
	totalFail := 0
	for _, r := range results {
		totalSuccess += r.success
		totalFail += r.fail
	}

	fmt.Printf("테스트 케이스 처리 완료. 성공: %d, 실패: %d, 소요 시간: %v\n",
		totalSuccess, totalFail, time.Since(start))
}

// 샘플링을 이용한 서로소 집합 계산 함수
func countDisjointSets(uf *MMapUnionFind) int {
	fmt.Println("서로소 집합 수 계산 중...")
	start := time.Now()

	// 샘플링 간격 설정 - 더 정확한 계산을 위해 간격 줄임
	const samplingInterval = 100 // 100개당 1개 샘플링 (더 정확한 결과)

	// 병렬 처리로 샘플 루트 계산
	numWorkers := runtime.NumCPU()
	chunkSize := uf.n / int32(numWorkers)
	var wg sync.WaitGroup

	for w := 0; w < numWorkers; w++ {
		startIdx := int32(w) * chunkSize
		endIdx := startIdx + chunkSize
		if w == numWorkers-1 {
			endIdx = uf.n
		}

		wg.Add(1)
		go func(s, e int32) {
			defer wg.Done()
			for i := s; i < e; i += samplingInterval {
				root := uf.Find(i)
				uf.rootSlice[i] = root

				if (i-s)%(samplingInterval*10000) == 0 && i > s {
					fmt.Printf("Worker[%d-%d] 샘플링 진행: %.2f%%\n",
						s, e, float64(i-s)/float64(e-s)*100)
				}
			}
		}(startIdx, endIdx)
	}

	wg.Wait()

	// 고유 루트 카운트
	rootMap := make(map[int32]int)
	sampleCount := 0

	for i := int32(0); i < uf.n; i += samplingInterval {
		if uf.rootSlice[i] != -1 {
			rootMap[uf.rootSlice[i]]++
			sampleCount++
		}
	}

	// 서로소 집합 수 계산
	disjointSetCount := len(rootMap)

	// 집합 크기 분포 계산
	sizeDistribution := make(map[int]int) // 크기 -> 개수

	for _, count := range rootMap {
		sizeDistribution[count]++
	}

	// 상위 10개 크기 출력
	fmt.Println("집합 크기 분포 (샘플링 기준):")
	sizes := make([]int, 0, len(sizeDistribution))
	for size := range sizeDistribution {
		sizes = append(sizes, size)
	}

	// 크기 기준 내림차순 정렬
	for i := 0; i < len(sizes); i++ {
		for j := i + 1; j < len(sizes); j++ {
			if sizes[i] < sizes[j] {
				sizes[i], sizes[j] = sizes[j], sizes[i]
			}
		}
	}

	// 상위 10개 또는 전체 (더 작은 쪽) 출력
	displayCount := 10
	if len(sizes) < displayCount {
		displayCount = len(sizes)
	}

	for i := 0; i < displayCount; i++ {
		size := sizes[i]
		count := sizeDistribution[size]
		fmt.Printf("  크기 %d의 집합: %d개\n", size, count)
	}

	// 평균 집합 크기 계산
	totalElements := sampleCount
	avgSetSize := float64(totalElements) / float64(disjointSetCount)

	fmt.Printf("샘플링된 %d개 원소, %d개 서로소 집합 발견\n",
		sampleCount, disjointSetCount)
	fmt.Printf("평균 집합 크기: %.2f\n", avgSetSize)
	fmt.Printf("서로소 집합 계산 완료. 소요 시간: %v\n", time.Since(start))

	return disjointSetCount
}

// 일관성 검증 함수 - 각 샘플링된 원소 집합이 올바르게 연결되어 있는지 확인
func verifyConsistency(uf *MMapUnionFind, testSets []TestSet) {
	fmt.Println("집합 일관성 검증 시작...")
	start := time.Now()

	// 무작위로 일부 테스트 집합 선택 (최대 1000개)
	sampleCount := 1000
	if len(testSets) < sampleCount {
		sampleCount = len(testSets)
	}

	// 검증할 집합 인덱스 무작위 선택
	indices := make([]int, sampleCount)
	for i := 0; i < sampleCount; i++ {
		indices[i] = rand.Intn(len(testSets))
	}

	inconsistentCount := 0
	verifiedCount := 0

	// 각 집합의 모든 원소가 같은 루트를 공유하는지 확인
	for _, idx := range indices {
		set := testSets[idx]

		if len(set.Elements) < 2 {
			continue // 원소가 1개 이하면 건너뜀
		}

		// 첫 번째 원소의 루트 찾기
		firstRoot := uf.Find(set.Elements[0])
		consistent := true

		// 나머지 원소들이 같은 루트를 가지는지 확인
		for i := 1; i < len(set.Elements); i++ {
			otherRoot := uf.Find(set.Elements[i])
			if firstRoot != otherRoot {
				consistent = false
				break
			}
		}

		if !consistent {
			inconsistentCount++
			if inconsistentCount < 10 { // 처음 10개만 출력
				fmt.Printf("불일치 발견: 집합 ID %d의 원소들이 서로 다른 루트를 가짐\n", set.ID)
			}
		}

		verifiedCount++
	}

	consistency := 100.0 * float64(verifiedCount-inconsistentCount) / float64(verifiedCount)
	fmt.Printf("집합 일관성 검증 완료: %d개 검증, %d개 불일치 발견 (일관성: %.2f%%)\n",
		verifiedCount, inconsistentCount, consistency)
	fmt.Printf("검증 소요 시간: %v\n", time.Since(start))
}

// 메모리 사용량 출력 함수
func printMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("할당된 메모리: %d MB\n", m.Alloc/1024/1024)
	fmt.Printf("시스템 메모리: %d MB\n", m.Sys/1024/1024)
	fmt.Printf("GC 횟수: %d\n", m.NumGC)
}

func main() {
	// 시작 시간 기록
	startTime := time.Now()

	// 랜덤 시드 설정
	rand.Seed(time.Now().UnixNano())

	// 메모리 사용량 출력
	fmt.Println("초기 메모리 사용량:")
	printMemUsage()

	// 1. 유니언 파인드 초기화
	fmt.Println("유니언 파인드 초기화 중...")
	uf, err := NewMMapUnionFind(MaxElements, ParentFilePath, RankFilePath, RootFilePath)
	if err != nil {
		log.Fatalf("유니언 파인드 초기화 실패: %v", err)
	}

	defer func() {
		fmt.Println("리소스 정리 중...")
		if err := uf.CloseAndCleanup(false); err != nil {
			log.Printf("리소스 정리 중 오류 발생: %v", err)
		}
	}()

	// 메모리 사용량 출력
	fmt.Println("초기화 후 메모리 사용량:")
	printMemUsage()

	// 2. 테스트 케이스 생성 (추적 가능)
	fmt.Println("===== 테스트 케이스 생성 및 처리 시작 =====")

	// 2원소 집합 및 다중 원소 집합 생성
	testSets := generateTestCases(TwoElementSets, MultiElementSets)

	// 메모리 사용량 출력
	fmt.Println("테스트 케이스 생성 후 메모리 사용량:")
	printMemUsage()

	// 3. 테스트 케이스 처리
	processTestCases(uf, testSets)

	// 메모리 사용량 출력
	fmt.Println("테스트 케이스 처리 후 메모리 사용량:")
	printMemUsage()

	// 4. 서로소 집합 수 계산
	disjointSetCount := countDisjointSets(uf)

	// 5. 일관성 검증
	verifyConsistency(uf, testSets)

	// 6. 결과 요약
	fmt.Println("\n===== 결과 요약 =====")
	fmt.Printf("총 원소 수: %d\n", MaxElements)
	fmt.Printf("서로소 집합 수: %d\n", disjointSetCount)
	fmt.Printf("총 소요 시간: %v\n", time.Since(startTime))
}
