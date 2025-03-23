package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"runtime"
	"sort"
	"sync"

	"slices"

	"golang.org/x/exp/mmap"
)

func (ds *MmapDisjointSet) Find(x uint64) uint64 {
	if x > ds.maxElement {
		return x // 범위를 벗어난 원소는 자기 자신이 대표
	}

	// 원소가 존재하지 않으면 생성
	parent := ds.readParent(x)
	if parent == 0 { // 0은 초기화되지 않은 값
		ds.MakeSet(x)
		return x
	}

	// 경로 압축을 위한 재귀 호출
	if parent != x {
		newParent := ds.Find(parent)
		ds.writeParent(x, newParent)
		return newParent
	}

	return parent
}

// BatchFind는 여러 원소에 대해 한 번에 Find 연산을 수행합니다.
func (ds *MmapDisjointSet) BatchFind(elements []uint64) map[uint64]uint64 {
	result := make(map[uint64]uint64)

	// 원소들을 청크별로 그룹화
	elementsByChunk := make(map[uint64][]uint64)
	for _, x := range elements {
		if x > ds.maxElement {
			result[x] = x // 범위를 벗어난 원소는 자기 자신이 대표
			continue
		}
		chunkID := ds.getChunkID(x)
		elementsByChunk[chunkID] = append(elementsByChunk[chunkID], x)
	}

	// 각 청크별로 처리
	for _, chunkElements := range elementsByChunk {
		for _, x := range chunkElements {
			result[x] = ds.Find(x)
		}
	}

	return result
}

// BatchUnion은 여러 원소 쌍에 대해 한 번에 Union 연산을 수행합니다.
func (ds *MmapDisjointSet) BatchUnion(pairs [][2]uint64) {
	// 처리하기 전에 모든 보류 중인 업데이트 반영
	ds.processUpdateQueue()

	// 모든 원소 추적 및 루트 정보 수집
	allElements := make([]uint64, 0, len(pairs)*2)
	for _, pair := range pairs {
		allElements = append(allElements, pair[0], pair[1])
	}

	// 중복 제거
	uniqueElements := make(map[uint64]bool)
	for _, elem := range allElements {
		uniqueElements[elem] = true
	}

	// 고유 원소 목록 생성
	elements := make([]uint64, 0, len(uniqueElements))
	for elem := range uniqueElements {
		elements = append(elements, elem)
	}

	// 모든 원소의 루트 찾기
	rootMap := ds.BatchFind(elements)

	// Union 작업 추적을 위한 구조
	type UnionOp struct {
		from, to uint64
	}

	// 루트 간 병합 계획 수립 (중복 제거)
	rootUnions := make(map[uint64]uint64) // from -> to

	// 각 쌍에 대해 처리
	for _, pair := range pairs {
		rootX := rootMap[pair[0]]
		rootY := rootMap[pair[1]]

		if rootX == rootY {
			continue // 이미 같은 집합
		}

		// 랭크 비교 및 병합 방향 결정
		rankX := ds.readRank(rootX)
		rankY := ds.readRank(rootY)

		var from, to uint64
		var rankIncrease bool

		if rankX < rankY {
			from, to = rootX, rootY
			rankIncrease = false
		} else {
			from, to = rootY, rootX
			rankIncrease = (rankX == rankY)

			// 랭크 증가가 필요하면 미리 계산
			if rankIncrease {
				ds.writeRank(rootX, rankX+1)
			}
		}

		// 루트 매핑 업데이트 (트랜지티브 클로저 처리)
		// 자체 경로압축 느낌.
		finalTo := to
		for {
			if nextTo, exists := rootUnions[finalTo]; exists {
				finalTo = nextTo
			} else {
				break
			}
		}
		rootUnions[from] = finalTo

		// 루트맵 업데이트 (같은 배치 내에서의 일관성)
		for elem, root := range rootMap {
			if root == from {
				rootMap[elem] = finalTo
			}
		}
	}

	// 3. 실제 병합 수행
	for from, to := range rootUnions {
		// 집합 부모 포인터 업데이트
		ds.writeParent(from, to)
	}

	// 변경사항 즉시 적용
	ds.Flush()
}

// Union은 두 원소가 속한 집합을 합칩니다.
// 랭크에 따른 병합 최적화를 적용합니다.
func (ds *MmapDisjointSet) Union(x, y uint64) {
	// 먼저 모든 보류 중인 업데이트 처리
	ds.processUpdateQueue()

	rootX := ds.Find(x)
	rootY := ds.Find(y)

	if rootX == rootY {
		return // 이미 같은 집합에 속해 있음
	}

	// 랭크에 따라 병합 (랭크가 작은 트리를 랭크가 큰 트리에 붙임)
	rankX := ds.readRank(rootX)
	rankY := ds.readRank(rootY)

	if rankX < rankY {
		// 부모 포인터 업데이트
		ds.writeParent(rootX, rootY)
	} else {
		// 부모 포인터 업데이트
		ds.writeParent(rootY, rootX)

		// 두 트리의 랭크가 같으면 결과 트리의 랭크를 증가
		if rankX == rankY {
			ds.writeRank(rootX, rankX+1)
		}
	}

	// 모든 변경사항 즉시 적용
	if ds.inMemoryMode {
		ds.Flush()
	}
}

// GetSetsChunked는 현재 분리된 모든 집합들을 청크 단위로 계산하여 반환합니다.
func (ds *MmapDisjointSet) GetSetsChunked() map[uint64][]uint64 {
	// 지연된 업데이트 모두 처리
	ds.processUpdateQueue()

	result := make(map[uint64][]uint64)
	chunkCount := (ds.maxElement / ChunkSize) + 1

	// 현재 청크 작업을 추적하기 위한 맵
	activeChunks := make(map[uint64]bool)

	// 청크 단위로 처리
	for chunk := uint64(0); chunk < chunkCount; chunk++ {
		chunkData, err := ds.loadChunk(chunk)
		if err != nil {
			continue
		}

		activeChunks[chunk] = true

		// 청크에 원소가 없으면 스킵
		if len(chunkData.Elements) == 0 {
			continue
		}

		// 현재 청크의 원소들을 처리
		for element := range chunkData.Elements {
			parent := ds.readParent(element)
			if parent == 0 { // 초기화되지 않은 원소 건너뛰기
				continue
			}

			root := ds.Find(element) // 경로 압축이 여기서도 발생

			if _, exists := result[root]; !exists {
				result[root] = []uint64{}
			}
			result[root] = append(result[root], element)
		}

		// 100개 청크마다 사용하지 않는 청크 언로드
		if chunk%HoldableChunks == HoldableChunks-1 {
			ds.unloadUnusedChunks(activeChunks)
			activeChunks = make(map[uint64]bool)
			for i := chunk - (HoldableChunks - 1); i <= chunk; i++ {
				activeChunks[i] = true
			}
		}
	}

	// 각 집합 내부를 정렬 (선택적)
	for root := range result {
		slices.Sort(result[root])
	}

	return result
}

// PrintSets는 현재 모든 분리 집합과 그 원소들을 출력합니다.
func (ds *MmapDisjointSet) PrintSets() {
	sets := ds.GetSetsChunked()

	fmt.Println("현재 분리 집합 상태:")
	fmt.Printf("총 %d개의 집합이 있습니다.\n", len(sets))

	for root, elements := range sets {
		fmt.Printf("집합 %d (원소 개수: %d): %v\n", root, len(elements), elements)
	}
}

// GetStats는 분리 집합의 통계 정보를 반환합니다.
func (ds *MmapDisjointSet) GetStats() map[string]interface{} {
	stats := make(map[string]interface{})

	stats["maxElement"] = ds.maxElement
	stats["elementCount"] = ds.elementCount
	stats["fileSize"] = fileSize(ds.dataFile)
	stats["loadedChunks"] = len(ds.chunks)
	stats["pendingUpdates"] = len(ds.updateQueue)
	stats["inMemoryMode"] = ds.inMemoryMode

	return stats
}

// 파일 크기 반환 헬퍼 함수
func fileSize(filePath string) int64 {
	info, err := os.Stat(filePath)
	if err != nil {
		return -1
	}
	return info.Size()
}

const (
	HeaderSize     = 16                                  // 헤더 크기 (바이트): maxElement(8) + elementCount(8)
	EntrySize      = 12                                  // 각 엔트리의 크기 (바이트): parent(8) + rank(4)
	PageSize       = 4096                                // 일반적인 시스템 페이지 크기
	EntriesPerPage = (PageSize - HeaderSize) / EntrySize // 한 페이지당 엔트리 수
	ChunkSize      = PageSize * 128                      // 한 번에 처리할 원소의 개수.
	// 차지하는 메모리는 여기에 x12
	// 차지하는 페이지는 상수값x12

	HoldableChunks = 12

	// 엔트리 내 필드 오프셋
	ParentOffset = 0 // 부모 포인터 오프셋 (8바이트)
	RankOffset   = 8 // 랭크 오프셋 (4바이트)

	// 청크 관련 상수
	MaxUpdatesBeforeFlush = 100_000 // 이 개수만큼 업데이트가 쌓이면 자동으로 플러시
	GapElementsForSync    = 100_000
)

// ElementData는 원소의 모든 데이터를 포함하는 구조체입니다.
type ElementData struct {
	Parent uint64
	Rank   int
}

// ChunkData는 하나의 청크에 대한 모든 데이터를 포함하는 구조체입니다.
type ChunkData struct {
	Elements map[uint64]ElementData // 원소 ID -> 데이터 매핑
	Modified bool                   // 변경 여부 플래그
}

type UpdateType int

const (
	UpdateParent UpdateType = iota
	UpdateRank
	UpdateAll
)

// UpdateOperation은 업데이트 종류와 관련 데이터를 포함하는 구조체입니다.
type UpdateOperation struct {
	ElementID uint64
	Type      UpdateType // "parent", "rank", "all"
	Data      ElementData
}

// MmapDisjointSet은 mmap 기반 분리 집합 구조체입니다.
type MmapDisjointSet struct {
	dataFile     string                // 데이터 정보를 저장하는 파일 경로
	dataReaderAt *mmap.ReaderAt        // 데이터 파일의 mmap 리더
	dataWriter   *os.File              // 데이터 파일 쓰기용
	maxElement   uint64                // 최대 원소 ID
	elementCount uint64                // 전체 원소 수
	chunks       map[uint64]*ChunkData // 청크 ID -> 청크 데이터 맵
	updateQueue  []UpdateOperation     // 지연된 업데이트 작업 큐
	chunkMutex   sync.RWMutex          // 청크 맵 접근 동기화를 위한 mutex
	queueMutex   sync.Mutex            // 업데이트 큐 접근 동기화를 위한 mutex
	inMemoryMode bool                  // 메모리 우선 모드 (true: 모든 변경을 메모리에 버퍼링, false: 즉시 디스크에 기록)
}

// NewMmapDisjointSet은 새로운 mmap 기반 DisjointSet 인스턴스를 생성합니다.
func NewMmapDisjointSet(dataFile string, maxElement uint64, inMemoryMode bool) (*MmapDisjointSet, error) {
	// 파일이 존재하는지 확인하고, 없으면 생성
	dataExists := fileExists(dataFile)

	// 데이터 파일 열기 또는 생성
	dataWriter, err := os.OpenFile(dataFile, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, fmt.Errorf("데이터 파일을 열 수 없음: %v", err)
	}

	// 새 파일이면 초기화 - 데이터 파일
	dataFileSize := HeaderSize + int64(maxElement+1)*EntrySize
	if !dataExists {
		if err := dataWriter.Truncate(dataFileSize); err != nil {
			dataWriter.Close()
			return nil, fmt.Errorf("데이터 파일 크기를 설정할 수 없음: %v", err)
		}
		// 헤더 초기화
		header := make([]byte, HeaderSize)
		binary.LittleEndian.PutUint64(header[0:8], maxElement)
		binary.LittleEndian.PutUint64(header[8:16], 0) // 초기 elementCount
		if _, err := dataWriter.WriteAt(header, 0); err != nil {
			dataWriter.Close()
			return nil, fmt.Errorf("데이터 파일 헤더를 쓸 수 없음: %v", err)
		}
	}

	// mmap 리더 생성 - 데이터 파일
	dataReaderAt, err := mmap.Open(dataFile)
	if err != nil {
		dataWriter.Close()
		return nil, fmt.Errorf("데이터 파일을 메모리에 매핑할 수 없음: %v", err)
	}

	// 인스턴스 생성
	ds := &MmapDisjointSet{
		dataFile:     dataFile,
		dataReaderAt: dataReaderAt,
		dataWriter:   dataWriter,
		maxElement:   maxElement,
		chunks:       make(map[uint64]*ChunkData),
		updateQueue:  make([]UpdateOperation, 0, MaxUpdatesBeforeFlush),
		inMemoryMode: inMemoryMode,
	}

	// 헤더에서 elementCount 읽기
	elementCountBytes := make([]byte, 8)
	ds.dataReaderAt.ReadAt(elementCountBytes, 8)
	ds.elementCount = binary.LittleEndian.Uint64(elementCountBytes)

	return ds, nil
}

// 파일 존재 여부 확인 헬퍼 함수
func fileExists(filename string) bool {
	_, err := os.Stat(filename)
	return err == nil
}

// getChunkID는 원소 ID에 해당하는 청크 ID를 반환합니다.
func (ds *MmapDisjointSet) getChunkID(elementID uint64) uint64 {
	return elementID / ChunkSize
}

// loadChunk는 주어진 청크 ID에 해당하는 청크 데이터를 로드합니다.
func (ds *MmapDisjointSet) loadChunk(chunkID uint64) (*ChunkData, error) {
	// 이미 로드된 청크인지 확인
	ds.chunkMutex.RLock()
	chunk, exists := ds.chunks[chunkID]
	ds.chunkMutex.RUnlock()

	if exists {
		return chunk, nil
	}

	// 새 청크 데이터 구조 생성
	chunk = &ChunkData{
		Elements: make(map[uint64]ElementData),
		Modified: false,
	}

	// 청크 시작 원소 ID와 끝 원소 ID 계산
	startElement := chunkID * ChunkSize
	endElement := (chunkID + 1) * ChunkSize
	if endElement > ds.maxElement {
		endElement = ds.maxElement + 1
	}

	// 청크 내 모든 원소 데이터 로드
	for elementID := startElement; elementID < endElement; elementID++ {
		offset := ds.getElementOffset(elementID)

		// 원소 데이터 읽기
		entryData := make([]byte, EntrySize)
		_, err := ds.dataReaderAt.ReadAt(entryData, offset)
		if err != nil {
			return nil, fmt.Errorf("청크 데이터 읽기 실패: %v", err)
		}

		// 바이트 데이터를 ElementData로 변환
		parent := binary.LittleEndian.Uint64(entryData[ParentOffset : ParentOffset+8])
		rank := int(binary.LittleEndian.Uint32(entryData[RankOffset : RankOffset+4]))

		// 초기화되지 않은 원소는 스킵 (parent가 0인 경우)
		if parent != 0 {
			chunk.Elements[elementID] = ElementData{
				Parent: parent,
				Rank:   rank,
			}
		}
	}

	// 청크 맵에 저장
	ds.chunkMutex.Lock()
	ds.chunks[chunkID] = chunk
	ds.chunkMutex.Unlock()

	return chunk, nil
}

// flushChunk는 변경된 청크 데이터를 디스크에 기록합니다.
func (ds *MmapDisjointSet) flushChunk(chunkID uint64) error {
	// 청크 데이터 가져오기
	ds.chunkMutex.RLock()
	chunk, exists := ds.chunks[chunkID]
	ds.chunkMutex.RUnlock()

	if !exists || !chunk.Modified {
		return nil // 청크가 없거나 변경되지 않은 경우 스킵
	}

	// 청크 내 원소 수에 따라 최적화 전략 결정
	elementCount := len(chunk.Elements)

	// 페이지폴트 2번이면 되는경우
	if elementCount <= 2 {
		// 버퍼 재사용으로 메모리 할당 줄이기
		entryData := make([]byte, EntrySize)

		for elementID, data := range chunk.Elements {
			offset := ds.getElementOffset(elementID)

			binary.LittleEndian.PutUint64(entryData[ParentOffset:ParentOffset+8], data.Parent)
			binary.LittleEndian.PutUint32(entryData[RankOffset:RankOffset+4], uint32(data.Rank))

			if _, err := ds.dataWriter.WriteAt(entryData, offset); err != nil {
				return fmt.Errorf("청크 데이터 쓰기 실패: %v", err)
			}
		}
	} else {

		// 청크 내 원소가 많을 경우, 변경 사항을 메모리에 모아서 일괄 쓰기

		// 1. pwrite를 위한 오프셋-데이터 쌍 수집
		type writeOp struct {
			offset int64
			data   []byte
		}

		writeOps := make([]writeOp, 0, elementCount)
		bufferPool := make([]byte, elementCount*EntrySize) // 전체 버퍼 미리 할당
		bufferOffset := 0

		for elementID, data := range chunk.Elements {
			offset := ds.getElementOffset(elementID)

			// 버퍼 풀에서 메모리 슬라이스 가져오기
			entryData := bufferPool[bufferOffset : bufferOffset+EntrySize]
			bufferOffset += EntrySize

			binary.LittleEndian.PutUint64(entryData[ParentOffset:ParentOffset+8], data.Parent)
			binary.LittleEndian.PutUint32(entryData[RankOffset:RankOffset+4], uint32(data.Rank))

			writeOps = append(writeOps, writeOp{
				offset: offset,
				data:   entryData,
			})
		}

		// 2. writev 시스템 콜과 유사하게 작동하는 함수 구현
		// 실제 writev가 있다면 그것을 사용하는 것이 더 좋음
		// 여기서는 여러 쓰기 작업을 하나의 함수 호출로 모음

		// 오프셋 기준으로 정렬 (디스크 접근 패턴 최적화)
		sort.Slice(writeOps, func(i, j int) bool {
			return writeOps[i].offset < writeOps[j].offset
		})

		if len(writeOps) > 0 {

			// 4. 병합된 작업 실행
			for _, op := range writeOps {
				if _, err := ds.dataWriter.WriteAt(op.data, op.offset); err != nil {
					return fmt.Errorf("병합된 청크 데이터 쓰기 실패: %v", err)
				}
			}

		}

	}

	// 청크 상태 업데이트
	ds.chunkMutex.Lock()
	chunk.Modified = false
	ds.chunkMutex.Unlock()

	return nil
}

// FlushAllChunks는 모든 변경된 청크를 디스크에 기록합니다.
func (ds *MmapDisjointSet) FlushAllChunks() error {
	// 지연된 업데이트 선처리
	ds.processUpdateQueue()

	// 모든 청크 ID 목록 가져오기
	ds.chunkMutex.RLock()
	chunkIDs := make([]uint64, 0, len(ds.chunks))
	for chunkID := range ds.chunks {
		chunkIDs = append(chunkIDs, chunkID)
	}
	ds.chunkMutex.RUnlock()

	// 각 청크 플러시
	for _, chunkID := range chunkIDs {
		if err := ds.flushChunk(chunkID); err != nil {
			return err
		}
	}

	// elementCount 업데이트
	countBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(countBytes, ds.elementCount)
	ds.dataWriter.WriteAt(countBytes, 8)

	// 명시적 파일 동기화
	ds.dataWriter.Sync()

	return nil
}

// unloadUnusedChunks는 메모리에서 사용하지 않는 청크를 언로드합니다.
func (ds *MmapDisjointSet) unloadUnusedChunks(keepChunkIDs map[uint64]bool) {
	// 유지할 청크 ID 집합에 없는 청크는 제거
	ds.chunkMutex.Lock()
	defer ds.chunkMutex.Unlock()

	for chunkID, chunk := range ds.chunks {
		if !keepChunkIDs[chunkID] {
			// 변경된 청크는 먼저 디스크에 기록
			if chunk.Modified {
				ds.flushChunk(chunkID) // 에러 처리는 단순화를 위해 생략
			}
			delete(ds.chunks, chunkID)
		}
	}
}

// Close는 리소스를 정리합니다.
func (ds *MmapDisjointSet) Close() error {
	// 모든 변경사항 디스크에 반영
	if err := ds.FlushAllChunks(); err != nil {
		return err
	}

	// 파일 핸들 닫기
	ds.dataReaderAt.Close()
	ds.dataWriter.Close()
	return nil
}

// Flush는 변경사항을 파일에 반영합니다.
func (ds *MmapDisjointSet) Flush() error {
	return ds.FlushAllChunks()
}

// getElementOffset은 주어진 원소의 데이터가 저장된 파일 오프셋을 반환합니다.
func (ds *MmapDisjointSet) getElementOffset(x uint64) int64 {
	return HeaderSize + int64(x)*EntrySize
}

// queueUpdate는 업데이트 작업을 큐에 추가합니다.
func (ds *MmapDisjointSet) queueUpdate(op UpdateOperation) {
	ds.queueMutex.Lock()
	defer ds.queueMutex.Unlock()

	ds.updateQueue = append(ds.updateQueue, op)

	// 큐 크기가 임계값을 넘으면 처리
	if len(ds.updateQueue) >= MaxUpdatesBeforeFlush {
		go ds.processUpdateQueue() // 비동기 처리
	}
}

// processUpdateQueue는 큐에 쌓인 업데이트 작업을 청크별로 그룹화하여 처리합니다.
func (ds *MmapDisjointSet) processUpdateQueue() {
	ds.queueMutex.Lock()
	updates := ds.updateQueue
	ds.updateQueue = make([]UpdateOperation, 0, MaxUpdatesBeforeFlush)
	ds.queueMutex.Unlock()

	if len(updates) == 0 {
		return
	}

	// 업데이트를 청크별로 그룹화
	chunkUpdates := make(map[uint64][]UpdateOperation)
	for _, update := range updates {
		chunkID := ds.getChunkID(update.ElementID)
		chunkUpdates[chunkID] = append(chunkUpdates[chunkID], update)
	}

	// 각 청크에 대해 처리
	for chunkID, chunkOps := range chunkUpdates {
		// 청크 로드
		chunk, err := ds.loadChunk(chunkID)
		if err != nil {
			fmt.Printf("청크 로드 실패: %v\n", err)
			continue
		}

		// 각 업데이트 적용
		for _, op := range chunkOps {
			element, exists := chunk.Elements[op.ElementID]
			if !exists && op.Type != UpdateAll {
				// 원소가 없고 'all' 타입이 아니면 초기 데이터 생성
				element = ElementData{
					Parent: op.ElementID, // 자기 자신이 부모
					Rank:   0,
				}
			}

			// 업데이트 타입에 따라 처리
			switch op.Type {
			case UpdateParent:
				element.Parent = op.Data.Parent
			case UpdateRank:
				element.Rank = op.Data.Rank
			case UpdateAll:
				element = op.Data
			}

			// 원소 데이터 저장
			chunk.Elements[op.ElementID] = element
			chunk.Modified = true
		}
	}

	// 메모리 모드가 아닌 경우 변경된 청크 즉시 디스크에 반영
	if !ds.inMemoryMode {
		for chunkID := range chunkUpdates {
			if err := ds.flushChunk(chunkID); err != nil {
				fmt.Printf("청크 플러시 실패: %v\n", err)
			}
		}
	}
}

// getElementData는 주어진 원소의 모든 데이터를 가져옵니다.
func (ds *MmapDisjointSet) getElementData(x uint64) (ElementData, bool) {
	if x > ds.maxElement {
		return ElementData{Parent: x, Rank: 0}, false
	}

	chunkID := ds.getChunkID(x)
	chunk, err := ds.loadChunk(chunkID)
	if err != nil {
		return ElementData{}, false
	}

	data, exists := chunk.Elements[x]
	if !exists {
		return ElementData{}, false
	}

	return data, true
}

// updateElementData는 주어진 원소의 데이터를 업데이트합니다.
func (ds *MmapDisjointSet) updateElementData(x uint64, data ElementData) {
	if x > ds.maxElement {
		return
	}

	// 메모리 우선 모드 또는 임의 접근 모드에 따라 처리
	if ds.inMemoryMode {
		// 업데이트 큐에 추가
		ds.queueUpdate(UpdateOperation{
			ElementID: x,
			Type:      UpdateAll,
			Data:      data,
		})
	} else {
		// 직접 청크에 접근하여 업데이트
		chunkID := ds.getChunkID(x)
		chunk, err := ds.loadChunk(chunkID)
		if err != nil {
			return
		}

		chunk.Elements[x] = data
		chunk.Modified = true

		// 직접 디스크에 쓰기 (non-batched mode)
		if !ds.inMemoryMode {
			offset := ds.getElementOffset(x)
			entryData := make([]byte, EntrySize)
			binary.LittleEndian.PutUint64(entryData[ParentOffset:ParentOffset+8], data.Parent)
			binary.LittleEndian.PutUint32(entryData[RankOffset:RankOffset+4], uint32(data.Rank))
			ds.dataWriter.WriteAt(entryData, offset)
		}
	}
}

// readParent는 주어진 원소의 부모를 읽습니다.
func (ds *MmapDisjointSet) readParent(x uint64) uint64 {
	if x > ds.maxElement {
		return x // 범위를 벗어난 원소는 자기 자신이 부모
	}

	data, exists := ds.getElementData(x)
	if !exists {
		return 0 // 초기화되지 않은 원소
	}

	return data.Parent
}

// writeParent는 주어진 원소의 부모를 씁니다.
func (ds *MmapDisjointSet) writeParent(x, parent uint64) {
	if x > ds.maxElement {
		return // 범위를 벗어난 원소는 무시
	}

	data, exists := ds.getElementData(x)
	if !exists {
		// 초기화되지 않은 원소는 기본값으로 생성
		data = ElementData{
			Parent: x,
			Rank:   0,
		}
	}

	data.Parent = parent
	ds.updateElementData(x, data)
}

// readRank는 주어진 원소의 랭크를 읽습니다.
func (ds *MmapDisjointSet) readRank(x uint64) int {
	if x > ds.maxElement {
		return 0 // 범위를 벗어난 원소는 랭크 0
	}

	data, exists := ds.getElementData(x)
	if !exists {
		return 0 // 초기화되지 않은 원소
	}

	return data.Rank
}

// writeRank는 주어진 원소의 랭크를 씁니다.
func (ds *MmapDisjointSet) writeRank(x uint64, rank int) {
	if x > ds.maxElement {
		return // 범위를 벗어난 원소는 무시
	}

	data, exists := ds.getElementData(x)
	if !exists {
		// 초기화되지 않은 원소는 기본값으로 생성
		data = ElementData{
			Parent: x,
			Rank:   0,
		}
	}

	data.Rank = rank
	ds.updateElementData(x, data)
}

// BatchMakeSet은 여러 원소를 한 번에 집합으로 추가합니다.
func (ds *MmapDisjointSet) BatchMakeSet(elements []uint64) {
	// 원소들을 청크별로 그룹화
	elementsByChunk := make(map[uint64][]uint64)
	for _, x := range elements {
		if x > ds.maxElement {
			continue
		}
		chunkID := ds.getChunkID(x)
		elementsByChunk[chunkID] = append(elementsByChunk[chunkID], x)
	}

	// 각 청크별로 처리
	for chunkID, chunkElements := range elementsByChunk {
		chunk, err := ds.loadChunk(chunkID)
		if err != nil {
			continue
		}

		for _, x := range chunkElements {
			_, exists := chunk.Elements[x]
			if exists {
				continue // 이미 존재하는 원소는 무시
			}

			// 자기 자신을 부모로 설정
			chunk.Elements[x] = ElementData{
				Parent: x,
				Rank:   0,
			}

			chunk.Modified = true
			ds.elementCount++
		}
	}

	// 메모리 모드가 아닌 경우 변경된 청크 즉시 디스크에 반영
	if !ds.inMemoryMode {
		for chunkID := range elementsByChunk {
			ds.flushChunk(chunkID)
		}
	}

	// 일정 간격으로 파일 동기화
	if ds.elementCount%GapElementsForSync == 0 {
		runtime.GC() // 명시적 GC 유도
		ds.Flush()   // 파일 동기화
	}
}

// MakeSet은 새로운 원소를 집합에 추가합니다.
func (ds *MmapDisjointSet) MakeSet(x uint64) {
	if x > ds.maxElement {
		return // 범위를 벗어난 원소는 무시
	}

	// 이미 존재하는지 확인
	parent := ds.readParent(x)
	if parent != 0 { // 0은 초기화되지 않은 값
		return
	}

	// 자기 자신을 부모로 설정
	ds.updateElementData(x, ElementData{
		Parent: x,
		Rank:   0,
	})

	ds.elementCount++

	// 일정 간격으로 파일 동기화
	if ds.elementCount%GapElementsForSync == 0 {
		runtime.GC() // 명시적 GC 유도
		ds.Flush()   // 파일 동기화
	}
}

//
// package main

// import (
// 	"encoding/binary"
// 	"fmt"
// 	"os"
// 	"runtime"
// 	"sort"
// 	"sync"

// 	"slices"

// 	"golang.org/x/exp/mmap"
// )

// const (
// 	// 파일 레이아웃 상수
// 	HeaderSize     = 16                                  // 헤더 크기 (바이트): maxElement(8) + elementCount(8)
// 	EntrySize      = 16                                  // 각 엔트리의 크기 (바이트): parent(8) + rank(4) + setSize(2) + padding(2)
// 	PageSize       = 4096                                // 일반적인 시스템 페이지 크기
// 	EntriesPerPage = (PageSize - HeaderSize) / EntrySize // 한 페이지당 엔트리 수
// 	ChunkSize      = PageSize * 128                      // 한 번에 처리할 원소의 개수.
// 	// 차지하는 메모리는 여기에 x16
// 	// 차지하는 페이지는 상수값x16

// 	HoldableChunks = 12

// 	// 엔트리 내 필드 오프셋
// 	ParentOffset  = 0  // 부모 포인터 오프셋 (8바이트)
// 	RankOffset    = 8  // 랭크 오프셋 (4바이트)
// 	SetSizeOffset = 12 // 집합 크기 오프셋 (2바이트)

// 	// 청크 관련 상수
// 	MaxUpdatesBeforeFlush = 100_000 // 이 개수만큼 업데이트가 쌓이면 자동으로 플러시
// 	GapElementsForSync    = 100_000
// )

// // ElementData는 원소의 모든 데이터를 포함하는 구조체입니다.
// type ElementData struct {
// 	Parent  uint64
// 	Rank    int
// 	SetSize uint16
// }

// // ChunkData는 하나의 청크에 대한 모든 데이터를 포함하는 구조체입니다.
// type ChunkData struct {
// 	Elements map[uint64]ElementData // 원소 ID -> 데이터 매핑
// 	Modified bool                   // 변경 여부 플래그
// }

// type UpdateType int

// const (
// 	UpdateParent UpdateType = iota
// 	UpdateRank
// 	UpdateSize
// 	UpdateAll
// )

// // UpdateOperation은 업데이트 종류와 관련 데이터를 포함하는 구조체입니다.
// type UpdateOperation struct {
// 	ElementID uint64
// 	Type      UpdateType // "parent", "rank", "setSize", "all"
// 	Data      ElementData
// }

// // SetSizeCache는 집합 크기 캐싱을 위한 구조체입니다.
// type SetSizeCache struct {
// 	// rootID -> setSize
// 	Sizes map[uint64]uint16
// 	sync.RWMutex
// }

// // MmapDisjointSet은 mmap 기반 분리 집합 구조체입니다.
// type MmapDisjointSet struct {
// 	dataFile     string                // 데이터 정보를 저장하는 파일 경로
// 	dataReaderAt *mmap.ReaderAt        // 데이터 파일의 mmap 리더
// 	dataWriter   *os.File              // 데이터 파일 쓰기용
// 	maxElement   uint64                // 최대 원소 ID
// 	elementCount uint64                // 전체 원소 수
// 	chunks       map[uint64]*ChunkData // 청크 ID -> 청크 데이터 맵
// 	updateQueue  []UpdateOperation     // 지연된 업데이트 작업 큐
// 	chunkMutex   sync.RWMutex          // 청크 맵 접근 동기화를 위한 mutex
// 	queueMutex   sync.Mutex            // 업데이트 큐 접근 동기화를 위한 mutex
// 	inMemoryMode bool                  // 메모리 우선 모드 (true: 모든 변경을 메모리에 버퍼링, false: 즉시 디스크에 기록)
// 	sizeCache    *SetSizeCache         // 집합 크기 캐시
// }

// // NewMmapDisjointSet은 새로운 mmap 기반 DisjointSet 인스턴스를 생성합니다.
// func NewMmapDisjointSet(dataFile string, maxElement uint64, inMemoryMode bool) (*MmapDisjointSet, error) {
// 	// 파일이 존재하는지 확인하고, 없으면 생성
// 	dataExists := fileExists(dataFile)

// 	// 데이터 파일 열기 또는 생성
// 	dataWriter, err := os.OpenFile(dataFile, os.O_RDWR|os.O_CREATE, 0666)
// 	if err != nil {
// 		return nil, fmt.Errorf("데이터 파일을 열 수 없음: %v", err)
// 	}

// 	// 새 파일이면 초기화 - 데이터 파일
// 	dataFileSize := HeaderSize + int64(maxElement+1)*EntrySize
// 	if !dataExists {
// 		if err := dataWriter.Truncate(dataFileSize); err != nil {
// 			dataWriter.Close()
// 			return nil, fmt.Errorf("데이터 파일 크기를 설정할 수 없음: %v", err)
// 		}
// 		// 헤더 초기화
// 		header := make([]byte, HeaderSize)
// 		binary.LittleEndian.PutUint64(header[0:8], maxElement)
// 		binary.LittleEndian.PutUint64(header[8:16], 0) // 초기 elementCount
// 		if _, err := dataWriter.WriteAt(header, 0); err != nil {
// 			dataWriter.Close()
// 			return nil, fmt.Errorf("데이터 파일 헤더를 쓸 수 없음: %v", err)
// 		}
// 	}

// 	// mmap 리더 생성 - 데이터 파일
// 	dataReaderAt, err := mmap.Open(dataFile)
// 	if err != nil {
// 		dataWriter.Close()
// 		return nil, fmt.Errorf("데이터 파일을 메모리에 매핑할 수 없음: %v", err)
// 	}

// 	// 인스턴스 생성
// 	ds := &MmapDisjointSet{
// 		dataFile:     dataFile,
// 		dataReaderAt: dataReaderAt,
// 		dataWriter:   dataWriter,
// 		maxElement:   maxElement,
// 		chunks:       make(map[uint64]*ChunkData),
// 		updateQueue:  make([]UpdateOperation, 0, MaxUpdatesBeforeFlush),
// 		inMemoryMode: inMemoryMode,
// 		sizeCache:    &SetSizeCache{Sizes: make(map[uint64]uint16)},
// 	}

// 	// 헤더에서 elementCount 읽기
// 	elementCountBytes := make([]byte, 8)
// 	ds.dataReaderAt.ReadAt(elementCountBytes, 8)
// 	ds.elementCount = binary.LittleEndian.Uint64(elementCountBytes)

// 	return ds, nil
// }

// // 파일 존재 여부 확인 헬퍼 함수
// func fileExists(filename string) bool {
// 	_, err := os.Stat(filename)
// 	return err == nil
// }

// // getChunkID는 원소 ID에 해당하는 청크 ID를 반환합니다.
// func (ds *MmapDisjointSet) getChunkID(elementID uint64) uint64 {
// 	return elementID / ChunkSize
// }

// // loadChunk는 주어진 청크 ID에 해당하는 청크 데이터를 로드합니다.
// func (ds *MmapDisjointSet) loadChunk(chunkID uint64) (*ChunkData, error) {
// 	// 이미 로드된 청크인지 확인
// 	ds.chunkMutex.RLock()
// 	chunk, exists := ds.chunks[chunkID]
// 	ds.chunkMutex.RUnlock()

// 	if exists {
// 		return chunk, nil
// 	}

// 	// 새 청크 데이터 구조 생성
// 	chunk = &ChunkData{
// 		Elements: make(map[uint64]ElementData),
// 		Modified: false,
// 	}

// 	// 청크 시작 원소 ID와 끝 원소 ID 계산
// 	startElement := chunkID * ChunkSize
// 	endElement := (chunkID + 1) * ChunkSize
// 	if endElement > ds.maxElement {
// 		endElement = ds.maxElement + 1
// 	}

// 	// 청크 내 모든 원소 데이터 로드
// 	for elementID := startElement; elementID < endElement; elementID++ {
// 		offset := ds.getElementOffset(elementID)

// 		// 원소 데이터 읽기
// 		entryData := make([]byte, EntrySize)
// 		_, err := ds.dataReaderAt.ReadAt(entryData, offset)
// 		if err != nil {
// 			return nil, fmt.Errorf("청크 데이터 읽기 실패: %v", err)
// 		}

// 		// 바이트 데이터를 ElementData로 변환
// 		parent := binary.LittleEndian.Uint64(entryData[ParentOffset : ParentOffset+8])
// 		rank := int(binary.LittleEndian.Uint32(entryData[RankOffset : RankOffset+4]))
// 		setSize := binary.LittleEndian.Uint16(entryData[SetSizeOffset : SetSizeOffset+2])

// 		// 초기화되지 않은 원소는 스킵 (parent가 0인 경우)
// 		if parent != 0 {
// 			chunk.Elements[elementID] = ElementData{
// 				Parent:  parent,
// 				Rank:    rank,
// 				SetSize: setSize,
// 			}

// 			// 루트 노드이면 크기 캐시에 추가
// 			if parent == elementID && setSize > 0 {
// 				ds.cacheSizeForRoot(elementID, setSize)
// 			}
// 		}
// 	}

// 	// 청크 맵에 저장
// 	ds.chunkMutex.Lock()
// 	ds.chunks[chunkID] = chunk
// 	ds.chunkMutex.Unlock()

// 	return chunk, nil
// }

// // flushChunk는 변경된 청크 데이터를 디스크에 기록합니다.
// func (ds *MmapDisjointSet) flushChunk(chunkID uint64) error {
// 	// 청크 데이터 가져오기
// 	ds.chunkMutex.RLock()
// 	chunk, exists := ds.chunks[chunkID]
// 	ds.chunkMutex.RUnlock()

// 	if !exists || !chunk.Modified {
// 		return nil // 청크가 없거나 변경되지 않은 경우 스킵
// 	}

// 	// 청크 내 원소 수에 따라 최적화 전략 결정
// 	elementCount := len(chunk.Elements)

// 	// 페이지폴트 2번이면 되는경우
// 	if elementCount <= 2 {
// 		// 버퍼 재사용으로 메모리 할당 줄이기
// 		entryData := make([]byte, EntrySize)

// 		for elementID, data := range chunk.Elements {
// 			offset := ds.getElementOffset(elementID)

// 			binary.LittleEndian.PutUint64(entryData[ParentOffset:ParentOffset+8], data.Parent)
// 			binary.LittleEndian.PutUint32(entryData[RankOffset:RankOffset+4], uint32(data.Rank))
// 			binary.LittleEndian.PutUint16(entryData[SetSizeOffset:SetSizeOffset+2], data.SetSize)

// 			if _, err := ds.dataWriter.WriteAt(entryData, offset); err != nil {
// 				return fmt.Errorf("청크 데이터 쓰기 실패: %v", err)
// 			}
// 		}
// 	} else {
// 		println("청크 내 원소가 임계값 이상임. 배치 처리로 판단함.")
// 		// 청크 내 원소가 많을 경우, 변경 사항을 메모리에 모아서 일괄 쓰기

// 		// 1. pwrite를 위한 오프셋-데이터 쌍 수집
// 		type writeOp struct {
// 			offset int64
// 			data   []byte
// 		}

// 		writeOps := make([]writeOp, 0, elementCount)
// 		bufferPool := make([]byte, elementCount*EntrySize) // 전체 버퍼 미리 할당
// 		bufferOffset := 0

// 		for elementID, data := range chunk.Elements {
// 			offset := ds.getElementOffset(elementID)

// 			// 버퍼 풀에서 메모리 슬라이스 가져오기
// 			entryData := bufferPool[bufferOffset : bufferOffset+EntrySize]
// 			bufferOffset += EntrySize

// 			binary.LittleEndian.PutUint64(entryData[ParentOffset:ParentOffset+8], data.Parent)
// 			binary.LittleEndian.PutUint32(entryData[RankOffset:RankOffset+4], uint32(data.Rank))
// 			binary.LittleEndian.PutUint16(entryData[SetSizeOffset:SetSizeOffset+2], data.SetSize)

// 			writeOps = append(writeOps, writeOp{
// 				offset: offset,
// 				data:   entryData,
// 			})
// 		}

// 		// 2. writev 시스템 콜과 유사하게 작동하는 함수 구현
// 		// 실제 writev가 있다면 그것을 사용하는 것이 더 좋음
// 		// 여기서는 여러 쓰기 작업을 하나의 함수 호출로 모음

// 		// 오프셋 기준으로 정렬 (디스크 접근 패턴 최적화)
// 		sort.Slice(writeOps, func(i, j int) bool {
// 			return writeOps[i].offset < writeOps[j].offset
// 		})

// 		if len(writeOps) > 0 {

// 			// 4. 병합된 작업 실행
// 			for _, op := range writeOps {
// 				if _, err := ds.dataWriter.WriteAt(op.data, op.offset); err != nil {
// 					return fmt.Errorf("병합된 청크 데이터 쓰기 실패: %v", err)
// 				}
// 			}

// 		}

// 	}

// 	// 청크 상태 업데이트
// 	ds.chunkMutex.Lock()
// 	chunk.Modified = false
// 	ds.chunkMutex.Unlock()

// 	return nil
// }

// // FlushAllChunks는 모든 변경된 청크를 디스크에 기록합니다.
// func (ds *MmapDisjointSet) FlushAllChunks() error {
// 	// 지연된 업데이트 선처리
// 	ds.processUpdateQueue()

// 	// 모든 청크 ID 목록 가져오기
// 	ds.chunkMutex.RLock()
// 	chunkIDs := make([]uint64, 0, len(ds.chunks))
// 	for chunkID := range ds.chunks {
// 		chunkIDs = append(chunkIDs, chunkID)
// 	}
// 	ds.chunkMutex.RUnlock()

// 	// 각 청크 플러시
// 	for _, chunkID := range chunkIDs {
// 		if err := ds.flushChunk(chunkID); err != nil {
// 			return err
// 		}
// 	}

// 	// elementCount 업데이트
// 	countBytes := make([]byte, 8)
// 	binary.LittleEndian.PutUint64(countBytes, ds.elementCount)
// 	ds.dataWriter.WriteAt(countBytes, 8)

// 	// 명시적 파일 동기화
// 	ds.dataWriter.Sync()

// 	return nil
// }

// // unloadUnusedChunks는 메모리에서 사용하지 않는 청크를 언로드합니다.
// func (ds *MmapDisjointSet) unloadUnusedChunks(keepChunkIDs map[uint64]bool) {
// 	// 유지할 청크 ID 집합에 없는 청크는 제거
// 	ds.chunkMutex.Lock()
// 	defer ds.chunkMutex.Unlock()

// 	for chunkID, chunk := range ds.chunks {
// 		if !keepChunkIDs[chunkID] {
// 			// 변경된 청크는 먼저 디스크에 기록
// 			if chunk.Modified {
// 				ds.flushChunk(chunkID) // 에러 처리는 단순화를 위해 생략
// 			}
// 			delete(ds.chunks, chunkID)
// 		}
// 	}
// }

// // Close는 리소스를 정리합니다.
// func (ds *MmapDisjointSet) Close() error {
// 	// 모든 변경사항 디스크에 반영
// 	if err := ds.FlushAllChunks(); err != nil {
// 		return err
// 	}

// 	// 파일 핸들 닫기
// 	ds.dataReaderAt.Close()
// 	ds.dataWriter.Close()
// 	return nil
// }

// // Flush는 변경사항을 파일에 반영합니다.
// func (ds *MmapDisjointSet) Flush() error {
// 	return ds.FlushAllChunks()
// }

// // getElementOffset은 주어진 원소의 데이터가 저장된 파일 오프셋을 반환합니다.
// func (ds *MmapDisjointSet) getElementOffset(x uint64) int64 {
// 	return HeaderSize + int64(x)*EntrySize
// }

// // cacheSizeForRoot는 루트의 집합 크기를 캐시에 저장합니다.
// func (ds *MmapDisjointSet) cacheSizeForRoot(root uint64, size uint16) {
// 	ds.sizeCache.Lock()
// 	defer ds.sizeCache.Unlock()
// 	ds.sizeCache.Sizes[root] = size
// }

// // getCachedSetSize는 캐시된 집합 크기를 가져옵니다. 없으면 0을 반환합니다.
// func (ds *MmapDisjointSet) getCachedSetSize(root uint64) (uint16, bool) {
// 	ds.sizeCache.RLock()
// 	defer ds.sizeCache.RUnlock()
// 	size, exists := ds.sizeCache.Sizes[root]
// 	return size, exists
// }

// // clearSizeCacheForRoot는 루트의 캐시된 크기를 제거합니다.
// func (ds *MmapDisjointSet) clearSizeCacheForRoot(root uint64) {
// 	ds.sizeCache.Lock()
// 	defer ds.sizeCache.Unlock()
// 	delete(ds.sizeCache.Sizes, root)
// }

// // queueUpdate는 업데이트 작업을 큐에 추가합니다.
// func (ds *MmapDisjointSet) queueUpdate(op UpdateOperation) {
// 	ds.queueMutex.Lock()
// 	defer ds.queueMutex.Unlock()

// 	ds.updateQueue = append(ds.updateQueue, op)

// 	// 큐 크기가 임계값을 넘으면 처리
// 	if len(ds.updateQueue) >= MaxUpdatesBeforeFlush {
// 		go ds.processUpdateQueue() // 비동기 처리
// 	}
// }

// // processUpdateQueue는 큐에 쌓인 업데이트 작업을 청크별로 그룹화하여 처리합니다.
// func (ds *MmapDisjointSet) processUpdateQueue() {
// 	ds.queueMutex.Lock()
// 	updates := ds.updateQueue
// 	ds.updateQueue = make([]UpdateOperation, 0, MaxUpdatesBeforeFlush)
// 	ds.queueMutex.Unlock()

// 	if len(updates) == 0 {
// 		return
// 	}

// 	// 업데이트를 청크별로 그룹화
// 	chunkUpdates := make(map[uint64][]UpdateOperation)
// 	for _, update := range updates {
// 		chunkID := ds.getChunkID(update.ElementID)
// 		chunkUpdates[chunkID] = append(chunkUpdates[chunkID], update)
// 	}

// 	// 각 청크에 대해 처리
// 	for chunkID, chunkOps := range chunkUpdates {
// 		// 청크 로드
// 		chunk, err := ds.loadChunk(chunkID)
// 		if err != nil {
// 			fmt.Printf("청크 로드 실패: %v\n", err)
// 			continue
// 		}

// 		// 각 업데이트 적용
// 		for _, op := range chunkOps {
// 			element, exists := chunk.Elements[op.ElementID]
// 			if !exists && op.Type != UpdateAll {
// 				// 원소가 없고 'all' 타입이 아니면 초기 데이터 생성
// 				element = ElementData{
// 					Parent:  op.ElementID, // 자기 자신이 부모
// 					Rank:    0,
// 					SetSize: 1,
// 				}
// 			}

// 			// 업데이트 타입에 따라 처리
// 			switch op.Type {
// 			case UpdateParent:
// 				element.Parent = op.Data.Parent
// 			case UpdateRank:
// 				element.Rank = op.Data.Rank
// 			case UpdateSize:
// 				element.SetSize = op.Data.SetSize
// 				// 루트 노드인 경우 캐시 업데이트
// 				if op.ElementID == element.Parent {
// 					ds.cacheSizeForRoot(op.ElementID, op.Data.SetSize)
// 				}
// 			case UpdateAll:
// 				element = op.Data
// 				// 루트 노드인 경우 캐시 업데이트
// 				if op.Data.Parent == op.ElementID && op.Data.SetSize > 0 {
// 					ds.cacheSizeForRoot(op.ElementID, op.Data.SetSize)
// 				}
// 			}

// 			// 원소 데이터 저장
// 			chunk.Elements[op.ElementID] = element
// 			chunk.Modified = true
// 		}
// 	}

// 	// 메모리 모드가 아닌 경우 변경된 청크 즉시 디스크에 반영
// 	if !ds.inMemoryMode {
// 		for chunkID := range chunkUpdates {
// 			if err := ds.flushChunk(chunkID); err != nil {
// 				fmt.Printf("청크 플러시 실패: %v\n", err)
// 			}
// 		}
// 	}
// }

// // getElementData는 주어진 원소의 모든 데이터를 가져옵니다.
// func (ds *MmapDisjointSet) getElementData(x uint64) (ElementData, bool) {
// 	if x > ds.maxElement {
// 		return ElementData{Parent: x, Rank: 0, SetSize: 0}, false
// 	}

// 	chunkID := ds.getChunkID(x)
// 	chunk, err := ds.loadChunk(chunkID)
// 	if err != nil {
// 		return ElementData{}, false
// 	}

// 	data, exists := chunk.Elements[x]
// 	if !exists {
// 		return ElementData{}, false
// 	}

// 	return data, true
// }

// // updateElementData는 주어진 원소의 데이터를 업데이트합니다.
// func (ds *MmapDisjointSet) updateElementData(x uint64, data ElementData) {
// 	if x > ds.maxElement {
// 		return
// 	}

// 	// 루트 노드인 경우 캐시 업데이트
// 	if data.Parent == x && data.SetSize > 0 {
// 		ds.cacheSizeForRoot(x, data.SetSize)
// 	}

// 	// 메모리 우선 모드 또는 임의 접근 모드에 따라 처리
// 	if ds.inMemoryMode {
// 		// 업데이트 큐에 추가
// 		ds.queueUpdate(UpdateOperation{
// 			ElementID: x,
// 			Type:      UpdateAll,
// 			Data:      data,
// 		})
// 	} else {
// 		// 직접 청크에 접근하여 업데이트
// 		chunkID := ds.getChunkID(x)
// 		chunk, err := ds.loadChunk(chunkID)
// 		if err != nil {
// 			return
// 		}

// 		chunk.Elements[x] = data
// 		chunk.Modified = true

// 		// 직접 디스크에 쓰기 (non-batched mode)
// 		if !ds.inMemoryMode {
// 			offset := ds.getElementOffset(x)
// 			entryData := make([]byte, EntrySize)
// 			binary.LittleEndian.PutUint64(entryData[ParentOffset:ParentOffset+8], data.Parent)
// 			binary.LittleEndian.PutUint32(entryData[RankOffset:RankOffset+4], uint32(data.Rank))
// 			binary.LittleEndian.PutUint16(entryData[SetSizeOffset:SetSizeOffset+2], data.SetSize)
// 			ds.dataWriter.WriteAt(entryData, offset)
// 		}
// 	}
// }

// // readParent는 주어진 원소의 부모를 읽습니다.
// func (ds *MmapDisjointSet) readParent(x uint64) uint64 {
// 	if x > ds.maxElement {
// 		return x // 범위를 벗어난 원소는 자기 자신이 부모
// 	}

// 	data, exists := ds.getElementData(x)
// 	if !exists {
// 		return 0 // 초기화되지 않은 원소
// 	}

// 	return data.Parent
// }

// // writeParent는 주어진 원소의 부모를 씁니다.
// func (ds *MmapDisjointSet) writeParent(x, parent uint64) {
// 	if x > ds.maxElement {
// 		return // 범위를 벗어난 원소는 무시
// 	}

// 	data, exists := ds.getElementData(x)
// 	if !exists {
// 		// 초기화되지 않은 원소는 기본값으로 생성
// 		data = ElementData{
// 			Parent:  x,
// 			Rank:    0,
// 			SetSize: 1,
// 		}
// 	}

// 	// 이전에 루트였다면 캐시에서 제거
// 	if data.Parent == x && x != parent {
// 		ds.clearSizeCacheForRoot(x)
// 	}

// 	data.Parent = parent
// 	ds.updateElementData(x, data)
// }

// // readRank는 주어진 원소의 랭크를 읽습니다.
// func (ds *MmapDisjointSet) readRank(x uint64) int {
// 	if x > ds.maxElement {
// 		return 0 // 범위를 벗어난 원소는 랭크 0
// 	}

// 	data, exists := ds.getElementData(x)
// 	if !exists {
// 		return 0 // 초기화되지 않은 원소
// 	}

// 	return data.Rank
// }

// // writeRank는 주어진 원소의 랭크를 씁니다.
// func (ds *MmapDisjointSet) writeRank(x uint64, rank int) {
// 	if x > ds.maxElement {
// 		return // 범위를 벗어난 원소는 무시
// 	}

// 	data, exists := ds.getElementData(x)
// 	if !exists {
// 		// 초기화되지 않은 원소는 기본값으로 생성
// 		data = ElementData{
// 			Parent:  x,
// 			Rank:    0,
// 			SetSize: 1,
// 		}
// 	}

// 	data.Rank = rank
// 	ds.updateElementData(x, data)
// }

// // readSetSize는 주어진 원소의 집합 크기를 읽습니다.
// func (ds *MmapDisjointSet) readSetSize(x uint64) uint16 {
// 	if x > ds.maxElement {
// 		return 0 // 범위를 벗어난 원소는 크기 0
// 	}

// 	// 먼저 캐시 확인
// 	cachedSize, exists := ds.getCachedSetSize(x)
// 	if exists {
// 		return cachedSize
// 	}

// 	data, exists := ds.getElementData(x)
// 	if !exists {
// 		return 0 // 초기화되지 않은 원소
// 	}

// 	// 캐시 업데이트 (루트 노드인 경우만)
// 	if data.Parent == x && data.SetSize > 0 {
// 		ds.cacheSizeForRoot(x, data.SetSize)
// 	}

// 	return data.SetSize
// }

// // writeSetSize는 주어진 원소의 집합 크기를 씁니다.
// func (ds *MmapDisjointSet) writeSetSize(x uint64, size uint16) {
// 	if x > ds.maxElement {
// 		return // 범위를 벗어난 원소는 무시
// 	}

// 	data, exists := ds.getElementData(x)
// 	if !exists {
// 		// 초기화되지 않은 원소는 기본값으로 생성
// 		data = ElementData{
// 			Parent:  x,
// 			Rank:    0,
// 			SetSize: 1,
// 		}
// 	}

// 	// 캐시 업데이트 (root인 경우)
// 	if data.Parent == x {
// 		ds.cacheSizeForRoot(x, size)
// 	}

// 	data.SetSize = size
// 	ds.updateElementData(x, data)
// }

// // directSetSize는 메모리에서 즉시 setSize를 업데이트합니다.
// func (ds *MmapDisjointSet) directSetSize(x uint64, size uint16) {
// 	if x > ds.maxElement {
// 		return
// 	}

// 	// 즉시 처리를 위해 큐 사용하지 않고 직접 업데이트
// 	chunkID := ds.getChunkID(x)
// 	chunk, err := ds.loadChunk(chunkID)
// 	if err != nil {
// 		return
// 	}

// 	data, exists := chunk.Elements[x]
// 	if !exists {
// 		data = ElementData{
// 			Parent:  x,
// 			Rank:    0,
// 			SetSize: size,
// 		}
// 	} else {
// 		data.SetSize = size
// 	}

// 	// 직접 데이터 업데이트
// 	chunk.Elements[x] = data
// 	chunk.Modified = true

// 	// 캐시 업데이트
// 	if data.Parent == x {
// 		ds.cacheSizeForRoot(x, size)
// 	}

// 	// 디스크에 직접 쓰기
// 	offset := ds.getElementOffset(x)
// 	entryData := make([]byte, EntrySize)
// 	binary.LittleEndian.PutUint64(entryData[ParentOffset:ParentOffset+8], data.Parent)
// 	binary.LittleEndian.PutUint32(entryData[RankOffset:RankOffset+4], uint32(data.Rank))
// 	binary.LittleEndian.PutUint16(entryData[SetSizeOffset:SetSizeOffset+2], size)
// 	ds.dataWriter.WriteAt(entryData, offset)
// }

// // BatchMakeSet은 여러 원소를 한 번에 집합으로 추가합니다.
// func (ds *MmapDisjointSet) BatchMakeSet(elements []uint64) {
// 	// 원소들을 청크별로 그룹화
// 	elementsByChunk := make(map[uint64][]uint64)
// 	for _, x := range elements {
// 		if x > ds.maxElement {
// 			continue
// 		}
// 		chunkID := ds.getChunkID(x)
// 		elementsByChunk[chunkID] = append(elementsByChunk[chunkID], x)
// 	}

// 	// 각 청크별로 처리
// 	for chunkID, chunkElements := range elementsByChunk {
// 		chunk, err := ds.loadChunk(chunkID)
// 		if err != nil {
// 			continue
// 		}

// 		for _, x := range chunkElements {
// 			_, exists := chunk.Elements[x]
// 			if exists {
// 				continue // 이미 존재하는 원소는 무시
// 			}

// 			// 자기 자신을 부모로 설정
// 			chunk.Elements[x] = ElementData{
// 				Parent:  x,
// 				Rank:    0,
// 				SetSize: 1,
// 			}

// 			// 캐시 업데이트 (단일 원소 집합이므로 크기는 1)
// 			ds.cacheSizeForRoot(x, 1)

// 			chunk.Modified = true
// 			ds.elementCount++
// 		}
// 	}

// 	// 메모리 모드가 아닌 경우 변경된 청크 즉시 디스크에 반영
// 	if !ds.inMemoryMode {
// 		for chunkID := range elementsByChunk {
// 			ds.flushChunk(chunkID)
// 		}
// 	}

// 	// 일정 간격으로 파일 동기화
// 	if ds.elementCount%GapElementsForSync == 0 {
// 		runtime.GC() // 명시적 GC 유도
// 		ds.Flush()   // 파일 동기화
// 	}
// }

// // MakeSet은 새로운 원소를 집합에 추가합니다.
// func (ds *MmapDisjointSet) MakeSet(x uint64) {
// 	if x > ds.maxElement {
// 		return // 범위를 벗어난 원소는 무시
// 	}

// 	// 이미 존재하는지 확인
// 	parent := ds.readParent(x)
// 	if parent != 0 { // 0은 초기화되지 않은 값
// 		return
// 	}

// 	// 자기 자신을 부모로 설정
// 	ds.updateElementData(x, ElementData{
// 		Parent:  x,
// 		Rank:    0,
// 		SetSize: 1, // 단일 원소 집합이므로 크기는 1
// 	})

// 	// 캐시 업데이트
// 	ds.cacheSizeForRoot(x, 1)

// 	ds.elementCount++

// 	// 일정 간격으로 파일 동기화
// 	if ds.elementCount%GapElementsForSync == 0 {
// 		runtime.GC() // 명시적 GC 유도
// 		ds.Flush()   // 파일 동기화
// 	}
// }

// // Find는 원소 x가 속한 집합의 대표(루트)를 찾습니다.
// // 경로 압축 최적화를 적용합니다.
// func (ds *MmapDisjointSet) Find(x uint64) uint64 {
// 	if x > ds.maxElement {
// 		return x // 범위를 벗어난 원소는 자기 자신이 대표
// 	}

// 	// 원소가 존재하지 않으면 생성
// 	parent := ds.readParent(x)
// 	if parent == 0 { // 0은 초기화되지 않은 값
// 		ds.MakeSet(x)
// 		return x
// 	}

// 	// 경로 압축을 위한 재귀 호출
// 	if parent != x {
// 		newParent := ds.Find(parent)
// 		ds.writeParent(x, newParent)
// 		return newParent
// 	}

// 	return parent
// }

// // BatchFind는 여러 원소에 대해 한 번에 Find 연산을 수행합니다.
// func (ds *MmapDisjointSet) BatchFind(elements []uint64) map[uint64]uint64 {
// 	result := make(map[uint64]uint64)

// 	// 원소들을 청크별로 그룹화
// 	elementsByChunk := make(map[uint64][]uint64)
// 	for _, x := range elements {
// 		if x > ds.maxElement {
// 			result[x] = x // 범위를 벗어난 원소는 자기 자신이 대표
// 			continue
// 		}
// 		chunkID := ds.getChunkID(x)
// 		elementsByChunk[chunkID] = append(elementsByChunk[chunkID], x)
// 	}

// 	// 각 청크별로 처리
// 	for _, chunkElements := range elementsByChunk {
// 		for _, x := range chunkElements {
// 			result[x] = ds.Find(x)
// 		}
// 	}

// 	return result
// }

// // calcSetSize는 주어진 루트 노드의 집합 크기를 직접 계산합니다.
// func (ds *MmapDisjointSet) calcSetSize(root uint64) uint16 {
// 	// 메모리 모드에서 큐 처리 보장
// 	if ds.inMemoryMode {
// 		ds.processUpdateQueue()
// 	}
// 	// 총 청크 수 계산
// 	totalChunks := (ds.maxElement / ChunkSize) + 1
// 	// 이미 로드된 청크들을, 언로드하는 것을 방지하기 위한 맵
// 	keepChunks := make(map[uint64]bool)
// 	// 모든 청크 검색하여 해당 루트에 속한 원소 카운트
// 	var count uint32 = 0
// 	// 모든 가능한 청크를, 하나씩 로드하면서 확인
// 	for chunkID := uint64(0); chunkID < totalChunks; chunkID++ {
// 		// 청크 로드 (이미 메모리에 있으면 그대로 사용)
// 		chunk, err := ds.loadChunk(chunkID)
// 		if err != nil {
// 			continue // 로드 실패하면 다음 청크로
// 		}

// 		// 나중에 언로드되지 않도록 표시
// 		keepChunks[chunkID] = true

// 		// 청크 내 모든 원소 확인
// 		for elemID := range chunk.Elements {
// 			// 해당 원소의 루트가 찾으려는 루트와 같은지 확인
// 			elemRoot := ds.Find(elemID)
// 			if elemRoot == root {
// 				count++
// 			}
// 		}

// 		// 메모리 부담을 줄이기 위해 일정 개수의 청크마다 불필요한 청크 언로드
// 		if chunkID%HoldableChunks == 0 {
// 			ds.unloadUnusedChunks(keepChunks)
// 		}
// 	}

// 	// 크기 캐싱
// 	var size uint16
// 	if count > 65535 {
// 		size = 65535
// 	} else {
// 		size = uint16(count)
// 	}

// 	// 계산된 크기를 디스크와 캐시에 반영
// 	ds.directSetSize(root, size)

// 	return size
// }

// // BatchUnion은 여러 원소 쌍에 대해 한 번에 Union 연산을 수행합니다.
// func (ds *MmapDisjointSet) BatchUnion(pairs [][2]uint64) {
// 	// 처리하기 전에 모든 보류 중인 업데이트 반영
// 	ds.processUpdateQueue()

// 	// 모든 원소 추적 및 루트 정보 수집
// 	allElements := make([]uint64, 0, len(pairs)*2)
// 	for _, pair := range pairs {
// 		allElements = append(allElements, pair[0], pair[1])
// 	}

// 	// 중복 제거
// 	uniqueElements := make(map[uint64]bool)
// 	for _, elem := range allElements {
// 		uniqueElements[elem] = true
// 	}

// 	// 고유 원소 목록 생성
// 	elements := make([]uint64, 0, len(uniqueElements))
// 	for elem := range uniqueElements {
// 		elements = append(elements, elem)
// 	}

// 	// 모든 원소의 루트 찾기
// 	rootMap := ds.BatchFind(elements)

// 	// Union 작업 추적을 위한 구조
// 	type UnionOp struct {
// 		from, to uint64
// 	}

// 	// 루트 간 병합 계획 수립 (중복 제거)
// 	rootUnions := make(map[uint64]uint64) // from -> to

// 	// 각 쌍에 대해 처리
// 	for _, pair := range pairs {
// 		rootX := rootMap[pair[0]]
// 		rootY := rootMap[pair[1]]

// 		if rootX == rootY {
// 			continue // 이미 같은 집합
// 		}

// 		// 랭크 비교 및 병합 방향 결정
// 		rankX := ds.readRank(rootX)
// 		rankY := ds.readRank(rootY)

// 		var from, to uint64
// 		var rankIncrease bool

// 		if rankX < rankY {
// 			from, to = rootX, rootY
// 			rankIncrease = false
// 		} else {
// 			from, to = rootY, rootX
// 			rankIncrease = (rankX == rankY)

// 			// 랭크 증가가 필요하면 미리 계산
// 			if rankIncrease {
// 				ds.writeRank(rootX, rankX+1)
// 			}
// 		}

// 		// 루트 매핑 업데이트 (트랜지티브 클로저 처리)
// 		// 자체 경로압축 느낌.
// 		finalTo := to
// 		for {
// 			if nextTo, exists := rootUnions[finalTo]; exists {
// 				finalTo = nextTo
// 			} else {
// 				break
// 			}
// 		}
// 		rootUnions[from] = finalTo

// 		// 루트맵 업데이트 (같은 배치 내에서의 일관성)
// 		for elem, root := range rootMap {
// 			if root == from {
// 				rootMap[elem] = finalTo
// 			}
// 		}
// 	}

// 	// 실제 병합 단계
// 	// 1. 먼저 모든 크기 정보 미리 캐싱
// 	uniqueRoots := make(map[uint64]bool)
// 	for _, rootID := range rootMap {
// 		uniqueRoots[rootID] = true
// 	}

// 	rootSizes := make(map[uint64]uint16)
// 	for root := range uniqueRoots {
// 		if _, exists := rootUnions[root]; !exists {
// 			// 다른 집합에 흡수되지 않는 루트만 계산
// 			size := ds.readSetSize(root)
// 			if size == 0 {
// 				size = ds.calcSetSize(root)
// 			}
// 			rootSizes[root] = size
// 		}
// 	}

// 	// 2. 집합 크기 합산
// 	mergedSizes := make(map[uint64]uint32)
// 	for from, to := range rootUnions {
// 		fromSize := rootSizes[from]
// 		if fromSize == 0 {
// 			fromSize = ds.readSetSize(from)
// 		}

// 		if _, exists := mergedSizes[to]; !exists {
// 			toSize := rootSizes[to]
// 			if toSize == 0 {
// 				toSize = ds.readSetSize(to)
// 			}
// 			mergedSizes[to] = uint32(toSize)
// 		}

// 		mergedSizes[to] += uint32(fromSize)
// 		if mergedSizes[to] > 65535 {
// 			mergedSizes[to] = 65535
// 		}
// 	}

// 	// 3. 실제 병합 및 크기 업데이트 수행
// 	for from, to := range rootUnions {
// 		// 집합 부모 포인터 업데이트
// 		ds.writeParent(from, to)

// 		// 병합된 집합 크기 업데이트
// 		if size, exists := mergedSizes[to]; exists {
// 			ds.directSetSize(to, uint16(size))
// 		}
// 	}

// 	// 변경사항 즉시 적용
// 	ds.Flush()
// }

// // Union은 두 원소가 속한 집합을 합칩니다.
// // 랭크에 따른 병합 최적화를 적용합니다.
// func (ds *MmapDisjointSet) Union(x, y uint64) {
// 	// 먼저 모든 보류 중인 업데이트 처리
// 	ds.processUpdateQueue()

// 	rootX := ds.Find(x)
// 	rootY := ds.Find(y)

// 	if rootX == rootY {
// 		return // 이미 같은 집합에 속해 있음
// 	}

// 	// 집합 크기 읽기 (캐시 우선)
// 	sizeX := ds.readSetSize(rootX)
// 	if sizeX == 0 {
// 		sizeX = ds.calcSetSize(rootX)
// 	}

// 	sizeY := ds.readSetSize(rootY)
// 	if sizeY == 0 {
// 		sizeY = ds.calcSetSize(rootY)
// 	}

// 	// 새 집합의 크기 계산 (오버플로 확인)
// 	var newSize uint16
// 	if uint32(sizeX)+uint32(sizeY) > 65535 {
// 		newSize = 65535 // uint16 최대값
// 	} else {
// 		newSize = sizeX + sizeY
// 	}

// 	// 랭크에 따라 병합 (랭크가 작은 트리를 랭크가 큰 트리에 붙임)
// 	rankX := ds.readRank(rootX)
// 	rankY := ds.readRank(rootY)

// 	if rankX < rankY {
// 		// 캐시에서 이전 루트 정보 제거
// 		ds.clearSizeCacheForRoot(rootX)

// 		// 부모 포인터 업데이트
// 		ds.writeParent(rootX, rootY)

// 		// 직접 크기 업데이트
// 		ds.directSetSize(rootY, newSize)
// 	} else {
// 		// 캐시에서 이전 루트 정보 제거
// 		ds.clearSizeCacheForRoot(rootY)

// 		// 부모 포인터 업데이트
// 		ds.writeParent(rootY, rootX)

// 		// 직접 크기 업데이트
// 		ds.directSetSize(rootX, newSize)

// 		// 두 트리의 랭크가 같으면 결과 트리의 랭크를 증가
// 		if rankX == rankY {
// 			ds.writeRank(rootX, rankX+1)
// 		}
// 	}

// 	// 모든 변경사항 즉시 적용
// 	if ds.inMemoryMode {
// 		ds.Flush()
// 	}
// }

// // GetSetSize는 주어진 원소가 속한 집합의 크기를 반환합니다.
// func (ds *MmapDisjointSet) GetSetSize(x uint64) uint16 {
// 	// 모든 보류 중인 업데이트 처리
// 	ds.processUpdateQueue()

// 	// 원소의 루트 찾기
// 	root := ds.Find(x)

// 	// 캐시에서 크기 확인
// 	cachedSize, exists := ds.getCachedSetSize(root)
// 	if exists && cachedSize > 0 {
// 		return cachedSize
// 	}

// 	// 디스크에서 크기 확인
// 	size := ds.readSetSize(root)
// 	if size == 0 {
// 		// 크기 정보가 없으면 직접 계산
// 		size = ds.calcSetSize(root)
// 	}

// 	return size
// }

// // GetSetsChunked는 현재 분리된 모든 집합들을 청크 단위로 계산하여 반환합니다.
// // ! 모든 disjointSet반환이므로, 대규모 연산에선 파일을 리턴시키기.
// // 그냥 맵 리턴하면 램 초과남
// func (ds *MmapDisjointSet) GetSetsChunked() map[uint64][]uint64 {
// 	// 지연된 업데이트 모두 처리
// 	ds.processUpdateQueue()

// 	result := make(map[uint64][]uint64)
// 	chunkCount := (ds.maxElement / ChunkSize) + 1

// 	// 현재 청크 작업을 추적하기 위한 맵
// 	activeChunks := make(map[uint64]bool)

// 	// 청크 단위로 처리
// 	for chunk := uint64(0); chunk < chunkCount; chunk++ {
// 		chunkData, err := ds.loadChunk(chunk)
// 		if err != nil {
// 			continue
// 		}

// 		activeChunks[chunk] = true

// 		// 청크에 원소가 없으면 스킵
// 		if len(chunkData.Elements) == 0 {
// 			continue
// 		}

// 		// 현재 청크의 원소들을 처리
// 		for element := range chunkData.Elements {
// 			parent := ds.readParent(element)
// 			if parent == 0 { // 초기화되지 않은 원소 건너뛰기
// 				continue
// 			}

// 			root := ds.Find(element) // 경로 압축이 여기서도 발생

// 			if _, exists := result[root]; !exists {
// 				result[root] = []uint64{}
// 			}
// 			result[root] = append(result[root], element)
// 		}

// 		// 100개 청크마다 사용하지 않는 청크 언로드
// 		if chunk%HoldableChunks == HoldableChunks-1 {
// 			ds.unloadUnusedChunks(activeChunks)
// 			activeChunks = make(map[uint64]bool)
// 			for i := chunk - (HoldableChunks - 1); i <= chunk; i++ {
// 				activeChunks[i] = true
// 			}
// 		}
// 	}

// 	// 각 집합 내부를 정렬 (선택적)
// 	for root := range result {
// 		slices.Sort(result[root])
// 	}

// 	return result
// }

// // GetSetsWithSize는 현재 분리된 모든 집합들과 그 크기를 반환합니다.
// func (ds *MmapDisjointSet) GetSetsWithSize() map[uint64]struct {
// 	Elements []uint64
// 	Size     uint16
// } {
// 	// 모든 집합 획득
// 	sets := ds.GetSetsChunked()

// 	// 집합과 크기 매핑
// 	result := make(map[uint64]struct {
// 		Elements []uint64
// 		Size     uint16
// 	})

// 	for root, elements := range sets {
// 		// 직접 크기 계산하여 일관성 보장
// 		size := uint16(len(elements))
// 		if size > 0 {
// 			// 크기 정보 캐싱 및 디스크에 반영
// 			ds.directSetSize(root, size)
// 		}

// 		result[root] = struct {
// 			Elements []uint64
// 			Size     uint16
// 		}{
// 			Elements: elements,
// 			Size:     size,
// 		}
// 	}

// 	return result
// }

// // PrintSets는 현재 모든 분리 집합과 그 원소들을 출력합니다.
// func (ds *MmapDisjointSet) PrintSets() {
// 	sets := ds.GetSetsWithSize()

// 	fmt.Println("현재 분리 집합 상태:")
// 	fmt.Printf("총 %d개의 집합이 있습니다.\n", len(sets))

// 	for root, setInfo := range sets {
// 		fmt.Printf("집합 %d (크기: %d): %v\n", root, setInfo.Size, setInfo.Elements)
// 	}
// }

// // GetStats는 분리 집합의 통계 정보를 반환합니다.
// func (ds *MmapDisjointSet) GetStats() map[string]interface{} {
// 	stats := make(map[string]interface{})

// 	stats["maxElement"] = ds.maxElement
// 	stats["elementCount"] = ds.elementCount
// 	stats["fileSize"] = fileSize(ds.dataFile)
// 	stats["loadedChunks"] = len(ds.chunks)
// 	stats["pendingUpdates"] = len(ds.updateQueue)
// 	stats["cachedSizes"] = len(ds.sizeCache.Sizes)
// 	stats["inMemoryMode"] = ds.inMemoryMode

// 	return stats
// }

// // 파일 크기 반환 헬퍼 함수
// func fileSize(filePath string) int64 {
// 	info, err := os.Stat(filePath)
// 	if err != nil {
// 		return -1
// 	}
// 	return info.Size()
// }

// // // 메인 함수 예시
// func main() {
// 	// 파일 경로 및 최대 원소 수 설정
// 	dataFile := "disjoint_set_data.bin"
// 	maxElement := uint64(1_000_000_000) // 최대 10만 개의 원소
// 	inMemoryMode := true                // 메모리 우선 모드 활성화

// 	// MmapDisjointSet 인스턴스 생성
// 	ds, err := NewMmapDisjointSet(dataFile, maxElement, inMemoryMode)
// 	if err != nil {
// 		fmt.Printf("Error: %v\n", err)
// 		return
// 	}
// 	defer ds.Close() // 리소스 정리

// 	// 6개의 집합 생성
// 	fmt.Println("6개의 분리 집합 생성 중...")

// 	// 배치 MakeSet 예시
// 	// 집합 1, 2, 3의 원소들 배치 생성
// 	set1Elements := []uint64{1, 2, 3, 4, 5}
// 	set2Elements := []uint64{10, 11, 12, 13}
// 	set3Elements := []uint64{20, 21, 22}

// 	allElements := append(append(set1Elements, set2Elements...), set3Elements...)
// 	ds.BatchMakeSet(allElements)

// 	// 배치 Union 예시 - 집합 1, 2, 3 내부 연결
// 	unionPairs1 := [][2]uint64{
// 		{1, 2}, {1, 3}, {1, 4}, {1, 5}, // 집합 1 내부 연결
// 		{10, 11}, {10, 12}, {10, 13}, // 집합 2 내부 연결
// 		{20, 21}, {20, 22}, // 집합 3 내부 연결
// 	}
// 	ds.BatchUnion(unionPairs1)

// 	// 집합 4, 5, 6도 유사하게 생성
// 	set4Elements := []uint64{30, 31, 32, 33, 34}
// 	set5Elements := []uint64{40, 41, 42}
// 	set6Elements := []uint64{50, 51, 52, 53}

// 	ds.BatchMakeSet(append(append(set4Elements, set5Elements...), set6Elements...))

// 	unionPairs2 := [][2]uint64{
// 		{30, 31}, {30, 32}, {30, 33}, {30, 34}, // 집합 4 내부 연결
// 		{40, 41}, {40, 42}, // 집합 5 내부 연결
// 		{50, 51}, {50, 52}, {50, 53}, // 집합 6 내부 연결
// 	}
// 	ds.BatchUnion(unionPairs2)

// 	// 청크 캐시를 디스크에 반영
// 	ds.Flush()

// 	// 초기 상태 출력
// 	fmt.Println("초기 상태 (6개의 분리 집합):")
// 	ds.PrintSets()

// 	// 6개의 집합을 3개로 합치기
// 	fmt.Println("\n6개의 집합을 3개로 합치는 중...")

// 	// 3개를 1개로 합치기 (집합 1, 3, 5 -> 집합 1)
// 	fmt.Println("\n집합 1, 3, 5 -> 집합 1로 합치기:")
// 	// 배치 Union 사용
// 	unionPairs3 := [][2]uint64{
// 		{1, 20}, // 집합 1과 집합 3 합치기
// 		{1, 40}, // 합쳐진 집합과 집합 5 합치기
// 	}
// 	ds.BatchUnion(unionPairs3)
// 	fmt.Println("합쳐진 집합 1의 크기:", ds.GetSetSize(1))

// 	// 2개를 1개로 합치기 (집합 2, 4 -> 집합 2)
// 	fmt.Println("\n집합 2, 4 -> 집합 2로 합치기:")
// 	unionPairs4 := [][2]uint64{
// 		{10, 30}, // 집합 2와 집합 4 합치기
// 	}
// 	ds.BatchUnion(unionPairs4)
// 	fmt.Println("합쳐진 집합 2의 크기:", ds.GetSetSize(10))

// 	// 집합 6은 그대로 유지
// 	fmt.Println("\n집합 6은 그대로 유지:")
// 	fmt.Println("집합 6의 크기:", ds.GetSetSize(50))

// 	// 모든 변경사항 디스크에 반영
// 	ds.Flush()

// 	// 최종 결과 출력
// 	fmt.Println("\n최종 상태 (3개의 분리 집합):")
// 	ds.PrintSets()

// 	// 합쳐진 집합의 크기 출력
// 	fmt.Printf("\n집합별 크기 확인:\n")
// 	fmt.Printf("집합 1(원소 1)의 크기: %d\n", ds.GetSetSize(1))
// 	fmt.Printf("집합 2(원소 10)의 크기: %d\n", ds.GetSetSize(10))
// 	fmt.Printf("집합 6(원소 50)의 크기: %d\n", ds.GetSetSize(50))

// 	// 통계 정보 출력
// 	fmt.Println("\n통계 정보:")
// 	stats := ds.GetStats()
// 	for k, v := range stats {
// 		fmt.Printf("%s: %v\n", k, v)
// 	}
// }

// // UserSet은 하나의 분리 집합을 나타내는 구조체입니다.
// type UserSet struct {
// 	Elements []uint64
// }

// // 테스트케이스 생성기
// // 특정 크기의 집합을 랜덤하게 생성합니다.
// func GenerateSets(rangeValue uint64, setSize int) []UserSet {
// 	result := make([]UserSet, 0)
// 	usedElements := make(map[uint64]bool)

// 	// 충분한 수의 집합 생성 (요청된 것보다 더 많이 생성하고 필요한 만큼 잘라서 사용)
// 	for i := 0; i < 1000000; i++ {
// 		set := UserSet{Elements: make([]uint64, 0, setSize)}

// 		// 중복되지 않는 원소를 setSize만큼 추가
// 		for j := 0; j < setSize; j++ {
// 			var element uint64
// 			for {
// 				element = uint64(rand.Int63n(int64(rangeValue)))
// 				if !usedElements[element] && element > 0 {
// 					break
// 				}
// 			}
// 			set.Elements = append(set.Elements, element)
// 			usedElements[element] = true
// 		}

// 		result = append(result, set)

// 		// 충분한 수의 집합을 생성했으면 종료
// 		if len(result) >= 1000000 {
// 			break
// 		}
// 	}

// 	return result
// }

// // 테스트케이스 생성기
// // 분포에 따라 모든 집합 생성
// func GenerateAllSets(numSets uint64, rangeValue uint64) []UserSet {
// 	// 분포에 따른 집합 수 계산
// 	singleCount := int(float64(numSets) * 0.90)   // 90%
// 	twoCount := int(float64(numSets) * 0.07)      // 7%
// 	threeCount := int(float64(numSets) * 0.01)    // 1%
// 	fourCount := int(float64(numSets) * 0.005)    // 0.5%
// 	fiveCount := int(float64(numSets) * 0.0025)   // 0.25%
// 	sixCount := int(float64(numSets) * 0.0025)    // 0.25%
// 	sevenCount := int(float64(numSets) * 0.0025)  // 0.25%
// 	eightCount := int(float64(numSets) * 0.0025)  // 0.25%
// 	nineCount := int(float64(numSets) * 0.001)    // 0.1%
// 	tenCount := int(float64(numSets) * 0.001)     // 0.1%
// 	elevenCount := int(float64(numSets) * 0.0005) // 0.05%
// 	twelveCount := int(float64(numSets) * 0.0005) // 0.05%
// 	twentyCount := int(float64(numSets) * 0.0003) // 0.03%
// 	thirtyCount := int(float64(numSets) * 0.0001) // 0.01%
// 	fiftyCount := int(float64(numSets) * 0.0001)  // 0.01%

// 	// 결과 슬라이스
// 	allSets := make([]UserSet, 0, numSets)

// 	// 각 크기별 집합 생성
// 	fmt.Println("1개 원소 집합 생성 중...")
// 	allSets = append(allSets, GenerateSets(rangeValue, 1)[:singleCount]...)

// 	fmt.Println("2개 원소 집합 생성 중...")
// 	allSets = append(allSets, GenerateSets(rangeValue, 2)[:twoCount]...)

// 	fmt.Println("3개 원소 집합 생성 중...")
// 	allSets = append(allSets, GenerateSets(rangeValue, 3)[:threeCount]...)

// 	fmt.Println("4-12개 원소 집합 생성 중...")
// 	allSets = append(allSets, GenerateSets(rangeValue, 4)[:fourCount]...)
// 	allSets = append(allSets, GenerateSets(rangeValue, 5)[:fiveCount]...)
// 	allSets = append(allSets, GenerateSets(rangeValue, 6)[:sixCount]...)
// 	allSets = append(allSets, GenerateSets(rangeValue, 7)[:sevenCount]...)
// 	allSets = append(allSets, GenerateSets(rangeValue, 8)[:eightCount]...)
// 	allSets = append(allSets, GenerateSets(rangeValue, 9)[:nineCount]...)
// 	allSets = append(allSets, GenerateSets(rangeValue, 10)[:tenCount]...)
// 	allSets = append(allSets, GenerateSets(rangeValue, 11)[:elevenCount]...)
// 	allSets = append(allSets, GenerateSets(rangeValue, 12)[:twelveCount]...)

// 	fmt.Println("다수 원소 집합 생성 중...")
// 	allSets = append(allSets, GenerateSets(rangeValue, 20)[:twentyCount]...)
// 	allSets = append(allSets, GenerateSets(rangeValue, 30)[:thirtyCount]...)
// 	allSets = append(allSets, GenerateSets(rangeValue, 50)[:fiftyCount]...)

// 	// 랜덤하게 섞기
// 	fmt.Println("집합 섞는 중...")
// 	rand.Seed(time.Now().UnixNano())
// 	rand.Shuffle(len(allSets), func(i, j int) {
// 		allSets[i], allSets[j] = allSets[j], allSets[i]
// 	})

// 	fmt.Printf("총 %d개 집합 생성 완료\n", len(allSets))
// 	return allSets
// }

// // 메인 함수 - 대규모 Union-Find 테스트 수행
// func main() {
// 	startTime := time.Now()

// 	// 파일 경로 및 최대 원소 수 설정
// 	dataFile := "large_disjoint_set.bin"
// 	maxElement := uint64(2_000_000_000) // 20억 개의 원소
// 	inMemoryMode := true                // 메모리 우선 모드 활성화

// 	// 파일이 이미 존재하면 삭제 (새로 시작)
// 	if _, err := os.Stat(dataFile); err == nil {
// 		fmt.Println("기존 파일을 삭제합니다...")
// 		os.Remove(dataFile)
// 	}

// 	// MmapDisjointSet 인스턴스 생성
// 	fmt.Println("Disjoint Set 초기화 중...")
// 	ds, err := NewMmapDisjointSet(dataFile, maxElement, inMemoryMode)
// 	if err != nil {
// 		fmt.Printf("Error: %v\n", err)
// 		return
// 	}
// 	defer ds.Close() // 리소스 정리

// 	fmt.Println("초기화 완료:", time.Since(startTime))

// 	// 첫 번째 단계: 2억 개 원소 처리
// 	processPhase(ds, 1, 200_000_000)

// 	// 두 번째 단계: 추가 2억 개 원소 처리
// 	processPhase(ds, 2, 200_000_000)

// 	// 최종 통계 정보 출력
// 	fmt.Println("\n=== 최종 통계 정보 ===")
// 	stats := ds.GetStats()
// 	for k, v := range stats {
// 		fmt.Printf("%s: %v\n", k, v)
// 	}

// 	fmt.Printf("\n총 실행 시간: %v\n", time.Since(startTime))
// }

// // 각 단계별 처리 함수
// func processPhase(ds *MmapDisjointSet, phase int, numElements uint64) {
// 	phaseStartTime := time.Now()
// 	fmt.Printf("\n=== 단계 %d 시작: %d 개 원소 처리 ===\n", phase, numElements)

// 	// 단계별 시작 범위 설정
// 	startRange := uint64((phase - 1) * 200_000_000 + 1)
// 	endRange := uint64(phase * 200_000_000)

// 	// 1. 테스트 데이터 생성
// 	fmt.Printf("테스트 데이터 생성 중 (범위: %d ~ %d)...\n", startRange, endRange)

// 	// 집합 수 계산 (원소 수보다 적어야 함)
// 	numSets := numElements / 5 // 약 4천만 개의 집합

// 	// 집합 생성
// 	setGenStart := time.Now()
// 	userSets := GenerateAllSets(numSets, endRange)
// 	fmt.Printf("데이터 생성 완료: %v\n", time.Since(setGenStart))

// 	// 2. BatchMakeSet으로 모든 원소 추가
// 	fmt.Println("모든 원소를 Disjoint Set에 추가하는 중...")
// 	makeSetStart := time.Now()

// 	// 모든 원소 수집
// 	allElements := make([]uint64, 0)
// 	for _, set := range userSets {
// 		allElements = append(allElements, set.Elements...)
// 	}

// 	// 청크 단위로 처리하여 메모리 사용량 제한
// 	const chunkSize = 1_000_000 // 한 번에 처리할 원소 수
// 	for i := 0; i < len(allElements); i += chunkSize {
// 		end := i + chunkSize
// 		if end > len(allElements) {
// 			end = len(allElements)
// 		}

// 		chunk := allElements[i:end]
// 		ds.BatchMakeSet(chunk)

// 		// 진행 상황 로깅 (10% 단위)
// 		if (i/chunkSize)%10 == 0 {
// 			fmt.Printf("  %d/%d 원소 추가 완료 (%.1f%%)\n", end, len(allElements), float64(end)*100/float64(len(allElements)))
// 		}
// 	}

// 	fmt.Printf("원소 추가 완료: %v\n", time.Since(makeSetStart))

// 	// 3. 각 집합 내의 원소들을 Union으로 연결
// 	fmt.Println("집합 내 원소들을 Union으로 연결하는 중...")
// 	unionStart := time.Now()

// 	// 진행 상황 추적용 카운터
// 	processedSets := 0
// 	totalPairs := 0

// 	// 모든 집합에 대해
// 	for _, set := range userSets {
// 		if len(set.Elements) <= 1 {
// 			continue // 원소가 1개인 집합은 Union 필요 없음
// 		}

// 		// 집합 내 모든 원소를 첫 번째 원소와 Union
// 		pairs := make([][2]uint64, 0, len(set.Elements)-1)
// 		for i := 1; i < len(set.Elements); i++ {
// 			pairs = append(pairs, [2]uint64{set.Elements[0], set.Elements[i]})
// 		}

// 		// BatchUnion 수행
// 		ds.BatchUnion(pairs)

// 		// 진행 상황 업데이트
// 		processedSets++
// 		totalPairs += len(pairs)

// 		// 진행 상황 로깅 (5% 단위)
// 		if processedSets%(len(userSets)/20) == 0 {
// 			fmt.Printf("  %d/%d 집합 처리 완료 (%.1f%%)\n",
// 				processedSets, len(userSets), float64(processedSets)*100/float64(len(userSets)))
// 		}

// 		// 메모리 관리: 주기적으로 GC 실행 및 디스크 플러시
// 		if processedSets%10000 == 0 {
// 			runtime.GC()
// 			ds.Flush()
// 		}
// 	}

// 	fmt.Printf("Union 연산 완료: %v (총 %d 페어 처리)\n", time.Since(unionStart), totalPairs)

// 	// 4. 무작위로 Find 연산 수행하여 검증
// 	fmt.Println("무작위 Find 연산으로 검증 중...")
// 	findStart := time.Now()

// 	// 무작위 원소 선택
// 	sampleSize := 10000
// 	if sampleSize > len(allElements) {
// 		sampleSize = len(allElements)
// 	}

// 	// 샘플 원소들
// 	sampleElements := make([]uint64, sampleSize)
// 	for i := 0; i < sampleSize; i++ {
// 		sampleElements[i] = allElements[rand.Intn(len(allElements))]
// 	}

// 	// BatchFind 수행
// 	roots := ds.BatchFind(sampleElements)

// 	// 결과 요약
// 	uniqueRoots := make(map[uint64]int)
// 	for _, root := range roots {
// 		uniqueRoots[root]++
// 	}

// 	fmt.Printf("Find 연산 완료: %v\n", time.Since(findStart))
// 	fmt.Printf("샘플 %d개 중 고유 루트 개수: %d\n", sampleSize, len(uniqueRoots))

// 	// 5. 모든 변경사항 디스크에 반영
// 	fmt.Println("모든 변경사항 디스크에 반영 중...")
// 	flushStart := time.Now()
// 	ds.Flush()
// 	fmt.Printf("디스크 반영 완료: %v\n", time.Since(flushStart))

// 	// 6. 통계 정보 출력
// 	fmt.Printf("\n=== 단계 %d 완료 ===\n", phase)
// 	fmt.Printf("단계별 처리 시간: %v\n", time.Since(phaseStartTime))

// 	stats := ds.GetStats()
// 	fmt.Println("현재 통계:")
// 	fmt.Printf("총 원소 수: %d\n", stats["elementCount"])
// 	fmt.Printf("메모리에 로드된 청크 수: %d\n", stats["loadedChunks"])
// 	fmt.Printf("파일 크기: %.2f GB\n", float64(stats["fileSize"].(int64))/(1024*1024*1024))
// }

// package main

// import (
// 	"encoding/binary"
// 	"fmt"
// 	"os"
// 	"runtime"
// 	"sort"

// 	"golang.org/x/exp/mmap"
// )

// const (
// 	// 파일 레이아웃 상수
// 	HeaderSize     = 16                                  // 헤더 크기 (바이트): maxElement(8) + elementCount(8)
// 	EntrySize      = 16                                  // 각 엔트리의 크기 (바이트): parent(8) + rank(4) + setSize(2) + padding(2)
// 	PageSize       = 4096                                // 일반적인 시스템 페이지 크기
// 	EntriesPerPage = (PageSize - HeaderSize) / EntrySize // 한 페이지당 엔트리 수
// 	ChunkSize      = PageSize * 16                       // 한 번에 처리할 청크 크기 (16 페이지)

// 	// 엔트리 내 필드 오프셋
// 	ParentOffset  = 0  // 부모 포인터 오프셋 (8바이트)
// 	RankOffset    = 8  // 랭크 오프셋 (4바이트)
// 	SetSizeOffset = 12 // 집합 크기 오프셋 (2바이트)
// )

// // MmapDisjointSet은 mmap 기반 분리 집합 구조체입니다.
// type MmapDisjointSet struct {
// 	dataFile     string         // 데이터 정보를 저장하는 파일 경로
// 	dataReaderAt *mmap.ReaderAt // 데이터 파일의 mmap 리더
// 	dataWriter   *os.File       // 데이터 파일 쓰기용
// 	maxElement   uint64         // 최대 원소 ID
// 	elementCount uint64         // 전체 원소 수
// }

// // NewMmapDisjointSet은 새로운 mmap 기반 DisjointSet 인스턴스를 생성합니다.
// func NewMmapDisjointSet(dataFile string, maxElement uint64) (*MmapDisjointSet, error) {
// 	// 파일이 존재하는지 확인하고, 없으면 생성
// 	dataExists := fileExists(dataFile)

// 	// 데이터 파일 열기 또는 생성
// 	dataWriter, err := os.OpenFile(dataFile, os.O_RDWR|os.O_CREATE, 0666)
// 	if err != nil {
// 		return nil, fmt.Errorf("데이터 파일을 열 수 없음: %v", err)
// 	}

// 	// 새 파일이면 초기화 - 데이터 파일
// 	dataFileSize := HeaderSize + int64(maxElement+1)*EntrySize
// 	if !dataExists {
// 		if err := dataWriter.Truncate(dataFileSize); err != nil {
// 			dataWriter.Close()
// 			return nil, fmt.Errorf("데이터 파일 크기를 설정할 수 없음: %v", err)
// 		}
// 		// 헤더 초기화
// 		header := make([]byte, HeaderSize)
// 		binary.LittleEndian.PutUint64(header[0:8], maxElement)
// 		binary.LittleEndian.PutUint64(header[8:16], 0) // 초기 elementCount
// 		if _, err := dataWriter.WriteAt(header, 0); err != nil {
// 			dataWriter.Close()
// 			return nil, fmt.Errorf("데이터 파일 헤더를 쓸 수 없음: %v", err)
// 		}
// 	}

// 	// mmap 리더 생성 - 데이터 파일
// 	dataReaderAt, err := mmap.Open(dataFile)
// 	if err != nil {
// 		dataWriter.Close()
// 		return nil, fmt.Errorf("데이터 파일을 메모리에 매핑할 수 없음: %v", err)
// 	}

// 	// 인스턴스 생성
// 	ds := &MmapDisjointSet{
// 		dataFile:     dataFile,
// 		dataReaderAt: dataReaderAt,
// 		dataWriter:   dataWriter,
// 		maxElement:   maxElement,
// 	}

// 	// 헤더에서 elementCount 읽기
// 	elementCountBytes := make([]byte, 8)
// 	ds.dataReaderAt.ReadAt(elementCountBytes, 8)
// 	ds.elementCount = binary.LittleEndian.Uint64(elementCountBytes)

// 	return ds, nil
// }

// // 파일 존재 여부 확인 헬퍼 함수
// func fileExists(filename string) bool {
// 	_, err := os.Stat(filename)
// 	return err == nil
// }

// // Close는 리소스를 정리합니다.
// func (ds *MmapDisjointSet) Close() error {
// 	ds.Flush() // 변경사항을 파일에 반영

// 	// 파일 핸들 닫기
// 	ds.dataReaderAt.Close()
// 	ds.dataWriter.Close()
// 	return nil
// }

// // Flush는 변경사항을 파일에 반영합니다.
// func (ds *MmapDisjointSet) Flush() {
// 	// elementCount 업데이트
// 	countBytes := make([]byte, 8)
// 	binary.LittleEndian.PutUint64(countBytes, ds.elementCount)
// 	ds.dataWriter.WriteAt(countBytes, 8)

// 	// 명시적 파일 동기화
// 	ds.dataWriter.Sync()
// }

// // getElementOffset은 주어진 원소의 데이터가 저장된 파일 오프셋을 반환합니다.
// func (ds *MmapDisjointSet) getElementOffset(x uint64) int64 {
// 	return HeaderSize + int64(x)*EntrySize
// }

// // readParent는 주어진 원소의 부모를 읽습니다.
// func (ds *MmapDisjointSet) readParent(x uint64) uint64 {
// 	if x > ds.maxElement {
// 		return x // 범위를 벗어난 원소는 자기 자신이 부모
// 	}

// 	offset := ds.getElementOffset(x)
// 	parentBytes := make([]byte, 8)
// 	ds.dataReaderAt.ReadAt(parentBytes, offset+ParentOffset)
// 	return binary.LittleEndian.Uint64(parentBytes)
// }

// // writeParent는 주어진 원소의 부모를 씁니다.
// func (ds *MmapDisjointSet) writeParent(x, parent uint64) {
// 	if x > ds.maxElement {
// 		return // 범위를 벗어난 원소는 무시
// 	}

// 	offset := ds.getElementOffset(x)
// 	parentBytes := make([]byte, 8)
// 	binary.LittleEndian.PutUint64(parentBytes, parent)

// 	// 파일에 직접 쓰기
// 	ds.dataWriter.WriteAt(parentBytes, offset+ParentOffset)
// }

// // readRank는 주어진 원소의 랭크를 읽습니다.
// func (ds *MmapDisjointSet) readRank(x uint64) int {
// 	if x > ds.maxElement {
// 		return 0 // 범위를 벗어난 원소는 랭크 0
// 	}

// 	offset := ds.getElementOffset(x)
// 	rankBytes := make([]byte, 4)
// 	ds.dataReaderAt.ReadAt(rankBytes, offset+RankOffset)
// 	return int(binary.LittleEndian.Uint32(rankBytes))
// }

// // writeRank는 주어진 원소의 랭크를 씁니다.
// func (ds *MmapDisjointSet) writeRank(x uint64, rank int) {
// 	if x > ds.maxElement {
// 		return // 범위를 벗어난 원소는 무시
// 	}

// 	offset := ds.getElementOffset(x)
// 	rankBytes := make([]byte, 4)
// 	binary.LittleEndian.PutUint32(rankBytes, uint32(rank))

// 	// 파일에 직접 쓰기
// 	ds.dataWriter.WriteAt(rankBytes, offset+RankOffset)
// }

// // readSetSize는 주어진 원소의 집합 크기를 읽습니다.
// func (ds *MmapDisjointSet) readSetSize(x uint64) uint16 {
// 	if x > ds.maxElement {
// 		return 0 // 범위를 벗어난 원소는 크기 0
// 	}

// 	offset := ds.getElementOffset(x)
// 	sizeBytes := make([]byte, 2)
// 	ds.dataReaderAt.ReadAt(sizeBytes, offset+SetSizeOffset)
// 	return binary.LittleEndian.Uint16(sizeBytes)
// }

// // writeSetSize는 주어진 원소의 집합 크기를 씁니다.
// func (ds *MmapDisjointSet) writeSetSize(x uint64, size uint16) {
// 	if x > ds.maxElement {
// 		return // 범위를 벗어난 원소는 무시
// 	}

// 	offset := ds.getElementOffset(x)
// 	sizeBytes := make([]byte, 2)
// 	binary.LittleEndian.PutUint16(sizeBytes, size)

// 	// 파일에 직접 쓰기
// 	ds.dataWriter.WriteAt(sizeBytes, offset+SetSizeOffset)
// }

// // MakeSet은 새로운 원소를 집합에 추가합니다.
// func (ds *MmapDisjointSet) MakeSet(x uint64) {
// 	if x > ds.maxElement {
// 		return // 범위를 벗어난 원소는 무시
// 	}

// 	// 이미 존재하는지 확인
// 	parent := ds.readParent(x)
// 	if parent != 0 { // 0은 초기화되지 않은 값
// 		return
// 	}

// 	// 자기 자신을 부모로 설정
// 	ds.writeParent(x, x)
// 	ds.writeRank(x, 0)
// 	ds.writeSetSize(x, 1) // 초기 집합 크기는 1
// 	ds.elementCount++

// 	// 일정 간격으로 파일 동기화
// 	if ds.elementCount%1000 == 0 {
// 		runtime.GC() // 명시적 GC 유도
// 		ds.Flush()   // 파일 동기화
// 	}
// }

// // Find는 원소 x가 속한 집합의 대표(루트)를 찾습니다.
// // 경로 압축 최적화를 적용합니다.
// func (ds *MmapDisjointSet) Find(x uint64) uint64 {
// 	if x > ds.maxElement {
// 		return x // 범위를 벗어난 원소는 자기 자신이 대표
// 	}

// 	// 원소가 존재하지 않으면 생성
// 	parent := ds.readParent(x)
// 	if parent == 0 { // 0은 초기화되지 않은 값
// 		ds.MakeSet(x)
// 		return x
// 	}

// 	// 경로 압축을 위한 재귀 호출
// 	if parent != x {
// 		newParent := ds.Find(parent)
// 		ds.writeParent(x, newParent)
// 		return newParent
// 	}

// 	return parent
// }

// // Union은 두 원소가 속한 집합을 합칩니다.
// // 랭크에 따른 병합 최적화를 적용합니다.
// func (ds *MmapDisjointSet) Union(x, y uint64) {
// 	rootX := ds.Find(x)
// 	rootY := ds.Find(y)

// 	if rootX == rootY {
// 		return // 이미 같은 집합에 속해 있음
// 	}

// 	// 집합 크기 읽기
// 	sizeX := ds.readSetSize(rootX)
// 	sizeY := ds.readSetSize(rootY)

// 	// 새 집합의 크기 계산 (오버플로 확인)
// 	var newSize uint16
// 	if uint32(sizeX)+uint32(sizeY) > 65535 {
// 		newSize = 65535 // uint16 최대값
// 	} else {
// 		newSize = sizeX + sizeY
// 	}

// 	// 랭크에 따라 병합 (랭크가 작은 트리를 랭크가 큰 트리에 붙임)
// 	rankX := ds.readRank(rootX)
// 	rankY := ds.readRank(rootY)

// 	if rankX < rankY {
// 		ds.writeParent(rootX, rootY)
// 		ds.writeSetSize(rootY, newSize)
// 	} else {
// 		ds.writeParent(rootY, rootX)
// 		ds.writeSetSize(rootX, newSize)

// 		// 두 트리의 랭크가 같으면 결과 트리의 랭크를 증가
// 		if rankX == rankY {
// 			ds.writeRank(rootX, rankX+1)
// 		}
// 	}
// }

// // GetSetSize는 주어진 원소가 속한 집합의 크기를 반환합니다.
// func (ds *MmapDisjointSet) GetSetSize(x uint64) uint16 {
// 	root := ds.Find(x)
// 	return ds.readSetSize(root)
// }

// // GetSetsChunked는 현재 분리된 모든 집합들을 청크 단위로 계산하여 반환합니다.
// func (ds *MmapDisjointSet) GetSetsChunked() map[uint64][]uint64 {
// 	result := make(map[uint64][]uint64)
// 	chunkCount := (ds.maxElement / ChunkSize) + 1

// 	// 청크 단위로 처리
// 	for chunk := uint64(0); chunk < chunkCount; chunk++ {
// 		startElem := chunk * ChunkSize
// 		endElem := (chunk + 1) * ChunkSize
// 		if endElem > ds.maxElement {
// 			endElem = ds.maxElement + 1
// 		}

// 		// 현재 청크의 원소들을 처리
// 		for element := startElem; element < endElem; element++ {
// 			parent := ds.readParent(element)
// 			if parent == 0 { // 초기화되지 않은 원소 건너뛰기
// 				continue
// 			}

// 			root := ds.Find(element) // 경로 압축이 여기서도 발생

// 			if _, exists := result[root]; !exists {
// 				result[root] = []uint64{}
// 			}
// 			result[root] = append(result[root], element)
// 		}

// 		// 각 청크 처리 후 가비지 컬렉션 유도
// 		runtime.GC()
// 	}

// 	// 각 집합 내부를 정렬 (선택적)
// 	for root := range result {
// 		sort.Slice(result[root], func(i, j int) bool {
// 			return result[root][i] < result[root][j]
// 		})
// 	}

// 	return result
// }

// // GetSetsWithSize는 현재 분리된 모든 집합들과 그 크기를 반환합니다.
// func (ds *MmapDisjointSet) GetSetsWithSize() map[uint64]struct {
// 	Elements []uint64
// 	Size     uint16
// } {
// 	// 모든 집합 획득
// 	sets := ds.GetSetsChunked()

// 	// 집합과 크기 매핑
// 	result := make(map[uint64]struct {
// 		Elements []uint64
// 		Size     uint16
// 	})

// 	for root, elements := range sets {
// 		result[root] = struct {
// 			Elements []uint64
// 			Size     uint16
// 		}{
// 			Elements: elements,
// 			Size:     ds.readSetSize(root),
// 		}
// 	}

// 	return result
// }

// // PrintSets는 현재 모든 분리 집합과 그 원소들을 출력합니다.
// func (ds *MmapDisjointSet) PrintSets() {
// 	sets := ds.GetSetsWithSize()

// 	fmt.Println("현재 분리 집합 상태:")
// 	fmt.Printf("총 %d개의 집합이 있습니다.\n", len(sets))

// 	for root, setInfo := range sets {
// 		fmt.Printf("집합 %d (크기: %d): %v\n", root, setInfo.Size, setInfo.Elements)
// 	}
// }

// // GetStats는 분리 집합의 통계 정보를 반환합니다.
// func (ds *MmapDisjointSet) GetStats() map[string]interface{} {
// 	stats := make(map[string]interface{})

// 	stats["maxElement"] = ds.maxElement
// 	stats["elementCount"] = ds.elementCount
// 	stats["fileSize"] = fileSize(ds.dataFile)

// 	return stats
// }

// // 파일 크기 반환 헬퍼 함수
// func fileSize(filePath string) int64 {
// 	info, err := os.Stat(filePath)
// 	if err != nil {
// 		return -1
// 	}
// 	return info.Size()
// }

// // 메인 함수 예시
// func main() {
// 	// 파일 경로 및 최대 원소 수 설정
// 	dataFile := "disjoint_set_data.bin"
// 	maxElement := uint64(100000) // 최대 10만 개의 원소

// 	// MmapDisjointSet 인스턴스 생성
// 	ds, err := NewMmapDisjointSet(dataFile, maxElement)
// 	if err != nil {
// 		fmt.Printf("Error: %v\n", err)
// 		return
// 	}
// 	defer ds.Close() // 리소스 정리

// 	// 6개의 집합 생성
// 	fmt.Println("6개의 분리 집합 생성 중...")

// 	// 집합 1 생성 (1, 2, 3, 4, 5)
// 	ds.MakeSet(1)
// 	for _, val := range []uint64{2, 3, 4, 5} {
// 		ds.MakeSet(val)
// 		ds.Union(1, val) // 1을 대표로 하여 모든 요소를 하나의 집합으로 연결
// 	}

// 	// 집합 2 생성 (10, 11, 12, 13)
// 	ds.MakeSet(10)
// 	for _, val := range []uint64{11, 12, 13} {
// 		ds.MakeSet(val)
// 		ds.Union(10, val) // 10을 대표로 하여 모든 요소를 하나의 집합으로 연결
// 	}

// 	// 집합 3 생성 (20, 21, 22)
// 	ds.MakeSet(20)
// 	for _, val := range []uint64{21, 22} {
// 		ds.MakeSet(val)
// 		ds.Union(20, val) // 20을 대표로 하여 모든 요소를 하나의 집합으로 연결
// 	}

// 	// 집합 4 생성 (30, 31, 32, 33, 34)
// 	ds.MakeSet(30)
// 	for _, val := range []uint64{31, 32, 33, 34} {
// 		ds.MakeSet(val)
// 		ds.Union(30, val) // 30을 대표로 하여 모든 요소를 하나의 집합으로 연결
// 	}

// 	// 집합 5 생성 (40, 41, 42)
// 	ds.MakeSet(40)
// 	for _, val := range []uint64{41, 42} {
// 		ds.MakeSet(val)
// 		ds.Union(40, val) // 40을 대표로 하여 모든 요소를 하나의 집합으로 연결
// 	}

// 	// 집합 6 생성 (50, 51, 52, 53)
// 	ds.MakeSet(50)
// 	for _, val := range []uint64{51, 52, 53} {
// 		ds.MakeSet(val)
// 		ds.Union(50, val) // 50을 대표로 하여 모든 요소를 하나의 집합으로 연결
// 	}

// 	// 초기 상태 출력
// 	fmt.Println("초기 상태 (6개의 분리 집합):")
// 	ds.PrintSets()

// 	// 6개의 집합을 3개로 합치기
// 	fmt.Println("\n6개의 집합을 3개로 합치는 중...")

// 	// 3개를 1개로 합치기 (집합 1, 3, 5 -> 집합 1)
// 	fmt.Println("\n집합 1, 3, 5 -> 집합 1로 합치기:")
// 	ds.Union(1, 20) // 집합 1과 집합 3 합치기
// 	ds.Union(1, 40) // 합쳐진 집합과 집합 5 합치기
// 	fmt.Println("합쳐진 집합 1의 크기:", ds.GetSetSize(1))

// 	// 2개를 1개로 합치기 (집합 2, 4 -> 집합 2)
// 	fmt.Println("\n집합 2, 4 -> 집합 2로 합치기:")
// 	ds.Union(10, 30) // 집합 2와 집합 4 합치기
// 	fmt.Println("합쳐진 집합 2의 크기:", ds.GetSetSize(10))

// 	// 집합 6은 그대로 유지
// 	fmt.Println("\n집합 6은 그대로 유지:")
// 	fmt.Println("집합 6의 크기:", ds.GetSetSize(50))

// 	// 최종 결과 출력
// 	fmt.Println("\n최종 상태 (3개의 분리 집합):")
// 	ds.PrintSets()

// 	// 합쳐진 집합의 크기 출력
// 	fmt.Printf("\n집합별 크기 확인:\n")
// 	fmt.Printf("집합 1(원소 1)의 크기: %d\n", ds.GetSetSize(1))
// 	fmt.Printf("집합 2(원소 10)의 크기: %d\n", ds.GetSetSize(10))
// 	fmt.Printf("집합 6(원소 50)의 크기: %d\n", ds.GetSetSize(50))

// 	// 통계 정보 출력
// 	fmt.Println("\n통계 정보:")
// 	stats := ds.GetStats()
// 	for k, v := range stats {
// 		fmt.Printf("%s: %v\n", k, v)
// 	}
// }
