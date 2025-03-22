package main

import (
	"fmt"
	"hash/fnv"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/edsrzf/mmap-go"
)

const (
	DEBUG           = true       // 디버깅 출력 활성화
	UnionSizeLimit  = 1000       // 집합의 원소 크기 제한
	ElementSize     = 13         // 원소 크기 (바이트) 11+2 (parent=8, rank=1, size=2, padding=2)
	InitialFileSize = 5000000000 // 초기 파일 크기 (50억 원소)
	WindowSize      = 100000000  // 메모리에 올리는 윈도우 크기 (1억 원소)
	BatchSize       = 10000      // 배치 크기 (1만 원소)
)

// 디버깅용 로그 함수
func debugLog(format string, args ...interface{}) {
	if DEBUG {
		fmt.Printf("[DEBUG] "+format+"\n", args...)
	}
}

// 블룸 필터 구조체
type BloomFilter struct {
	// Bloom Filter 크기
	size uint64
	// Bloom Filter 배열
	bf []uint64
	// 해시 함수 개수
	hashFunctions uint64
}

// 사용자 집합 구조체
type UserSet struct {
	// 집합 크기
	size uint16
	// 집합 원소
	elements []uint64
}

// 점진적 유니온 파인드 프로세서
type IncrementalUnionFindProcessor struct {
	dataDir string // 데이터 디렉토리

	// 파일 핸들러
	parentFile *os.File
	rankFile   *os.File
	sizeFile   *os.File

	// mmap 영역 (영구적인 데이터)
	parentArchive mmap.MMap
	rankArchive   mmap.MMap
	sizeArchive   mmap.MMap

	// 현재 윈도우 데이터
	parentSlice []uint64
	rankSlice   []uint8
	sizeSlice   []uint16

	// 윈도우 정보
	windowStart uint64
	windowEnd   uint64

	// 프리페치 관련 필드
	nextParentArchive mmap.MMap
	nextRankArchive   mmap.MMap
	nextSizeArchive   mmap.MMap
	nextParentSlice   []uint64
	nextRankSlice     []uint8
	nextSizeSlice     []uint16
	nextWindowStart   uint64
	nextWindowEnd     uint64
	prefetchDone      chan bool
	isShuttingDown    int32 // int32로 변경하여 atomic 연산 가능하게 함

	// 블룸 필터
	totalBloomFilter *BloomFilter // 전체 원소
	multiBloomFilter *BloomFilter // 다중 원소 집합

	// 총 원소 수 및 파일 용량
	totalElements uint64
	fileCapacity  uint64

	// 뮤텍스
	mu           sync.Mutex
	muLocked     int32    // 뮤텍스 락 상태 추적
	mutexHistory []string // 디버깅용 뮤텍스 락 히스토리
	// 시스템 페이지 크기 (초기화 시 설정)
	pageSize int
}

// 안전한 뮤텍스 락/언락 헬퍼 함수
func (i *IncrementalUnionFindProcessor) safeLock(caller string) {
	debugLog("%s: 뮤텍스 락 시도", caller)
	if atomic.LoadInt32(&i.muLocked) == 1 {
		debugLog("%s: 이미 락이 걸린 상태! 뮤텍스 히스토리: %v", caller, i.mutexHistory)
	}
	i.mu.Lock()
	atomic.StoreInt32(&i.muLocked, 1)
	i.mutexHistory = append(i.mutexHistory, caller+" locked")
	debugLog("%s: 뮤텍스 락 획득", caller)
}

func (i *IncrementalUnionFindProcessor) safeUnlock(caller string) {
	debugLog("%s: 뮤텍스 언락 시도", caller)
	if atomic.LoadInt32(&i.muLocked) == 0 {
		debugLog("%s: 락이 걸려있지 않은데 언락 시도! 뮤텍스 히스토리: %v", caller, i.mutexHistory)
		// 함수 호출 스택 출력
		buf := make([]byte, 4096)
		n := runtime.Stack(buf, false)
		debugLog("Stack trace:\n%s", buf[:n])
		return
	}
	atomic.StoreInt32(&i.muLocked, 0)
	i.mutexHistory = append(i.mutexHistory, caller+" unlocked")
	i.mu.Unlock()
	debugLog("%s: 뮤텍스 언락 완료", caller)
}

// 블룸 필터 생성 함수
func NewBloomFilter(expectedElements uint64, falsePositiveRate float64) *BloomFilter {
	// 최적의 비트 수 계산 (m = -n * ln(p) / (ln(2)^2))
	bitsNeeded := uint64(math.Ceil(-float64(expectedElements) * math.Log(falsePositiveRate) / math.Pow(math.Log(2), 2)))

	// 최적의 해시 함수 개수 계산 (k = (m/n) * ln(2))
	hashFunctions := uint64(math.Ceil(float64(bitsNeeded) / float64(expectedElements) * math.Log(2)))

	// 64비트 워드 단위로 배열 크기 계산
	arraySize := (bitsNeeded + 63) / 64

	return &BloomFilter{
		size:          bitsNeeded,
		bf:            make([]uint64, arraySize),
		hashFunctions: hashFunctions,
	}
}

// 단일 원소에 대한 해시 값 계산
func (bf *BloomFilter) getHashes(element uint64) []uint64 {
	// FNV 해시 함수 사용
	h := fnv.New64a()

	hashes := make([]uint64, bf.hashFunctions)
	for i := uint64(0); i < bf.hashFunctions; i++ {
		h.Reset()

		// 원소와 해시 함수 인덱스를 결합하여 다른 해시 생성
		binary := make([]byte, 9)
		binary[0] = byte(i)
		binary[1] = byte(element)
		binary[2] = byte(element >> 8)
		binary[3] = byte(element >> 16)
		binary[4] = byte(element >> 24)
		binary[5] = byte(element >> 32)
		binary[6] = byte(element >> 40)
		binary[7] = byte(element >> 48)
		binary[8] = byte(element >> 56)

		h.Write(binary)
		hashValue := h.Sum64() % bf.size
		hashes[i] = hashValue
	}

	return hashes
}

// 블룸 필터에 원소 추가
func (bf *BloomFilter) Add(element uint64) {
	hashes := bf.getHashes(element)
	for _, hash := range hashes {
		// 해당 비트 설정
		wordIdx := hash / 64
		bitIdx := hash % 64
		if wordIdx < uint64(len(bf.bf)) {
			bf.bf[wordIdx] |= (1 << bitIdx)
		}
	}
}

// 블룸 필터에 원소가 존재하는지 확인
func (bf *BloomFilter) Check(element uint64) bool {
	hashes := bf.getHashes(element)
	for _, hash := range hashes {
		wordIdx := hash / 64
		bitIdx := hash % 64

		// 하나라도 0이면 원소가 없는 것
		if wordIdx >= uint64(len(bf.bf)) || (bf.bf[wordIdx]&(1<<bitIdx)) == 0 {
			return false
		}
	}

	// 모든 비트가 1이면 원소가 있을 가능성이 있음
	return true
}

// 인크리멘탈 유니온 파인드 프로세서 생성
func NewIncrementalUnionFindProcessor(dataDir string) (*IncrementalUnionFindProcessor, error) {
	debugLog("NewIncrementalUnionFindProcessor: 시작")
	// 시스템 페이지 크기 가져오기
	pageSize := syscall.Getpagesize()

	// 디렉토리 생성
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("디렉토리 생성 실패: %v", err)
	}

	// 파일 경로 설정
	parentPath := filepath.Join(dataDir, "parent.bin")
	rankPath := filepath.Join(dataDir, "rank.bin")
	sizePath := filepath.Join(dataDir, "size.bin")

	// 파일 크기 계산 - 페이지 크기의 배수로 조정
	parentSize := ((int64(InitialFileSize) * 8) + int64(pageSize) - 1) / int64(pageSize) * int64(pageSize)
	rankSize := ((int64(InitialFileSize) * 1) + int64(pageSize) - 1) / int64(pageSize) * int64(pageSize)
	sizeSize := ((int64(InitialFileSize) * 2) + int64(pageSize) - 1) / int64(pageSize) * int64(pageSize)

	fmt.Printf("페이지 크기: %d 바이트\n", pageSize)
	fmt.Printf("파일 생성 - 초기 크기: %.2f GB (부모), %.2f GB (랭크), %.2f GB (크기)\n",
		float64(parentSize)/1024/1024/1024,
		float64(rankSize)/1024/1024/1024,
		float64(sizeSize)/1024/1024/1024)

	// 파일 생성
	parentFile, err := createLargeFile(parentPath, parentSize)
	if err != nil {
		return nil, err
	}

	rankFile, err := createLargeFile(rankPath, rankSize)
	if err != nil {
		parentFile.Close()
		return nil, err
	}

	sizeFile, err := createLargeFile(sizePath, sizeSize)
	if err != nil {
		parentFile.Close()
		rankFile.Close()
		return nil, err
	}

	// 블룸 필터 생성 (오탐율 0.1%)
	totalBloomFilter := NewBloomFilter(InitialFileSize, 0.001)
	multiBloomFilter := NewBloomFilter(InitialFileSize/10, 0.001) // 다중 집합은 적을 것으로 예상

	// prefetchDone 채널의 버퍼 크기를 2로 설정하여 데드락 방지
	processor := &IncrementalUnionFindProcessor{
		dataDir:          dataDir,
		parentFile:       parentFile,
		rankFile:         rankFile,
		sizeFile:         sizeFile,
		totalBloomFilter: totalBloomFilter,
		multiBloomFilter: multiBloomFilter,
		totalElements:    0,
		fileCapacity:     InitialFileSize,
		prefetchDone:     make(chan bool, 2), // 버퍼 크기 증가
		isShuttingDown:   0,                  // 0은 false에 해당 (int32 타입)
		pageSize:         pageSize,
	}

	// 초기 윈도우 매핑
	if err := processor.mapWindow(0); err != nil {
		parentFile.Close()
		rankFile.Close()
		sizeFile.Close()
		return nil, err
	}

	// 다음 윈도우 프리페칭 시작
	processor.prefetchNextWindow()
	debugLog("NewIncrementalUnionFindProcessor: 완료")
	return processor, nil
}

// 특정 위치에 윈도우 매핑
func (i *IncrementalUnionFindProcessor) mapWindow(startElement uint64) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	// 종료 중인 경우 작업 중지
	if atomic.LoadInt32((*int32)(&i.isShuttingDown)) == 1 {
		return fmt.Errorf("프로세서가 종료 중입니다")
	}

	// 이미 매핑된 경우, 현재 윈도우 내에 있는지 확인
	if i.windowStart <= startElement && startElement < i.windowEnd {
		return nil // 이미 적절한 윈도우에 매핑됨
	}

	// 프리페치된 윈도우가 있고, 요청한 위치가 프리페치된 윈도우 범위 내인지 확인
	if i.nextParentArchive != nil && i.nextWindowStart <= startElement && startElement < i.nextWindowEnd {
		// 프리페치된 윈도우로 전환
		fmt.Printf("프리페치된 윈도우로 전환: %d-%d\n", i.nextWindowStart, i.nextWindowEnd)

		// 현재 윈도우 해제
		if i.parentArchive != nil {
			if err := i.flushWindowInternal(); err != nil {
				return fmt.Errorf("이전 윈도우 flush 실패: %v", err)
			}

			if err := i.parentArchive.Unmap(); err != nil {
				return fmt.Errorf("parent mmap 해제 실패: %v", err)
			}
			if err := i.rankArchive.Unmap(); err != nil {
				return fmt.Errorf("rank mmap 해제 실패: %v", err)
			}
			if err := i.sizeArchive.Unmap(); err != nil {
				return fmt.Errorf("size mmap 해제 실패: %v", err)
			}
		}

		// 프리페치된 윈도우를 현재 윈도우로 설정
		i.windowStart = i.nextWindowStart
		i.windowEnd = i.nextWindowEnd
		i.parentArchive = i.nextParentArchive
		i.rankArchive = i.nextRankArchive
		i.sizeArchive = i.nextSizeArchive
		i.parentSlice = i.nextParentSlice
		i.rankSlice = i.nextRankSlice
		i.sizeSlice = i.nextSizeSlice

		// 프리페치 상태 초기화
		i.nextParentArchive = nil
		i.nextRankArchive = nil
		i.nextSizeArchive = nil
		i.nextParentSlice = nil
		i.nextRankSlice = nil
		i.nextSizeSlice = nil
		i.nextWindowStart = 0
		i.nextWindowEnd = 0

		// 다음 윈도우 프리페칭 시작
		i.prefetchNextWindow()
		return nil
	}

	// 기존 mmap이 있으면 해제
	if i.parentArchive != nil {
		if err := i.flushWindowInternal(); err != nil {
			return fmt.Errorf("이전 윈도우 flush 실패: %v", err)
		}

		if err := i.parentArchive.Unmap(); err != nil {
			return fmt.Errorf("parent mmap 해제 실패: %v", err)
		}
		if err := i.rankArchive.Unmap(); err != nil {
			return fmt.Errorf("rank mmap 해제 실패: %v", err)
		}
		if err := i.sizeArchive.Unmap(); err != nil {
			return fmt.Errorf("size mmap 해제 실패: %v", err)
		}
	}

	// 윈도우 시작점을 페이지 크기의 배수로 정렬
	alignedStartElement := (startElement / uint64(i.pageSize)) * uint64(i.pageSize)

	// 윈도우 범위 계산
	i.windowStart = alignedStartElement
	i.windowEnd = alignedStartElement + WindowSize
	if i.windowEnd > i.fileCapacity {
		i.windowEnd = i.fileCapacity
	}

	windowSize := i.windowEnd - i.windowStart
	fmt.Printf("새 윈도우 매핑: %d-%d (크기: %d 원소, 정렬됨: %d)\n",
		i.windowStart, i.windowEnd, windowSize, alignedStartElement)

	// 파일 오프셋 계산 (페이지 정렬 적용)
	parentOffset := int64(i.windowStart * 8) // uint64 = 8 bytes
	// 랭크와 사이즈 오프셋도 페이지 크기에 맞게 정렬
	rankOffset := (int64(i.windowStart) / int64(i.pageSize)) * int64(i.pageSize)
	sizeOffset := (int64(i.windowStart) / int64(i.pageSize)) * int64(i.pageSize) * 2

	// 오프셋 정보 출력 (디버깅용)
	fmt.Printf("파일 오프셋: parent=%d, rank=%d, size=%d\n", parentOffset, rankOffset, sizeOffset)

	// mmap 생성
	var err error
	i.parentArchive, err = mmap.MapRegion(i.parentFile, int(windowSize*8), mmap.RDWR, 0, parentOffset)
	if err != nil {
		return fmt.Errorf("parent mmap 생성 실패: %v", err)
	}
	i.rankArchive, err = mmap.MapRegion(i.rankFile, int(windowSize), mmap.RDWR, 0, rankOffset)
	if err != nil {
		i.parentArchive.Unmap()
		return fmt.Errorf("rank mmap 생성 실패: %v (오프셋: %d)", err, rankOffset)
	}
	i.sizeArchive, err = mmap.MapRegion(i.sizeFile, int(windowSize*2), mmap.RDWR, 0, sizeOffset)
	if err != nil {
		i.parentArchive.Unmap()
		i.rankArchive.Unmap()
		return fmt.Errorf("size mmap 생성 실패: %v", err)
	}

	// 메모리 슬라이스 생성
	i.parentSlice = make([]uint64, windowSize)
	i.rankSlice = make([]uint8, windowSize)
	i.sizeSlice = make([]uint16, windowSize)

	// mmap에서 슬라이스로 데이터 로드
	for idx := uint64(0); idx < windowSize; idx++ {
		parentOffset := idx * 8
		if parentOffset+8 <= uint64(len(i.parentArchive)) {
			i.parentSlice[idx] = uint64(i.parentArchive[parentOffset]) |
				uint64(i.parentArchive[parentOffset+1])<<8 |
				uint64(i.parentArchive[parentOffset+2])<<16 |
				uint64(i.parentArchive[parentOffset+3])<<24 |
				uint64(i.parentArchive[parentOffset+4])<<32 |
				uint64(i.parentArchive[parentOffset+5])<<40 |
				uint64(i.parentArchive[parentOffset+6])<<48 |
				uint64(i.parentArchive[parentOffset+7])<<56

			// 초기화되지 않은 원소는 초기화 (0인 경우)
			if i.parentSlice[idx] == 0 {
				globalIdx := i.windowStart + idx
				i.parentSlice[idx] = globalIdx
			}
		}
	}

	// rank와 size 데이터의 오프셋을 고려하여 로드
	// 윈도우 시작점에 해당하는 rank 또는 size 데이터의 상대적 위치 계산
	rankOffset = int64(i.windowStart % uint64(i.pageSize))
	for idx := uint64(0); idx < windowSize && idx < uint64(len(i.rankArchive)); idx++ {
		if rankOffset+int64(idx) < int64(len(i.rankArchive)) {
			i.rankSlice[idx] = i.rankArchive[rankOffset+int64(idx)]
		}
	}

	sizeOffset = int64((i.windowStart % uint64(i.pageSize)) * 2)
	for idx := uint64(0); idx < windowSize; idx++ {
		offset := sizeOffset + int64(idx*2)
		if offset+2 <= int64(len(i.sizeArchive)) {
			i.sizeSlice[idx] = uint16(i.sizeArchive[offset]) | uint16(i.sizeArchive[offset+1])<<8

			// 초기화되지 않은 원소는 초기화 (0인 경우)
			if i.sizeSlice[idx] == 0 {
				i.sizeSlice[idx] = 1
			}
		}
	}

	// 다음 윈도우 프리페칭 시작
	i.prefetchNextWindow()

	return nil
}

// 현재 윈도우 변경사항 디스크에 flush (외부 호출용)
func (i *IncrementalUnionFindProcessor) flushWindow() error {
	i.mu.Lock()
	defer i.mu.Unlock()

	return i.flushWindowInternal()
}

// 내부 구현 (뮤텍스 락 없이 호출)
func (i *IncrementalUnionFindProcessor) flushWindowInternal() error {
	if i.parentArchive == nil || i.rankArchive == nil || i.sizeArchive == nil {
		return nil // 아직 매핑된 윈도우가 없음
	}

	windowSize := uint64(len(i.parentSlice))

	// parentSlice를 parentArchive에 flush
	for idx := uint64(0); idx < windowSize; idx++ {
		offset := idx * 8
		if offset+8 <= uint64(len(i.parentArchive)) {
			val := i.parentSlice[idx]
			i.parentArchive[offset] = byte(val)
			i.parentArchive[offset+1] = byte(val >> 8)
			i.parentArchive[offset+2] = byte(val >> 16)
			i.parentArchive[offset+3] = byte(val >> 24)
			i.parentArchive[offset+4] = byte(val >> 32)
			i.parentArchive[offset+5] = byte(val >> 40)
			i.parentArchive[offset+6] = byte(val >> 48)
			i.parentArchive[offset+7] = byte(val >> 56)
		}
	}

	// rankSlice를 rankArchive에 flush (오프셋 고려)
	rankOffset := int64(i.windowStart % uint64(i.pageSize))
	for idx := uint64(0); idx < windowSize && idx+uint64(rankOffset) < uint64(len(i.rankArchive)); idx++ {
		i.rankArchive[idx+uint64(rankOffset)] = i.rankSlice[idx]
	}

	// sizeSlice를 sizeArchive에 flush (오프셋 고려)
	sizeOffset := int64((i.windowStart % uint64(i.pageSize)) * 2)
	for idx := uint64(0); idx < windowSize; idx++ {
		offset := uint64(sizeOffset) + idx*2
		if offset+2 <= uint64(len(i.sizeArchive)) {
			val := i.sizeSlice[idx]
			i.sizeArchive[offset] = byte(val)
			i.sizeArchive[offset+1] = byte(val >> 8)
		}
	}

	// 디스크에 동기화
	if err := i.parentArchive.Flush(); err != nil {
		return fmt.Errorf("parent 아카이브 flush 실패: %v", err)
	}
	if err := i.rankArchive.Flush(); err != nil {
		return fmt.Errorf("rank 아카이브 flush 실패: %v", err)
	}
	if err := i.sizeArchive.Flush(); err != nil {
		return fmt.Errorf("size 아카이브 flush 실패: %v", err)
	}

	return nil
}

// 파일 확장 (필요시)
func (i *IncrementalUnionFindProcessor) extendFiles(requiredSize uint64) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	if requiredSize <= i.fileCapacity {
		return nil // 확장 필요 없음
	}

	// 현재 윈도우 flush
	if err := i.flushWindowInternal(); err != nil {
		return fmt.Errorf("파일 확장 전 flush 실패: %v", err)
	}

	// 기존 mmap 해제
	if i.parentArchive != nil {
		if err := i.parentArchive.Unmap(); err != nil {
			return fmt.Errorf("parent mmap 해제 실패: %v", err)
		}
		if err := i.rankArchive.Unmap(); err != nil {
			return fmt.Errorf("rank mmap 해제 실패: %v", err)
		}
		if err := i.sizeArchive.Unmap(); err != nil {
			return fmt.Errorf("size mmap 해제 실패: %v", err)
		}

		i.parentArchive = nil
		i.rankArchive = nil
		i.sizeArchive = nil
	}

	// 프리페치된 다음 윈도우가 있으면 해제
	if i.nextParentArchive != nil {
		i.nextParentArchive.Unmap()
		i.nextRankArchive.Unmap()
		i.nextSizeArchive.Unmap()
		i.nextParentArchive = nil
		i.nextRankArchive = nil
		i.nextSizeArchive = nil
	}

	// 필요한 크기보다 큰 새 용량 계산
	newCapacity := uint64(float64(i.fileCapacity) * 1.5)
	if newCapacity < requiredSize {
		newCapacity = requiredSize
	}

	fmt.Printf("파일 확장 중: %d → %d 원소 (%.2f GB → %.2f GB)\n",
		i.fileCapacity, newCapacity,
		float64(i.fileCapacity*8)/1024/1024/1024,
		float64(newCapacity*8)/1024/1024/1024)

	// 파일 크기 확장 (페이지 크기 정렬 적용)
	parentSize := ((int64(newCapacity) * 8) + int64(i.pageSize) - 1) / int64(i.pageSize) * int64(i.pageSize)
	rankSize := ((int64(newCapacity) * 1) + int64(i.pageSize) - 1) / int64(i.pageSize) * int64(i.pageSize)
	sizeSize := ((int64(newCapacity) * 2) + int64(i.pageSize) - 1) / int64(i.pageSize) * int64(i.pageSize)

	if err := i.parentFile.Truncate(parentSize); err != nil {
		return fmt.Errorf("parent 파일 확장 실패: %v", err)
	}
	if err := i.rankFile.Truncate(rankSize); err != nil {
		return fmt.Errorf("rank 파일 확장 실패: %v", err)
	}
	if err := i.sizeFile.Truncate(sizeSize); err != nil {
		return fmt.Errorf("size 파일 확장 실패: %v", err)
	}

	// 파일 용량 업데이트
	i.fileCapacity = newCapacity

	// 새 윈도우 매핑
	return i.mapWindowInternal(i.windowStart)
}

// 내부 구현 (뮤텍스 락 없이 호출)
func (i *IncrementalUnionFindProcessor) mapWindowInternal(startElement uint64) error {
	// 종료 중인 경우 작업 중지
	if atomic.LoadInt32((*int32)(&i.isShuttingDown)) == 1 {
		return fmt.Errorf("프로세서가 종료 중입니다")
	}

	// 이미 매핑된 경우, 현재 윈도우 내에 있는지 확인
	if i.windowStart <= startElement && startElement < i.windowEnd {
		return nil // 이미 적절한 윈도우에 매핑됨
	}

	// 프리페치된 윈도우가 있고, 요청한 위치가 프리페치된 윈도우 범위 내인지 확인
	if i.nextParentArchive != nil && i.nextWindowStart <= startElement && startElement < i.nextWindowEnd {
		// 프리페치된 윈도우로 전환
		fmt.Printf("프리페치된 윈도우로 전환: %d-%d\n", i.nextWindowStart, i.nextWindowEnd)

		// 현재 윈도우 해제
		if i.parentArchive != nil {
			if err := i.flushWindowInternal(); err != nil {
				return fmt.Errorf("이전 윈도우 flush 실패: %v", err)
			}

			if err := i.parentArchive.Unmap(); err != nil {
				return fmt.Errorf("parent mmap 해제 실패: %v", err)
			}
			if err := i.rankArchive.Unmap(); err != nil {
				return fmt.Errorf("rank mmap 해제 실패: %v", err)
			}
			if err := i.sizeArchive.Unmap(); err != nil {
				return fmt.Errorf("size mmap 해제 실패: %v", err)
			}
		}

		// 프리페치된 윈도우를 현재 윈도우로 설정
		i.windowStart = i.nextWindowStart
		i.windowEnd = i.nextWindowEnd
		i.parentArchive = i.nextParentArchive
		i.rankArchive = i.nextRankArchive
		i.sizeArchive = i.nextSizeArchive
		i.parentSlice = i.nextParentSlice
		i.rankSlice = i.nextRankSlice
		i.sizeSlice = i.nextSizeSlice

		// 프리페치 상태 초기화
		i.nextParentArchive = nil
		i.nextRankArchive = nil
		i.nextSizeArchive = nil
		i.nextParentSlice = nil
		i.nextRankSlice = nil
		i.nextSizeSlice = nil
		i.nextWindowStart = 0
		i.nextWindowEnd = 0

		// 다음 윈도우 프리페칭 시작
		i.prefetchNextWindow()
		return nil
	}

	// 기존 mmap이 있으면 해제
	if i.parentArchive != nil {
		if err := i.flushWindowInternal(); err != nil {
			return fmt.Errorf("이전 윈도우 flush 실패: %v", err)
		}

		if err := i.parentArchive.Unmap(); err != nil {
			return fmt.Errorf("parent mmap 해제 실패: %v", err)
		}
		if err := i.rankArchive.Unmap(); err != nil {
			return fmt.Errorf("rank mmap 해제 실패: %v", err)
		}
		if err := i.sizeArchive.Unmap(); err != nil {
			return fmt.Errorf("size mmap 해제 실패: %v", err)
		}
	}

	// 윈도우 시작점을 페이지 크기의 배수로 정렬
	alignedStartElement := (startElement / uint64(i.pageSize)) * uint64(i.pageSize)

	// 윈도우 범위 계산
	i.windowStart = alignedStartElement
	i.windowEnd = alignedStartElement + WindowSize
	if i.windowEnd > i.fileCapacity {
		i.windowEnd = i.fileCapacity
	}

	windowSize := i.windowEnd - i.windowStart
	fmt.Printf("새 윈도우 매핑: %d-%d (크기: %d 원소, 정렬됨: %d)\n",
		i.windowStart, i.windowEnd, windowSize, alignedStartElement)

	// 파일 오프셋 계산 (페이지 정렬 적용)
	parentOffset := int64(i.windowStart * 8) // uint64 = 8 bytes
	// 랭크와 사이즈 오프셋도 페이지 크기에 맞게 정렬
	rankOffset := (int64(i.windowStart) / int64(i.pageSize)) * int64(i.pageSize)
	sizeOffset := (int64(i.windowStart) / int64(i.pageSize)) * int64(i.pageSize) * 2

	// 오프셋 정보 출력 (디버깅용)
	fmt.Printf("파일 오프셋: parent=%d, rank=%d, size=%d\n", parentOffset, rankOffset, sizeOffset)

	// mmap 생성
	var err error
	i.parentArchive, err = mmap.MapRegion(i.parentFile, int(windowSize*8), mmap.RDWR, 0, parentOffset)
	if err != nil {
		return fmt.Errorf("parent mmap 생성 실패: %v", err)
	}
	i.rankArchive, err = mmap.MapRegion(i.rankFile, int(windowSize), mmap.RDWR, 0, rankOffset)
	if err != nil {
		i.parentArchive.Unmap()
		return fmt.Errorf("rank mmap 생성 실패: %v (오프셋: %d)", err, rankOffset)
	}
	i.sizeArchive, err = mmap.MapRegion(i.sizeFile, int(windowSize*2), mmap.RDWR, 0, sizeOffset)
	if err != nil {
		i.parentArchive.Unmap()
		i.rankArchive.Unmap()
		return fmt.Errorf("size mmap 생성 실패: %v", err)
	}

	// 메모리 슬라이스 생성
	i.parentSlice = make([]uint64, windowSize)
	i.rankSlice = make([]uint8, windowSize)
	i.sizeSlice = make([]uint16, windowSize)

	// mmap에서 슬라이스로 데이터 로드
	for idx := uint64(0); idx < windowSize; idx++ {
		parentOffset := idx * 8
		if parentOffset+8 <= uint64(len(i.parentArchive)) {
			i.parentSlice[idx] = uint64(i.parentArchive[parentOffset]) |
				uint64(i.parentArchive[parentOffset+1])<<8 |
				uint64(i.parentArchive[parentOffset+2])<<16 |
				uint64(i.parentArchive[parentOffset+3])<<24 |
				uint64(i.parentArchive[parentOffset+4])<<32 |
				uint64(i.parentArchive[parentOffset+5])<<40 |
				uint64(i.parentArchive[parentOffset+6])<<48 |
				uint64(i.parentArchive[parentOffset+7])<<56

			// 초기화되지 않은 원소는 초기화 (0인 경우)
			if i.parentSlice[idx] == 0 {
				globalIdx := i.windowStart + idx
				i.parentSlice[idx] = globalIdx
			}
		}
	}

	// rank와 size 데이터의 오프셋을 고려하여 로드
	// 윈도우 시작점에 해당하는 rank 또는 size 데이터의 상대적 위치 계산
	rankOffset = int64(i.windowStart % uint64(i.pageSize))
	for idx := uint64(0); idx < windowSize && idx < uint64(len(i.rankArchive)); idx++ {
		if rankOffset+int64(idx) < int64(len(i.rankArchive)) {
			i.rankSlice[idx] = i.rankArchive[rankOffset+int64(idx)]
		}
	}

	sizeOffset = int64((i.windowStart % uint64(i.pageSize)) * 2)
	for idx := uint64(0); idx < windowSize; idx++ {
		offset := sizeOffset + int64(idx*2)
		if offset+2 <= int64(len(i.sizeArchive)) {
			i.sizeSlice[idx] = uint16(i.sizeArchive[offset]) | uint16(i.sizeArchive[offset+1])<<8

			// 초기화되지 않은 원소는 초기화 (0인 경우)
			if i.sizeSlice[idx] == 0 {
				i.sizeSlice[idx] = 1
			}
		}
	}

	// 다음 윈도우 프리페칭 시작
	i.prefetchNextWindow()

	return nil
}

// 리소스 해제

// 원소의 윈도우 준비
func (i *IncrementalUnionFindProcessor) ensureWindowContains(element uint64) error {
	// 파일 확장 (필요시)
	if element >= i.fileCapacity {
		if err := i.extendFiles(element + 1); err != nil {
			return fmt.Errorf("파일 확장 실패: %v", err)
		}
	}

	// 현재 윈도우 범위 확인
	if element < i.windowStart || element >= i.windowEnd {
		// 윈도우 이동 필요
		newStart := (element / WindowSize) * WindowSize // 윈도우 경계에 맞추기
		// 페이지 정렬도 적용
		newStart = (newStart / uint64(i.pageSize)) * uint64(i.pageSize)

		if err := i.mapWindow(newStart); err != nil {
			return fmt.Errorf("윈도우 매핑 실패: %v", err)
		}
	}

	return nil
}

// 원소의 로컬 인덱스 계산
func (i *IncrementalUnionFindProcessor) getLocalIndex(element uint64) (uint64, error) {
	if err := i.ensureWindowContains(element); err != nil {
		return 0, err
	}

	return element - i.windowStart, nil
}

// Find 함수 - 경로 압축을 이용한 Find
func (i *IncrementalUnionFindProcessor) Find(element uint64) (uint64, error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	// 원소를 포함하는 윈도우 확보
	if err := i.ensureWindowContains(element); err != nil {
		return element, err // 오류 발생 시 자신 반환
	}

	// 로컬 인덱스 계산
	localIdx, err := i.getLocalIndex(element)
	if err != nil {
		return element, err
	}

	// 미초기화된 원소이면 초기화
	if i.parentSlice[localIdx] == 0 {
		i.parentSlice[localIdx] = element
		i.rankSlice[localIdx] = 0
		i.sizeSlice[localIdx] = 1

		if element+1 > i.totalElements {
			i.totalElements = element + 1
		}
	}

	// 경로 압축 구현
	if i.parentSlice[localIdx] != element {
		// 재귀 호출 (락은 이미 잡고 있음)
		root, err := i.findInternal(i.parentSlice[localIdx])
		if err != nil {
			return element, err
		}

		// 경로 압축
		i.parentSlice[localIdx] = root
		return root, nil
	}

	return element, nil
}

// find 함수 - 내부용 (뮤텍스 잠금 없이 호출)
func (i *IncrementalUnionFindProcessor) findInternal(element uint64) (uint64, error) {
	// 원소를 포함하는 윈도우 확보
	if err := i.ensureWindowContains(element); err != nil {
		return element, err // 오류 발생 시 자신 반환
	}

	// 로컬 인덱스 계산
	localIdx, err := i.getLocalIndex(element)
	if err != nil {
		return element, err
	}

	// 미초기화된 원소이면 초기화
	if i.parentSlice[localIdx] == 0 {
		i.parentSlice[localIdx] = element
		i.rankSlice[localIdx] = 0
		i.sizeSlice[localIdx] = 1

		if element+1 > i.totalElements {
			i.totalElements = element + 1
		}
	}

	// 경로 압축 구현
	if i.parentSlice[localIdx] != element {
		// 재귀 호출
		root, err := i.findInternal(i.parentSlice[localIdx])
		if err != nil {
			return element, err
		}

		// 경로 압축
		i.parentSlice[localIdx] = root
		return root, nil
	}

	return element, nil
}

// Union 함수 - 랭크를 이용한 Union
func (i *IncrementalUnionFindProcessor) Union(x uint64, y uint64) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	// 각 원소의 루트 찾기
	rootX, err := i.findInternal(x)
	if err != nil {
		return err
	}

	rootY, err := i.findInternal(y)
	if err != nil {
		return err
	}

	// 같은 집합이면 아무것도 하지 않음
	if rootX == rootY {
		return nil
	}

	// rootX 정보 가져오기
	if err := i.ensureWindowContains(rootX); err != nil {
		return err
	}

	localRootX, err := i.getLocalIndex(rootX)
	if err != nil {
		return err
	}

	rankX := i.rankSlice[localRootX]
	sizeX := i.sizeSlice[localRootX]

	// rootY 정보 가져오기
	if err := i.ensureWindowContains(rootY); err != nil {
		return err
	}

	localRootY, err := i.getLocalIndex(rootY)
	if err != nil {
		return err
	}

	rankY := i.rankSlice[localRootY]
	sizeY := i.sizeSlice[localRootY]

	// 랭크가 낮은 트리를 랭크가 높은 트리 아래에 붙임
	if rankX < rankY {
		// rootX를 rootY 아래에 붙임
		if err := i.ensureWindowContains(rootX); err != nil {
			return err
		}
		localRootX, _ = i.getLocalIndex(rootX)
		i.parentSlice[localRootX] = rootY

		// rootY의 크기 업데이트
		if err := i.ensureWindowContains(rootY); err != nil {
			return err
		}
		localRootY, _ = i.getLocalIndex(rootY)
		i.sizeSlice[localRootY] = sizeY + sizeX

		// 크기가 2 이상이 된 경우 multiBloomFilter에 추가
		if sizeY+sizeX >= 2 {
			i.multiBloomFilter.Add(rootY)
		}
	} else if rankX > rankY {
		// rootY를 rootX 아래에 붙임
		if err := i.ensureWindowContains(rootY); err != nil {
			return err
		}
		localRootY, _ = i.getLocalIndex(rootY)
		i.parentSlice[localRootY] = rootX

		// rootX의 크기 업데이트
		if err := i.ensureWindowContains(rootX); err != nil {
			return err
		}
		localRootX, _ = i.getLocalIndex(rootX)
		i.sizeSlice[localRootX] = sizeX + sizeY

		// 크기가 2 이상이 된 경우 multiBloomFilter에 추가
		if sizeX+sizeY >= 2 {
			i.multiBloomFilter.Add(rootX)
		}
	} else {
		// 랭크가 같으면 한쪽을 다른쪽 아래에 붙이고 랭크 증가
		if err := i.ensureWindowContains(rootY); err != nil {
			return err
		}
		localRootY, _ = i.getLocalIndex(rootY)
		i.parentSlice[localRootY] = rootX

		// rootX의 랭크와 크기 업데이트
		if err := i.ensureWindowContains(rootX); err != nil {
			return err
		}
		localRootX, _ = i.getLocalIndex(rootX)
		i.rankSlice[localRootX]++
		i.sizeSlice[localRootX] = sizeX + sizeY

		// 크기가 2 이상이 된 경우 multiBloomFilter에 추가
		if sizeX+sizeY >= 2 {
			i.multiBloomFilter.Add(rootX)
		}
	}

	return nil
}

// 원소 초기화 (없는 경우)
// 원소 초기화 (없는 경우)
// 원소 초기화 (없는 경우)
// 원소 초기화 (없는 경우)

// 원소 초기화 - 데드락 방지 및 안전성 강화
func (i *IncrementalUnionFindProcessor) initializeElement(element uint64) error {
	// 종료 중인지 먼저 확인
	if atomic.LoadInt32(&i.isShuttingDown) == 1 {
		return fmt.Errorf("프로세서가 종료 중입니다")
	}

	// 타임아웃 있는 락 획득 시도
	lockAcquired := make(chan struct{}, 1)
	go func() {
		i.mu.Lock()
		select {
		case lockAcquired <- struct{}{}:
		default:
			// 채널 버퍼가 가득 찬 경우
			i.mu.Unlock()
		}
	}()

	// 최대 100ms 기다림
	select {
	case <-lockAcquired:
		// 락 획득 성공
		defer func() {
			if r := recover(); r != nil {
				fmt.Printf("initializeElement 패닉 복구: %v\n", r)
				i.mu.Unlock()
			}
		}()
		defer i.mu.Unlock()
	case <-time.After(100 * time.Millisecond):
		return fmt.Errorf("뮤텍스 획득 타임아웃: element=%d", element)
	}

	// 파일 확장 필요 시
	if element >= i.fileCapacity {
		i.mu.Unlock() // 임시로 락 해제
		if err := i.extendFiles(element + 1); err != nil {
			return fmt.Errorf("파일 확장 실패: %v", err)
		}

		// 다시 락 획득 시도 (타임아웃 포함)
		acquiredAgain := make(chan struct{}, 1)
		go func() {
			i.mu.Lock()
			select {
			case acquiredAgain <- struct{}{}:
			default:
				i.mu.Unlock()
			}
		}()

		select {
		case <-acquiredAgain:
			// 다시 락 획득
			defer i.mu.Unlock()
		case <-time.After(100 * time.Millisecond):
			return fmt.Errorf("파일 확장 후 뮤텍스 재획득 타임아웃")
		}
	}

	// 윈도우 범위 체크
	if element < i.windowStart || element >= i.windowEnd {
		newStart := (element / WindowSize) * WindowSize
		newStart = (newStart / uint64(i.pageSize)) * uint64(i.pageSize)

		// 임시로 락 해제
		i.mu.Unlock()
		if err := i.mapWindow(newStart); err != nil {
			return fmt.Errorf("윈도우 매핑 실패: %v", err)
		}

		// 다시 락 획득 시도
		acquiredAgain := make(chan struct{}, 1)
		go func() {
			i.mu.Lock()
			select {
			case acquiredAgain <- struct{}{}:
			default:
				i.mu.Unlock()
			}
		}()

		select {
		case <-acquiredAgain:
			// 다시 락 획득
			defer i.mu.Unlock()
		case <-time.After(100 * time.Millisecond):
			return fmt.Errorf("윈도우 매핑 후 뮤텍스 재획득 타임아웃")
		}
	}

	// 윈도우 범위 재확인
	if element < i.windowStart || element >= i.windowEnd {
		return fmt.Errorf("윈도우 범위 오류: element=%d, 윈도우=[%d-%d]", element, i.windowStart, i.windowEnd)
	}

	// 로컬 인덱스 계산 및 범위 체크
	localIdx := element - i.windowStart
	if localIdx >= uint64(len(i.parentSlice)) {
		return fmt.Errorf("배열 범위 초과: localIdx=%d, 크기=%d", localIdx, len(i.parentSlice))
	}

	// 이미 초기화되었으면 빠른 반환
	if i.parentSlice[localIdx] != 0 {
		return nil
	}

	// 원소 초기화
	i.parentSlice[localIdx] = element
	i.rankSlice[localIdx] = 0
	i.sizeSlice[localIdx] = 1

	// 총 원소 수 업데이트
	if element+1 > i.totalElements {
		i.totalElements = element + 1
	}

	return nil
}

// 다음 윈도우 프리페칭 - 데드락 방지 타임아웃 추가
func (i *IncrementalUnionFindProcessor) prefetchNextWindow() {
	// 종료 중인지 확인
	if atomic.LoadInt32(&i.isShuttingDown) == 1 {
		return
	}

	// 이미 프리페치된 윈도우가 있으면 무시
	if i.nextParentArchive != nil {
		return
	}

	// 다음 윈도우 위치 계산
	nextWindowStart := i.windowEnd
	if nextWindowStart >= i.fileCapacity {
		return // 더 이상 다음 윈도우가 없음
	}

	// 비동기적으로 다음 윈도우 로드
	go func() {
		// 고루틴 패닉 처리
		defer func() {
			if r := recover(); r != nil {
				fmt.Printf("프리페치 고루틴 패닉: %v\n", r)
			}
			// 채널 통지 (데드락 방지)
			select {
			case i.prefetchDone <- true:
			default:
				// 채널이 가득 찬 경우 무시
			}
		}()

		// 종료 중이면 중단
		if atomic.LoadInt32(&i.isShuttingDown) == 1 {
			return
		}

		// 윈도우 위치 및 크기 계산
		alignedNextWindowStart := (nextWindowStart / uint64(i.pageSize)) * uint64(i.pageSize)
		windowEnd := alignedNextWindowStart + WindowSize
		if windowEnd > i.fileCapacity {
			windowEnd = i.fileCapacity
		}
		windowSize := windowEnd - alignedNextWindowStart

		fmt.Printf("다음 윈도우 프리페칭: %d-%d (크기: %d 원소, 정렬됨: %d)\n",
			alignedNextWindowStart, windowEnd, windowSize, alignedNextWindowStart)

		// 파일 오프셋 계산
		parentOffset := int64(alignedNextWindowStart * 8)
		rankOffset := (int64(alignedNextWindowStart) / int64(i.pageSize)) * int64(i.pageSize)
		sizeOffset := (int64(alignedNextWindowStart) / int64(i.pageSize)) * int64(i.pageSize) * 2

		// mmap 생성
		parentArchive, err := mmap.MapRegion(i.parentFile, int(windowSize*8), mmap.RDWR, 0, parentOffset)
		if err != nil {
			fmt.Printf("다음 윈도우 parent mmap 생성 실패: %v\n", err)
			return
		}

		rankArchive, err := mmap.MapRegion(i.rankFile, int(windowSize), mmap.RDWR, 0, rankOffset)
		if err != nil {
			parentArchive.Unmap()
			fmt.Printf("다음 윈도우 rank mmap 생성 실패: %v\n", err)
			return
		}

		sizeArchive, err := mmap.MapRegion(i.sizeFile, int(windowSize*2), mmap.RDWR, 0, sizeOffset)
		if err != nil {
			parentArchive.Unmap()
			rankArchive.Unmap()
			fmt.Printf("다음 윈도우 size mmap 생성 실패: %v\n", err)
			return
		}

		// 중간에 종료됐는지 확인
		if atomic.LoadInt32(&i.isShuttingDown) == 1 {
			parentArchive.Unmap()
			rankArchive.Unmap()
			sizeArchive.Unmap()
			return
		}

		// 메모리 슬라이스 생성
		parentSlice := make([]uint64, windowSize)
		rankSlice := make([]uint8, windowSize)
		sizeSlice := make([]uint16, windowSize)

		// 데이터 로드 작업
		for idx := uint64(0); idx < windowSize; idx++ {
			pOffset := idx * 8
			if pOffset+8 <= uint64(len(parentArchive)) {
				parentSlice[idx] = uint64(parentArchive[pOffset]) |
					uint64(parentArchive[pOffset+1])<<8 |
					uint64(parentArchive[pOffset+2])<<16 |
					uint64(parentArchive[pOffset+3])<<24 |
					uint64(parentArchive[pOffset+4])<<32 |
					uint64(parentArchive[pOffset+5])<<40 |
					uint64(parentArchive[pOffset+6])<<48 |
					uint64(parentArchive[pOffset+7])<<56

				// 초기화되지 않은 원소는 초기화
				if parentSlice[idx] == 0 {
					globalIdx := alignedNextWindowStart + idx
					parentSlice[idx] = globalIdx
				}
			}
		}

		// rank/size 데이터 로드
		rOffset := int64(alignedNextWindowStart % uint64(i.pageSize))
		for idx := uint64(0); idx < windowSize && idx < uint64(len(rankArchive)); idx++ {
			if rOffset+int64(idx) < int64(len(rankArchive)) {
				rankSlice[idx] = rankArchive[rOffset+int64(idx)]
			}
		}

		sOffset := int64((alignedNextWindowStart % uint64(i.pageSize)) * 2)
		for idx := uint64(0); idx < windowSize; idx++ {
			offset := sOffset + int64(idx*2)
			if offset+2 <= int64(len(sizeArchive)) {
				sizeSlice[idx] = uint16(sizeArchive[offset]) | uint16(sizeArchive[offset+1])<<8

				// 초기화
				if sizeSlice[idx] == 0 {
					sizeSlice[idx] = 1
				}
			}
		}

		// 마지막으로 종료 확인
		if atomic.LoadInt32(&i.isShuttingDown) == 1 {
			parentArchive.Unmap()
			rankArchive.Unmap()
			sizeArchive.Unmap()
			return
		}

		// 타임아웃 적용된 락 획득 시도
		lockAcquired := make(chan struct{}, 1)
		var gotLock bool

		go func() {
			i.mu.Lock()
			lockAcquired <- struct{}{}
		}()

		select {
		case <-lockAcquired:
			gotLock = true
		case <-time.After(200 * time.Millisecond):
			fmt.Println("프리페치 결과 저장 시 뮤텍스 획득 타임아웃")
			parentArchive.Unmap()
			rankArchive.Unmap()
			sizeArchive.Unmap()
			return
		}

		// 락 획득 성공 시에만 결과 저장
		if gotLock {
			// 종료 확인 한번 더
			if atomic.LoadInt32(&i.isShuttingDown) == 1 {
				i.mu.Unlock()
				parentArchive.Unmap()
				rankArchive.Unmap()
				sizeArchive.Unmap()
				return
			}

			// 프리페치 결과 저장
			i.nextWindowStart = alignedNextWindowStart
			i.nextWindowEnd = windowEnd
			i.nextParentArchive = parentArchive
			i.nextRankArchive = rankArchive
			i.nextSizeArchive = sizeArchive
			i.nextParentSlice = parentSlice
			i.nextRankSlice = rankSlice
			i.nextSizeSlice = sizeSlice
			i.mu.Unlock()

			fmt.Printf("다음 윈도우 프리페칭 완료: %d-%d\n", alignedNextWindowStart, windowEnd)
		}
	}()
}

// Close 함수 - 뮤텍스 없이 안전하게 종료
func (i *IncrementalUnionFindProcessor) Close() error {
	fmt.Println("프로세서 종료 시작...")

	// 종료 플래그 설정 - 모든 고루틴에게 신호
	atomic.StoreInt32(&i.isShuttingDown, 1)
	fmt.Println("종료 플래그 설정 완료")

	// 고루틴들이 종료될 시간 부여
	time.Sleep(1 * time.Second)

	// 에러 수집
	var closeErrors []string

	// mmap 및 파일 리소스 정리 (뮤텍스 사용하지 않음)
	// 1. 현재 윈도우 해제
	if i.parentArchive != nil {
		if err := i.parentArchive.Unmap(); err != nil {
			closeErrors = append(closeErrors, fmt.Sprintf("parent mmap 해제 실패: %v", err))
		}
	}

	if i.rankArchive != nil {
		if err := i.rankArchive.Unmap(); err != nil {
			closeErrors = append(closeErrors, fmt.Sprintf("rank mmap 해제 실패: %v", err))
		}
	}

	if i.sizeArchive != nil {
		if err := i.sizeArchive.Unmap(); err != nil {
			closeErrors = append(closeErrors, fmt.Sprintf("size mmap 해제 실패: %v", err))
		}
	}

	// 2. 프리페치된 윈도우 해제
	if i.nextParentArchive != nil {
		if err := i.nextParentArchive.Unmap(); err != nil {
			closeErrors = append(closeErrors, fmt.Sprintf("next parent mmap 해제 실패: %v", err))
		}
	}

	if i.nextRankArchive != nil {
		if err := i.nextRankArchive.Unmap(); err != nil {
			closeErrors = append(closeErrors, fmt.Sprintf("next rank mmap 해제 실패: %v", err))
		}
	}

	if i.nextSizeArchive != nil {
		if err := i.nextSizeArchive.Unmap(); err != nil {
			closeErrors = append(closeErrors, fmt.Sprintf("next size mmap 해제 실패: %v", err))
		}
	}

	// 3. 파일 핸들 닫기
	if i.parentFile != nil {
		if err := i.parentFile.Close(); err != nil {
			closeErrors = append(closeErrors, fmt.Sprintf("parent 파일 닫기 실패: %v", err))
		}
	}

	if i.rankFile != nil {
		if err := i.rankFile.Close(); err != nil {
			closeErrors = append(closeErrors, fmt.Sprintf("rank 파일 닫기 실패: %v", err))
		}
	}

	if i.sizeFile != nil {
		if err := i.sizeFile.Close(); err != nil {
			closeErrors = append(closeErrors, fmt.Sprintf("size 파일 닫기 실패: %v", err))
		}
	}

	fmt.Println("모든 리소스 정리 완료")

	// 에러 반환
	if len(closeErrors) > 0 {
		return fmt.Errorf("리소스 정리 중 오류 발생: %s", strings.Join(closeErrors, "; "))
	}

	return nil
}

// Close 함수도 안전하게 수정

// 내부 구현 - 뮤텍스 락이 이미 획득된 상태에서 호출
func (i *IncrementalUnionFindProcessor) ensureWindowContainsInternal(element uint64) error {
	// 파일 확장 (필요시)
	if element >= i.fileCapacity {
		i.mu.Unlock() // 일시적으로 락 해제
		err := i.extendFiles(element + 1)
		i.mu.Lock() // 다시 락 획득
		if err != nil {
			return fmt.Errorf("파일 확장 실패: %v", err)
		}
	}

	// 현재 윈도우 범위 확인
	if element < i.windowStart || element >= i.windowEnd {
		// 윈도우 이동 필요
		newStart := (element / WindowSize) * WindowSize
		newStart = (newStart / uint64(i.pageSize)) * uint64(i.pageSize)

		i.mu.Unlock() // 일시적으로 락 해제
		err := i.mapWindowInternal(newStart)
		i.mu.Lock() // 다시 락 획득
		if err != nil {
			return fmt.Errorf("윈도우 매핑 실패: %v", err)
		}
	}

	return nil
}

// 집합 처리
func (i *IncrementalUnionFindProcessor) ProcessSets(userSets []UserSet) (error, []UserSet) {
	// 종료 중인지 확인
	i.mu.Lock()
	if atomic.LoadInt32((*int32)(&i.isShuttingDown)) == 1 {
		i.mu.Unlock()
		return fmt.Errorf("프로세서가 종료 중입니다"), nil
	}
	i.mu.Unlock()

	// 모든 셋 중, 단일 크기 셋에 대해서만 전처리
	singleSets := []UserSet{}
	multiSets := []UserSet{}

	// 단일 집합과 다중 집합 분리
	for _, set := range userSets {
		if set.size == 1 && len(set.elements) == 1 {
			singleSets = append(singleSets, set)
		} else if set.size > 1 && len(set.elements) > 1 {
			multiSets = append(multiSets, set)
		}
	}

	// 단일 집합 처리
	err, processedSingleSets := i.ProcessSingleSets(singleSets)
	if err != nil {
		return err, nil
	}

	// 다중 집합 처리
	err, processedMultiSets := i.ProcessMultiSets(multiSets)
	if err != nil {
		return err, nil
	}

	// 현재 윈도우 flush
	if err := i.flushWindow(); err != nil {
		return err, nil
	}

	// 결과 합치기
	return nil, append(processedSingleSets, processedMultiSets...)
}

// 단일 원소 집합 처리
func (i *IncrementalUnionFindProcessor) ProcessSingleSets(userSets []UserSet) (error, []UserSet) {
	if len(userSets) == 0 {
		return nil, []UserSet{}
	}

	// 배치 크기 조정
	batchSize := BatchSize
	if batchSize > len(userSets) {
		batchSize = len(userSets)
	}

	processedSets := make([]UserSet, 0, len(userSets))

	// 배치 단위로 처리
	for batchStart := 0; batchStart < len(userSets); batchStart += batchSize {
		batchEnd := batchStart + batchSize
		if batchEnd > len(userSets) {
			batchEnd = len(userSets)
		}

		// 현재 배치
		batch := userSets[batchStart:batchEnd]

		// 배치 버퍼 생성
		batchBuffer := make([]uint64, 0, len(batch))

		// 각 집합에 대해 ReflectSingleSet 호출
		for _, set := range batch {
			err := i.ReflectSingleSet(set, &batchBuffer)
			if err != nil {
				return err, nil
			}
			processedSets = append(processedSets, set)
		}

		// 배치 버퍼 처리
		// 배치 버퍼 처리
		for _, element := range batchBuffer {
			// 원소 초기화
			if err := i.initializeElement(element); err != nil {
				fmt.Printf("원소 초기화 실패 (계속 진행): %v\n", err)
				continue // 오류가 발생해도 계속 진행
			}

			// 블룸 필터에 추가
			i.mu.Lock()
			i.totalBloomFilter.Add(element)
			i.mu.Unlock()
		}
	}

	return nil, processedSets
}

// 단일 집합 반영
// 단일 집합 반영
func (i *IncrementalUnionFindProcessor) ReflectSingleSet(userSet UserSet, batchBuffer *[]uint64) error {
	// 사이즈가 1인지 체크
	if userSet.size != 1 || len(userSet.elements) != 1 {
		return nil // 크기가 1이 아니면 무시
	}

	element := userSet.elements[0]

	// 블룸 필터에서 원소 존재 여부 확인
	i.mu.Lock()
	exists := i.totalBloomFilter.Check(element)
	i.mu.Unlock()

	if exists {
		// 실제로 존재하는지 확인 (블룸 필터 오탐 가능성)
		_, err := i.Find(element)
		if err == nil {
			return nil // 이미 존재하는 원소라면 무시
		}
	}

	// 배치 버퍼에 추가
	*batchBuffer = append(*batchBuffer, element)

	return nil
}

// 다중 집합 처리
func (i *IncrementalUnionFindProcessor) ProcessMultiSets(userSets []UserSet) (error, []UserSet) {
	if len(userSets) == 0 {
		return nil, []UserSet{}
	}

	// 배치 크기 조정
	batchSize := BatchSize / 10 // 다중 집합은 처리가 더 복잡하므로 더 작은 배치 사이즈
	if batchSize < 100 {
		batchSize = 100
	}
	if batchSize > len(userSets) {
		batchSize = len(userSets)
	}

	processedSets := make([]UserSet, 0, len(userSets))

	// 배치 단위로 처리
	for batchStart := 0; batchStart < len(userSets); batchStart += batchSize {
		batchEnd := batchStart + batchSize
		if batchEnd > len(userSets) {
			batchEnd = len(userSets)
		}

		fmt.Printf("다중 집합 처리 중: %d-%d/%d\n", batchStart, batchEnd, len(userSets))

		// 현재 배치
		batch := userSets[batchStart:batchEnd]

		// 각 집합에 대해 ReflectMultiSet 호출
		for _, set := range batch {
			if err := i.ReflectMultiSet(set); err != nil {
				return err, nil
			}
			processedSets = append(processedSets, set)
		}

		// 주기적으로 flush (메모리 관리)
		if err := i.flushWindow(); err != nil {
			return err, nil
		}
	}

	return nil, processedSets
}

// 다중 집합 반영
func (i *IncrementalUnionFindProcessor) ReflectMultiSet(userSet UserSet) error {
	// 사이즈가 2 이상인지 체크
	if userSet.size <= 1 || len(userSet.elements) <= 1 {
		return nil // 다중 집합이 아니면 무시
	}

	i.mu.Lock()

	// 종료 중인지 확인
	if atomic.LoadInt32((*int32)(&i.isShuttingDown)) == 1 {
		i.mu.Unlock()
		return fmt.Errorf("프로세서가 종료 중입니다")
	}

	// 대표 원소 선택 (첫 번째 원소로 시작)
	var representative uint64 = userSet.elements[0]
	var foundMultiSet bool = false

	// 다중 블룸 필터에서 원소 확인
	for _, element := range userSet.elements {
		if i.multiBloomFilter.Check(element) {
			// 실제로 있는지 체크
			root, err := i.findInternal(element)
			if err != nil {
				continue
			}

			// 해당 루트가 속한 윈도우로 전환
			if err := i.ensureWindowContains(root); err != nil {
				continue
			}

			localRoot, err := i.getLocalIndex(root)
			if err != nil {
				continue
			}

			if i.sizeSlice[localRoot] >= 2 {
				// 다중 집합 발견
				representative = root
				foundMultiSet = true
				break
			}
		}
	}

	// 모든 원소 초기화
	for _, element := range userSet.elements {
		// 원소 초기화 (없는 경우)
		if err := i.initializeElement(element); err != nil {
			i.mu.Unlock()
			return err
		}

		// 블룸 필터에 추가
		i.totalBloomFilter.Add(element)
	}

	// 모든 원소를 대표 원소와 Union
	for _, element := range userSet.elements {
		if element != representative {
			rootX, err := i.findInternal(representative)
			if err != nil {
				i.mu.Unlock()
				return err
			}

			rootY, err := i.findInternal(element)
			if err != nil {
				i.mu.Unlock()
				return err
			}

			// Union 로직 (뮤텍스는 이미 잡고 있음)
			if rootX != rootY {
				// rootX 정보 가져오기
				if err := i.ensureWindowContains(rootX); err != nil {
					i.mu.Unlock()
					return err
				}

				localRootX, err := i.getLocalIndex(rootX)
				if err != nil {
					i.mu.Unlock()
					return err
				}

				rankX := i.rankSlice[localRootX]
				sizeX := i.sizeSlice[localRootX]

				// rootY 정보 가져오기
				if err := i.ensureWindowContains(rootY); err != nil {
					i.mu.Unlock()
					return err
				}

				localRootY, err := i.getLocalIndex(rootY)
				if err != nil {
					i.mu.Unlock()
					return err
				}

				rankY := i.rankSlice[localRootY]
				sizeY := i.sizeSlice[localRootY]

				// 랭크 기반 병합
				if rankX < rankY {
					i.parentSlice[localRootX] = rootY
					i.sizeSlice[localRootY] = sizeY + sizeX
					if sizeY+sizeX >= 2 {
						i.multiBloomFilter.Add(rootY)
					}
				} else if rankX > rankY {
					i.parentSlice[localRootY] = rootX
					i.sizeSlice[localRootX] = sizeX + sizeY
					if sizeX+sizeY >= 2 {
						i.multiBloomFilter.Add(rootX)
					}
				} else {
					i.parentSlice[localRootY] = rootX
					i.rankSlice[localRootX]++
					i.sizeSlice[localRootX] = sizeX + sizeY
					if sizeX+sizeY >= 2 {
						i.multiBloomFilter.Add(rootX)
					}
				}
			}
		}
	}

	// 새로운 다중 집합 생성
	if !foundMultiSet {
		root, err := i.findInternal(representative)
		if err == nil {
			i.multiBloomFilter.Add(root)
		}
	}

	i.mu.Unlock()
	return nil
}

// 랜덤 집합 생성
func GenerateSets(n uint64, size int) []UserSet {
	// 1. 원소는 0~n-1까지
	// 2. 각 집합의 크기는 1~size
	// 3. 각 집합의 원소는 중복되지 않음
	numSets := int(n) / size // 대략적인 집합 수
	sets := make([]UserSet, 0, numSets)

	// 사용된 원소 추적
	used := make(map[uint64]bool)

	rand.Seed(time.Now().UnixNano())

	for i := 0; i < numSets && len(used) < int(n); i++ {
		// 집합 크기 결정 (1~size)
		setSize := rand.Intn(size) + 1

		// 남은 원소 수가 부족하면 조정
		if len(used)+setSize > int(n) {
			setSize = int(n) - len(used)
		}

		// 중복되지 않는 원소 선택
		elements := make([]uint64, 0, setSize)
		for j := 0; j < setSize; j++ {
			var element uint64
			for {
				element = uint64(rand.Intn(int(n)))
				if !used[element] {
					used[element] = true
					break
				}
			}
			elements = append(elements, element)
		}

		// 유저 집합 생성
		sets = append(sets, UserSet{
			size:     uint16(setSize),
			elements: elements,
		})
	}

	return sets
}

// 분포에 따라 모든 집합 생성
func GenerateAllSets(numSets uint64, rangeValue uint64) []UserSet {
	// 분포에 따른 집합 수 계산
	singleCount := int(float64(numSets) * 0.90)   // 90%
	twoCount := int(float64(numSets) * 0.07)      // 7%
	threeCount := int(float64(numSets) * 0.01)    // 1%
	fourCount := int(float64(numSets) * 0.005)    // 0.5%
	fiveCount := int(float64(numSets) * 0.0025)   // 0.25%
	sixCount := int(float64(numSets) * 0.0025)    // 0.25%
	sevenCount := int(float64(numSets) * 0.0025)  // 0.25%
	eightCount := int(float64(numSets) * 0.0025)  // 0.25%
	nineCount := int(float64(numSets) * 0.001)    // 0.1%
	tenCount := int(float64(numSets) * 0.001)     // 0.1%
	elevenCount := int(float64(numSets) * 0.0005) // 0.05%
	twelveCount := int(float64(numSets) * 0.0005) // 0.05%
	twentyCount := int(float64(numSets) * 0.0003) // 0.03%
	thirtyCount := int(float64(numSets) * 0.0001) // 0.01%
	fiftyCount := int(float64(numSets) * 0.0001)  // 0.01%

	// 결과 슬라이스
	allSets := make([]UserSet, 0, numSets)

	// 각 크기별 집합 생성
	fmt.Println("1개 원소 집합 생성 중...")
	allSets = append(allSets, GenerateSets(rangeValue, 1)[:singleCount]...)

	fmt.Println("2개 원소 집합 생성 중...")
	allSets = append(allSets, GenerateSets(rangeValue, 2)[:twoCount]...)

	fmt.Println("3개 원소 집합 생성 중...")
	allSets = append(allSets, GenerateSets(rangeValue, 3)[:threeCount]...)

	fmt.Println("4-12개 원소 집합 생성 중...")
	allSets = append(allSets, GenerateSets(rangeValue, 4)[:fourCount]...)
	allSets = append(allSets, GenerateSets(rangeValue, 5)[:fiveCount]...)
	allSets = append(allSets, GenerateSets(rangeValue, 6)[:sixCount]...)
	allSets = append(allSets, GenerateSets(rangeValue, 7)[:sevenCount]...)
	allSets = append(allSets, GenerateSets(rangeValue, 8)[:eightCount]...)
	allSets = append(allSets, GenerateSets(rangeValue, 9)[:nineCount]...)
	allSets = append(allSets, GenerateSets(rangeValue, 10)[:tenCount]...)
	allSets = append(allSets, GenerateSets(rangeValue, 11)[:elevenCount]...)
	allSets = append(allSets, GenerateSets(rangeValue, 12)[:twelveCount]...)

	fmt.Println("다수 원소 집합 생성 중...")
	allSets = append(allSets, GenerateSets(rangeValue, 20)[:twentyCount]...)
	allSets = append(allSets, GenerateSets(rangeValue, 30)[:thirtyCount]...)
	allSets = append(allSets, GenerateSets(rangeValue, 50)[:fiftyCount]...)

	// 랜덤하게 섞기
	fmt.Println("집합 섞는 중...")
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(allSets), func(i, j int) {
		allSets[i], allSets[j] = allSets[j], allSets[i]
	})

	fmt.Printf("총 %d개 집합 생성 완료\n", len(allSets))
	return allSets
}

// 청크 단위 집합 생성
func GenerateSetChunk(chunkSize, rangeValue uint64, chunkIndex uint64) []UserSet {
	// 총 생성할 집합 수
	totalToGenerate := chunkSize

	// 분포에 따른 집합 수 계산
	singleCount := int(float64(totalToGenerate) * 0.90)
	twoCount := int(float64(totalToGenerate) * 0.07)
	threeCount := int(float64(totalToGenerate) * 0.01)
	fourCount := int(float64(totalToGenerate) * 0.005)
	fiveCount := int(float64(totalToGenerate) * 0.0025)
	remainingCount := int(totalToGenerate) - (singleCount + twoCount + threeCount + fourCount + fiveCount)

	// 샘플링될 원소를 결정하기 위한 시드 값 설정
	seed := time.Now().UnixNano() + int64(chunkIndex*1000)
	rand.Seed(seed)

	// 결과 슬라이스
	chunkSets := make([]UserSet, 0, chunkSize)
	used := make(map[uint64]bool) // 중복 방지

	// 1개 원소 집합 생성
	for i := 0; i < singleCount; i++ {
		element := uint64(rand.Intn(int(rangeValue)))
		chunkSets = append(chunkSets, UserSet{
			size:     1,
			elements: []uint64{element},
		})
	}

	// 2개 원소 집합 생성
	for i := 0; i < twoCount; i++ {
		elements := make([]uint64, 0, 2)
		for len(elements) < 2 {
			element := uint64(rand.Intn(int(rangeValue)))
			if !used[element] {
				used[element] = true
				elements = append(elements, element)
			}
		}
		chunkSets = append(chunkSets, UserSet{
			size:     2,
			elements: elements,
		})
		// 다음 집합을 위해 used 초기화
		for _, e := range elements {
			delete(used, e)
		}
	}

	// 3개 원소 집합 생성
	for i := 0; i < threeCount; i++ {
		elements := make([]uint64, 0, 3)
		for len(elements) < 3 {
			element := uint64(rand.Intn(int(rangeValue)))
			if !used[element] {
				used[element] = true
				elements = append(elements, element)
			}
		}
		chunkSets = append(chunkSets, UserSet{
			size:     3,
			elements: elements,
		})
		// 다음 집합을 위해 used 초기화
		for _, e := range elements {
			delete(used, e)
		}
	}

	// 4개 원소 집합 생성
	for i := 0; i < fourCount; i++ {
		elements := make([]uint64, 0, 4)
		for len(elements) < 4 {
			element := uint64(rand.Intn(int(rangeValue)))
			if !used[element] {
				used[element] = true
				elements = append(elements, element)
			}
		}
		chunkSets = append(chunkSets, UserSet{
			size:     4,
			elements: elements,
		})
		// 다음 집합을 위해 used 초기화
		for _, e := range elements {
			delete(used, e)
		}
	}

	// 5개 원소 집합 생성
	for i := 0; i < fiveCount; i++ {
		elements := make([]uint64, 0, 5)
		for len(elements) < 5 {
			element := uint64(rand.Intn(int(rangeValue)))
			if !used[element] {
				used[element] = true
				elements = append(elements, element)
			}
		}
		chunkSets = append(chunkSets, UserSet{
			size:     5,
			elements: elements,
		})
		// 다음 집합을 위해 used 초기화
		for _, e := range elements {
			delete(used, e)
		}
	}

	// 나머지 집합 (단일 원소로 채움)
	for i := 0; i < remainingCount; i++ {
		element := uint64(rand.Intn(int(rangeValue)))
		chunkSets = append(chunkSets, UserSet{
			size:     1,
			elements: []uint64{element},
		})
	}

	// 랜덤하게 섞기
	rand.Shuffle(len(chunkSets), func(i, j int) {
		chunkSets[i], chunkSets[j] = chunkSets[j], chunkSets[i]
	})

	return chunkSets
}

// 테스트 함수
func TestArchiveMmapUnionfind() {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("테스트 중 패닉 발생: %v\n", r)
			// 스택 트레이스 출력
			buf := make([]byte, 4096)
			n := runtime.Stack(buf, false)
			fmt.Printf("스택 트레이스:\n%s\n", buf[:n])
		}
	}()

	// 1. 메모리 및 시간 추적
	startTime := time.Now()
	startStats := getMemoryStats(startTime)
	printMemoryUsage("테스트 시작", startStats)

	// 데이터 디렉토리 설정
	dataDir := fmt.Sprintf("union_find_data_%d", time.Now().Unix())

	// 2. 인크리멘터 생성
	fmt.Println("인크리멘터 생성 중...")
	processor, err := NewIncrementalUnionFindProcessor(dataDir)
	if err != nil {
		fmt.Printf("인크리멘터 생성 실패: %v\n", err)
		return
	}
	// 리소스 정리 코드를 별도 변수로 분리
	cleanup := func() {
		fmt.Println("리소스 정리 중...")
		err := processor.Close()
		if err != nil {
			fmt.Printf("프로세서 종료 중 오류 발생: %v\n", err)
		}
	}
	// defer 대신 별도 지점에서 호출
	defer func() {
		// 패닉 복구 후 안전하게 정리
		if r := recover(); r != nil {
			fmt.Printf("청소 전 패닉 복구: %v\n", r)
			cleanup()
			panic(r) // 원래 패닉 다시 발생
		} else {
			cleanup()
		}
	}()
	// 청크 단위로 처리
	totalSets := uint64(200000000)  // 2억 개 집합
	rangeValue := uint64(300000000) // 3억 범위 값
	chunkSize := uint64(10000000)   // 1천만 개씩 청크 처리
	numChunks := (totalSets + chunkSize - 1) / chunkSize

	// 첫 번째 데이터셋 처리
	fmt.Println("첫 번째 데이터셋 처리 중...")
	firstProcessStart := time.Now()

	for i := uint64(0); i < numChunks; i++ {
		fmt.Printf("첫 번째 데이터셋 청크 %d/%d 생성 중...\n", i+1, numChunks)
		chunkStart := time.Now()

		// 청크 생성
		chunk := GenerateSetChunk(chunkSize, rangeValue, i)
		fmt.Printf("청크 %d 생성 완료: %d 집합 (소요시간: %v)\n", i+1, len(chunk), time.Since(chunkStart))

		// 청크 처리
		processStart := time.Now()
		err, _ := processor.ProcessSets(chunk)
		if err != nil {
			fmt.Printf("첫 번째 데이터셋 청크 %d 처리 실패: %v\n", i+1, err)
			fmt.Printf("프로세서 상태 덤프:\n")
			fmt.Printf("  뮤텍스 상태: %v\n", atomic.LoadInt32(&processor.muLocked))
			fmt.Printf("  뮤텍스 히스토리: %v\n", processor.mutexHistory)
			// 대신 중단하지 않고 다음으로 진행
			continue
		}
		fmt.Printf("청크 %d 처리 완료 (소요시간: %v)\n", i+1, time.Since(processStart))

		// 메모리 정리
		chunk = nil
		runtime.GC()

		// 중간 메모리 상태 출력
		memStats := getMemoryStats(startTime)
		printMemoryUsage(fmt.Sprintf("첫 번째 데이터셋 청크 %d/%d 처리 후", i+1, numChunks), memStats)
	}

	// 첫 번째 데이터셋 처리 완료
	firstProcessStats := getMemoryStats(startTime)
	printMemoryUsage("첫 번째 데이터셋 처리 완료", firstProcessStats)
	fmt.Printf("첫 번째 데이터셋 총 처리 시간: %v\n", time.Since(firstProcessStart))

	// 두 번째 데이터셋 처리
	fmt.Println("두 번째 데이터셋 처리 중...")
	secondProcessStart := time.Now()

	for i := uint64(0); i < numChunks; i++ {
		fmt.Printf("두 번째 데이터셋 청크 %d/%d 생성 중...\n", i+1, numChunks)
		chunkStart := time.Now()

		// 청크 생성 (시드 값을 변경하여 다른 집합 생성)
		chunk := GenerateSetChunk(chunkSize, rangeValue, i+numChunks)
		fmt.Printf("청크 %d 생성 완료: %d 집합 (소요시간: %v)\n", i+1, len(chunk), time.Since(chunkStart))

		// 청크 처리
		processStart := time.Now()
		err, _ = processor.ProcessSets(chunk)
		if err != nil {
			fmt.Printf("두 번째 데이터셋 청크 %d 처리 실패: %v\n", i+1, err)
			return
		}
		fmt.Printf("청크 %d 처리 완료 (소요시간: %v)\n", i+1, time.Since(processStart))

		// 메모리 정리
		chunk = nil
		runtime.GC()

		// 중간 메모리 상태 출력
		memStats := getMemoryStats(startTime)
		printMemoryUsage(fmt.Sprintf("두 번째 데이터셋 청크 %d/%d 처리 후", i+1, numChunks), memStats)
	}

	// 두 번째 데이터셋 처리 완료
	secondProcessStats := getMemoryStats(startTime)
	printMemoryUsage("두 번째 데이터셋 처리 완료", secondProcessStats)
	fmt.Printf("두 번째 데이터셋 총 처리 시간: %v\n", time.Since(secondProcessStart))

	// 테스트 종료
	fmt.Println("테스트 완료")
	finalStats := getMemoryStats(startTime)
	printMemoryUsage("테스트 최종 결과", finalStats)
	fmt.Printf("총 테스트 시간: %v\n", time.Since(startTime))
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

func main() {
	TestArchiveMmapUnionfind()
}
