package main

import (
	"fmt"
	"math"
	"runtime"
	"sync"
)

// 블룸 필터 구조체 - 병렬 처리 지원
type BloomFilter struct {
	// Bloom Filter 크기
	size uint64
	// Bloom Filter 배열
	bf []uint64
	// 해시 함수 개수
	hashFunctions uint64
	// 동시성 제어를 위한 샤드 뮤텍스
	shardMutexes []sync.Mutex
	// 샤드 수
	numShards uint64
}

// 블룸 필터 생성 함수 - 메모리 사용량 최대 2-4GB 허용
func NewBloomFilter(expectedElements uint64, falsePositiveRate float64) *BloomFilter {
	// 블룸 필터 적정 크기 계산 (m = -n*ln(p)/(ln(2)^2))
	// 여기서 n은 예상 요소 수, p는 허용 오탐률
	size := uint64(float64(expectedElements) * (-1.0) * math.Log(falsePositiveRate) / (math.Log(2) * math.Log(2)))

	// 크기 제한 (2-4GB 사이로 조정)
	const minSize = 2 * 1024 * 1024 * 1024 * 8 // 2GB (비트 단위)
	const maxSize = 4 * 1024 * 1024 * 1024 * 8 // 4GB (비트 단위)

	if size < minSize {
		size = minSize
	}
	if size > maxSize {
		size = maxSize
	}

	// 최적 해시 함수 개수 계산 (k = m/n*ln(2))
	hashFunctions := uint64(float64(size) / float64(expectedElements) * math.Log(2))
	if hashFunctions < 8 {
		hashFunctions = 8 // 최소 8개
	}
	if hashFunctions > 16 {
		hashFunctions = 16 // 최대 16개
	}

	// 사이즈를 64bit 단위로 올림 정렬
	arraySize := (size + 63) / 64

	// 샤드 수 계산 - 배열 크기에 기반하여 결정
	// 작은 필터는 적은 샤드를, 큰 필터는 많은 샤드를 사용
	numShards := uint64(1024)
	if arraySize > 1000000 { // 매우 큰 블룸필터
		numShards = 4096
	} else if arraySize < 100000 { // 작은 블룸필터
		numShards = 256
	}

	fmt.Printf("블룸 필터 생성: 크기 %.2f GB, 해시 함수 %d개, 예상 오탐률 %.4f%%, 샤드 %d개\n",
		float64(arraySize*8)/(1024*1024*1024), hashFunctions, falsePositiveRate*100, numShards)

	return &BloomFilter{
		size:          size,
		bf:            make([]uint64, arraySize),
		hashFunctions: hashFunctions,
		shardMutexes:  make([]sync.Mutex, numShards),
		numShards:     numShards,
	}
}

// 해시 값 계산 - 재사용 가능한 형태로 분리
func (bf *BloomFilter) calculateHashes(element uint64) [][2]uint64 {
	results := make([][2]uint64, bf.hashFunctions)

	// 두 개의 기본 해시 계산
	h1 := element
	h1 = (h1 ^ (h1 >> 30)) * 0xbf58476d1ce4e5b9
	h1 = (h1 ^ (h1 >> 27)) * 0x94d049bb133111eb
	h1 = h1 ^ (h1 >> 31)

	h2 := element
	h2 = ((h2 >> 16) ^ h2) * 0x45d9f3b
	h2 = ((h2 >> 16) ^ h2) * 0x45d9f3b
	h2 = (h2 >> 16) ^ h2

	// 각 해시 함수에 대한 인덱스와 비트 위치 계산
	for i := uint64(0); i < bf.hashFunctions; i++ {
		hash := (h1 + i*h2) % bf.size
		index := hash / 64
		bit := hash % 64
		results[i] = [2]uint64{index, bit}
	}

	return results
}

// 블룸 필터에 요소 추가
func (bf *BloomFilter) Add(element uint64) {
	// 해시 계산
	hashes := bf.calculateHashes(element)

	// 샤드별로 그룹화
	shardToBits := make(map[uint64][][2]uint64)
	for _, hash := range hashes {
		index, bit := hash[0], hash[1]
		shardIdx := index % bf.numShards
		shardToBits[shardIdx] = append(shardToBits[shardIdx], [2]uint64{index, bit})
	}

	// 각 샤드에 대해 한 번만 락 획득
	for shardIdx, bits := range shardToBits {
		bf.shardMutexes[shardIdx].Lock()
		for _, pair := range bits {
			index, bit := pair[0], pair[1]
			bf.bf[index] |= (1 << bit)
		}
		bf.shardMutexes[shardIdx].Unlock()
	}
}

// 블룸 필터에 요소 있는지 확인
func (bf *BloomFilter) MayContain(element uint64) bool {
	hashes := bf.calculateHashes(element)

	for _, hash := range hashes {
		index, bit := hash[0], hash[1]

		// 비트 확인 (샤드 락 필요 없음 - 읽기만 함)
		if (bf.bf[index] & (1 << bit)) == 0 {
			return false
		}
	}
	return true
}

// 배치로 요소 추가 (병렬 처리) - 최적화 버전
func (bf *BloomFilter) BatchAdd(elements []uint64, numWorkers int) {
	// 요소가 적으면 일반 추가 사용
	// 워커당 최소 작업량 확보를 위해 임계값 조정
	if len(elements) < numWorkers*4 {
		for _, element := range elements {
			bf.Add(element)
		}
		return
	}

	// CPU 기반 최적 워커 수 계산 (16코어 기준)
	maxCPU := runtime.NumCPU()
	if numWorkers <= 0 || numWorkers > maxCPU*2 {
		numWorkers = maxCPU
	}

	// 병렬 처리
	var wg sync.WaitGroup
	chunkSize := (len(elements) + numWorkers - 1) / numWorkers

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			// 작업 범위 계산
			start := workerID * chunkSize
			end := start + chunkSize
			if end > len(elements) {
				end = len(elements)
			}

			// 작업할 요소가 없으면 종료
			if start >= end {
				return
			}

			// 블룸필터에 대한 로컬 락 카운터 - 디버깅용
			// var lockCount int64

			// 각 요소 처리
			for j := start; j < end; j++ {
				element := elements[j]
				hashes := bf.calculateHashes(element)

				// 샤드별로 그룹화 (각 요소의 비트들을 샤드별로 모음)
				shardToBits := make(map[uint64][][2]uint64)
				for _, hash := range hashes {
					index, bit := hash[0], hash[1]
					shardIdx := index % bf.numShards
					shardToBits[shardIdx] = append(shardToBits[shardIdx], [2]uint64{index, bit})
				}

				// 각 샤드에 대해 한 번만 락 획득
				for shardIdx, bits := range shardToBits {
					bf.shardMutexes[shardIdx].Lock()
					// atomic.AddInt64(&lockCount, 1) // 디버깅용 락 카운터

					// 해당 샤드에 속한 모든 비트 설정
					for _, pair := range bits {
						index, bit := pair[0], pair[1]
						bf.bf[index] |= (1 << bit)
					}

					bf.shardMutexes[shardIdx].Unlock()
				}
			}

			// 디버깅 정보 출력
			// fmt.Printf("Worker %d: processed %d elements, acquired %d locks\n",
			//    workerID, end-start, lockCount)
		}(w)
	}

	// 모든 워커 완료 대기
	wg.Wait()
}

// 벌크 배치 처리 (대용량 배치 최적화)
func (bf *BloomFilter) BulkBatchAdd(elements []uint64) {
	// 요소 수 기반으로 최적 워커 수 계산
	numWorkers := runtime.NumCPU()

	// 매우 큰 배치의 경우 워커 수 약간 증가
	if len(elements) > 1000000 {
		numWorkers = runtime.NumCPU() * 2
	}

	// 최대 워커 수 제한
	if numWorkers > 32 {
		numWorkers = 32
	}

	// 배치 처리
	bf.BatchAdd(elements, numWorkers)
}
