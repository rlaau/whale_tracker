package main

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"

	"github.com/edsrzf/mmap-go"
)

// 약 1천만 개의 원소까지는 홀딩 전까지 clean up을 하지 않는다.
// 근데 이게 운이 좋아야 1천만이지. 페이지 다양하게 뜨면 무리임.
// 경우에 따라서 적절히 줄이기. 다만 너무 줄이면 madvise오버헤드 너무 커짐.
const (
	MaxLoadStack   = 50_000_000
	ChunkSize      = 512         // 5000만개씩 처리
	DataDir        = "./data"    // 데이터 디렉토리
	TestResultsDir = "./results" // 테스트 결과 디렉토리
)

// 사용자 집합 구조체
type UserSet struct {
	// 집합 크기
	Size uint16
	// 집합 원소
	Elements []uint64
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

	//현재 얼마만큼 페이지와 데이터를 홀딩하고 있는지
	currentHoldingElements uint64

	// 블룸 필터
	//오탐률 0.5%정도, 해시함수 8개.
	totalBloomFilter *BloomFilter // 전체 원소
	multiBloomFilter *BloomFilter // 다중 원소 집합

	// 총 원소 수 및 파일 용량
	totalElements uint64
	fileCapacity  uint64
}

func (i *IncrementalUnionFindProcessor) ProcessSets(userSets []UserSet) (error, []UserSet) {
	// 1. 해당 함수는, 집합의 리스트를 받아서, IncrementalUnionFindProcessor의 아카이브를 업데이트 한다.
	err, multiSets := i.ProcessSingleSets(userSets)
	if err != nil {
		return err, nil
	}

	// 2. ProcessMultiSets(multiSets)을 호출한다.
	err = i.ProcessMultiSets(multiSets)
	if err != nil {
		return err, nil
	}

	return nil, multiSets
}

func ParseByChunk(elements []uint64) map[int][]uint64 {
	// 해당 함수는, uint64배열을 받아서, 그 값을 ChunkSize을 기준으로 청킹한다.
	parsedByChunk := make(map[int][]uint64)

	for _, element := range elements {
		chunkIndex := int(element / ChunkSize)
		if _, exists := parsedByChunk[chunkIndex]; !exists {
			parsedByChunk[chunkIndex] = []uint64{}
		}
		parsedByChunk[chunkIndex] = append(parsedByChunk[chunkIndex], element)
	}

	return parsedByChunk
}

func (i *IncrementalUnionFindProcessor) ProcessSingleSets(userSets []UserSet) (error, []UserSet) {
	// 해당 함수는 단일 셋을 골라서 그것을 통해 IncrementalUnionFindProcessor의 아카이브를 업데이트 한다.

	// 1. multiSets, batchBuffer를 초기화한다.
	multiSets := []UserSet{}
	batchBuffer := []uint64{}

	// 2. userSets를 순회한다.
	for _, userSet := range userSets {
		// 3. 만약 userSets의 userSet의 size가 2 이상이면 이를 multiSets에 어펜드한다.
		if userSet.Size >= 2 {
			multiSets = append(multiSets, userSet)
		} else if userSet.Size == 1 {
			// 4. 만약 UserSets의의 userSet의 size가 1이면 그 요소를 batchBuffer에 추가한다.
			batchBuffer = append(batchBuffer, userSet.Elements[0])
		}
	}

	// 5. 순회를 끝낸다.

	// 6. batchBuffer를 순회하며 처리한다.
	err := i.ProcessBatch(batchBuffer)
	if err != nil {
		err = fmt.Errorf("failed to process batch: %v", err)
		return err, nil
	}
	// 7. multiSets를 리턴한다.
	return nil, multiSets
}

// 병렬화된 배치 처리 함수 (기존 함수의 일부분)
func (i *IncrementalUnionFindProcessor) ProcessBatch(batchBuffer []uint64) error {
	// 6. batchBuffer를 병렬 순회하며 처리한다.
	if len(batchBuffer) > 0 {
		// CPU 수에 따른 워커 수 결정 (CPUx2)
		numCPU := runtime.NumCPU()
		numWorkers := numCPU

		// 워커 수 제한
		if numWorkers > 256 {
			numWorkers = 256
		}

		// 작은 배치는 병렬화 오버헤드가 더 클 수 있음
		if len(batchBuffer) < numWorkers*10 {
			return i.processSequentially(batchBuffer)
		}

		// 6-1: 블룸필터로 중복 확인 (병렬처리)
		// 스레드 안전한 슬라이스를 위한 뮤텍스
		var needToCheckMutex sync.Mutex
		var needToBatchMutex sync.Mutex

		// 결과 슬라이스 준비
		needToCheck := []uint64{}
		needToBatch := []uint64{}

		// 작업 분할
		chunkSize := (len(batchBuffer) + numWorkers - 1) / numWorkers
		var wg sync.WaitGroup

		// 블룸필터 확인 병렬화
		for w := 0; w < numWorkers; w++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()

				// 각 워커의 로컬 결과 슬라이스
				localNeedToCheck := make([]uint64, 0, chunkSize)
				localNeedToBatch := make([]uint64, 0, chunkSize)

				// 각 워커의 작업 범위 계산
				start := workerID * chunkSize
				end := min(start+chunkSize, len(batchBuffer))

				// 로컬에서 블룸필터 확인
				for j := start; j < end; j++ {
					element := batchBuffer[j]
					// i의 totalBloomfilter에서 값이 있는지 확인
					if i.totalBloomFilter.MayContain(element) {
						// 6-2: 블룸필터에 있다고 나오면 실제 확인 필요
						localNeedToCheck = append(localNeedToCheck, element)
					} else {
						// 6-2: 없다고 나오면 새로 추가
						localNeedToBatch = append(localNeedToBatch, element)
					}
				}

				// 로컬 결과를 전역 결과에 안전하게 병합
				if len(localNeedToCheck) > 0 {
					needToCheckMutex.Lock()
					needToCheck = append(needToCheck, localNeedToCheck...)
					needToCheckMutex.Unlock()
				}

				if len(localNeedToBatch) > 0 {
					needToBatchMutex.Lock()
					needToBatch = append(needToBatch, localNeedToBatch...)
					needToBatchMutex.Unlock()
				}
			}(w)
		}

		// 모든 블룸필터 확인 완료 대기
		wg.Wait()

		// 6-3: 실제 존재 여부 확인 (병렬처리)
		if len(needToCheck) > 0 {
			// 청크로 분리하여 처리
			parsedByChunk := ParseByChunk(needToCheck)

			// 결과를 저장할 채널
			resultChan := make(chan uint64, len(needToCheck))

			// 청크 수가 많으면 병렬 처리, 적으면 순차 처리
			if len(parsedByChunk) >= numCPU {
				// 병렬로 각 청크 처리
				chunkWg := sync.WaitGroup{}

				for _, elements := range parsedByChunk {
					chunkWg.Add(1)
					go func(chunk []uint64) {
						defer chunkWg.Done()

						// 각 요소 확인
						for _, element := range chunk {
							// parentSlice에 값이 없으면(0이면) 실제로 없는 요소
							if i.accessParent(element) == 0 {
								resultChan <- element
							}
						}
					}(elements)
				}

				// 결과 수집 고루틴
				go func() {
					chunkWg.Wait()
					close(resultChan)
				}()

				// 결과 채널에서 데이터 수집
				for element := range resultChan {
					needToBatchMutex.Lock()
					needToBatch = append(needToBatch, element)
					needToBatchMutex.Unlock()
				}
			} else {
				// 청크 수가 적으면 순차적으로 처리
				for _, elements := range parsedByChunk {
					for _, element := range elements {
						if i.accessParent(element) == 0 {
							needToBatch = append(needToBatch, element)
						}
					}
				}
			}
		}

		// 6-4: 새 요소 배치 추가
		if len(needToBatch) > 0 {
			err := i.BatchSingleSet(needToBatch)
			if err != nil {
				return err
			}
		}

		return nil
	}

	return nil
}

// 배치 수가 적을 때 순차 처리를 위한 헬퍼 함수
func (i *IncrementalUnionFindProcessor) processSequentially(batchBuffer []uint64) error {
	// 6-1: 블룸필터로 중복 확인
	needToCheck := []uint64{}
	needToBatch := []uint64{}

	for _, element := range batchBuffer {
		// i의 totalBloomfilter에서 batchBuffer값이 있는지 확인한다.
		if i.totalBloomFilter.MayContain(element) {
			// 6-2: 블룸필터에 있다고 나오면 실제 확인 필요
			needToCheck = append(needToCheck, element)
		} else {
			// 6-2: 없다고 나오면 새로 추가
			needToBatch = append(needToBatch, element)
		}
	}

	// 6-3: 실제 존재 여부 확인
	if len(needToCheck) > 0 {
		// 청크로 분리하여 처리
		parsedByChunk := ParseByChunk(needToCheck)

		for _, elements := range parsedByChunk {
			// 각 요소 확인
			for _, element := range elements {
				// parentSlice에 값이 없으면(0이면) 실제로 없는 요소
				if i.accessParent(element) == 0 {
					needToBatch = append(needToBatch, element)
				}
			}
		}
	}

	// 6-4: 새 요소 배치 추가
	if len(needToBatch) > 0 {
		err := i.BatchSingleSet(needToBatch)
		if err != nil {
			return err
		}
	}

	return nil
}
func (i *IncrementalUnionFindProcessor) BatchSingleSet(batchBuffer []uint64) error {
	// 해당 함수는 단일 크기 집합을 청크 단위로 IncrementalUnionFindProcessor의 아카이브에 업데이트한다.

	// 배치 버퍼가 비어있으면 바로 종료
	if len(batchBuffer) == 0 {
		return nil
	}

	// CPU 코어 수 기반으로 병렬 처리 설정
	// 16코어 환경이므로 그에 맞게 최적화
	numCPU := runtime.NumCPU() // 16
	numWorkers := numCPU       // 최대 16개 워커 사용

	// 1. batchBuffer를 블룸필터에 추가 (최적화된 BatchAdd 메서드 사용)
	// 최적화된 BulkBatchAdd는 내부적으로 CPU 코어 수에 따라 워커 수 최적화
	i.totalBloomFilter.BulkBatchAdd(batchBuffer)

	// 요소 수 업데이트 (원자적 연산)
	atomic.AddUint64(&i.totalElements, uint64(len(batchBuffer)))

	// 2. parsedByChunk 딕셔너리를 생성 후 ParseByChunk(batchBuffer)를 대입한다.
	parsedByChunk := ParseByChunk(batchBuffer)

	// 3. 청크별로 병렬 처리
	// 청크 수가 너무 많으면 메모리 사용량이 증가하므로 제한
	maxConcurrentChunks := 1000

	if len(parsedByChunk) < numWorkers {
		// 청크 수가 적으면 순차 처리
		for _, elements := range parsedByChunk {
			for _, element := range elements {
				i.setElement(element, element, 0, 1)
			}
		}
	} else {
		// 청크 수가 충분히 많으면 병렬 처리
		// 워커 풀 패턴 사용
		var wg sync.WaitGroup

		// 청크 작업 채널 생성 - 버퍼 크기 제한
		chunkJobs := make(chan []uint64, min(len(parsedByChunk), maxConcurrentChunks))

		// 워커 생성
		for w := 0; w < numWorkers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				// 로컬 맵을 미리 할당하여 메모리 재사용 (옵션)
				seen := make(map[uint64]bool, 1000)

				for elements := range chunkJobs {
					// 각 청크를 처리
					// 옵션: 중복 제거를 위한 맵 사용
					seen = make(map[uint64]bool, len(elements))
					for _, element := range elements {
						if !seen[element] {
							i.setElement(element, element, 0, 1)
							seen[element] = true
						}
					}
				}
			}()
		}

		// 청크 작업 제출 (별도 고루틴으로 실행)
		go func() {
			for _, elements := range parsedByChunk {
				chunkJobs <- elements
			}
			close(chunkJobs)
		}()

		// 모든 작업 완료 대기
		wg.Wait()
	}

	return nil
}

// func (i *IncrementalUnionFindProcessor) BatchAddMultiSet(userSets []UserSet) error {
// 	// 해당 함수는 []userSet을 순회하면서 첫 원소를 루트로 여기고, 이들 연산을 청크 단위로 업데이트 한다.

// 	// 1. map[int][2][]uint64를 만든다. - 청크별 [인덱스, 루트값] 쌍 저장
// 	chunkData := make(map[int][2][]uint64)

// 	// 2. 모든 요소를 청크별로 분류하고 루트 정보 기록
// 	for _, userSet := range userSets {
// 		if len(userSet.Elements) == 0 {
// 			continue
// 		}

// 		// 첫 번째 요소를 루트로 사용
// 		root := userSet.Elements[0]

// 		for _, element := range userSet.Elements {
// 			chunkIndex := int(element / ChunkSize)

// 			if _, exists := chunkData[chunkIndex]; !exists {
// 				chunkData[chunkIndex] = [2][]uint64{{}, {}}
// 			}

// 			// 인덱스와 루트 정보 저장
// 			temp := chunkData[chunkIndex]
// 			temp[0] = append(temp[0], element)
// 			temp[1] = append(temp[1], root)
// 			chunkData[chunkIndex] = temp
// 		}
// 	}

// 	// 3. 청크별로 처리
// 	for _, data := range chunkData {
// 		elements := data[0]
// 		roots := data[1]

// 		// 각 요소 업데이트
// 		for idx, element := range elements {

// 			// 부모 설정
// 			i.parentSlice[element] = roots[idx]

// 			// 루트 요소인 경우 (자기 자신이 부모)
// 			if element == roots[idx] {
// 				i.rankSlice[element] = 0
// 			} else {
// 				// 루트가 아닌 요소는 랭크 0, 크기 0
// 				i.rankSlice[element] = 0
// 				i.sizeSlice[element] = 0
// 			}

// 		}

// 	}

// 	// 4. 루트 요소의 크기 업데이트
// 	for _, userSet := range userSets {
// 		if len(userSet.Elements) == 0 {
// 			continue
// 		}

// 		// 첫 번째 요소를 루트로 사용
// 		root := userSet.Elements[0]

// 		// 루트 요소의 크기 업데이트
// 		i.sizeSlice[root] = userSet.Size

// 	}

// 	return nil
// }

func (i *IncrementalUnionFindProcessor) GrowMultiSet(userSet UserSet, rootElement uint64) error {
	// 해당 함수는 다른 집합과 머지할 필요 없는, 닫힌 집합을 IncrementalUnionFindProcessor의 아카이브에 반영한다.
	if len(userSet.Elements) == 0 {
		return nil
	}

	// 1. 지정된 루트 요소 사용

	// 2. parentMap, rankMap 초기화
	parentMap := make(map[uint64]uint64)
	rankMap := make(map[uint64]uint8)
	sizeMap := make(map[uint64]uint16)

	// 모든 요소의 부모를 루트로 설정
	for _, element := range userSet.Elements {
		parentMap[element] = rootElement

		// 루트 요소인 경우
		if element == rootElement {
			rankMap[element] = 0
			// 기존 크기 가져오기
			existingSize := i.accessSize(element)
			// 크기 업데이트 (기존 크기 + 새 요소 수)
			sizeMap[element] = existingSize + uint16(len(userSet.Elements)) - 1
		} else {
			// 루트가 아닌 요소는 랭크 0, 크기 0
			rankMap[element] = 0
			sizeMap[element] = 0
		}
	}

	// 3. 청크별로 요소 분류하여 처리
	parsedByChunk := ParseByChunk(userSet.Elements)

	for _, elements := range parsedByChunk {
		// 청크 경계 계산

		// 각 요소 업데이트
		for _, element := range elements {
			// 상대적 인덱스 계산

			// 부모, 랭크, 크기 설정
			i.parentSlice[element] = parentMap[element]
			i.rankSlice[element] = rankMap[element]
			i.sizeSlice[element] = sizeMap[element]

		}

		// 변경사항 저장

	}

	return nil
}

func (i *IncrementalUnionFindProcessor) MergeMultiSet(userSet UserSet, anchorToRoot map[uint64]uint64) error {
	// 여러 앵커가 존재하는 상황에서, 아카이브의 여러 트리를 병합하는 함수이다.
	if len(userSet.Elements) == 0 || len(anchorToRoot) == 0 {
		return nil
	}

	// 4. rootUnions 맵 생성 (from -> to)
	rootUnions := make(map[uint64]uint64)

	// 루트 랭크와 크기 정보 수집
	rootRanks := make(map[uint64]uint8)
	rootSizes := make(map[uint64]uint16)

	// 모든 루트 수집
	var allRoots []uint64
	for _, root := range anchorToRoot {
		if _, exists := rootRanks[root]; !exists {
			allRoots = append(allRoots, root)

			// 루트 정보 가져오

			// 랭크와 크기 저장
			rootRanks[root] = i.accessRank(root)
			rootSizes[root] = i.accessSize(root)
		}
	}

	// 5. 랭크를 기준으로 루트 병합 방향 결정
	if len(allRoots) > 1 {
		// 랭크가 높은 순서로 정렬
		sort.Slice(allRoots, func(i, j int) bool {
			if rootRanks[allRoots[i]] != rootRanks[allRoots[j]] {
				return rootRanks[allRoots[i]] > rootRanks[allRoots[j]]
			}
			if rootSizes[allRoots[i]] != rootSizes[allRoots[j]] {
				return rootSizes[allRoots[i]] > rootSizes[allRoots[j]]
			}
			return allRoots[i] < allRoots[j]
		})

		// 최종 루트 결정
		finalRoot := allRoots[0]

		// 다른 모든 루트를 최종 루트에 병합
		for _, root := range allRoots[1:] {
			rootUnions[root] = finalRoot
		}

		// 6. 전이적 닫힘 - 모든 경로를 최종 루트로 압축
		for from, to := range rootUnions {
			finalTo := to
			for {
				if nextTo, exists := rootUnions[finalTo]; exists {
					finalTo = nextTo
				} else {
					break
				}
			}
			rootUnions[from] = finalTo
		}

		// 7. 새 랭크 계산 및 크기 합산
		newRank := rootRanks[finalRoot]
		if newRank == rootRanks[allRoots[1]] {
			newRank++
		}

		var newSize uint16 = rootSizes[finalRoot]
		for _, root := range allRoots[1:] {
			newSize += rootSizes[root]
		}

		// 사용자 집합에서 기존 앵커에 없는 요소 수 계산
		for _, element := range userSet.Elements {
			if _, exists := anchorToRoot[element]; !exists {
				newSize++
			}
		}

		// 8. 업데이트할 요소 기록
		elementsToUpdate := make(map[uint64]struct {
			parent uint64
			rank   uint8
			size   uint16
		})

		// 루트 외 요소들은 finalRoot를 부모로 설정
		for _, root := range allRoots {
			if root != finalRoot {
				elementsToUpdate[root] = struct {
					parent uint64
					rank   uint8
					size   uint16
				}{
					parent: finalRoot,
					rank:   0,
					size:   0,
				}
			}
		}

		// 최종 루트 정보 업데이트
		elementsToUpdate[finalRoot] = struct {
			parent uint64
			rank   uint8
			size   uint16
		}{
			parent: finalRoot,
			rank:   newRank,
			size:   newSize,
		}

		// userSet의 모든 요소 추가
		for _, element := range userSet.Elements {
			if _, exists := anchorToRoot[element]; !exists {
				elementsToUpdate[element] = struct {
					parent uint64
					rank   uint8
					size   uint16
				}{
					parent: finalRoot,
					rank:   0,
					size:   0,
				}
			}
		}

		// 청크별로 요소 분류하여 업데이트
		elementsByChunk := make(map[int][]uint64)

		for element := range elementsToUpdate {
			chunkIndex := int(element / ChunkSize)
			if _, exists := elementsByChunk[chunkIndex]; !exists {
				elementsByChunk[chunkIndex] = []uint64{}
			}
			elementsByChunk[chunkIndex] = append(elementsByChunk[chunkIndex], element)
		}

		// 각 청크별로 처리
		for _, elements := range elementsByChunk {

			// 각 요소 업데이트
			for _, element := range elements {
				update := elementsToUpdate[element]

				// 부모, 랭크, 크기 업데이트
				i.parentSlice[element] = update.parent
				i.rankSlice[element] = update.rank
				i.sizeSlice[element] = update.size

			}

		}
	}

	return nil
}

// 루트 요소 찾기 헬퍼 함수 (경로 압축 구현)
func (i *IncrementalUnionFindProcessor) findRoot(element uint64) uint64 {
	var current uint64 = element
	var root uint64 = element
	var path []uint64

	// 루트 찾기
	for {
		parent := i.accessParent(current)

		// 자기 자신이 부모이면 루트 찾음
		if parent == current {
			root = current
			break
		}

		// 경로에 현재 요소 추가
		path = append(path, current)

		// 다음 요소로 이동
		current = parent
	}

	// 경로 압축 - 모든 요소의 부모를 루트로 설정
	for _, element := range path {
		i.parentSlice[element] = root
	}

	return root
}

// ! 참고: 이거 시스템콜이라 더럽게 비쌈. 대략 0.1초 걸림. 엄청 신중히 쓸 것.
// ! 아님 차라리 element로드를 조금씩 하고, 1억번마다 한번씩 cleanup하던지.
// 윈도우 관리 함수 - 특정 범위의 메모리를 활성화하고 나머지는 해제
func (i *IncrementalUnionFindProcessor) tidyWindow(start uint64, end uint64) error {
	// 윈도우 범위가 파일 용량을 초과하면 파일 확장
	if end > i.fileCapacity {
		err := i.extendFiles(end)
		if err != nil {
			return fmt.Errorf("파일 확장 실패: %w", err)
		}
	}

	// 이전 윈도우 범위 저장

	// 총 파일 용량
	totalCapacity := i.fileCapacity

	// 현재 윈도우 외 영역 계산 (앞부분)
	if start > 0 {
		// 부모 슬라이스 앞부분 메모리 해제
		frontParentLength := start * 8 // uint64 = 8바이트
		if err := madviseBytes(i.parentArchive[:frontParentLength], false); err != nil {
			return fmt.Errorf("부모 슬라이스 앞부분 madvise 실패: %w", err)
		}

		// 랭크 슬라이스 앞부분 메모리 해제
		frontRankLength := start // uint8 = 1바이트
		if err := madviseBytes(i.rankArchive[:frontRankLength], false); err != nil {
			return fmt.Errorf("랭크 슬라이스 앞부분 madvise 실패: %w", err)
		}

		// 크기 슬라이스 앞부분 메모리 해제
		frontSizeLength := start * 2 // uint16 = 2바이트
		if err := madviseBytes(i.sizeArchive[:frontSizeLength], false); err != nil {
			return fmt.Errorf("크기 슬라이스 앞부분 madvise 실패: %w", err)
		}
	}

	// 현재 윈도우 외 영역 계산 (뒷부분)
	if end < totalCapacity {
		// 부모 슬라이스 뒷부분 메모리 해제
		backParentOffset := end * 8 // uint64 = 8바이트
		backParentLength := (totalCapacity - end) * 8
		if err := madviseBytes(i.parentArchive[backParentOffset:backParentOffset+backParentLength], false); err != nil {
			return fmt.Errorf("부모 슬라이스 뒷부분 madvise 실패: %w", err)
		}

		// 랭크 슬라이스 뒷부분 메모리 해제
		backRankOffset := end // uint8 = 1바이트
		backRankLength := totalCapacity - end
		if err := madviseBytes(i.rankArchive[backRankOffset:backRankOffset+backRankLength], false); err != nil {
			return fmt.Errorf("랭크 슬라이스 뒷부분 madvise 실패: %w", err)
		}

		// 크기 슬라이스 뒷부분 메모리 해제
		backSizeOffset := end * 2 // uint16 = 2바이트
		backSizeLength := (totalCapacity - end) * 2
		if err := madviseBytes(i.sizeArchive[backSizeOffset:backSizeOffset+backSizeLength], false); err != nil {
			return fmt.Errorf("크기 슬라이스 뒷부분 madvise 실패: %w", err)
		}
	}

	// 현재 윈도우 영역은 필요함을 표시
	// 부모 슬라이스 메모리 확보
	parentStartOffset := start * 8 // uint64 = 8바이트
	parentEnd := end * 8
	if err := madviseBytes(i.parentArchive[parentStartOffset:parentEnd], true); err != nil {
		return fmt.Errorf("부모 슬라이스 윈도우 madvise 실패: %w", err)
	}

	// 랭크 슬라이스 메모리 확보
	rankStartOffset := start // uint8 = 1바이트
	rankEnd := end
	if err := madviseBytes(i.rankArchive[rankStartOffset:rankEnd], true); err != nil {
		return fmt.Errorf("랭크 슬라이스 윈도우 madvise 실패: %w", err)
	}

	// 크기 슬라이스 메모리 확보
	sizeStartOffset := start * 2 // uint16 = 2바이트
	sizeEnd := end * 2
	if err := madviseBytes(i.sizeArchive[sizeStartOffset:sizeEnd], true); err != nil {
		return fmt.Errorf("크기 슬라이스 윈도우 madvise 실패: %w", err)
	}

	return nil
}

// Cleanup 함수 - 현재 윈도우를 기준으로 tidyWindow 실행
func (i *IncrementalUnionFindProcessor) Cleanup() error {
	fmt.Println("[Cleanup] tidyWindow triggered due to load threshold")
	i.currentHoldingElements = 0
	return i.tidyWindow(0, 0)
}

// madvise 래퍼 함수 - 운영체제에 메모리 사용 힌트 제공
func madviseBytes(b []byte, needed bool) error {
	// 바이트 슬라이스가 비어있으면 아무것도 하지 않음
	if len(b) == 0 {
		return nil
	}

	const (
		// Linux/WSL2 시스템 콜 상수
		MADV_NORMAL     = 0
		MADV_WILLNEED   = 3
		MADV_DONTNEED   = 4
		MADV_SEQUENTIAL = 2 // 순차적 접근 패턴
	)

	// 슬라이스 시작 주소와 길이 얻기
	addr := uintptr(unsafe.Pointer(&b[0]))
	length := uintptr(len(b))

	// 필요 여부에 따라 어드바이스 설정
	advice := MADV_DONTNEED
	if needed {
		advice = MADV_WILLNEED
	}

	// 시스템 콜 호출
	_, _, err := syscall.Syscall(
		syscall.SYS_MADVISE,
		addr,
		length,
		uintptr(advice),
	)

	// 시스템 콜 오류 확인
	if err != 0 {
		return fmt.Errorf("madvise failed: %v", err)
	}

	return nil
}

// 파일 확장 헬퍼 함수

// 파일 확장 함수 - reflect.SliceHeader 업데이트 포함
func (i *IncrementalUnionFindProcessor) extendFiles(newCapacity uint64) error {
	// 기존 mmap 해제
	if i.parentArchive != nil {
		i.parentArchive.Unmap()
	}
	if i.rankArchive != nil {
		i.rankArchive.Unmap()
	}
	if i.sizeArchive != nil {
		i.sizeArchive.Unmap()
	}

	// 파일 크기 확장
	err := i.parentFile.Truncate(int64(newCapacity * 8)) // uint64 = 8바이트
	if err != nil {
		return fmt.Errorf("부모 파일 확장 실패: %w", err)
	}

	err = i.rankFile.Truncate(int64(newCapacity)) // uint8 = 1바이트
	if err != nil {
		return fmt.Errorf("랭크 파일 확장 실패: %w", err)
	}

	err = i.sizeFile.Truncate(int64(newCapacity * 2)) // uint16 = 2바이트
	if err != nil {
		return fmt.Errorf("크기 파일 확장 실패: %w", err)
	}

	// 새로운 mmap 생성
	parentArchive, err := mmap.Map(i.parentFile, mmap.RDWR, 0)
	if err != nil {
		return fmt.Errorf("부모 파일 재매핑 실패: %w", err)
	}
	i.parentArchive = parentArchive

	rankArchive, err := mmap.Map(i.rankFile, mmap.RDWR, 0)
	if err != nil {
		i.parentArchive.Unmap()
		return fmt.Errorf("랭크 파일 재매핑 실패: %w", err)
	}
	i.rankArchive = rankArchive

	sizeArchive, err := mmap.Map(i.sizeFile, mmap.RDWR, 0)
	if err != nil {
		i.parentArchive.Unmap()
		i.rankArchive.Unmap()
		return fmt.Errorf("크기 파일 재매핑 실패: %w", err)
	}
	i.sizeArchive = sizeArchive

	// 슬라이스 헤더 업데이트
	{
		i.parentSlice = unsafe.Slice((*uint64)(unsafe.Pointer(&parentArchive[0])), int(newCapacity))
		i.rankSlice = unsafe.Slice((*uint8)(unsafe.Pointer(&rankArchive[0])), int(newCapacity))
		i.sizeSlice = unsafe.Slice((*uint16)(unsafe.Pointer(&sizeArchive[0])), int(newCapacity))

	}

	// 파일 용량 업데이트
	i.fileCapacity = newCapacity

	// GC 강제 실행으로 메모리 정리
	runtime.GC()

	return nil
}

func (i *IncrementalUnionFindProcessor) accessParent(element uint64) uint64 {
	i.currentHoldingElements++
	if i.currentHoldingElements > MaxLoadStack {
		_ = i.Cleanup()

	}
	// 바로 슬라이스 인덱스로 접근
	return i.parentSlice[element]
}
func (i *IncrementalUnionFindProcessor) setParent(element uint64, parent uint64) {
	// 바로 슬라이스 인덱스로 접근하여 값 설정
	i.parentSlice[element] = parent
}

func (i *IncrementalUnionFindProcessor) accessRank(element uint64) uint8 {
	i.currentHoldingElements++
	if i.currentHoldingElements > MaxLoadStack {
		_ = i.Cleanup()

	}
	// 바로 슬라이스 인덱스로 접근
	return i.rankSlice[element]
}
func (i *IncrementalUnionFindProcessor) setRank(element uint64, rank uint8) {
	// 바로 슬라이스 인덱스로 접근하여 값 설정
	i.rankSlice[element] = rank
}
func (i *IncrementalUnionFindProcessor) accessSize(element uint64) uint16 {
	i.currentHoldingElements++
	if i.currentHoldingElements > MaxLoadStack {
		_ = i.Cleanup()
	}
	// 바로 슬라이스 인덱스로 접근
	return i.sizeSlice[element]
}
func (i *IncrementalUnionFindProcessor) setSize(element uint64, size uint16) {
	// 바로 슬라이스 인덱스로 접근하여 값 설정
	i.sizeSlice[element] = size
}

// 참고: 복사 방식이 아닌 포인터 방식으로 요소 접근 예시
func (i *IncrementalUnionFindProcessor) accessElement(element uint64) (uint64, uint8, uint16) {
	i.currentHoldingElements++
	if i.currentHoldingElements > MaxLoadStack {
		_ = i.Cleanup()

	}
	// 바로 슬라이스 인덱스로 접근
	return i.parentSlice[element], i.rankSlice[element], i.sizeSlice[element]
}
func (i *IncrementalUnionFindProcessor) setElement(element uint64, parent uint64, rank uint8, size uint16) {
	// 바로 슬라이스 인덱스로 접근하여 값 설정
	i.parentSlice[element] = parent
	i.rankSlice[element] = rank
	i.sizeSlice[element] = size
}

// NewDisjointSet은 새로운 DisjointSet 인스턴스를 생성합니다.
type DisjointSet struct {
	parent map[uint64]uint64 // 각 원소의 부모를 저장
	rank   map[uint64]int    // 트리의 높이를 저장 (최적화용)
}

// NewDisjointSet은 새로운 DisjointSet 인스턴스를 생성합니다.
func NewDisjointSet() *DisjointSet {
	return &DisjointSet{
		parent: make(map[uint64]uint64),
		rank:   make(map[uint64]int),
	}
}

// MakeSet은 새로운 원소를 집합에 추가합니다.
func (ds *DisjointSet) MakeSet(x uint64) {
	if _, exists := ds.parent[x]; exists {
		return // 이미 존재하는 원소는 무시
	}

	ds.parent[x] = x // 자기 자신을 부모로 설정
	ds.rank[x] = 0   // 초기 랭크는 0
}

// Find는 원소 x가 속한 집합의 대표(루트)를 찾습니다.
// 경로 압축 최적화를 적용합니다.
func (ds *DisjointSet) Find(x uint64) uint64 {
	if _, exists := ds.parent[x]; !exists {
		ds.MakeSet(x)
	}

	if ds.parent[x] != x {
		ds.parent[x] = ds.Find(ds.parent[x]) // 경로 압축
	}
	return ds.parent[x]
}

// Union은 두 원소가 속한 집합을 합칩니다.
// 랭크에 따른 병합 최적화를 적용합니다.
func (ds *DisjointSet) Union(x, y uint64) {
	rootX := ds.Find(x)
	rootY := ds.Find(y)

	if rootX == rootY {
		return // 이미 같은 집합에 속해 있음
	}

	// 랭크에 따라 병합 (랭크가 작은 트리를 랭크가 큰 트리에 붙임)
	if ds.rank[rootX] < ds.rank[rootY] {
		ds.parent[rootX] = rootY
	} else {
		ds.parent[rootY] = rootX

		// 두 트리의 랭크가 같으면 결과 트리의 랭크를 증가
		if ds.rank[rootX] == ds.rank[rootY] {
			ds.rank[rootX]++
		}
	}
}

// GetSets는 현재 분리된 모든 집합들을 반환합니다.
func (ds *DisjointSet) GetSets() map[uint64][]uint64 {
	result := make(map[uint64][]uint64)

	// 모든 요소를 순회하며 집합을 계산
	for element := range ds.parent {
		root := ds.Find(element) // 경로 압축이 여기서도 발생

		if _, exists := result[root]; !exists {
			result[root] = []uint64{}
		}
		result[root] = append(result[root], element)
	}

	return result
}

// PrintSets는 현재 모든 분리 집합과 그 원소들을 출력합니다.
func (ds *DisjointSet) PrintSets() {
	sets := ds.GetSets()

	fmt.Println("현재 분리 집합 상태:")

	for _, set := range sets {
		fmt.Printf("집합의 원소들: %v\n", set)
	}
}

// 대용량 파일 생성
func createOrOpenLargeFile(fileName string, size int64) (*os.File, error) {
	// 디렉토리 확인 및 생성
	dir := filepath.Dir(fileName)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("디렉토리 생성 실패: %v", err)
	}
	// 이미 파일이 존재하면 그냥 열기
	if stat, err := os.Stat(fileName); err == nil {
		fmt.Printf("기존 파일 사용: %s (%.2f GB)\n", fileName, float64(stat.Size())/1024/1024/1024)
		return os.OpenFile(fileName, os.O_RDWR, 0644)
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

// Close closes the processor and saves BloomFilters
func (p *IncrementalUnionFindProcessor) Close() error {
	// Save BloomFilters first
	if err := p.SaveAllBloomFilters(); err != nil {
		fmt.Printf("블룸 필터 저장 실패: %v\n", err)
		// Continue with cleanup even if BloomFilter save fails
	} else {
		fmt.Println("블룸 필터 저장 완료")
	}

	// Unmap memory-mapped files
	if p.parentArchive != nil {
		p.parentArchive.Unmap()
	}
	if p.rankArchive != nil {
		p.rankArchive.Unmap()
	}
	if p.sizeArchive != nil {
		p.sizeArchive.Unmap()
	}

	// Close files
	if p.parentFile != nil {
		p.parentFile.Close()
	}
	if p.rankFile != nil {
		p.rankFile.Close()
	}
	if p.sizeFile != nil {
		p.sizeFile.Close()
	}

	return nil
}

// 생성자 함수
func NewIncrementalUnionFindProcessor(dataDir string, initialCapacity uint64) (*IncrementalUnionFindProcessor, error) {
	// 디렉토리 생성

	// 디렉토리 생성
	err := os.MkdirAll(dataDir, 0755)
	if err != nil {
		return nil, fmt.Errorf("데이터 디렉토리 생성 실패: %w", err)
	}

	// 파일 경로
	parentFilePath := filepath.Join(dataDir, "parent.bin")
	rankFilePath := filepath.Join(dataDir, "rank.bin")
	sizeFilePath := filepath.Join(dataDir, "size.bin")

	// 대용량 파일 한번에 생성
	parentSize := int64(initialCapacity * 8) // uint64 = 8바이트
	parentFile, err := createOrOpenLargeFile(parentFilePath, parentSize)
	if err != nil {
		return nil, fmt.Errorf("부모 파일 생성 실패: %w", err)
	}
	defer parentFile.Close()

	rankSize := int64(initialCapacity) // uint8 = 1바이트
	rankFile, err := createOrOpenLargeFile(rankFilePath, rankSize)
	if err != nil {
		return nil, fmt.Errorf("랭크 파일 생성 실패: %w", err)
	}
	defer rankFile.Close()

	sizeSize := int64(initialCapacity * 2) // uint16 = 2바이트
	sizeFile, err := createOrOpenLargeFile(sizeFilePath, sizeSize)
	if err != nil {
		return nil, fmt.Errorf("크기 파일 생성 실패: %w", err)
	}
	defer sizeFile.Close()

	// mmap 생성
	parentArchive, err := mmap.Map(parentFile, mmap.RDWR, 0)
	if err != nil {
		parentFile.Close()
		rankFile.Close()
		sizeFile.Close()
		return nil, fmt.Errorf("부모 파일 메모리 매핑 실패: %w", err)
	}

	rankArchive, err := mmap.Map(rankFile, mmap.RDWR, 0)
	if err != nil {
		parentArchive.Unmap()
		parentFile.Close()
		rankFile.Close()
		sizeFile.Close()
		return nil, fmt.Errorf("랭크 파일 메모리 매핑 실패: %w", err)
	}

	sizeArchive, err := mmap.Map(sizeFile, mmap.RDWR, 0)
	if err != nil {
		parentArchive.Unmap()
		rankArchive.Unmap()
		parentFile.Close()
		rankFile.Close()
		sizeFile.Close()
		return nil, fmt.Errorf("크기 파일 메모리 매핑 실패: %w", err)
	}
	// 슬라이스 변수 선언
	var parentSlice []uint64
	var rankSlice []uint8
	var sizeSlice []uint16

	// 메모리 맵을 Go 슬라이스로 직접 재해석
	{
		parentSlice = unsafe.Slice((*uint64)(unsafe.Pointer(&parentArchive[0])), int(initialCapacity))
		rankSlice = unsafe.Slice((*uint8)(unsafe.Pointer(&rankArchive[0])), int(initialCapacity))
		sizeSlice = unsafe.Slice((*uint16)(unsafe.Pointer(&sizeArchive[0])), int(initialCapacity))

	}

	// 블룸 필터 로드 또는 생성
	totalBloomFilter, err := loadTotalBloomFilter(dataDir)
	if err != nil {
		parentArchive.Unmap()
		rankArchive.Unmap()
		sizeArchive.Unmap()
		parentFile.Close()
		rankFile.Close()
		sizeFile.Close()
		return nil, fmt.Errorf("토탈 블룸 필터 로드 실패: %w", err)
	}

	multiBloomFilter, err := loadMultiBloomFilter(dataDir)
	if err != nil {
		parentArchive.Unmap()
		rankArchive.Unmap()
		sizeArchive.Unmap()
		parentFile.Close()
		rankFile.Close()
		sizeFile.Close()
		return nil, fmt.Errorf("멀티 블룸 필터 로드 실패: %w", err)
	}

	return &IncrementalUnionFindProcessor{
		dataDir:                dataDir,
		parentFile:             parentFile,
		rankFile:               rankFile,
		sizeFile:               sizeFile,
		parentArchive:          parentArchive,
		rankArchive:            rankArchive,
		sizeArchive:            sizeArchive,
		parentSlice:            parentSlice,
		rankSlice:              rankSlice,
		sizeSlice:              sizeSlice,
		currentHoldingElements: 0,
		totalBloomFilter:       totalBloomFilter,
		multiBloomFilter:       multiBloomFilter,
		totalElements:          0,
		fileCapacity:           initialCapacity,
	}, nil
}

// 결과 조회 함수: 특정 요소의 루트 찾기
func (i *IncrementalUnionFindProcessor) Find(element uint64) (uint64, error) {
	// element가 존재하는지 블룸 필터로 확인
	if !i.totalBloomFilter.MayContain(element) {
		return 0, fmt.Errorf("요소가 존재하지 않음: %d", element)
	}

	// 요소가 존재하는 청크 찾기

	// 요소가 실제로 존재하는지 확인
	if i.parentSlice[element] == 0 {
		return 0, fmt.Errorf("요소가 존재하지 않음: %d", element)
	}

	// 루트 찾기
	return i.findRoot(element), nil
}

// 결과 조회 함수: 특정 요소가 속한 집합의 크기 찾기
func (i *IncrementalUnionFindProcessor) Size(element uint64) (uint16, error) {
	// 요소의 루트 찾기
	root, err := i.Find(element)
	if err != nil {
		return 0, err
	}

	// 루트 요소의 크기 반환
	return i.sizeSlice[root], nil
}

// 두 요소가 같은 집합에 속하는지 확인
func (i *IncrementalUnionFindProcessor) Connected(element1 uint64, element2 uint64) (bool, error) {
	// 각 요소의 루트 찾기
	root1, err := i.Find(element1)
	if err != nil {
		return false, err
	}

	root2, err := i.Find(element2)
	if err != nil {
		return false, err
	}

	// 루트가 같으면 연결됨
	return root1 == root2, nil
}

//TODO 더 나은 접근법 찾기. parent기반이 최선이긴 한데
//TODO 이 겨웅 순차 로드로 한다거나.
// // 특정 요소가 속한 집합의 모든 요소 가져오기 (주의: 매우 비효율적, 큰 집합에는 사용 금지)
// func (i *IncrementalUnionFindProcessor) GetSet(element uint64) ([]uint64, error) {
// 	// 요소의 루트 찾기
// 	root, err := i.Find(element)
// 	if err != nil {
// 		return nil, err
// 	}

// 	// 집합 크기 확인
// 	size, err := i.Size(root)
// 	if err != nil {
// 		return nil, err
// 	}

// 	// 결과 집합 초기화
// 	result := make([]uint64, 0, size)

// 	// 전체 요소 수 확인
// 	maxElement := i.totalElements

// 	// 모든 요소를 청크 단위로 확인
// 	for startIdx := uint64(0); startIdx < maxElement; startIdx += ChunkSize {
// 		endIdx := startIdx + ChunkSize
// 		if endIdx > maxElement {
// 			endIdx = maxElement
// 		}

// 		// 청크 내 모든 요소 확인
// 		for j := uint64(0); j < endIdx-startIdx; j++ {
// 			if i.parentSlice[j] != 0 {
// 				// 이 요소의 루트 찾기
// 				elementRoot := i.findRoot(startIdx + j)
// 				if elementRoot == root {
// 					result = append(result, startIdx+j)
// 				}
// 			}
// 		}
// 	}

// 	return result, nil
// }

// func (i *IncrementalUnionFindProcessor) ProcessMultiSets(userSets []UserSet) error {
// 	// 해당 함수는 크기가 2이상은 집합들을 IncrementalUnionFindProcessor의 아카이브에 반영한다.
// 	if len(userSets) == 0 {
// 		return nil
// 	}

// 	// 1. 우선 메모리 내에서, DisjointSet구조체를 이용해서 빠르게 유니언 파인드를 처리
// 	ds := NewDisjointSet()

// 	// 모든 요소를 DisjointSet에 추가하고 같은 셋끼리 유니언
// 	for _, userSet := range userSets {
// 		if len(userSet.Elements) < 2 {
// 			continue
// 		}

// 		root := userSet.Elements[0]
// 		ds.MakeSet(root)

// 		for _, element := range userSet.Elements[1:] {
// 			ds.MakeSet(element)
// 			ds.Union(root, element)
// 		}
// 	}
// 	println("자체 disjointSet 연산 완료")
// 	disjointSets := ds.GetSets()
// 	println("자체 disjointSet 획득 완료")
// 	println(len(disjointSets), "개의 disjointSet 획득")

// 	// 1-1. disjointSets 구하기

// 	// SetWithAnchor 구조체 정의
// 	type SetWithAnchor struct {
// 		anchorToRoot map[uint64]uint64
// 		set          []uint64
// 	}

// 	// 각 분리 집합 처리
// 	for _, elements := range disjointSets {
// 		// 2-1. 앵커 후보 찾기 - multiBloomFilter에 있는 요소들
// 		maybeAnchor := []uint64{}
// 		for _, element := range elements {
// 			if i.multiBloomFilter.MayContain(element) {
// 				maybeAnchor = append(maybeAnchor, element)
// 			}
// 		}

// 		// 2-1. 청크별로 분류
// 		maybeAnchorByChunk := ParseByChunk(maybeAnchor)

// 		// 3. 실제 앵커 확인 및 루트 찾기
// 		anchorToRoot := make(map[uint64]uint64)

// 		for chunkIndex, anchors := range maybeAnchorByChunk {
// 			// 청크 경계 계산
// 			start := uint64(chunkIndex) * ChunkSize
// 			end := start + ChunkSize

// 			// 청크 데이터 로드
// 			if i.windowStart != start || i.windowEnd != end {
// 				err := i.loadWindow(start, end)
// 				if err != nil {
// 					return err
// 				}
// 			}

// 			// 각 앵커 확인
// 			for _, anchor := range anchors {
// 				// 상대적 인덱스 계산
// 				relativeIndex := anchor - start

// 				// 실제 존재하는 요소인지 확인
// 				if i.parentSlice[relativeIndex] != 0 {
// 					// 3-1. 루트 찾기
// 					root := i.findRoot(anchor)
// 					anchorToRoot[anchor] = root
// 				}
// 			}
// 		}

// 		// SetWithAnchor 생성
// 		swa := SetWithAnchor{
// 			anchorToRoot: anchorToRoot,
// 			set:          elements,
// 		}

// 		// 4. 앵커 상황에 따른 처리
// 		if len(swa.anchorToRoot) == 0 {
// 			// 4-1. 앵커가 없는 경우: 새 집합 추가
// 			newSet := UserSet{
// 				Size:     uint16(len(swa.set)),
// 				Elements: swa.set,
// 			}

// 			// 새로운 다중 집합 일괄 추가
// 			err := i.BatchAddMultiSet([]UserSet{newSet})
// 			if err != nil {
// 				return err
// 			}
// 		} else if len(swa.anchorToRoot) == 1 {
// 			// 4-2. 앵커가 하나인 경우: 기존 집합 확장

// 			var root uint64

// 			// 유일한 앵커와 루트 가져오기
// 			for _, r := range swa.anchorToRoot {
// 				root = r
// 				break
// 			}

// 			// UserSet 생성
// 			newSet := UserSet{
// 				Size:     uint16(len(swa.set)),
// 				Elements: swa.set,
// 			}

// 			// 집합 확장
// 			err := i.GrowMultiSet(newSet, root)
// 			if err != nil {
// 				return err
// 			}
// 		} else {
// 			// 4-3. 앵커가 여러 개인 경우: 집합 병합
// 			newSet := UserSet{
// 				Size:     uint16(len(swa.set)),
// 				Elements: swa.set,
// 			}

// 			// 여러 집합 병합
// 			err := i.MergeMultiSet(newSet, swa.anchorToRoot)
// 			if err != nil {
// 				return err
// 			}
// 		}

// 		// 4-4. 블룸 필터 업데이트
// 		for _, element := range elements {
// 			i.totalBloomFilter.Add(element)
// 			i.multiBloomFilter.Add(element)
// 		}
// 	}

//		return nil
//	}

// // mmap 관련 헬퍼 함수
// func (i *IncrementalUnionFindProcessor) loadWindow(start uint64, end uint64) error {
// 	// 윈도우 범위가 파일 용량을 초과하면 파일 확장
// 	if end > i.fileCapacity {
// 		err := i.extendFiles(end)
// 		if err != nil {
// 			return err
// 		}
// 	}

// 	windowSize := end - start
// 	// 이전 슬라이스 해제를 유도하기 위해 nil 설정``
// 	i.parentSlice = nil
// 	i.rankSlice = nil
// 	i.sizeSlice = nil
// 	runtime.GC()

// 	// 슬라이스 초기화
// 	i.parentSlice = make([]uint64, windowSize)
// 	i.rankSlice = make([]uint8, windowSize)
// 	i.sizeSlice = make([]uint16, windowSize)

// 	// parent 슬라이스 로드
// 	for j := uint64(0); j < windowSize; j++ {
// 		elementIndex := start + j

// 		if elementIndex*8+8 <= uint64(len(i.parentArchive)) {
// 			value := binary.LittleEndian.Uint64(i.parentArchive[elementIndex*8 : elementIndex*8+8])
// 			i.parentSlice[j] = value
// 		} else {
// 			i.parentSlice[j] = 0
// 		}
// 	}

// 	// rank 슬라이스 로드
// 	for j := uint64(0); j < windowSize; j++ {
// 		elementIndex := start + j

// 		if elementIndex < uint64(len(i.rankArchive)) {
// 			i.rankSlice[j] = i.rankArchive[elementIndex]
// 		} else {
// 			i.rankSlice[j] = 0
// 		}
// 	}

// 	// size 슬라이스 로드
// 	for j := uint64(0); j < windowSize; j++ {
// 		elementIndex := start + j

// 		if elementIndex*2+2 <= uint64(len(i.sizeArchive)) {
// 			i.sizeSlice[j] = binary.LittleEndian.Uint16(i.sizeArchive[elementIndex*2 : elementIndex*2+2])
// 		} else {
// 			i.sizeSlice[j] = 0
// 		}
// 	}

// 	// 윈도우 범위 업데이트
// 	i.windowStart = start
// 	i.windowEnd = end

// 	return nil
// }

// func (i *IncrementalUnionFindProcessor) saveWindow() error {
// 	windowSize := i.windowEnd - i.windowStart

// 	// parent 슬라이스 저장
// 	for j := uint64(0); j < windowSize; j++ {
// 		if i.parentSlice[j] != 0 {
// 			elementIndex := i.windowStart + j
// 			binary.LittleEndian.PutUint64(i.parentArchive[elementIndex*8:elementIndex*8+8], i.parentSlice[j])
// 		}
// 	}

// 	// rank 슬라이스 저장
// 	for j := uint64(0); j < windowSize; j++ {
// 		if i.parentSlice[j] != 0 {
// 			elementIndex := i.windowStart + j
// 			i.rankArchive[elementIndex] = i.rankSlice[j]
// 		}
// 	}

// 	// size 슬라이스 저장
// 	for j := uint64(0); j < windowSize; j++ {
// 		if i.parentSlice[j] != 0 {
// 			elementIndex := i.windowStart + j
// 			binary.LittleEndian.PutUint16(i.sizeArchive[elementIndex*2:elementIndex*2+2], i.sizeSlice[j])
// 		}
// 	}

// 	return nil
// }
