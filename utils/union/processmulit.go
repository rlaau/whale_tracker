package main

import (
	"maps"
	"runtime"
	"sort"
	"sync"
	"time"
)

type SetWithRoots struct {
	roots        []uint64          // 관련된 모든 루트 값들
	elements     []uint64          // 집합의 모든 요소들
	anchorToRoot map[uint64]uint64 // 앵커에서 실제 루트로의 매핑
}

// 청크별 업데이트 정보를 저장할 구조체
type ChunkUpdate struct {
	// 각 요소의 상대적 인덱스 -> 업데이트 정보
	parentUpdates map[uint64]uint64 // 부모 값 업데이트
	rankUpdates   map[uint64]uint8  // 랭크 값 업데이트
	sizeUpdates   map[uint64]uint16 // 크기 값 업데이트
}

func (ic *IncrementalUnionFindProcessor) ProcessMultiSets(userSets []UserSet) error {
	// 해당 함수는 크기가 2이상은 집합들을 IncrementalUnionFindProcessor의 아카이브에 반영한다.
	if len(userSets) == 0 {
		return nil
	}

	println("다중 집합 처리 시작...")
	startTime := time.Now()

	// 1. 우선 메모리 내에서, DisjointSet구조체를 이용해서 빠르게 유니언 파인드를 처리
	ds := NewDisjointSet()

	// 모든 요소를 DisjointSet에 추가하고 같은 셋끼리 유니언
	for _, userSet := range userSets {
		if len(userSet.Elements) < 2 {
			//어차피 여긴 멀티 셋 처리하는 곳임
			continue
		}

		root := userSet.Elements[0]
		ds.MakeSet(root)

		for _, element := range userSet.Elements[1:] {
			ds.MakeSet(element)
			ds.Union(root, element)
		}
	}

	println("자체 disjointSet 연산 완료:", time.Since(startTime))

	// map[uint64][]uint64를 [][]uint64로 변환 (루트 정보 제거)
	disjointSetsMap := ds.GetSets()
	println("자체 disjointSet 획득 완료")
	println(len(disjointSetsMap), "개의 disjointSet 획득")

	// 1. 배열의 배열로 변환
	disjointLists := make([][]uint64, 0, len(disjointSetsMap))
	for _, elements := range disjointSetsMap {
		disjointLists = append(disjointLists, elements)
	}

	// 2. 모든 집합에서 앵커 후보 수집
	println("앵커 후보 수집 시작...")
	anchorCandidates := make([]uint64, 0)
	// 어떤 요소가 어떤 disjointList에 속하는지 역매핑
	maybeAnchorToListIndex := make(map[uint64]int)

	for listIdx, list := range disjointLists {
		for _, element := range list {
			if ic.multiBloomFilter.MayContain(element) {
				anchorCandidates = append(anchorCandidates, element)
				maybeAnchorToListIndex[element] = listIdx
			}
		}
	}

	anchorCandidates, maybeAnchorToListIndex, anchorToRoot := ic.collectAndVerifyAnchors(disjointLists)

	multiAnchorSets, singleAnchorSets, nonAnchorSets := classifyDisjointSets(disjointLists, anchorToRoot, maybeAnchorToListIndex)

	// 6.1 다중 앵커 그룹 먼저 처리 (병합 로직)
	println("다중 앵커 집합 처리 시작...")
	// 멀티 앵커 집합을 처리하기 위한 구조체

	// 루트를 기준으로 유니언 파인드 수행
	rootDSU := NewDisjointSet()
	// 모든 루트를 DSU에 추가
	for _, multiAnchorSet := range multiAnchorSets {
		for _, root := range multiAnchorSet.Roots {
			rootDSU.MakeSet(root)
		}
	}
	// 같은 disjointList에 속한 루트들끼리 유니언
	for _, multiAnchorSet := range multiAnchorSets {
		for i := 1; i < len(multiAnchorSet.Roots); i++ {
			rootDSU.Union(multiAnchorSet.Roots[0], multiAnchorSet.Roots[i])
		}
	}

	// 루트 그룹화 - 같은 대표 루트를 가진 disjointList끼리 묶기
	rootToMultiAnchorSetUnion := make(map[uint64][]*MultiAnchorSet)
	for _, multiAnchorSet := range multiAnchorSets {
		representativeRoot := rootDSU.Find(multiAnchorSet.Roots[0])
		rootToMultiAnchorSetUnion[representativeRoot] = append(rootToMultiAnchorSetUnion[representativeRoot], &multiAnchorSet)
	}
	// 각 대표 루트별로 MultiAnchorSet 병합 및 MergeMultiSet 호출

	multiAnchorBatchElements := make([]uint64, 0)
	for _, mulAncSetList := range rootToMultiAnchorSetUnion {
		// 병합된 멀티앵커 집합 구성
		mergedElements := make([]uint64, 0)
		mergedAnchorMap := make(map[uint64]uint64)

		// 해당 루트에 속한 모든 disjointList 처리
		for _, mulAncSet := range mulAncSetList {
			// 요소 추가
			mergedElements = append(mergedElements, mulAncSet.Elements...)

			// 앵커 맵 추가
			for anchor, root := range mulAncSet.AnchorToRoot {
				mergedAnchorMap[anchor] = root
			}
		}

		// UserSet 생성
		newSet := UserSet{
			Size:     uint16(len(mergedElements)),
			Elements: mergedElements,
		}

		// 여러 집합 병합
		err := ic.MergeMultiSet(newSet, mergedAnchorMap)
		if err != nil {
			return err
		}
		multiAnchorBatchElements = append(multiAnchorBatchElements, mergedElements...)
	}

	println("다중 앵커 집합 병합 완료:", len(multiAnchorSets), "개 처리됨")

	// 6.2 단일 앵커 집합 처리
	println("단일 앵커 집합 처리 시작...")
	singleAnchorBatchElements := make([]uint64, 0)
	for _, set := range singleAnchorSets {

		// UserSet 생성
		newSet := UserSet{
			Size:     uint16(len(set.Elements)),
			Elements: set.Elements,
		}

		// 집합 확장 - 루트 정보 활용
		err := ic.GrowMultiSet(newSet, set.Root)
		if err != nil {
			return err
		}

		singleAnchorBatchElements = append(singleAnchorBatchElements, set.Elements...)
	}

	// 6.3 앵커 없는 집합 처리
	// 앵커 없는 집합 처리 - 청크 단위 배칭 및 병렬 처리
	println("앵커 없는 집합 처리 시작...")

	// 청크별 업데이트 정보 맵

	chunkUpdates, noAnchorCount := ic.ProcessNoAnchorSetsInParallel(nonAnchorSets)
	noAnchorBatchElements := make([]uint64, 0)
	for _, noAnchorSet := range nonAnchorSets {
		noAnchorBatchElements = append(noAnchorBatchElements, noAnchorSet.Elements...)
	}

	// 청크 인덱스 정렬 (순차적 접근 최적화)
	chunkIndices := make([]int, 0, len(chunkUpdates))
	for chunkIdx := range chunkUpdates {
		chunkIndices = append(chunkIndices, chunkIdx)
	}
	sort.Ints(chunkIndices)

	println("앵커 없는 집합:", noAnchorCount, "개,", len(chunkIndices), "개 청크에 대한 업데이트 필요")

	// 병렬로 청크 업데이트 적용
	maxGoroutines := min(runtime.NumCPU()*2, 32)
	// 작업 분배 - 인접 청크는 같은 고루틴에 할당
	chunkBatches := make([][]int, maxGoroutines)
	for i, chunkIdx := range chunkIndices {
		batchIdx := i % maxGoroutines
		chunkBatches[batchIdx] = append(chunkBatches[batchIdx], chunkIdx)
	}

	// 병렬 처리
	var wg sync.WaitGroup

	for i := 0; i < maxGoroutines; i++ {
		if len(chunkBatches[i]) == 0 {
			continue
		}

		wg.Add(1)
		go func(workerID int, chunks []int) {
			defer wg.Done()

			// 각 청크 순차 처리
			for _, chunkIdx := range chunks {
				update := chunkUpdates[chunkIdx]

				// 청크 데이터 로드

				// 변경 여부 추적
				modified := false

				// 업데이트 적용
				for relIdx, parent := range update.parentUpdates {
					absIdx := uint64(chunkIdx)*ChunkSize + uint64(relIdx)
					ic.parentSlice[absIdx] = parent
					ic.rankSlice[absIdx] = update.rankUpdates[relIdx]
					ic.sizeSlice[absIdx] = update.sizeUpdates[relIdx]

					modified = true

				}

				// 변경된 경우만 저장
				if !modified {
					println("Worker", workerID, "청크", chunkIdx, "변경 없음")
				}
			}
		}(i, chunkBatches[i])
	}

	// 모든 고루틴 완료 대기
	wg.Wait()

	//블룸필터 및 개수를 최종 업데이트
	totalBatchElements := append(append(multiAnchorBatchElements, singleAnchorBatchElements...), noAnchorBatchElements...)
	ic.totalBloomFilter.BulkBatchAdd(totalBatchElements)
	ic.multiBloomFilter.BulkBatchAdd(totalBatchElements)
	ic.totalElements += uint64(len(totalBatchElements))
	return nil

}

// 각 세트 유형별 구조체 정의
type MultiAnchorSet struct {
	Elements     []uint64
	AnchorToRoot map[uint64]uint64
	Roots        []uint64
	ListIndex    int
}

type SingleAnchorSet struct {
	Elements  []uint64
	Anchor    uint64
	Root      uint64
	ListIndex int
}

type NonAnchorSet struct {
	Elements  []uint64
	ListIndex int
}

// 앵커 특성에 따라 disjointLists를 세 가지 유형으로 분류하는 함수
func classifyDisjointSets(
	disjointLists [][]uint64,
	anchorToRoot map[uint64]uint64,
	maybeAnchorToListIndex map[uint64]int,
) ([]MultiAnchorSet, []SingleAnchorSet, []NonAnchorSet) {
	startTime := time.Now()
	println("앵커 특성에 따른 집합 분류 시작...")

	// 결과 슬라이스 초기화
	multiAnchorSets := make([]MultiAnchorSet, 0)
	singleAnchorSets := make([]SingleAnchorSet, 0)
	nonAnchorSets := make([]NonAnchorSet, 0)

	// 각 리스트별 앵커-루트 관계 수집
	listAnchorMap := make(map[int]map[uint64]uint64) // listIdx -> {anchor -> root}
	listRootsMap := make(map[int]map[uint64]bool)    // listIdx -> {roots}

	// 각 앵커에 대해 리스트별로 정보 수집
	for anchor, root := range anchorToRoot {
		listIdx, exists := maybeAnchorToListIndex[anchor]
		if !exists {
			continue
		}

		// 이 리스트의 앵커-루트 맵 초기화
		if _, exists := listAnchorMap[listIdx]; !exists {
			listAnchorMap[listIdx] = make(map[uint64]uint64)
			listRootsMap[listIdx] = make(map[uint64]bool)
		}

		// 앵커-루트 관계 저장
		listAnchorMap[listIdx][anchor] = root
		listRootsMap[listIdx][root] = true
	}

	// 각 리스트를 앵커 특성에 따라 분류
	for listIdx, elements := range disjointLists {

		// 앵커-루트 관계 획득
		anchorMap, hasAnchors := listAnchorMap[listIdx]

		if !hasAnchors || len(anchorMap) == 0 {
			// 앵커가 없는 경우
			nonAnchorSets = append(nonAnchorSets, NonAnchorSet{
				Elements:  elements,
				ListIndex: listIdx,
			})
		} else {
			// 루트 목록 구성
			rootsMap := listRootsMap[listIdx]
			roots := make([]uint64, 0, len(rootsMap))
			for root := range rootsMap {
				roots = append(roots, root)
			}

			if len(roots) == 1 {
				// 단일 앵커 케이스 (정확히 하나의 루트)
				// 앵커 중 하나를 대표로 선택 (첫 번째 앵커)
				var anchor uint64
				for a := range anchorMap {
					anchor = a
					break
				}

				singleAnchorSets = append(singleAnchorSets, SingleAnchorSet{
					Elements:  elements,
					Anchor:    anchor,
					Root:      roots[0],
					ListIndex: listIdx,
				})
			} else if len(roots) > 1 {
				// 다중 앵커 케이스 (여러 루트)
				multiAnchorSets = append(multiAnchorSets, MultiAnchorSet{
					Elements:     elements,
					AnchorToRoot: anchorMap,
					Roots:        roots,
					ListIndex:    listIdx,
				})
			}
		}
	}

	// 통계 정보 출력
	println("앵커 특성에 따른 집합 분류 완료 (", time.Since(startTime), "):")
	println("- 다중 앵커 집합:", len(multiAnchorSets), "개")
	println("- 단일 앵커 집합:", len(singleAnchorSets), "개")
	println("- 앵커 없는 집합:", len(nonAnchorSets), "개")

	return multiAnchorSets, singleAnchorSets, nonAnchorSets
}

// 앵커 후보 수집 및 검증을 병렬로 처리하는 함수
func (ic *IncrementalUnionFindProcessor) collectAndVerifyAnchors(disjointLists [][]uint64) ([]uint64, map[uint64]int, map[uint64]uint64) {
	startTime := time.Now()

	// 2. 모든 집합에서 앵커 후보 수집 (병렬 처리)
	println("앵커 후보 수집 시작...")

	// CPU 코어 수 기반으로 병렬 처리 설정
	numCPU := runtime.NumCPU() // 16코어 환경
	numWorkers := numCPU

	// 매우 큰 입력의 경우 워커 수 증가
	if len(disjointLists) > numCPU*10 {
		numWorkers = numCPU * 2
	}

	// 최대 워커 수 제한
	if numWorkers > 32 {
		numWorkers = 32
	}

	// 작업 분할
	var wg sync.WaitGroup
	chunkSize := (len(disjointLists) + numWorkers - 1) / numWorkers

	// 각 워커의 로컬 결과를 저장할 슬라이스
	type localResult struct {
		candidates []uint64
		listMap    map[uint64]int
	}

	localResults := make([]localResult, numWorkers)

	// 병렬로 앵커 후보 수집
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			// 작업 범위 계산
			start := workerID * chunkSize
			end := min(start+chunkSize, len(disjointLists))

			// 로컬 결과 초기화
			localCandidates := make([]uint64, 0)
			localMap := make(map[uint64]int)

			// 할당된 disjointLists 처리
			for listIdx := start; listIdx < end; listIdx++ {
				list := disjointLists[listIdx]

				for _, element := range list {
					if ic.multiBloomFilter.MayContain(element) {
						localCandidates = append(localCandidates, element)
						localMap[element] = listIdx
					}
				}
			}

			// 로컬 결과 저장
			localResults[workerID] = localResult{
				candidates: localCandidates,
				listMap:    localMap,
			}
		}(w)
	}

	// 모든 워커 완료 대기
	wg.Wait()

	// 로컬 결과 병합
	totalCandidatesSize := 0
	for _, result := range localResults {
		totalCandidatesSize += len(result.candidates)
	}

	// 최종 결과 슬라이스 및 맵 초기화
	anchorCandidates := make([]uint64, 0, totalCandidatesSize)
	maybeAnchorToListIndex := make(map[uint64]int, totalCandidatesSize)

	for _, result := range localResults {
		anchorCandidates = append(anchorCandidates, result.candidates...)

		maps.Copy(maybeAnchorToListIndex, result.listMap)
	}

	println("총", len(anchorCandidates), "개 앵커 후보 발견 (소요 시간:", time.Since(startTime), ")")

	// 3. 앵커 후보를 청크별로 분류
	anchorsByChunk := ParseByChunk(anchorCandidates)
	println("앵커 후보", len(anchorCandidates), "개를", len(anchorsByChunk), "개 청크로 분류")

	// 4. 각 청크별로 병렬 처리하여 앵커 및 루트 정보 수집
	println("앵커 병렬 검증 시작...")
	verifyStartTime := time.Now()

	// 청크 키 목록 생성
	chunkIndices := make([]int, 0, len(anchorsByChunk))
	for k := range anchorsByChunk {
		chunkIndices = append(chunkIndices, k)
	}

	// 각 워커의 결과를 저장할 슬라이스
	anchorResults := make([]map[uint64]uint64, numWorkers)
	for i := range anchorResults {
		anchorResults[i] = make(map[uint64]uint64)
	}

	// 청크 검증 병렬 처리 준비
	var verifyWg sync.WaitGroup
	chunkJobs := make(chan int, min(len(chunkIndices), 1000))

	// 워커 생성
	for w := 0; w < numWorkers; w++ {
		verifyWg.Add(1)
		go func(workerID int) {
			defer verifyWg.Done()

			// 이 워커의 로컬 맵
			localMap := anchorResults[workerID]

			// 작업 채널에서 청크 인덱스를 가져와 처리
			for chunkIndex := range chunkJobs {
				anchors := anchorsByChunk[chunkIndex]

				// 각 앵커 확인
				for _, anchor := range anchors {
					// 실제 존재하는 요소인지 확인
					if ic.accessParent(anchor) != 0 {
						// 루트 찾기
						root := ic.findRoot(anchor)
						localMap[anchor] = root
					}
				}
			}
		}(w)
	}

	// 작업 분배 고루틴
	go func() {
		for _, chunkIndex := range chunkIndices {
			chunkJobs <- chunkIndex
		}
		close(chunkJobs)
	}()

	// 모든 워커 완료 대기
	verifyWg.Wait()

	// 검증 결과 병합
	totalVerifiedSize := 0
	for _, result := range anchorResults {
		totalVerifiedSize += len(result)
	}

	anchorToRoot := make(map[uint64]uint64, totalVerifiedSize)

	for _, result := range anchorResults {
		maps.Copy(anchorToRoot, result)
	}

	println("모든 앵커 확인 완료:", len(anchorToRoot), "개 앵커 검증됨 (검증 시간:", time.Since(verifyStartTime), ", 총 소요 시간:", time.Since(startTime), ")")

	return anchorCandidates, maybeAnchorToListIndex, anchorToRoot
}

// 앵커 없는 집합을 병렬로 처리하는 함수
func (ic *IncrementalUnionFindProcessor) ProcessNoAnchorSetsInParallel(
	nonAnchorSets []NonAnchorSet) (map[int]*ChunkUpdate, int) {

	// 워커 수 계산 (CPU 코어 수 기반)
	numWorkers := runtime.NumCPU() * 2
	// 너무 많은 고루틴은 오히려 성능 저하 가능성 있음
	if numWorkers > 32 {
		numWorkers = 32
	}

	// 결과를 저장할 변수
	var mutex sync.Mutex
	chunkUpdates := make(map[int]*ChunkUpdate)
	noAnchorCount := 0

	// 작업을 분할하기 위한 준비
	type workItem struct {
		listIdx  int
		elements []uint64
	}

	// 앵커 없는 집합을 먼저 식별
	workItems := make([]workItem, 0)
	for _, set := range nonAnchorSets {

		if len(set.Elements) > 0 {
			workItems = append(workItems, workItem{
				listIdx:  set.ListIndex,
				elements: set.Elements,
			})
		}

	}

	// 작업이 없으면 빈 결과 반환
	if len(workItems) == 0 {
		return chunkUpdates, 0
	}

	// 처리할 작업이 있으면 병렬 처리 진행
	noAnchorCount = len(workItems)

	// 작업 분배 방식: 워커 수에 따라 균등 분할
	workerBatches := make([][]workItem, numWorkers)
	for i, item := range workItems {
		workerIndex := i % numWorkers
		workerBatches[workerIndex] = append(workerBatches[workerIndex], item)
	}

	// 동기화를 위한 WaitGroup
	var wg sync.WaitGroup

	// 각 워커 시작
	for workerID := 0; workerID < numWorkers; workerID++ {
		// 이 워커가 처리할 작업이 있는 경우에만 고루틴 생성
		if len(workerBatches[workerID]) > 0 {
			wg.Add(1)

			go func(workerID int, items []workItem) {
				defer wg.Done()

				// 로컬 맵 생성 (뮤텍스 경합 최소화)
				localChunkUpdates := make(map[int]*ChunkUpdate)

				// 작업 처리
				for _, item := range items {

					elements := item.elements

					// 집합의 첫 요소를 루트로 지정
					rootElement := elements[0]
					rootChunkIdx := int(rootElement / ChunkSize)
					rootRelIdx := rootElement % ChunkSize

					// 루트 요소가 속한 청크의 업데이트 정보 초기화
					if _, exists := localChunkUpdates[rootChunkIdx]; !exists {
						localChunkUpdates[rootChunkIdx] = &ChunkUpdate{
							parentUpdates: make(map[uint64]uint64),
							rankUpdates:   make(map[uint64]uint8),
							sizeUpdates:   make(map[uint64]uint16),
						}
					}

					// 루트 요소 업데이트 정보 (자기 자신이 부모, 랭크 0, 크기는 전체 집합 크기)
					localChunkUpdates[rootChunkIdx].parentUpdates[rootRelIdx] = rootElement
					localChunkUpdates[rootChunkIdx].rankUpdates[rootRelIdx] = 0
					localChunkUpdates[rootChunkIdx].sizeUpdates[rootRelIdx] = uint16(len(elements))

					// 나머지 요소들에 대한 업데이트 정보
					for _, element := range elements[1:] {
						elemChunkIdx := int(element / ChunkSize)
						elemRelIdx := element % ChunkSize

						// 요소가 속한 청크의 업데이트 정보 초기화
						if _, exists := localChunkUpdates[elemChunkIdx]; !exists {
							localChunkUpdates[elemChunkIdx] = &ChunkUpdate{
								parentUpdates: make(map[uint64]uint64),
								rankUpdates:   make(map[uint64]uint8),
								sizeUpdates:   make(map[uint64]uint16),
							}
						}

						// 요소 업데이트 정보 (부모는 루트, 랭크 0, 크기 1)
						localChunkUpdates[elemChunkIdx].parentUpdates[elemRelIdx] = rootElement
						localChunkUpdates[elemChunkIdx].rankUpdates[elemRelIdx] = 0
						localChunkUpdates[elemChunkIdx].sizeUpdates[elemRelIdx] = 1
					}

				}

				// 로컬 결과를 전역 결과에 병합
				mutex.Lock()
				for chunkIdx, localUpdate := range localChunkUpdates {
					// 해당 청크 업데이트가 없으면 생성
					if _, exists := chunkUpdates[chunkIdx]; !exists {
						chunkUpdates[chunkIdx] = &ChunkUpdate{
							parentUpdates: make(map[uint64]uint64),
							rankUpdates:   make(map[uint64]uint8),
							sizeUpdates:   make(map[uint64]uint16),
						}
					}

					// 각 맵 병합
					maps.Copy(chunkUpdates[chunkIdx].parentUpdates, localUpdate.parentUpdates)
					maps.Copy(chunkUpdates[chunkIdx].rankUpdates, localUpdate.rankUpdates)
					maps.Copy(chunkUpdates[chunkIdx].sizeUpdates, localUpdate.sizeUpdates)
				}
				mutex.Unlock()

			}(workerID, workerBatches[workerID])
		}
	}

	// 모든 워커가 작업을 완료할 때까지 대기
	wg.Wait()

	return chunkUpdates, noAnchorCount
}

// // 앵커 없는 집합들을 순회하면서 청크별 업데이트 정보 구성
// for listIdx, set := range totalListSets {
// 	// 이미 처리된 리스트 건너뛰기
// 	if processedLists[listIdx] {
// 		continue
// 	}

// 	// 루트가 없는 경우 (앵커가 없는 경우)
// 	if len(set.roots) == 0 {
// 		noAnchorCount++
// 		processedLists[listIdx] = true

// 		elements := set.elements
// 		if len(elements) == 0 {
// 			continue
// 		}

// 		// 집합의 첫 요소를 루트로 지정
// 		rootElement := elements[0]
// 		rootChunkIdx := int(rootElement / ChunkSize)
// 		rootRelIdx := rootElement % ChunkSize

// 		// 루트 요소가 속한 청크의 업데이트 정보 초기화
// 		if _, exists := chunkUpdates[rootChunkIdx]; !exists {
// 			chunkUpdates[rootChunkIdx] = &ChunkUpdate{
// 				parentUpdates: make(map[uint64]uint64),
// 				rankUpdates:   make(map[uint64]uint8),
// 				sizeUpdates:   make(map[uint64]uint16),
// 			}
// 		}

// 		// 루트 요소 업데이트 정보 (자기 자신이 부모, 랭크 0, 크기는 전체 집합 크기)
// 		chunkUpdates[rootChunkIdx].parentUpdates[rootRelIdx] = rootElement
// 		chunkUpdates[rootChunkIdx].rankUpdates[rootRelIdx] = 0
// 		chunkUpdates[rootChunkIdx].sizeUpdates[rootRelIdx] = uint16(len(elements))

// 		// 나머지 요소들에 대한 업데이트 정보
// 		for _, element := range elements[1:] {
// 			elemChunkIdx := int(element / ChunkSize)
// 			elemRelIdx := element % ChunkSize

// 			// 요소가 속한 청크의 업데이트 정보 초기화
// 			if _, exists := chunkUpdates[elemChunkIdx]; !exists {
// 				chunkUpdates[elemChunkIdx] = &ChunkUpdate{
// 					parentUpdates: make(map[uint64]uint64),
// 					rankUpdates:   make(map[uint64]uint8),
// 					sizeUpdates:   make(map[uint64]uint16),
// 				}
// 			}

// 			// 요소 업데이트 정보 (부모는 루트, 랭크 0, 크기 1)
// 			chunkUpdates[elemChunkIdx].parentUpdates[elemRelIdx] = rootElement
// 			chunkUpdates[elemChunkIdx].rankUpdates[elemRelIdx] = 0
// 			chunkUpdates[elemChunkIdx].sizeUpdates[elemRelIdx] = 1
// 		}

// 		// 블룸 필터 업데이트 (메모리 작업이므로 즉시 수행)
// 		for _, element := range elements {
// 			ic.totalBloomFilter.Add(element)
// 			ic.multiBloomFilter.Add(element)
// 		}
// 	}
// }
