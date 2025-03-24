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

	println("총", len(anchorCandidates), "개 앵커 후보 발견")

	// 3. 앵커 후보를 청크별로 분류
	anchorsByChunk := ParseByChunk(anchorCandidates)
	println("앵커 후보", len(anchorCandidates), "개를", len(anchorsByChunk), "개 청크로 분류")

	// 4. 각 청크별로 한 번에 파일 접근하여 앵커 및 루트 정보 수집
	// 앵커 -> 루트 매핑
	anchorToRoot := make(map[uint64]uint64)

	for chunkIndex, anchors := range anchorsByChunk {
		println("청크", chunkIndex, "처리 중 (", len(anchors), "개 앵커)")

		// 청크 경계 계산

		// 각 앵커 확인
		for _, anchor := range anchors {
			// 상대적 인덱스 계산
			// 실제 존재하는 요소인지 확인
			if ic.parentSlice[anchor] != 0 {
				// 루트 찾기
				root := ic.findRoot(anchor)
				anchorToRoot[anchor] = root
			}
		}
	}

	println("모든 앵커 확인 완료:", len(anchorToRoot), "개 앵커 검증됨")

	// 6. 각 disjointList 처리
	// 처리된 disjointList 추적
	processedLists := make([]bool, len(disjointLists))
	singleAnchorCount := 0
	multiAnchorCount := 0
	noAnchorCount := 0

	// 6.1 다중 앵커 그룹 먼저 처리 (병합 로직)
	println("다중 앵커 집합 처리 시작...")
	// 멀티 앵커 집합을 처리하기 위한 구조체

	// 각 disjointList에 대해 MultiAnchorSet 준비
	totalListSets := make([]*SetWithRoots, len(disjointLists))

	for listIdx, elements := range disjointLists {
		// 해당 리스트의 앵커-루트 맵핑 수집
		listAnchorMap := make(map[uint64]uint64)
		listRoots := make([]uint64, 0)
		rootsMap := make(map[uint64]bool) // 중복 방지용

		// 이 리스트에 속한 앵커들과 해당 루트 찾기
		//TODO: maybeAnchorToListIndex를 사용해서 최적화 가능
		//TODO AI 멍청한
		for _, element := range elements {
			if root, exists := anchorToRoot[element]; exists {
				listAnchorMap[element] = root
				if !rootsMap[root] {
					rootsMap[root] = true
					listRoots = append(listRoots, root)
				}
			}
		}

		// MultiAnchorSet 생성

		totalListSets[listIdx] = &SetWithRoots{
			roots:        listRoots,
			elements:     elements,
			anchorToRoot: listAnchorMap,
		}
	}

	// 멀티루 트 리스트 식별 (루트가 2개 이상인 리스트)
	multiRootListIndices := make([]int, 0)
	for listIdx, set := range totalListSets {
		if set != nil && len(set.roots) > 1 {
			multiRootListIndices = append(multiRootListIndices, listIdx)
		}
	}

	// 루트를 기준으로 유니언 파인드 수행
	rootDSU := NewDisjointSet()
	// 모든 루트를 DSU에 추가
	for _, listIdx := range multiRootListIndices {
		for _, root := range totalListSets[listIdx].roots {
			rootDSU.MakeSet(root)
		}
	}
	// 같은 disjointList에 속한 루트들끼리 유니언
	for _, listIdx := range multiRootListIndices {
		set := totalListSets[listIdx]
		if len(set.roots) > 1 {
			for i := 1; i < len(set.roots); i++ {
				rootDSU.Union(set.roots[0], set.roots[i])
			}
		}
	}

	// 루트 그룹화 - 같은 대표 루트를 가진 disjointList끼리 묶기
	rootToListIndexUnion := make(map[uint64][]int)
	for _, listIdx := range multiRootListIndices {
		representativeRoot := rootDSU.Find(totalListSets[listIdx].roots[0])
		rootToListIndexUnion[representativeRoot] = append(rootToListIndexUnion[representativeRoot], listIdx)

	}
	// 각 대표 루트별로 MultiAnchorSet 병합 및 MergeMultiSet 호출
	multiAnchorCount = 0
	for _, listIndices := range rootToListIndexUnion {
		if len(listIndices) <= 0 {
			continue
		}

		multiAnchorCount++

		// 병합된 멀티앵커 집합 구성
		mergedElements := make([]uint64, 0)
		mergedAnchorMap := make(map[uint64]uint64)

		// 해당 루트에 속한 모든 disjointList 처리
		for _, listIdx := range listIndices {
			// 요소 추가
			mergedElements = append(mergedElements, totalListSets[listIdx].elements...)

			// 앵커 맵 추가
			for anchor, root := range totalListSets[listIdx].anchorToRoot {
				mergedAnchorMap[anchor] = root
			}

			// 처리 완료 표시
			processedLists[listIdx] = true
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

		// 블룸 필터 업데이트
		for _, element := range mergedElements {
			ic.totalBloomFilter.Add(element)
			ic.totalElements++
			ic.multiBloomFilter.Add(element)
		}
	}

	println("다중 앵커 집합 병합 완료:", multiAnchorCount, "개 처리됨")

	// 6.2 단일 앵커 집합 처리
	println("단일 앵커 집합 처리 시작...")
	for listIdx, set := range totalListSets {
		// 이미 처리된 리스트 건너뛰기
		if processedLists[listIdx] {
			continue
		}

		// 루트가 정확히 1개인 경우만 처리
		if len(set.roots) == 1 {
			singleAnchorCount++

			// 단일 앵커 케이스
			elements := set.elements
			processedLists[listIdx] = true

			// UserSet 생성
			newSet := UserSet{
				Size:     uint16(len(elements)),
				Elements: elements,
			}

			// 집합 확장 - 루트 정보 활용
			err := ic.GrowMultiSet(newSet, set.roots[0])
			if err != nil {
				return err
			}

			// 블룸 필터 업데이트
			for _, element := range elements {
				ic.totalBloomFilter.Add(element)
				ic.totalElements++
				ic.multiBloomFilter.Add(element)
			}
		}
	}

	// 6.3 앵커 없는 집합 처리
	// 앵커 없는 집합 처리 - 청크 단위 배칭 및 병렬 처리
	println("앵커 없는 집합 처리 시작...")

	// 청크별 업데이트 정보 맵
	chunkUpdates, noAnchorCount := ProcessNoAnchorSetsInParallel(ic, totalListSets, processedLists)

	// 청크 인덱스 정렬 (순차적 접근 최적화)
	chunkIndices := make([]int, 0, len(chunkUpdates))
	for chunkIdx := range chunkUpdates {
		chunkIndices = append(chunkIndices, chunkIdx)
	}
	sort.Ints(chunkIndices)

	println("앵커 없는 집합:", noAnchorCount, "개,", len(chunkIndices), "개 청크에 대한 업데이트 필요")

	// 병렬로 청크 업데이트 적용
	maxGoroutines := runtime.NumCPU() * 2
	if maxGoroutines > 32 {
		maxGoroutines = 32
	}
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
	return nil

}

// 앵커 없는 집합을 병렬로 처리하는 함수
func ProcessNoAnchorSetsInParallel(ic *IncrementalUnionFindProcessor,
	totalListSets []*SetWithRoots, processedLists []bool) (map[int]*ChunkUpdate, int) {

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
		listIdx int
		set     *SetWithRoots
	}

	// 앵커 없는 집합을 먼저 식별
	workItems := make([]workItem, 0)
	for listIdx, set := range totalListSets {
		// 이미 처리된 리스트 건너뛰기
		if processedLists[listIdx] {
			continue
		}

		// 루트가 없는 경우 (앵커가 없는 경우) 작업 아이템에 추가
		if len(set.roots) == 0 {
			processedLists[listIdx] = true

			if len(set.elements) > 0 {
				workItems = append(workItems, workItem{
					listIdx: listIdx,
					set:     set,
				})
			}
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
					set := item.set
					elements := set.elements

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

					// 블룸 필터 업데이트 (뮤텍스로 보호)
					mutex.Lock()
					for _, element := range elements {
						ic.totalBloomFilter.Add(element)
						ic.totalElements++
						ic.multiBloomFilter.Add(element)
					}
					mutex.Unlock()
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
