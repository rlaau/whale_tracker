package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/edsrzf/mmap-go"
)

const (
	ChunkSize = 50_000_000 // 5000만개씩 처리
)

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
		if userSet.size >= 2 {
			multiSets = append(multiSets, userSet)
		} else if userSet.size == 1 {
			// 4. 만약 UserSets의의 userSet의 size가 1이면 그 요소를 batchBuffer에 추가한다.
			batchBuffer = append(batchBuffer, userSet.elements[0])
		}
	}

	// 5. 순회를 끝낸다.

	// 6. batchBuffer를 순회하며 처리한다.
	if len(batchBuffer) > 0 {
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

			for chunkIndex, elements := range parsedByChunk {
				// 청크 경계 계산
				start := uint64(chunkIndex) * ChunkSize
				end := start + ChunkSize

				// 청크 데이터 로드
				if i.windowStart != start || i.windowEnd != end {
					err := i.loadWindow(start, end)
					if err != nil {
						return err, nil
					}
				}

				// 각 요소 확인
				for _, element := range elements {
					// 상대적 인덱스 계산
					relativeIndex := element - start

					// parentSlice에 값이 없으면(0이면) 실제로 없는 요소
					if i.parentSlice[relativeIndex] == 0 {
						needToBatch = append(needToBatch, element)
					}
				}
			}
		}

		// 6-4: 새 요소 배치 추가
		if len(needToBatch) > 0 {
			err := i.BatchSingleSet(needToBatch)
			if err != nil {
				return err, nil
			}
		}
	}

	// 7. multiSets를 리턴한다.
	return nil, multiSets
}

func (i *IncrementalUnionFindProcessor) BatchSingleSet(batchBuffer []uint64) error {
	// 해당 함수는 단일 크기 집합을 청크 단위로 IncrementalUnionFindProcessor의 아카이브에 업데이트한다.

	// 1. batchBuffer를 순회하면서 각 값을 totalBloomFilter에 추가한다.
	for _, element := range batchBuffer {
		i.totalBloomFilter.Add(element)
	}

	// 2: parsedByChunk 딕셔너리를 생성 후 ParseByChunk(batchBuffer)를 대입한다.
	parsedByChunk := ParseByChunk(batchBuffer)

	// 3: 청크별로 처리
	for chunkIndex, elements := range parsedByChunk {
		// 청크 경계 계산
		start := uint64(chunkIndex) * ChunkSize
		end := start + ChunkSize

		// 청크 데이터 로드
		if i.windowStart != start || i.windowEnd != end {
			err := i.loadWindow(start, end)
			if err != nil {
				return err
			}
		}

		// 각 요소 초기화 (자기 자신이 루트)
		for _, element := range elements {
			// 상대적 인덱스 계산
			relativeIndex := element - start

			// 유니언 파인드 초기화: 부모는 자기 자신, 랭크 0, 크기 1
			i.parentSlice[relativeIndex] = element
			i.rankSlice[relativeIndex] = 0
			i.sizeSlice[relativeIndex] = 1

			// 총 요소 수 업데이트
			if i.windowStart+relativeIndex >= i.totalElements {
				i.totalElements = i.windowStart + relativeIndex + 1
			}
		}

		// 변경사항 저장
		err := i.saveWindow()
		if err != nil {
			return err
		}
	}

	return nil
}

func (i *IncrementalUnionFindProcessor) ProcessMultiSets(userSets []UserSet) error {
	// 해당 함수는 크기가 2이상은 집합들을 IncrementalUnionFindProcessor의 아카이브에 반영한다.
	if len(userSets) == 0 {
		return nil
	}

	// 1. 우선 메모리 내에서, DisjointSet구조체를 이용해서 빠르게 유니언 파인드를 처리
	ds := NewDisjointSet()

	// 모든 요소를 DisjointSet에 추가하고 같은 셋끼리 유니언
	for _, userSet := range userSets {
		if len(userSet.elements) < 2 {
			continue
		}

		root := userSet.elements[0]
		ds.MakeSet(root)

		for _, element := range userSet.elements[1:] {
			ds.MakeSet(element)
			ds.Union(root, element)
		}
	}

	// 1-1. disjointSets 구하기
	disjointSets := ds.GetSets()

	// SetWithAnchor 구조체 정의
	type SetWithAnchor struct {
		anchorToRoot map[uint64]uint64
		set          []uint64
	}

	// 각 분리 집합 처리
	for _, elements := range disjointSets {
		// 2-1. 앵커 후보 찾기 - multiBloomFilter에 있는 요소들
		maybeAnchor := []uint64{}
		for _, element := range elements {
			if i.multiBloomFilter.MayContain(element) {
				maybeAnchor = append(maybeAnchor, element)
			}
		}

		// 2-1. 청크별로 분류
		maybeAnchorByChunk := ParseByChunk(maybeAnchor)

		// 3. 실제 앵커 확인 및 루트 찾기
		anchorToRoot := make(map[uint64]uint64)

		for chunkIndex, anchors := range maybeAnchorByChunk {
			// 청크 경계 계산
			start := uint64(chunkIndex) * ChunkSize
			end := start + ChunkSize

			// 청크 데이터 로드
			if i.windowStart != start || i.windowEnd != end {
				err := i.loadWindow(start, end)
				if err != nil {
					return err
				}
			}

			// 각 앵커 확인
			for _, anchor := range anchors {
				// 상대적 인덱스 계산
				relativeIndex := anchor - start

				// 실제 존재하는 요소인지 확인
				if i.parentSlice[relativeIndex] != 0 {
					// 3-1. 루트 찾기
					root := i.findRoot(anchor)
					anchorToRoot[anchor] = root
				}
			}
		}

		// SetWithAnchor 생성
		swa := SetWithAnchor{
			anchorToRoot: anchorToRoot,
			set:          elements,
		}

		// 4. 앵커 상황에 따른 처리
		if len(swa.anchorToRoot) == 0 {
			// 4-1. 앵커가 없는 경우: 새 집합 추가
			newSet := UserSet{
				size:     uint16(len(swa.set)),
				elements: swa.set,
			}

			// 새로운 다중 집합 일괄 추가
			err := i.BatchAddMultiSet([]UserSet{newSet})
			if err != nil {
				return err
			}
		} else if len(swa.anchorToRoot) == 1 {
			// 4-2. 앵커가 하나인 경우: 기존 집합 확장

			var root uint64

			// 유일한 앵커와 루트 가져오기
			for _, r := range swa.anchorToRoot {
				root = r
				break
			}

			// UserSet 생성
			newSet := UserSet{
				size:     uint16(len(swa.set)),
				elements: swa.set,
			}

			// 집합 확장
			err := i.GrowMultiSet(newSet, root)
			if err != nil {
				return err
			}
		} else {
			// 4-3. 앵커가 여러 개인 경우: 집합 병합
			newSet := UserSet{
				size:     uint16(len(swa.set)),
				elements: swa.set,
			}

			// 여러 집합 병합
			err := i.MergeMultiSet(newSet, swa.anchorToRoot)
			if err != nil {
				return err
			}
		}

		// 4-4. 블룸 필터 업데이트
		for _, element := range elements {
			i.totalBloomFilter.Add(element)
			i.multiBloomFilter.Add(element)
		}
	}

	return nil
}

func (i *IncrementalUnionFindProcessor) BatchAddMultiSet(userSets []UserSet) error {
	// 해당 함수는 []userSet을 순회하면서 첫 원소를 루트로 여기고, 이들 연산을 청크 단위로 업데이트 한다.

	// 1. map[int][2][]uint64를 만든다. - 청크별 [인덱스, 루트값] 쌍 저장
	chunkData := make(map[int][2][]uint64)

	// 2. 모든 요소를 청크별로 분류하고 루트 정보 기록
	for _, userSet := range userSets {
		if len(userSet.elements) == 0 {
			continue
		}

		// 첫 번째 요소를 루트로 사용
		root := userSet.elements[0]

		for _, element := range userSet.elements {
			chunkIndex := int(element / ChunkSize)

			if _, exists := chunkData[chunkIndex]; !exists {
				chunkData[chunkIndex] = [2][]uint64{{}, {}}
			}

			// 인덱스와 루트 정보 저장
			temp := chunkData[chunkIndex]
			temp[0] = append(temp[0], element)
			temp[1] = append(temp[1], root)
			chunkData[chunkIndex] = temp
		}
	}

	// 3. 청크별로 처리
	for chunkIndex, data := range chunkData {
		elements := data[0]
		roots := data[1]

		// 청크 경계 계산
		start := uint64(chunkIndex) * ChunkSize
		end := start + ChunkSize

		// 청크 데이터 로드
		if i.windowStart != start || i.windowEnd != end {
			err := i.loadWindow(start, end)
			if err != nil {
				return err
			}
		}

		// 각 요소 업데이트
		for idx, element := range elements {
			// 상대적 인덱스 계산
			relativeIndex := element - start

			// 부모 설정
			i.parentSlice[relativeIndex] = roots[idx]

			// 루트 요소인 경우 (자기 자신이 부모)
			if element == roots[idx] {
				// 루트는 랭크 0, 크기는 나중에 업데이트
				i.rankSlice[relativeIndex] = 0
			} else {
				// 루트가 아닌 요소는 랭크 0, 크기 0
				i.rankSlice[relativeIndex] = 0
				i.sizeSlice[relativeIndex] = 0
			}

			// 총 요소 수 업데이트
			if i.windowStart+relativeIndex >= i.totalElements {
				i.totalElements = i.windowStart + relativeIndex + 1
			}
		}

		// 변경사항 저장
		err := i.saveWindow()
		if err != nil {
			return err
		}
	}

	// 4. 루트 요소의 크기 업데이트
	for _, userSet := range userSets {
		if len(userSet.elements) == 0 {
			continue
		}

		// 첫 번째 요소를 루트로 사용
		root := userSet.elements[0]
		chunkIndex := int(root / ChunkSize)

		// 청크 경계 계산
		start := uint64(chunkIndex) * ChunkSize
		end := start + ChunkSize

		// 청크 데이터 로드
		if i.windowStart != start || i.windowEnd != end {
			err := i.loadWindow(start, end)
			if err != nil {
				return err
			}
		}

		// 루트 요소의 크기 업데이트
		relativeIndex := root - start
		i.sizeSlice[relativeIndex] = userSet.size

		// 변경사항 저장
		err := i.saveWindow()
		if err != nil {
			return err
		}
	}

	return nil
}

func (i *IncrementalUnionFindProcessor) GrowMultiSet(userSet UserSet, rootElement uint64) error {
	// 해당 함수는 다른 집합과 머지할 필요 없는, 닫힌 집합을 IncrementalUnionFindProcessor의 아카이브에 반영한다.
	if len(userSet.elements) == 0 {
		return nil
	}

	// 1. 지정된 루트 요소 사용

	// 2. parentMap, rankMap 초기화
	parentMap := make(map[uint64]uint64)
	rankMap := make(map[uint64]uint8)
	sizeMap := make(map[uint64]uint16)

	// 모든 요소의 부모를 루트로 설정
	for _, element := range userSet.elements {
		parentMap[element] = rootElement

		// 루트 요소인 경우
		if element == rootElement {
			rankMap[element] = 0
			// 기존 크기 가져오기
			rootChunkIndex := int(rootElement / ChunkSize)
			rootStart := uint64(rootChunkIndex) * ChunkSize

			// 이미 현재 청크가 아니라면 로드
			if i.windowStart != rootStart || i.windowEnd != rootStart+ChunkSize {
				err := i.loadWindow(rootStart, rootStart+ChunkSize)
				if err != nil {
					return err
				}
			}

			relativeIndex := rootElement - rootStart
			existingSize := i.sizeSlice[relativeIndex]

			// 크기 업데이트 (기존 크기 + 새 요소 수)
			sizeMap[element] = existingSize + uint16(len(userSet.elements)) - 1
		} else {
			// 루트가 아닌 요소는 랭크 0, 크기 0
			rankMap[element] = 0
			sizeMap[element] = 0
		}
	}

	// 3. 청크별로 요소 분류하여 처리
	parsedByChunk := ParseByChunk(userSet.elements)

	for chunkIndex, elements := range parsedByChunk {
		// 청크 경계 계산
		start := uint64(chunkIndex) * ChunkSize
		end := start + ChunkSize

		// 청크 데이터 로드
		if i.windowStart != start || i.windowEnd != end {
			err := i.loadWindow(start, end)
			if err != nil {
				return err
			}
		}

		// 각 요소 업데이트
		for _, element := range elements {
			// 상대적 인덱스 계산
			relativeIndex := element - start

			// 부모, 랭크, 크기 설정
			i.parentSlice[relativeIndex] = parentMap[element]
			i.rankSlice[relativeIndex] = rankMap[element]
			i.sizeSlice[relativeIndex] = sizeMap[element]

			// 총 요소 수 업데이트
			if i.windowStart+relativeIndex >= i.totalElements {
				i.totalElements = i.windowStart + relativeIndex + 1
			}
		}

		// 변경사항 저장
		err := i.saveWindow()
		if err != nil {
			return err
		}
	}

	// 블룸 필터 업데이트
	for _, element := range userSet.elements {
		i.totalBloomFilter.Add(element)
		i.multiBloomFilter.Add(element)
	}

	return nil
}

func (i *IncrementalUnionFindProcessor) MergeMultiSet(userSet UserSet, anchorToRoot map[uint64]uint64) error {
	// 여러 앵커가 존재하는 상황에서, 아카이브의 여러 트리를 병합하는 함수이다.
	if len(userSet.elements) == 0 || len(anchorToRoot) == 0 {
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

			// 루트 정보 가져오기
			rootChunkIndex := int(root / ChunkSize)
			rootStart := uint64(rootChunkIndex) * ChunkSize
			rootEnd := rootStart + ChunkSize

			// 루트 청크 로드
			if i.windowStart != rootStart || i.windowEnd != rootEnd {
				err := i.loadWindow(rootStart, rootEnd)
				if err != nil {
					return err
				}
			}

			// 상대적 인덱스 계산
			relativeIndex := root - rootStart

			// 랭크와 크기 저장
			rootRanks[root] = i.rankSlice[relativeIndex]
			rootSizes[root] = i.sizeSlice[relativeIndex]
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
		for _, element := range userSet.elements {
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
		for _, element := range userSet.elements {
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
		for chunkIndex, elements := range elementsByChunk {
			// 청크 경계 계산
			start := uint64(chunkIndex) * ChunkSize
			end := start + ChunkSize

			// 청크 데이터 로드
			if i.windowStart != start || i.windowEnd != end {
				err := i.loadWindow(start, end)
				if err != nil {
					return err
				}
			}

			// 각 요소 업데이트
			for _, element := range elements {
				update := elementsToUpdate[element]

				// 상대적 인덱스 계산
				relativeIndex := element - start

				// 부모, 랭크, 크기 업데이트
				i.parentSlice[relativeIndex] = update.parent
				i.rankSlice[relativeIndex] = update.rank
				i.sizeSlice[relativeIndex] = update.size

				// 총 요소 수 업데이트
				if i.windowStart+relativeIndex >= i.totalElements {
					i.totalElements = i.windowStart + relativeIndex + 1
				}
			}

			// 변경사항 저장
			err := i.saveWindow()
			if err != nil {
				return err
			}
		}
	}

	// 블룸 필터 업데이트
	for _, element := range userSet.elements {
		i.totalBloomFilter.Add(element)
		i.multiBloomFilter.Add(element)
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
		chunkIndex := int(current / ChunkSize)
		start := uint64(chunkIndex) * ChunkSize
		end := start + ChunkSize

		// 청크 데이터 로드
		if i.windowStart != start || i.windowEnd != end {
			err := i.loadWindow(start, end)
			if err != nil {
				return current // 오류 시 현재 요소 반환
			}
		}

		relativeIndex := current - start
		parent := i.parentSlice[relativeIndex]

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
		chunkIndex := int(element / ChunkSize)
		start := uint64(chunkIndex) * ChunkSize
		end := start + ChunkSize

		// 청크 데이터 로드
		if i.windowStart != start || i.windowEnd != end {
			err := i.loadWindow(start, end)
			if err != nil {
				continue // 오류 시 다음 요소로
			}
		}

		relativeIndex := element - start
		i.parentSlice[relativeIndex] = root

		// 변경사항 저장
		i.saveWindow()
	}

	return root
}

// mmap 관련 헬퍼 함수
func (i *IncrementalUnionFindProcessor) loadWindow(start uint64, end uint64) error {
	// 윈도우 범위가 파일 용량을 초과하면 파일 확장
	if end > i.fileCapacity {
		err := i.extendFiles(end)
		if err != nil {
			return err
		}
	}

	windowSize := end - start

	// 슬라이스 초기화
	i.parentSlice = make([]uint64, windowSize)
	i.rankSlice = make([]uint8, windowSize)
	i.sizeSlice = make([]uint16, windowSize)

	// null값(초기화되지 않은 값) 체크 플래그
	foundNullValue := false

	// parent 슬라이스 로드
	for j := uint64(0); j < windowSize && !foundNullValue; j++ {
		elementIndex := start + j

		if elementIndex*8+8 <= uint64(len(i.parentArchive)) {
			value := binary.LittleEndian.Uint64(i.parentArchive[elementIndex*8 : elementIndex*8+8])
			if value == 0 {
				// null값(0)이 나오면 순회 중단
				foundNullValue = true
			}
			i.parentSlice[j] = value
		} else {
			i.parentSlice[j] = 0
		}
	}

	// rank 슬라이스 로드
	for j := uint64(0); j < windowSize && !foundNullValue; j++ {
		elementIndex := start + j

		if elementIndex < uint64(len(i.rankArchive)) {
			i.rankSlice[j] = i.rankArchive[elementIndex]
		} else {
			i.rankSlice[j] = 0
		}
	}

	// size 슬라이스 로드
	for j := uint64(0); j < windowSize && !foundNullValue; j++ {
		elementIndex := start + j

		if elementIndex*2+2 <= uint64(len(i.sizeArchive)) {
			i.sizeSlice[j] = binary.LittleEndian.Uint16(i.sizeArchive[elementIndex*2 : elementIndex*2+2])
		} else {
			i.sizeSlice[j] = 0
		}
	}

	// 윈도우 범위 업데이트
	i.windowStart = start
	i.windowEnd = end

	return nil
}

func (i *IncrementalUnionFindProcessor) saveWindow() error {
	windowSize := i.windowEnd - i.windowStart

	// parent 슬라이스 저장
	for j := uint64(0); j < windowSize; j++ {
		if i.parentSlice[j] != 0 {
			elementIndex := i.windowStart + j
			binary.LittleEndian.PutUint64(i.parentArchive[elementIndex*8:elementIndex*8+8], i.parentSlice[j])

			// 총 원소 수 업데이트
			if elementIndex >= i.totalElements {
				i.totalElements = elementIndex + 1
			}
		}
	}

	// rank 슬라이스 저장
	for j := uint64(0); j < windowSize; j++ {
		if i.parentSlice[j] != 0 {
			elementIndex := i.windowStart + j
			i.rankArchive[elementIndex] = i.rankSlice[j]
		}
	}

	// size 슬라이스 저장
	for j := uint64(0); j < windowSize; j++ {
		if i.parentSlice[j] != 0 {
			elementIndex := i.windowStart + j
			binary.LittleEndian.PutUint16(i.sizeArchive[elementIndex*2:elementIndex*2+2], i.sizeSlice[j])
		}
	}

	return nil
}

// 파일 확장 헬퍼 함수
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
		return err
	}

	err = i.rankFile.Truncate(int64(newCapacity)) // uint8 = 1바이트
	if err != nil {
		return err
	}

	err = i.sizeFile.Truncate(int64(newCapacity * 2)) // uint16 = 2바이트
	if err != nil {
		return err
	}

	// 새로운 mmap 생성
	parentArchive, err := mmap.Map(i.parentFile, mmap.RDWR, 0)
	if err != nil {
		return err
	}
	i.parentArchive = parentArchive

	rankArchive, err := mmap.Map(i.rankFile, mmap.RDWR, 0)
	if err != nil {
		return err
	}
	i.rankArchive = rankArchive

	sizeArchive, err := mmap.Map(i.sizeFile, mmap.RDWR, 0)
	if err != nil {
		return err
	}
	i.sizeArchive = sizeArchive

	// 파일 용량 업데이트
	i.fileCapacity = newCapacity

	return nil
}

// 블룸 필터 메서드 구현
func (bf *BloomFilter) Add(element uint64) {
	for i := uint64(0); i < bf.hashFunctions; i++ {
		// 간단한 해시 함수
		hash := (element + i*0x517cc1b727220a95) % bf.size
		index := hash / 64
		bit := hash % 64
		bf.bf[index] |= (1 << bit)
	}
}

func (bf *BloomFilter) MayContain(element uint64) bool {
	for i := uint64(0); i < bf.hashFunctions; i++ {
		// 간단한 해시 함수
		hash := (element + i*0x517cc1b727220a95) % bf.size
		index := hash / 64
		bit := hash % 64
		if (bf.bf[index] & (1 << bit)) == 0 {
			return false
		}
	}
	return true
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

// 생성자 함수
func NewIncrementalUnionFindProcessor(dataDir string, initialCapacity uint64) (*IncrementalUnionFindProcessor, error) {
	// 디렉토리 생성
	err := os.MkdirAll(dataDir, 0755)
	if err != nil {
		return nil, err
	}

	// 파일 경로
	parentFilePath := filepath.Join(dataDir, "parent.bin")
	rankFilePath := filepath.Join(dataDir, "rank.bin")
	sizeFilePath := filepath.Join(dataDir, "size.bin")

	// 파일 열기
	parentFile, err := os.OpenFile(parentFilePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	rankFile, err := os.OpenFile(rankFilePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	sizeFile, err := os.OpenFile(sizeFilePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	// 파일 크기 확인
	parentInfo, err := parentFile.Stat()
	if err != nil {
		return nil, err
	}

	fileCapacity := uint64(parentInfo.Size()) / 8 // uint64 = 8바이트

	// 필요시 파일 크기 확장
	if fileCapacity < initialCapacity {
		err = parentFile.Truncate(int64(initialCapacity * 8))
		if err != nil {
			return nil, err
		}

		err = rankFile.Truncate(int64(initialCapacity))
		if err != nil {
			return nil, err
		}

		err = sizeFile.Truncate(int64(initialCapacity * 2))
		if err != nil {
			return nil, err
		}

		fileCapacity = initialCapacity
	}

	// mmap 생성
	parentArchive, err := mmap.Map(parentFile, mmap.RDWR, 0)
	if err != nil {
		return nil, err
	}

	rankArchive, err := mmap.Map(rankFile, mmap.RDWR, 0)
	if err != nil {
		return nil, err
	}

	sizeArchive, err := mmap.Map(sizeFile, mmap.RDWR, 0)
	if err != nil {
		return nil, err
	}

	// 블룸 필터 생성 (오탐률 0.5% 정도로 설정)
	totalBloomFilterSize := uint64(4_000_000_000 * 10 / 8) // 약 5GB
	multiBloomFilterSize := totalBloomFilterSize / 10      // 약 500MB

	totalBloomFilter := &BloomFilter{
		size:          totalBloomFilterSize * 8, // 비트 단위 크기
		bf:            make([]uint64, totalBloomFilterSize),
		hashFunctions: 8,
	}

	multiBloomFilter := &BloomFilter{
		size:          multiBloomFilterSize * 8, // 비트 단위 크기
		bf:            make([]uint64, multiBloomFilterSize),
		hashFunctions: 8,
	}

	return &IncrementalUnionFindProcessor{
		dataDir:          dataDir,
		parentFile:       parentFile,
		rankFile:         rankFile,
		sizeFile:         sizeFile,
		parentArchive:    parentArchive,
		rankArchive:      rankArchive,
		sizeArchive:      sizeArchive,
		parentSlice:      nil,
		rankSlice:        nil,
		sizeSlice:        nil,
		windowStart:      0,
		windowEnd:        0,
		totalBloomFilter: totalBloomFilter,
		multiBloomFilter: multiBloomFilter,
		totalElements:    0,
		fileCapacity:     fileCapacity,
	}, nil
}
