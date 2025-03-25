package main

// import (
// 	"fmt"
// 	"os"
// 	"path/filepath"
// 	"runtime"
// 	"sort"
// 	"sync"
// 	"time"
// )

// // 디스조인트 집합 통계 정보 구조체
// type DisjointSetStats struct {
// 	TotalSets     int     // 총 집합 수
// 	TotalElements uint64  // 총 요소 수
// 	AverageSize   float64 // 평균 집합 크기
// 	LargestSet    uint64  // 가장 큰 집합의 크기
// 	SmallestSet   uint64  // 가장 작은 집합의 크기
// }

// // 집합 정보 구조체
// type SetInfo struct {
// 	Root uint64 // 루트 요소
// 	Size uint16 // 집합 크기
// 	Rank uint8  // 루트의 랭크
// }

// // AnalyzeDisjointSets는 모든 디스조인트 집합을 분석하고 통계를 반환합니다
// func (ic *IncrementalUnionFindProcessor) AnalyzeDisjointSets() (*DisjointSetStats, error) {
// 	startTime := time.Now()
// 	fmt.Println("디스조인트 집합 분석 시작...")

// 	// 청크 크기를 기반으로 파일을 분할하여 병렬로 처리
// 	totalChunks := int((ic.fileCapacity + ChunkSize - 1) / ChunkSize)
// 	fmt.Printf("총 %d개 청크 분석 예정 (파일 용량: %d)\n", totalChunks, ic.fileCapacity)

// 	// 첫 번째 청크를 샘플로 검사하여 데이터 구조 확인
// 	err := validateDataStructure(ic)
// 	if err != nil {
// 		fmt.Printf("경고: 데이터 구조 검증 실패: %s\n", err)
// 	}

// 	// 루트 요소와 집합 정보 수집
// 	var mutex sync.Mutex
// 	allSets := make([]SetInfo, 0)

// 	// 병렬 처리를 위한 워커 수 설정
// 	numWorkers := runtime.NumCPU()
// 	if numWorkers > 8 {
// 		numWorkers = 8 // 최대 8개 워커로 제한 (파일 I/O 때문)
// 	}

// 	// 청크 분배
// 	var wg sync.WaitGroup
// 	chunkResults := make(chan []SetInfo, numWorkers)

// 	// 청크를 워커에 분배
// 	chunkSize := (totalChunks + numWorkers - 1) / numWorkers

// 	for w := 0; w < numWorkers; w++ {
// 		wg.Add(1)
// 		startChunk := w * chunkSize
// 		endChunk := (w + 1) * chunkSize
// 		if endChunk > totalChunks {
// 			endChunk = totalChunks
// 		}

// 		go func(workerID, start, end int) {
// 			defer wg.Done()
// 			workerSets := make([]SetInfo, 0)

// 			for chunkIdx := start; chunkIdx < end; chunkIdx++ {
// 				// 청크 경계 계산
// 				startIdx := uint64(chunkIdx) * ChunkSize
// 				endIdx := startIdx + ChunkSize
// 				if endIdx > ic.fileCapacity {
// 					endIdx = ic.fileCapacity
// 				}

// 				// 청크 데이터 로드
// 				err := ic.loadWindow(startIdx, endIdx)
// 				if err != nil {
// 					fmt.Printf("Worker %d: 청크 %d 로드 실패: %s\n", workerID, chunkIdx, err.Error())
// 					continue
// 				}

// 				// 청크의 첫 몇 개 요소 디버깅 출력 (첫 번째 청크만)
// 				if chunkIdx == 0 && workerID == 0 {
// 					printChunkDebugInfo(ic, startIdx, endIdx)
// 				}

// 				// 청크 내 루트 요소 찾기 (parent == self)
// 				for relIdx := uint64(0); relIdx < endIdx-startIdx; relIdx++ {
// 					absIdx := startIdx + relIdx

// 					// 초기화된 요소인지 확인 (parent가 0이 아님)
// 					if ic.parentSlice[relIdx] != 0 {
// 						// 루트 요소는 자기 자신을 가리킴
// 						if ic.parentSlice[relIdx] == absIdx {
// 							// 비정상적인 값 필터링 (크기가 0인데 500000000 같은 특별한 값)
// 							if ic.sizeSlice[relIdx] == 0 && absIdx >= 500000000 {
// 								fmt.Printf("경고: 무시된 특수 루트 요소 - 인덱스: %d, 크기: %d, 랭크: %d\n",
// 									absIdx, ic.sizeSlice[relIdx], ic.rankSlice[relIdx])
// 								continue
// 							}

// 							// 크기가 0이면 실제로 1로 간주 (데이터 불일치 수정)
// 							size := ic.sizeSlice[relIdx]
// 							if size == 0 {
// 								size = 1 // 최소 자기 자신은 포함하므로 1로 설정
// 							}

// 							// 루트 요소와 집합 정보 저장
// 							workerSets = append(workerSets, SetInfo{
// 								Root: absIdx,
// 								Size: size,
// 								Rank: ic.rankSlice[relIdx],
// 							})
// 						}
// 					}
// 				}
// 			}

// 			// 결과 전송
// 			chunkResults <- workerSets

// 		}(w, startChunk, endChunk)
// 	}

// 	// 모든 워커가 완료될 때까지 대기
// 	go func() {
// 		wg.Wait()
// 		close(chunkResults)
// 	}()

// 	// 결과 수집
// 	for results := range chunkResults {
// 		mutex.Lock()
// 		allSets = append(allSets, results...)
// 		mutex.Unlock()
// 	}

// 	// 분석 결과 생성
// 	stats := &DisjointSetStats{
// 		TotalSets: len(allSets),
// 	}

// 	if len(allSets) > 0 {
// 		// 총 요소 수 계산
// 		var totalElements uint64
// 		for _, set := range allSets {
// 			totalElements += uint64(set.Size)
// 		}

// 		stats.TotalElements = totalElements
// 		stats.AverageSize = float64(totalElements) / float64(len(allSets))

// 		// 가장 큰/작은 집합 찾기
// 		stats.LargestSet = uint64(allSets[0].Size)
// 		stats.SmallestSet = uint64(allSets[0].Size)

// 		for _, set := range allSets {
// 			if uint64(set.Size) > stats.LargestSet {
// 				stats.LargestSet = uint64(set.Size)
// 			}
// 			if uint64(set.Size) < stats.SmallestSet {
// 				stats.SmallestSet = uint64(set.Size)
// 			}
// 		}
// 	}

// 	fmt.Printf("디스조인트 집합 분석 완료 (소요 시간: %v)\n", time.Since(startTime))
// 	fmt.Printf("- 총 집합 수: %d\n", stats.TotalSets)
// 	fmt.Printf("- 총 요소 수: %d\n", stats.TotalElements)
// 	fmt.Printf("- 평균 집합 크기: %.2f\n", stats.AverageSize)
// 	fmt.Printf("- 가장 큰 집합 크기: %d\n", stats.LargestSet)
// 	fmt.Printf("- 가장 작은 집합 크기: %d\n", stats.SmallestSet)

// 	return stats, nil
// }

// // 데이터 구조 검증 함수
// func validateDataStructure(ic *IncrementalUnionFindProcessor) error {
// 	// 첫 번째 청크 로드
// 	err := ic.loadWindow(0, ChunkSize)
// 	if err != nil {
// 		return fmt.Errorf("첫 번째 청크 로드 실패: %w", err)
// 	}

// 	// 파일 구조 유효성 확인
// 	nonZeroCount := 0
// 	selfParentCount := 0

// 	for i := uint64(0); i < ChunkSize && i < ic.fileCapacity; i++ {
// 		if ic.parentSlice[i] != 0 {
// 			nonZeroCount++

// 			if ic.parentSlice[i] == i {
// 				selfParentCount++
// 			}
// 		}
// 	}

// 	fmt.Printf("파일 구조 검증 결과:\n")
// 	fmt.Printf("- 첫 청크 크기: %d\n", ChunkSize)
// 	fmt.Printf("- 초기화된 요소 수: %d\n", nonZeroCount)
// 	fmt.Printf("- 루트 요소 수: %d\n", selfParentCount)

// 	if nonZeroCount == 0 {
// 		return fmt.Errorf("초기화된 요소가 없음, 빈 파일 또는 잘못된 형식일 수 있음")
// 	}

// 	return nil
// }

// // 청크 데이터 디버깅 정보 출력
// func printChunkDebugInfo(ic *IncrementalUnionFindProcessor, startIdx, endIdx uint64) {
// 	fmt.Printf("\n======= 청크 데이터 샘플 (시작 인덱스: %d) =======\n", startIdx)

// 	// 첫 15개 요소와 마지막 몇 개 요소 출력
// 	sampleCount := uint64(15)
// 	if endIdx-startIdx < sampleCount {
// 		sampleCount = endIdx - startIdx
// 	}

// 	for i := uint64(0); i < sampleCount; i++ {
// 		fmt.Printf("인덱스 %d: parent=%d, size=%d, rank=%d\n",
// 			startIdx+i, ic.parentSlice[i], ic.sizeSlice[i], ic.rankSlice[i])
// 	}

// 	fmt.Printf("...\n")

// 	// 마지막 부분에서 몇 개 요소 샘플링
// 	lastSampleStart := endIdx - startIdx - 5
// 	if lastSampleStart > sampleCount {
// 		for i := lastSampleStart; i < endIdx-startIdx; i++ {
// 			fmt.Printf("인덱스 %d: parent=%d, size=%d, rank=%d\n",
// 				startIdx+i, ic.parentSlice[i], ic.sizeSlice[i], ic.rankSlice[i])
// 		}
// 	}

// 	// 비어있지 않은 요소 수집
// 	nonZeroCount := 0
// 	for i := uint64(0); i < endIdx-startIdx; i++ {
// 		if ic.parentSlice[i] != 0 {
// 			nonZeroCount++
// 		}
// 	}

// 	fmt.Printf("청크 내 초기화된 요소 수: %d/%d\n", nonZeroCount, endIdx-startIdx)
// 	fmt.Printf("=============================================\n\n")
// }

// // FindRealRootSets는 실제 의미 있는 디스조인트 집합을 찾습니다
// func (ic *IncrementalUnionFindProcessor) FindRealRootSets() ([]SetInfo, error) {
// 	startTime := time.Now()
// 	fmt.Println("실제 의미 있는 디스조인트 집합 검색 시작...")

// 	// 초기화
// 	allSets := make([]SetInfo, 0)
// 	var mutex sync.Mutex

// 	// 최대 50000개 요소만 처리하는 간단한 스캔 수행
// 	var maxElementsToScan uint64 = 50000000
// 	if ic.fileCapacity < maxElementsToScan {
// 		maxElementsToScan = ic.fileCapacity
// 	}

// 	fmt.Printf("최대 %d개 요소 스캔 중...\n", maxElementsToScan)

// 	// 청크 크기를 기반으로 파일을 분할하여 병렬로 처리
// 	totalChunks := int((maxElementsToScan + ChunkSize - 1) / ChunkSize)

// 	// 병렬 처리를 위한 워커 수 설정
// 	numWorkers := runtime.NumCPU()
// 	if numWorkers > 8 {
// 		numWorkers = 8 // 최대 8개 워커로 제한
// 	}

// 	// 청크 분배
// 	var wg sync.WaitGroup
// 	chunkResults := make(chan []SetInfo, numWorkers)

// 	// 청크를 워커에 분배
// 	chunkSize := (totalChunks + numWorkers - 1) / numWorkers

// 	for w := 0; w < numWorkers; w++ {
// 		wg.Add(1)
// 		startChunk := w * chunkSize
// 		endChunk := (w + 1) * chunkSize
// 		if endChunk > totalChunks {
// 			endChunk = totalChunks
// 		}

// 		go func(workerID, start, end int) {
// 			defer wg.Done()
// 			workerSets := make([]SetInfo, 0)

// 			for chunkIdx := start; chunkIdx < end; chunkIdx++ {
// 				// 청크 경계 계산
// 				startIdx := uint64(chunkIdx) * ChunkSize
// 				endIdx := startIdx + ChunkSize
// 				if endIdx > maxElementsToScan {
// 					endIdx = maxElementsToScan
// 				}

// 				// 청크 데이터 로드
// 				err := ic.loadWindow(startIdx, endIdx)
// 				if err != nil {
// 					fmt.Printf("Worker %d: 청크 %d 로드 실패: %s\n", workerID, chunkIdx, err.Error())
// 					continue
// 				}

// 				// 청크 내 루트 요소 찾기 (parent == self) 및 확장된 검증
// 				for relIdx := uint64(0); relIdx < endIdx-startIdx; relIdx++ {
// 					absIdx := startIdx + relIdx

// 					// 초기화된 요소인지 확인 (parent가 0이 아님)
// 					if ic.parentSlice[relIdx] != 0 {
// 						// 루트 요소는 자기 자신을 가리킴
// 						if ic.parentSlice[relIdx] == absIdx {
// 							// 500000000 같은 특별한 값 필터링
// 							if absIdx >= 500000000 {
// 								continue
// 							}

// 							// 집합 크기 검증 - 비정상적으로 큰 값 필터링 (2^16에 가까운 값)
// 							if ic.sizeSlice[relIdx] >= 65500 {
// 								continue
// 							}

// 							// 크기가 0인 경우 추가 검증
// 							size := ic.sizeSlice[relIdx]
// 							if size == 0 {
// 								// 이 요소가 실제로 유효한지 확인 (다른 요소가 이를 가리키는지)
// 								hasChildren := false

// 								// 같은 청크 내에서 이 요소를 가리키는 다른 요소 찾기
// 								for childRelIdx := uint64(0); childRelIdx < endIdx-startIdx; childRelIdx++ {
// 									if childRelIdx != relIdx && ic.parentSlice[childRelIdx] == absIdx {
// 										hasChildren = true
// 										break
// 									}
// 								}

// 								if !hasChildren {
// 									// 이 요소는 고립되어 있으므로 크기 1로 설정
// 									size = 1
// 								}
// 							}

// 							// 유효한 요소라고 판단되면 추가
// 							if size > 0 {
// 								workerSets = append(workerSets, SetInfo{
// 									Root: absIdx,
// 									Size: size,
// 									Rank: ic.rankSlice[relIdx],
// 								})
// 							}
// 						}
// 					}
// 				}
// 			}

// 			// 결과 전송
// 			chunkResults <- workerSets

// 		}(w, startChunk, endChunk)
// 	}

// 	// 모든 워커가 완료될 때까지 대기
// 	go func() {
// 		wg.Wait()
// 		close(chunkResults)
// 	}()

// 	// 결과 수집
// 	for results := range chunkResults {
// 		mutex.Lock()
// 		allSets = append(allSets, results...)
// 		mutex.Unlock()
// 	}

// 	// 크기별로 정렬 (내림차순)
// 	sort.Slice(allSets, func(i, j int) bool {
// 		return allSets[i].Size > allSets[j].Size
// 	})

// 	// 유의미한 집합만 필터링 (크기가 1보다 큰 집합)
// 	realSets := make([]SetInfo, 0)
// 	for _, set := range allSets {
// 		if set.Size > 1 {
// 			realSets = append(realSets, set)
// 		}
// 	}

// 	fmt.Printf("검색 완료: 총 %d개 집합 발견 (크기 > 1 집합: %d개)\n",
// 		len(allSets), len(realSets))

// 	// 상위 100개 집합 정보 출력
// 	topN := 100
// 	if len(realSets) < topN {
// 		topN = len(realSets)
// 	}

// 	if topN > 0 {
// 		fmt.Printf("\n상위 %d개 집합 목록:\n", topN)
// 		for i := 0; i < topN; i++ {
// 			fmt.Printf("%d. 루트: %d, 크기: %d, 랭크: %d\n",
// 				i+1, realSets[i].Root, realSets[i].Size, realSets[i].Rank)
// 		}
// 	} else {
// 		fmt.Println("실제 유의미한 집합(크기 > 1)이 발견되지 않았습니다.")
// 	}

// 	fmt.Printf("실제 집합 검색 완료 (소요 시간: %v)\n", time.Since(startTime))
// 	return realSets, nil
// }

// // ExtractSetMembers는 루트를 기반으로 전체 집합 요소를 추출합니다
// func (ic *IncrementalUnionFindProcessor) ExtractSetMembers(rootElement uint64, maxElements int) ([]uint64, error) {
// 	startTime := time.Now()
// 	fmt.Printf("루트 %d 집합의 요소 추출 시작...\n", rootElement)

// 	// 루트 요소가 속한 청크 로드
// 	rootChunkIdx := int(rootElement / ChunkSize)
// 	startIdx := uint64(rootChunkIdx) * ChunkSize
// 	endIdx := startIdx + ChunkSize

// 	err := ic.loadWindow(startIdx, endIdx)
// 	if err != nil {
// 		return nil, fmt.Errorf("루트 청크 로드 실패: %w", err)
// 	}

// 	// 루트 요소 상대 인덱스
// 	rootRelIdx := rootElement - startIdx

// 	// 루트 정보 출력
// 	fmt.Printf("루트 정보: 인덱스=%d, parent=%d, size=%d, rank=%d\n",
// 		rootElement, ic.parentSlice[rootRelIdx], ic.sizeSlice[rootRelIdx], ic.rankSlice[rootRelIdx])

// 	// 결과 집합
// 	members := []uint64{rootElement}

// 	// 루트가 실제로 자기 자신을 가리키는지 확인
// 	if ic.parentSlice[rootRelIdx] != rootElement {
// 		fmt.Printf("경고: 지정된 요소(%d)는 실제 루트가 아닙니다. 부모: %d\n",
// 			rootElement, ic.parentSlice[rootRelIdx])
// 	}

// 	// 메모리에 모든 요소-부모 관계를 로드하여 검색 속도 향상
// 	// 최대 500만 요소로 제한하여 메모리 사용량 관리
// 	var maxElementsToScan uint64 = 5000000
// 	if ic.fileCapacity < maxElementsToScan {
// 		maxElementsToScan = ic.fileCapacity
// 	}

// 	// 부모 맵 구성
// 	elementToParent := make(map[uint64]uint64)

// 	fmt.Printf("부모-자식 관계 스캔 중... (최대 %d개 요소)\n", maxElementsToScan)

// 	// 청크 단위로 스캔
// 	chunksToScan := int((maxElementsToScan + ChunkSize - 1) / ChunkSize)

// 	for chunkIdx := 0; chunkIdx < chunksToScan; chunkIdx++ {
// 		chunkStart := uint64(chunkIdx) * ChunkSize
// 		chunkEnd := chunkStart + ChunkSize
// 		if chunkEnd > maxElementsToScan {
// 			chunkEnd = maxElementsToScan
// 		}

// 		// 청크 로드
// 		err := ic.loadWindow(chunkStart, chunkEnd)
// 		if err != nil {
// 			fmt.Printf("청크 %d 로드 실패: %s\n", chunkIdx, err.Error())
// 			continue
// 		}

// 		// 요소-부모 관계 수집
// 		for relIdx := uint64(0); relIdx < chunkEnd-chunkStart; relIdx++ {
// 			absIdx := chunkStart + relIdx

// 			if ic.parentSlice[relIdx] != 0 {
// 				elementToParent[absIdx] = ic.parentSlice[relIdx]
// 			}
// 		}
// 	}

// 	fmt.Printf("총 %d개 요소-부모 관계 수집 완료\n", len(elementToParent))

// 	// 루트를 직접 가리키는 자식 요소 찾기
// 	fmt.Println("자식 요소 찾는 중...")
// 	directChildren := 0

// 	for element, parent := range elementToParent {
// 		if parent == rootElement && element != rootElement {
// 			members = append(members, element)
// 			directChildren++

// 			// 최대 요소 수 제한
// 			if len(members) >= maxElements {
// 				break
// 			}
// 		}
// 	}

// 	fmt.Printf("직접 연결된 자식 요소: %d개\n", directChildren)

// 	// 추가 검증: 간접 자식 찾기
// 	if len(members) < maxElements && len(members) < int(ic.sizeSlice[rootRelIdx]) {
// 		fmt.Println("간접 연결된 자식 요소 찾는 중...")

// 		// 2단계까지의 간접 자식 찾기
// 		indirectChildren := 0
// 		newMembers := make([]uint64, 0)

// 		for child := range members {
// 			for element, parent := range elementToParent {
// 				if parent == members[child] && element != members[child] && !contains(members, element) {
// 					newMembers = append(newMembers, element)
// 					indirectChildren++

// 					// 최대 요소 수 제한
// 					if len(members)+len(newMembers) >= maxElements {
// 						break
// 					}
// 				}
// 			}

// 			if len(members)+len(newMembers) >= maxElements {
// 				break
// 			}
// 		}

// 		members = append(members, newMembers...)
// 		fmt.Printf("간접 연결된 자식 요소: %d개\n", indirectChildren)
// 	}

// 	fmt.Printf("집합 요소 추출 완료: 총 %d개 요소 (소요 시간: %v)\n",
// 		len(members), time.Since(startTime))

// 	if uint16(len(members)) != ic.sizeSlice[rootRelIdx] {
// 		fmt.Printf("주의: 발견된 요소 수(%d)가 저장된 크기(%d)와 다릅니다.\n",
// 			len(members), ic.sizeSlice[rootRelIdx])
// 	}

// 	return members, nil
// }

// // 배열에 요소가 포함되어 있는지 확인하는 헬퍼 함수
// func contains(arr []uint64, value uint64) bool {
// 	for _, v := range arr {
// 		if v == value {
// 			return true
// 		}
// 	}
// 	return false
// }

// // 전체 디스조인트 집합 분석 및 통계 계산 메인 함수
// func AnalyzeUnionFindArchive(dataDir string) {
// 	startTime := time.Now()
// 	fmt.Println("Union-Find 아카이브 분석 시작...")

// 	// 아카이브 경로 유효성 확인
// 	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
// 		fmt.Printf("오류: 지정된 디렉토리가 존재하지 않습니다: %s\n", dataDir)
// 		return
// 	}

// 	// 파일 경로 확인
// 	parentPath := filepath.Join(dataDir, "parent.bin")
// 	rankPath := filepath.Join(dataDir, "rank.bin")
// 	sizePath := filepath.Join(dataDir, "size.bin")

// 	for _, path := range []string{parentPath, rankPath, sizePath} {
// 		if _, err := os.Stat(path); os.IsNotExist(err) {
// 			fmt.Printf("오류: 필요한 파일이 없습니다: %s\n", path)
// 			return
// 		}

// 		// 파일 크기 확인
// 		info, err := os.Stat(path)
// 		if err == nil {
// 			fmt.Printf("파일 크기: %s: %.2f MB\n", filepath.Base(path), float64(info.Size())/(1024*1024))
// 		}
// 	}

// 	// UnionFind 프로세서 생성
// 	processor, err := NewIncrementalUnionFindProcessor(dataDir, 0)
// 	if err != nil {
// 		fmt.Printf("UnionFind 프로세서 생성 실패: %s\n", err.Error())
// 		return
// 	}
// 	defer processor.Close() // 리소스 정리

// 	// 1. 먼저 실제 의미 있는 집합 찾기 (개선된 방법)
// 	realSets, err := processor.FindRealRootSets()
// 	if err != nil {
// 		fmt.Printf("실제 집합 검색 실패: %s\n", err.Error())
// 	}

// 	// 2. 기본 통계 계산
// 	stats, err := processor.AnalyzeDisjointSets()
// 	if err != nil {
// 		fmt.Printf("디스조인트 집합 분석 실패: %s\n", err.Error())
// 	}

// 	// 3. 실제 집합이 발견되면 가장 큰 집합의 요소 탐색
// 	if len(realSets) > 0 {
// 		largestSetRoot := realSets[0].Root
// 		fmt.Printf("\n가장 큰 실제 집합(루트: %d, 크기: %d)의 요소 샘플링:\n",
// 			largestSetRoot, realSets[0].Size)

// 		// 최대 1000개 요소만 추출
// 		members, err := processor.ExtractSetMembers(largestSetRoot, 1000)
// 		if err != nil {
// 			fmt.Printf("집합 요소 추출 실패: %s\n", err.Error())
// 		} else {
// 			// 처음 10개 요소만 출력
// 			sampleSize := 10
// 			if len(members) < sampleSize {
// 				sampleSize = len(members)
// 			}

// 			fmt.Printf("첫 %d개 요소:\n", sampleSize)
// 			for i := 0; i < sampleSize; i++ {
// 				fmt.Printf("  %d\n", members[i])
// 			}
// 		}
// 	}

// 	fmt.Printf("\n분석 완료! 총 소요 시간: %v\n", time.Since(startTime))
// 	if stats != nil {
// 		fmt.Printf("총 집합 수: %d, 평균 크기: %.2f\n", stats.TotalSets, stats.AverageSize)
// 	}
// 	if len(realSets) > 0 {
// 		fmt.Printf("실제 의미 있는 집합 수: %d\n", len(realSets))
// 	}
// }

// // // 메인 함수 예시
// // func main() {
// // 	// 사용 예시
// // 	dataDir := "./data" // 아카이브 디렉토리 경로 지정
// // 	AnalyzeUnionFindArchive(dataDir)
// // }
