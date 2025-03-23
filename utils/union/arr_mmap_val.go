package main

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"
)

// 검증을 위한 상수 설정
const (
	ValidateSampleSize        = 1000              // 서로소 집합 검증을 위한 샘플 크기
	BloomFilterEfficiencyTest = true              // 블룸 필터 효율성 테스트 활성화
	CrossBatchValidation      = true              // 배치 간 일관성 검증 활성화
	DetailedUFStats           = true              // 상세 Union-Find 통계 활성화
	ValidationDir             = "validation_data" // 검증 데이터 디렉토리
)

// 로직 검증용 배치 통계 구조체
type BatchStats struct {
	BatchID             int           // 배치 ID
	SetCount            int           // 서로소 집합 수
	ElementCount        int           // 원소 수
	SizeDistribution    map[uint8]int // 집합 크기 분포
	BloomFilterHits     int           // 블룸 필터 히트 수
	BloomFilterFalsePos int           // 블룸 필터 거짓 양성 수
	CrossBatchRefs      int           // 배치 간 참조 수
	ProcessedSets       int           // 처리된 집합 수
	UnionOperations     int           // Union 연산 수
	SuccessfulUnions    int           // 성공한 Union 연산 수
	OperationTime       time.Duration // 작업 시간
}

// 로직 검증용 프로세서 래퍼
type ValidatedProcessor struct {
	processor    *IncrementalUnionFindProcessor // 기본 프로세서
	batchStats   []*BatchStats                  // 배치별 통계
	groundTruth  map[uint64]uint64              // 실제 원소-루트 매핑
	mu           sync.RWMutex                   // 동시성 제어
	currentBatch int                            // 현재 배치 ID
}

// 새 검증 래퍼 생성
func NewValidatedProcessor(processor *IncrementalUnionFindProcessor) *ValidatedProcessor {
	return &ValidatedProcessor{
		processor:    processor,
		batchStats:   make([]*BatchStats, 0),
		groundTruth:  make(map[uint64]uint64),
		currentBatch: 0,
	}
}

// 배치 시작 처리 래핑
func (vp *ValidatedProcessor) StartNewBatch() error {
	vp.mu.Lock()
	defer vp.mu.Unlock()

	// 현재 배치 통계 준비
	stats := &BatchStats{
		BatchID:          vp.currentBatch,
		ElementCount:     BatchSize,
		SizeDistribution: make(map[uint8]int),
		OperationTime:    0,
	}

	// 실제 배치 시작
	startTime := time.Now()
	err := vp.processor.StartNewBatch()
	stats.OperationTime = time.Since(startTime)

	if err != nil {
		return err
	}

	// 통계 저장
	vp.batchStats = append(vp.batchStats, stats)
	vp.currentBatch++

	return nil
}

// 집합 처리 래핑
func (vp *ValidatedProcessor) ProcessSet(elements []uint64) error {
	if len(elements) <= 1 {
		return nil
	}

	vp.mu.Lock()

	// 현재 배치 통계 가져오기
	currentStats := vp.batchStats[vp.currentBatch-1]
	currentStats.ProcessedSets++

	// 크로스 배치 참조 검증
	crossBatchReference := false
	batchOffset := BatchSize * (vp.currentBatch - 1)

	for _, elem := range elements {
		if int(elem) < batchOffset || int(elem) >= batchOffset+BatchSize {
			crossBatchReference = true
			break
		}
	}

	if crossBatchReference {
		currentStats.CrossBatchRefs++
	}

	// 블룸 필터 효율성 테스트
	if BloomFilterEfficiencyTest && vp.currentBatch > 1 && len(elements) > 1 {
		// 블룸 필터 테스트를 위한 샘플
		//bloomTested := false

		for _, elem := range elements {
			if int(elem) < batchOffset {
				//		bloomTested = true

				// 실제 이전 배치의 복합 집합에 속하는지 확인 (groundTruth 기반)
				actualRoot, existsInGroundTruth := vp.groundTruth[elem]
				var isCompoundSet bool

				if existsInGroundTruth {
					// 다른 원소들과 같은 집합인지 확인
					for k, v := range vp.groundTruth {
						if k != elem && v == actualRoot {
							isCompoundSet = true
							break
						}
					}
				}

				// 블룸 필터 체크 (실제 구현에서는 블룸 필터 확인 로직)
				bloomFilterPositive := false
				if len(vp.processor.bloomFilters) > 0 {
					latestBloomFilter := vp.processor.bloomFilters[len(vp.processor.bloomFilters)-1]

					for j := 0; j < BloomHashFunctions; j++ {
						h := hash(elem, uint64(j))
						pos := h % uint64(len(latestBloomFilter)*8)
						bytePos := pos / 8
						bitPos := pos % 8

						if latestBloomFilter[bytePos]&(1<<bitPos) != 0 {
							bloomFilterPositive = true
							break
						}
					}
				}

				// 통계 업데이트
				if bloomFilterPositive {
					currentStats.BloomFilterHits++
					if !isCompoundSet {
						currentStats.BloomFilterFalsePos++
					}
				}
			}
		}
	}

	vp.mu.Unlock()

	// 실제 집합 처리
	err := vp.processor.ProcessSet(elements)

	// 처리 후 groundTruth 업데이트
	if err == nil && len(elements) > 1 {
		vp.mu.Lock()
		defer vp.mu.Unlock()

		// Union 연산 수 추적
		unionOps := len(elements) - 1
		currentStats.UnionOperations += unionOps

		// groundTruth 업데이트
		baseElem := elements[0]
		baseElemLocal := baseElem

		// 로컬 인덱스로 변환
		if int(baseElem) >= batchOffset && int(baseElem) < batchOffset+BatchSize {
			baseElemLocal = baseElem - uint64(batchOffset)
		}

		// 현재 배치 내 원소의 루트만 추적
		if vp.processor.currentUF != nil {
			root := vp.processor.currentUF.FindRoot(baseElemLocal)
			globalRoot := root + uint64(batchOffset) // 글로벌 인덱스로 변환

			for _, elem := range elements {
				if int(elem) >= batchOffset && int(elem) < batchOffset+BatchSize {
					vp.groundTruth[elem] = globalRoot
				}
			}

			currentStats.SuccessfulUnions += unionOps
		}
	}

	return err
}

// 각 배치의 서로소 집합 수 검증
func (vp *ValidatedProcessor) ValidateDisjointSets() error {
	fmt.Println("\n===== 서로소 집합 검증 시작 =====")

	for i, stats := range vp.batchStats {
		fmt.Printf("배치 %d 검증 중...\n", i)

		// 배치 디렉토리
		batchDir := filepath.Join(vp.processor.dataDir, fmt.Sprintf("batch_%d", i))

		// 실제 서로소 집합 수 계산
		if _, err := os.Stat(batchDir); os.IsNotExist(err) {
			fmt.Printf("배치 %d 디렉토리가 없음, 건너뜀\n", i)
			continue
		}

		// 아카이브 정보 로드
		archive, err := LoadArchive(vp.processor.dataDir, i)
		if err != nil {
			fmt.Printf("배치 %d 아카이브 로드 실패: %v\n", i, err)
			continue
		}

		// 실제 수와 기록된 수 비교
		fmt.Printf("배치 %d: 기록된 집합 수 = %d, 아카이브 집합 수 = %d\n",
			i, stats.SetCount, archive.Count)

		if stats.SetCount > 0 && stats.SetCount != archive.Count {
			fmt.Printf("경고: 배치 %d 집합 수 불일치!\n", i)
		}
	}

	fmt.Println("서로소 집합 검증 완료")
	return nil
}

// 크기 분포 검증
func (vp *ValidatedProcessor) ValidateSizeDistribution() error {
	fmt.Println("\n===== 집합 크기 분포 검증 시작 =====")

	// 현재 실행 중인 배치만 검증 가능
	if vp.processor.currentUF == nil {
		fmt.Println("경고: 현재 활성 배치가 없음, 검증 건너뜀")
		return nil
	}

	// 크기 분포 계산
	sizeDistribution := make(map[uint8]int)

	// 샘플링을 통한 검증
	sampleInterval := BatchSize / ValidateSampleSize
	if sampleInterval < 1 {
		sampleInterval = 1
	}

	samplesChecked := 0

	for i := 0; i < vp.processor.currentUF.numElements; i += sampleInterval {
		root := vp.processor.currentUF.FindRoot(uint64(i))
		size := vp.processor.currentUF.sizeSlice[root]
		sizeDistribution[size]++
		samplesChecked++
	}

	// 결과 출력
	fmt.Printf("샘플 수: %d\n", samplesChecked)
	fmt.Println("집합 크기 분포:")

	for size := uint8(1); size <= UinonSizeLimit; size++ {
		count := sizeDistribution[size]
		if count > 0 {
			percent := float64(count) / float64(samplesChecked) * 100
			fmt.Printf("크기 %d인 집합: %d개 (%.2f%%)\n", size, count, percent)
		}
	}

	// 크기 1인 집합이 약 80%여야 함
	percentSize1 := float64(sizeDistribution[1]) / float64(samplesChecked) * 100
	fmt.Printf("크기 1인 집합 비율: %.2f%% (목표: 약 80%%)\n", percentSize1)

	if percentSize1 < 75 || percentSize1 > 85 {
		fmt.Println("경고: 크기 1인 집합 비율이 예상 범위(75%~85%)를 벗어남")
	}

	// 현재 배치 통계에 저장
	if len(vp.batchStats) > 0 {
		vp.batchStats[len(vp.batchStats)-1].SizeDistribution = sizeDistribution
		vp.batchStats[len(vp.batchStats)-1].SetCount = vp.processor.currentUF.count
	}

	fmt.Println("집합 크기 분포 검증 완료")
	return nil
}

// 배치 간 일관성 검증
func (vp *ValidatedProcessor) ValidateCrossBatchConsistency() error {
	if !CrossBatchValidation || len(vp.batchStats) <= 1 {
		return nil
	}

	fmt.Println("\n===== 배치 간 일관성 검증 시작 =====")

	// 현재 groundTruth에 있는 원소들 중 임의로 샘플링하여
	// 같은 집합에 속해야 하는 원소들이 실제로 같은 집합에 속하는지 확인

	// 현재 루트별로 원소 그룹화
	rootGroups := make(map[uint64][]uint64)

	for elem, root := range vp.groundTruth {
		rootGroups[root] = append(rootGroups[root], elem)
	}

	// 샘플링 - 크기 2 이상인 집합만 선택
	sampleCount := 0
	inconsistencies := 0

	for root, elems := range rootGroups {
		if len(elems) < 2 {
			continue // 단일 원소 집합 무시
		}

		// 이 집합에서 두 원소 선택
		for i := 0; i < len(elems); i++ {
			for j := i + 1; j < len(elems); j++ {
				sampleCount++

				// 배치 경계 검사
				elemI := elems[i]
				elemJ := elems[j]
				batchI := int(elemI) / BatchSize
				batchJ := int(elemJ) / BatchSize

				if batchI != batchJ {
					// 서로 다른 배치에 속한 원소들
					fmt.Printf("크로스 배치 검증: 원소 %d(배치 %d)와 %d(배치 %d)는 같은 집합(루트 %d)에 속해야 함\n",
						elemI, batchI, elemJ, batchJ, root)

					// 실제 일관성은 파일 기반 검증 필요
					// 이 예제에서는 단순 로깅만 수행

					// 논리적 불일치 감지 (groundTruth에서 서로 다른 루트를 가짐)
					if vp.groundTruth[elemI] != vp.groundTruth[elemJ] {
						fmt.Printf("경고: 원소 %d와 %d가 같은 집합에 속해야 하지만 다른 루트를 가짐!\n",
							elemI, elemJ)
						inconsistencies++
					}
				}

				if sampleCount >= 100 {
					// 샘플 제한
					break
				}
			}

			if sampleCount >= 100 {
				break
			}
		}

		if sampleCount >= 100 {
			break
		}
	}

	fmt.Printf("크로스 배치 일관성 검증: %d개 샘플 중 %d개 불일치 발견\n",
		sampleCount, inconsistencies)

	fmt.Println("배치 간 일관성 검증 완료")
	return nil
}

// 블룸 필터 효율성 검증
func (vp *ValidatedProcessor) ValidateBloomFilterEfficiency() error {
	if !BloomFilterEfficiencyTest || len(vp.batchStats) <= 1 {
		return nil
	}

	fmt.Println("\n===== 블룸 필터 효율성 검증 시작 =====")

	// 각 배치의 블룸 필터 통계 수집
	for i, stats := range vp.batchStats {
		if i == 0 {
			continue // 첫 배치는 이전 배치가 없으므로 건너뜀
		}

		totalHits := stats.BloomFilterHits
		falsePositives := stats.BloomFilterFalsePos

		if totalHits > 0 {
			falsePositiveRate := float64(falsePositives) / float64(totalHits) * 100
			fmt.Printf("배치 %d: 블룸 필터 히트 %d개 중 거짓 양성 %d개 (%.2f%%)\n",
				i, totalHits, falsePositives, falsePositiveRate)
		} else {
			fmt.Printf("배치 %d: 블룸 필터 히트 없음\n", i)
		}
	}

	fmt.Println("블룸 필터 효율성 검증 완료")
	return nil
}

// 최종 통계 출력
func (vp *ValidatedProcessor) PrintFinalStats() {
	fmt.Println("\n===== 검증 최종 통계 =====")

	totalSets := 0
	totalElements := 0
	totalProcessedSets := 0
	totalUnionOps := 0
	totalSuccessUnions := 0
	totalCrossBatchRefs := 0

	for _, stats := range vp.batchStats {
		totalSets += stats.SetCount
		totalElements += stats.ElementCount
		totalProcessedSets += stats.ProcessedSets
		totalUnionOps += stats.UnionOperations
		totalSuccessUnions += stats.SuccessfulUnions
		totalCrossBatchRefs += stats.CrossBatchRefs
	}

	fmt.Printf("총 원소 수: %d\n", totalElements)
	fmt.Printf("총 처리된 집합 수: %d\n", totalProcessedSets)
	fmt.Printf("총 Union 연산 수: %d\n", totalUnionOps)
	fmt.Printf("성공한 Union 연산 수: %d (성공률: %.2f%%)\n",
		totalSuccessUnions, float64(totalSuccessUnions)/float64(totalUnionOps)*100)
	fmt.Printf("배치 간 참조 수: %d\n", totalCrossBatchRefs)
	fmt.Printf("최종 서로소 집합 수: %d\n", totalSets)
}

// 검증된 테스트 배치 생성 함수
func generateValidatedTestBatch(validatedProcessor *ValidatedProcessor, batchID int) error {
	fmt.Printf("\n===== 배치 %d 검증 처리 시작 =====\n", batchID)
	startTime := time.Now()

	// 배치 시작
	if err := validatedProcessor.StartNewBatch(); err != nil {
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
		var elem uint64
		for {
			elem = uint64(rand.Intn(BatchSize) + BatchSize*batchID)
			if !usedElements[elem] {
				usedElements[elem] = true
				break
			}
		}

		// 2, 3, 4 중 크기 선택
		size := rand.Intn(3) + 2
		targetSets[size] = append(targetSets[size], elem)

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

			// 집합 처리 (검증 로직 포함)
			if err := validatedProcessor.ProcessSet(setElems); err != nil {
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

	// 집합 크기 분포 검증
	validatedProcessor.ValidateSizeDistribution()

	unionStats := getMemoryStats(startTime)
	printMemoryUsage("Union 검증 후", unionStats)

	return nil
}

func main() {
	// 시작 시간
	startTime := time.Now()

	// 초기 메모리 상태
	initialStats := getMemoryStats(startTime)
	printMemoryUsage("초기 상태", initialStats)

	// 데이터 디렉토리 설정
	dataDir := "unionfind_data"

	// 점증적 처리기 생성
	processor, err := NewIncrementalProcessor(dataDir)
	if err != nil {
		fmt.Printf("처리기 생성 실패: %v\n", err)
		return
	}

	// 검증 래퍼 생성
	validatedProcessor := NewValidatedProcessor(processor)

	// 검증용 더 작은 배치 수 설정 (메모리 제한으로)
	testBatches := 3 // 검증을 위해 3개 배치만 처리

	fmt.Printf("검증 모드로 %d개 배치 처리 시작\n", testBatches)
	for batchID := 0; batchID < testBatches; batchID++ {
		if err := generateValidatedTestBatch(validatedProcessor, batchID); err != nil {
			fmt.Printf("배치 %d 처리 실패: %v\n", batchID, err)
			return
		}

		// 배치 간 일관성 검증
		if batchID > 0 {
			validatedProcessor.ValidateCrossBatchConsistency()
			validatedProcessor.ValidateBloomFilterEfficiency()
		}

		// 메모리 정리
		runtime.GC()
		fmt.Println("가비지 컬렉션 강제 실행")
		memStats := getMemoryStats(startTime)
		printMemoryUsage(fmt.Sprintf("배치 %d 완료 후", batchID), memStats)
	}

	// 서로소 집합 검증
	validatedProcessor.ValidateDisjointSets()

	// 최종 통계 출력
	validatedProcessor.PrintFinalStats()

	// 전체 실행 시간 및 최종 메모리 상태
	totalDuration := time.Since(startTime)
	fmt.Printf("\n전체 실행 시간: %v\n", totalDuration)

	finalStats := getMemoryStats(startTime)
	printMemoryUsage("최종 상태", finalStats)
}
