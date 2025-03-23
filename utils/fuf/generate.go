package main

import (
	"encoding/gob"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"time"
)

type UserSet struct {
	Elements []uint64
}

// ! 현재는 1/10 스케일로 테스트 중임!
// TODO 1/10 스케일로 테스트 중임! 나중에 1/1로 변경할 것
// TODO 이 및 데이터에 0 두개씩 추가하기
// 집합 파일 관련 상수 및 구조체
const (
	MaxElement       = 2_000_000_0 // 원소 범위
	SetCountPerPhase = 200_000_0   // 생성할 집합 수
	SetChunkSize     = 10_000_0    // 한 번에 처리할 집합 수
	SetFilePath      = "sets_data" // 집합 데이터 파일 기본 경로
)

// 집합을 파일에 저장하는 함수
func SaveSetsToFile(sets []UserSet, filePath string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	return encoder.Encode(sets)
}

// 파일에서 집합을 로드하는 함수
func LoadSetsFromFile(filePath string) ([]UserSet, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var sets []UserSet
	decoder := gob.NewDecoder(file)
	err = decoder.Decode(&sets)
	return sets, err
}

// 청크 단위로 집합을 생성하고 파일에 저장하는 함수
func GenerateAndSaveSets(numSets uint64, rangeValue uint64, phase int) (int, error) {
	// 청크별로 파일 생성
	totalChunks := (numSets + SetChunkSize - 1) / SetChunkSize // 올림 나눗셈
	var totalSets int

	for chunk := uint64(0); chunk < totalChunks; chunk++ {
		// 현재 청크에서 생성할 집합 수 계산
		chunkSetCount := SetChunkSize
		if (chunk+1)*SetChunkSize > numSets {
			chunkSetCount = int(numSets - chunk*SetChunkSize)
		}

		fmt.Printf("청크 %d/%d: %d개 집합 생성 중...\n", chunk+1, totalChunks, chunkSetCount)

		// 결과 슬라이스
		chunkSets := make([]UserSet, 0, chunkSetCount)

		// 각 크기별 집합 생성
		chunkSets = GenerateAllSets(uint64(chunkSetCount), rangeValue)

		// 파일에 저장
		fileName := fmt.Sprintf("%s_phase%d_chunk_%d.gob", SetFilePath, phase, chunk)
		if err := SaveSetsToFile(chunkSets, fileName); err != nil {
			return totalSets, err
		}

		totalSets += len(chunkSets)

		// 메모리 정리
		chunkSets = nil
		runtime.GC()

		fmt.Printf("  청크 %d 저장 완료 (%s)\n", chunk+1, fileName)
	}

	return totalSets, nil
}

// 더 메모리 효율적인 GenerateSets 함수
func GenerateSets(rangeValue uint64, setSize int, count int) []UserSet {
	result := make([]UserSet, 0, count)

	// 작은 청크로 나누어 진행
	chunkSize := 100_000
	for len(result) < count {
		// 지역 맵을 사용하여 청크마다 메모리 해제
		usedElements := make(map[uint64]bool)

		for i := 0; i < chunkSize && len(result) < count; i++ {
			set := UserSet{Elements: make([]uint64, 0, setSize)}

			// 중복되지 않는 원소를 setSize만큼 추가
			for j := 0; j < setSize; j++ {
				var element uint64
				maxAttempts := 100 // 무한 루프 방지
				attempts := 0

				for attempts < maxAttempts {
					element = uint64(rand.Int63n(int64(rangeValue))) + 1 // 1부터 시작
					if !usedElements[element] {
						break
					}
					attempts++
				}

				// maxAttempts에 도달하면 중복 허용
				set.Elements = append(set.Elements, element)
				usedElements[element] = true
			}

			result = append(result, set)
		}

		// 청크 완료 후 명시적 GC 호출
		runtime.GC()
	}

	return result
}

// 대폭 개선된 GenerateAllSets 함수
func GenerateAllSets(numSets uint64, rangeValue uint64) []UserSet {

	// 분포 계산은 동일하게 유지
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
	allSets = append(allSets, GenerateSets(rangeValue, 1, singleCount)...)
	fmt.Println("2개 원소 집합 생성 중...")
	allSets = append(allSets, GenerateSets(rangeValue, 2, twoCount)...)
	fmt.Println("3개 원소 집합 생성 중...")
	allSets = append(allSets, GenerateSets(rangeValue, 3, threeCount)...)
	fmt.Println("4-12개 원소 집합 생성 중...")
	allSets = append(allSets, GenerateSets(rangeValue, 4, fourCount)...)
	allSets = append(allSets, GenerateSets(rangeValue, 5, fiveCount)...)
	allSets = append(allSets, GenerateSets(rangeValue, 6, sixCount)...)
	allSets = append(allSets, GenerateSets(rangeValue, 7, sevenCount)...)
	allSets = append(allSets, GenerateSets(rangeValue, 8, eightCount)...)
	allSets = append(allSets, GenerateSets(rangeValue, 9, nineCount)...)
	allSets = append(allSets, GenerateSets(rangeValue, 10, tenCount)...)
	allSets = append(allSets, GenerateSets(rangeValue, 11, elevenCount)...)
	allSets = append(allSets, GenerateSets(rangeValue, 12, twelveCount)...)
	fmt.Println("다수 원소 집합 생성 중...")
	allSets = append(allSets, GenerateSets(rangeValue, 20, twentyCount)...)
	allSets = append(allSets, GenerateSets(rangeValue, 30, thirtyCount)...)
	allSets = append(allSets, GenerateSets(rangeValue, 50, fiftyCount)...)

	// 랜덤하게 섞기
	fmt.Println("집합 섞는 중...")
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(allSets), func(i, j int) {
		allSets[i], allSets[j] = allSets[j], allSets[i]
	})

	fmt.Printf("총 %d개 집합 생성 완료\n", len(allSets))

	return allSets
}

func processPhase(ds *MmapDisjointSet, phase int) {
	phaseStartTime := time.Now()
	fmt.Printf("\n=== 단계 %d 시작: %d 개 집합 처리 ===\n", phase, SetCountPerPhase)

	// 1. 테스트 데이터 확인 및 생성
	fmt.Printf("테스트 데이터 확인 중 (범위: %d ~ %d)...\n", 0, MaxElement)

	// 기존 파일 존재 확인
	setGenStart := time.Now()
	setFileExists := true
	chunkCount := (SetCountPerPhase + SetChunkSize - 1) / SetChunkSize

	for chunk := 0; chunk < chunkCount; chunk++ {
		fileName := fmt.Sprintf("%s_phase%d_chunk_%d.gob", SetFilePath, phase, chunk)
		if _, err := os.Stat(fileName); os.IsNotExist(err) {
			setFileExists = false
			break
		}
	}

	if !setFileExists {
		fmt.Println("집합 데이터 파일 생성 중...")
		totalSets, err := GenerateAndSaveSets(SetCountPerPhase, MaxElement, phase)
		if err != nil {
			fmt.Printf("집합 생성 중 오류: %v\n", err)
			return
		}
		fmt.Printf("총 %d개 집합 생성 및 저장 완료: %v\n", totalSets, time.Since(setGenStart))
	} else {
		fmt.Println("기존 집합 데이터 파일 사용")
	}

	// 2. 모든 청크 순차적으로 처리
	fmt.Println("모든 청크 순차적으로 처리 중...")
	totalProcessedSets := 0
	totalProcessedElements := 0
	totalUnionPairs := 0

	for chunk := 0; chunk < chunkCount; chunk++ {
		chunkStartTime := time.Now()
		fileName := fmt.Sprintf("%s_phase%d_chunk_%d.gob", SetFilePath, phase, chunk)
		fmt.Printf("청크 %d/%d 처리 중 (%s)...\n", chunk+1, chunkCount, fileName)

		// 파일에서 집합 로드
		loadStart := time.Now()
		chunkSets, err := LoadSetsFromFile(fileName)
		if err != nil {
			fmt.Printf("파일 로드 중 오류: %v\n", err)
			continue
		}
		fmt.Printf("  파일 로드 완료: %d개 집합 (%v)\n", len(chunkSets), time.Since(loadStart))

		// 2-1. 모든 원소를 먼저 DisjointSet에 추가
		fmt.Printf("  원소 추가 중...\n")
		makeSetStart := time.Now()

		// 청크 내 모든 원소 수집
		collectStart := time.Now()
		allElements := make([]uint64, 0)
		for _, set := range chunkSets {
			allElements = append(allElements, set.Elements...)
		}
		fmt.Printf("    원소 수집 완료: %d개 (%v)\n", len(allElements), time.Since(collectStart))

		// 특히 대용량에서는 BatchMakeSet이 효율적
		const batchSize = 1_000_000 // 한 번에 처리할 원소 수
		makeSetCount := 0

		for i := 0; i < len(allElements); i += batchSize {
			batchStart := time.Now()
			end := i + batchSize
			if end > len(allElements) {
				end = len(allElements)
			}

			batch := allElements[i:end]
			ds.BatchMakeSet(batch)
			makeSetCount += len(batch)

			fmt.Printf("    MakeSet 진행: %d/%d개 원소 (%d%%) - %v\n",
				makeSetCount, len(allElements),
				makeSetCount*100/len(allElements),
				time.Since(batchStart))
		}

		totalProcessedElements += len(allElements)
		fmt.Printf("  총 %d개 원소 추가 완료: %v\n", len(allElements), time.Since(makeSetStart))

		// 2-2. 각 집합 내 원소들을 Union
		fmt.Printf("  Union 연산 수행 중...\n")
		unionStart := time.Now()
		unionCount := 0
		unionBatchSize := 10000 // 로깅을 위한 배치 크기
		unionBatchStartTime := time.Now()

		for setIdx, set := range chunkSets {
			if len(set.Elements) <= 1 {
				continue // 원소가 1개인 집합은 Union 필요 없음
			}

			// 집합 내 모든 원소를 첫 번째 원소와 Union
			pairs := make([][2]uint64, 0, len(set.Elements)-1)
			for i := 1; i < len(set.Elements); i++ {
				pairs = append(pairs, [2]uint64{set.Elements[0], set.Elements[i]})
			}

			// Union 연산 시작 시간
			pairUnionStart := time.Now()

			// BatchUnion 수행 - 이 집합의 모든 원소를 연결
			ds.BatchUnion(pairs)

			currentPairs := len(pairs)
			unionCount += currentPairs
			totalUnionPairs += currentPairs
			totalProcessedSets++

			// 주기적으로 진행 상황 로깅
			if unionCount >= unionBatchSize || setIdx == len(chunkSets)-1 {
				elapsedBatch := time.Since(unionBatchStartTime)
				totalElapsed := time.Since(unionStart)
				pairsPerSec := float64(unionCount) / elapsedBatch.Seconds()

				fmt.Printf("    Union 진행: %d개 집합, %d개 페어 완료 (%d%%) - %v (%.1f 페어/초) | 최근 집합: %d개 페어, %v\n",
					totalProcessedSets, unionCount,
					totalProcessedSets*100/len(chunkSets),
					totalElapsed, pairsPerSec,
					currentPairs, time.Since(pairUnionStart))

				unionCount = 0
				unionBatchStartTime = time.Now()
			}

			// 메모리 관리: 백만개 집합마다 GC 수행
			if totalProcessedSets%1_000_000 == 0 {
				gcStart := time.Now()
				runtime.GC()
				fmt.Printf("    GC 수행: %v\n", time.Since(gcStart))
			}
		}

		fmt.Printf("  총 %d개 Union 연산 완료: %v\n", totalUnionPairs, time.Since(unionStart))

		// 2-3. 진행 중 상태 저장
		fmt.Printf("  디스크에 변경사항 반영 중...\n")
		flushStart := time.Now()
		ds.Flush()
		fmt.Printf("  디스크 반영 완료: %v\n", time.Since(flushStart))

		// 메모리 해제
		memStart := time.Now()
		chunkSets = nil
		allElements = nil
		runtime.GC()
		fmt.Printf("  메모리 정리 완료: %v\n", time.Since(memStart))

		fmt.Printf("청크 %d 처리 완료: %d개 집합, %d개 원소, %d개 유니온 페어 (총 %v)\n",
			chunk+1, totalProcessedSets, totalProcessedElements, totalUnionPairs,
			time.Since(chunkStartTime))
	}

	// 3. Find 연산으로 결과 검증
	fmt.Println("무작위 Find 연산으로 검증 중...")
	findStart := time.Now()

	// 무작위 원소 선택 (테스트용)
	sampleSize := 10000
	sampleElements := make([]uint64, sampleSize)

	// 범위 내에서 무작위 원소 선택
	sampleGenStart := time.Now()
	for i := 0; i < sampleSize; i++ {
		// 0에서 ElementRange 사이의 값으로 설정
		sampleElements[i] = uint64(rand.Int63n(int64(MaxElement))) + 1
	}
	fmt.Printf("  샘플 생성 완료: %d개 (%v)\n", sampleSize, time.Since(sampleGenStart))

	// BatchFind 수행 - 작은 배치로 나누어 진행 상황 표시
	batchFindSize := 1000
	findCount := 0
	allRoots := make(map[uint64]uint64, sampleSize)

	for i := 0; i < sampleSize; i += batchFindSize {
		batchStart := time.Now()
		end := i + batchFindSize
		if end > sampleSize {
			end = sampleSize
		}

		batchElements := sampleElements[i:end]
		batchRoots := ds.BatchFind(batchElements)

		// 결과 병합
		for k, v := range batchRoots {
			allRoots[k] = v
		}

		findCount += len(batchElements)
		fmt.Printf("  Find 진행: %d/%d개 원소 (%d%%) - %v\n",
			findCount, sampleSize,
			findCount*100/sampleSize,
			time.Since(batchStart))
	}

	// 결과 요약
	uniqueRoots := make(map[uint64]int)
	for _, root := range allRoots {
		uniqueRoots[root]++
	}

	fmt.Printf("Find 연산 완료: %v (총 %d개 원소, %d개 고유 루트)\n",
		time.Since(findStart), sampleSize, len(uniqueRoots))

	// 루트별 분포 출력 (상위 10개)
	rootCounts := make([]struct {
		Root  uint64
		Count int
	}, 0, len(uniqueRoots))

	for root, count := range uniqueRoots {
		rootCounts = append(rootCounts, struct {
			Root  uint64
			Count int
		}{root, count})
	}

	// 개수로 정렬
	sort.Slice(rootCounts, func(i, j int) bool {
		return rootCounts[i].Count > rootCounts[j].Count
	})

	// 상위 10개 또는 전체 출력
	countToShow := 10
	if len(rootCounts) < countToShow {
		countToShow = len(rootCounts)
	}

	fmt.Println("  상위 루트 분포:")
	for i := 0; i < countToShow; i++ {
		fmt.Printf("    루트 %d: %d개 원소 (%.2f%%)\n",
			rootCounts[i].Root, rootCounts[i].Count,
			float64(rootCounts[i].Count)*100/float64(sampleSize))
	}

	// 4. 모든 변경사항 디스크에 반영
	fmt.Println("최종 상태 디스크에 반영 중...")
	finalFlushStart := time.Now()
	ds.Flush()
	fmt.Printf("최종 반영 완료: %v\n", time.Since(finalFlushStart))

	// 5. 통계 정보 출력
	fmt.Printf("\n=== 단계 %d 완료 ===\n", phase)
	fmt.Printf("총 처리: %d개 집합, %d개 원소, %d개 유니온 페어\n",
		totalProcessedSets, totalProcessedElements, totalUnionPairs)
	fmt.Printf("처리 속도: %.2f 집합/초, %.2f 원소/초, %.2f 유니온 페어/초\n",
		float64(totalProcessedSets)/time.Since(phaseStartTime).Seconds(),
		float64(totalProcessedElements)/time.Since(phaseStartTime).Seconds(),
		float64(totalUnionPairs)/time.Since(phaseStartTime).Seconds())
	fmt.Printf("단계별 처리 시간: %v\n", time.Since(phaseStartTime))

	stats := ds.GetStats()
	fmt.Println("현재 DisjointSet 통계:")
	fmt.Printf("총 원소 수: %d\n", stats["elementCount"])
	fmt.Printf("메모리에 로드된 청크 수: %d\n", stats["loadedChunks"])
	fmt.Printf("파일 크기: %.2f GB\n", float64(stats["fileSize"].(int64))/(1024*1024*1024))
}
