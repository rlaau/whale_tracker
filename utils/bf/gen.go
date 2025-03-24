package main

import (
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"time"

	"net/http"
	_ "net/http/pprof"
)

const (
	ChunkSize      = 512         // 5000만개씩 처리
	SetChunkSize   = 100_000_000 // 집합 생성 시 한 번에 처리할 집합 수
	DataDir        = "./data"    // 데이터 디렉토리
	TestResultsDir = "./results" // 테스트 결과 디렉토리
)

// 테스트용 상수
const (
	SampleSetCount = 1000 // 샘플링할 집합 수
)

// GenerateSets 함수 - 메모리 효율성 개선
func GenerateSets(rangeValue uint64, setSize int, count int) []UserSet {
	const genChunkSize = 10_000 // 한 번에 생성할 집합 수

	result := make([]UserSet, 0, count)

	// 작은 청크로 나누어 진행
	for len(result) < count {
		// 현재 청크에서 생성할 집합 수 결정
		currentChunkSize := genChunkSize
		if count-len(result) < genChunkSize {
			currentChunkSize = count - len(result)
		}

		// 지역 맵을 사용하여 청크마다 메모리 해제
		usedElements := make(map[uint64]bool)

		for i := 0; i < currentChunkSize; i++ {
			elements := make([]uint64, 0, setSize)

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
				elements = append(elements, element)
				usedElements[element] = true
			}

			// UserSet 생성 (elements 필드와 size 필드를 올바르게 설정)
			result = append(result, UserSet{
				Size:     uint16(len(elements)),
				Elements: elements,
			})
		}

		// 청크 단위로 메모리 정리
		usedElements = nil
		if len(result)%10_000_000 == 0 {
			runtime.GC()
			fmt.Printf("  %d/%d 집합 생성 완료\n", len(result), count)
		}
	}

	runtime.GC()
	return result
}

// GenerateAllSets 함수 - 다양한 크기의 집합을 생성
func GenerateAllSets(numSets uint64, rangeValue uint64) []UserSet {
	// 분포 계산
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

// 파일에 집합 저장
func SaveSetsToFile(sets []UserSet, filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("파일 생성 실패: %w", err)
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	err = encoder.Encode(sets)
	if err != nil {
		return fmt.Errorf("인코딩 실패: %w", err)
	}

	return nil
}

// 파일에서 집합 로드
func LoadSetsFromFile(filename string) ([]UserSet, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("파일 열기 실패: %w", err)
	}
	defer file.Close()

	var sets []UserSet
	decoder := gob.NewDecoder(file)
	err = decoder.Decode(&sets)
	if err != nil {
		return nil, fmt.Errorf("디코딩 실패: %w", err)
	}

	return sets, nil
}

// 청크 단위로 집합을 생성하고 파일에 저장하는 함수
func GenerateAndSaveSets(numSets uint64, rangeValue uint64, outputDir string) (int, error) {
	// 디렉토리 생성
	err := os.MkdirAll(outputDir, 0755)
	if err != nil {
		return 0, fmt.Errorf("디렉토리 생성 실패: %w", err)
	}

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
		chunkSets := GenerateAllSets(uint64(chunkSetCount), rangeValue)

		// 파일에 저장
		fileName := filepath.Join(outputDir, fmt.Sprintf("chunk_%d.gob", chunk))
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

// 테스트 결과를 저장하는 구조체
type TestResult struct {
	TotalProcessingTime time.Duration
	SingleSetTime       time.Duration
	MultiSetTime        time.Duration
	TotalSets           int
	SingleSets          int
	MultiSets           int
	MaxMemoryUsage      uint64 // MB 단위
}

// 메모리 사용량 측정 함수
func GetMemoryUsage() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.Alloc / (1024 * 1024) // MB 단위로 변환
}

// 랜덤 샘플 집합 확인 함수
func VerifyRandomSamples(processor *IncrementalUnionFindProcessor, count int) error {
	// 랜덤하게 1부터 rangeValue 사이의 숫자를 선택하여 존재 여부 확인
	rangeValue := uint64(4_000_000_000)
	for i := 0; i < count; i++ {
		element := uint64(rand.Int63n(int64(rangeValue))) + 1

		// Find 함수 호출
		root, err := processor.Find(element)
		if err != nil {
			fmt.Printf("요소 %d는 존재하지 않음: %v\n", element, err)
			continue
		}

		// 집합 크기 확인
		size, err := processor.Size(element)
		if err != nil {
			fmt.Printf("요소 %d의 집합 크기 구하기 실패: %v\n", element, err)
			continue
		}

		fmt.Printf("요소 %d는 루트 %d를 가진 크기 %d의 집합에 속함\n",
			element, root, size)
	}

	return nil
}
func autoDumpHeap() {
	os.MkdirAll("heap_profiles", 0755)
	for {
		timestamp := time.Now().Format("20060102_150405")
		path := "heap_profiles/heap_" + timestamp + ".prof"

		f, err := os.Create(path)
		if err == nil {
			pprof.WriteHeapProfile(f)
			f.Close()
		} else {
			log.Printf("failed to create heap profile: %v\n", err)
		}
		time.Sleep(5 * time.Second) // 5초마다 찍기
	}
}

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
	go autoDumpHeap()

	// 시드 설정

	// 테스트 파라미터 설정
	totalSets := uint64(200_000_000)         // 2억 집합
	rangeValue := uint64(300_000_000)        // 20억 범위
	initialCapacity := uint64(4_000_000_000) // 40억 엔트리 용량

	// 결과 디렉토리 생성
	err := os.MkdirAll(TestResultsDir, 0755)
	if err != nil {
		fmt.Printf("결과 디렉토리 생성 실패: %v\n", err)
		return
	}

	// 테스트 결과 파일 열기
	resultFile, err := os.Create(filepath.Join(TestResultsDir, "test_result.txt"))
	if err != nil {
		fmt.Printf("결과 파일 생성 실패: %v\n", err)
		return
	}
	defer resultFile.Close()

	// 로그 출력 함수
	logf := func(format string, args ...interface{}) {
		message := fmt.Sprintf(format, args...)
		fmt.Print(message)
		resultFile.WriteString(message)
	}

	logf("=== Incremental Union-Find Processor 성능 테스트 ===\n")
	logf("시작 시간: %s\n", time.Now().Format("2006-01-02 15:04:05"))
	logf("총 집합 수: %d\n", totalSets)
	logf("범위 값: %d\n", rangeValue)
	logf("초기 용량: %d\n\n", initialCapacity)

	// 1. 유니온-파인드 프로세서 초기화
	logf("1. Union-Find 프로세서 초기화 중...\n")
	startInit := time.Now()

	processor, err := NewIncrementalUnionFindProcessor(DataDir, initialCapacity)
	if err != nil {
		logf("초기화 실패: %v\n", err)
		return
	}
	defer processor.Close()

	initTime := time.Since(startInit)
	logf("초기화 완료! (소요 시간: %v)\n\n", initTime)

	// 측정을 위한 변수들
	var testResult TestResult
	testResult.TotalSets = int(totalSets)

	// 메모리 사용량 주기적으로 확인
	maxMemoryUsage := GetMemoryUsage()

	// 2. 데이터 처리
	// Plan A: 모든 데이터를 메모리에 한 번에 로드하여 처리
	logf("2. 집합 생성 및 처리 시작...\n")

	// Plan A 시도 (메모리 부담이 있음)
	planA := true
	var allSets []UserSet

	if planA {
		// 하나의 큰 데이터셋으로 처리 시도
		logf("Plan A: 전체 집합을 한 번에 처리 시도\n")

		// GenerateAllSets 함수로 데이터셋 생성
		logf("집합 생성 중...\n")
		startGenerate := time.Now()

		// 총 집합 수가 너무 크면 청크로 나누어 생성 (메모리 고려)
		// if totalSets > 50_000_000 {
		// 	chunkSize := uint64(50_000_000) // 5천만 개 단위로 생성
		// 	for i := uint64(0); i < totalSets; i += chunkSize {
		// 		currentChunkSize := chunkSize
		// 		if i+chunkSize > totalSets {
		// 			currentChunkSize = totalSets - i
		// 		}

		// 		logf("  %d/%d 집합 생성 중...\n", i+1, totalSets)
		// 		chunk := GenerateAllSets(currentChunkSize, rangeValue)
		// 		allSets = append(allSets, chunk...)

		// 		// 메모리 사용량 체크
		// 		currentMemory := GetMemoryUsage()
		// 		if currentMemory > maxMemoryUsage {
		// 			maxMemoryUsage = currentMemory
		// 		}

		// 		// 메모리 사용량이 너무 높으면 Plan B로 전환
		// 		if currentMemory > 32*1024 { // 32GB 초과
		// 			logf("  경고: 메모리 사용량이 32GB를 초과했습니다 (%d MB). Plan B로 전환합니다.\n",
		// 				currentMemory)
		// 			planA = false
		// 			break
		// 		}
		// 	}
		// } else {
		allSets = GenerateAllSets(totalSets, rangeValue)
		//}

		generateTime := time.Since(startGenerate)
		logf("집합 생성 완료! (소요 시간: %v)\n", generateTime)

		if planA {
			// 메모리 사용량 체크
			currentMemory := GetMemoryUsage()
			if currentMemory > maxMemoryUsage {
				maxMemoryUsage = currentMemory
			}

			// 전체 집합 처리
			logf("전체 집합 처리 중...\n")
			startProcessing := time.Now()

			// 처리할 집합 배치 크기
			batchSize := 100_000_000 // 1djr 개씩 처리

			for i := 0; i < len(allSets); i += batchSize {
				end := i + batchSize
				if end > len(allSets) {
					end = len(allSets)
				}

				// 단일 집합과 다중 집합 분리 처리 시간 측정
				singleStart := time.Now()
				err, multiSets := processor.ProcessSingleSets(allSets[i:end])
				if err != nil {
					logf("단일 집합 처리 실패: %v\n", err)
					return
				} else {
					fmt.Println("단일 집합 처리 성공")
				}
				singleTime := time.Since(singleStart)
				testResult.SingleSetTime += singleTime
				testResult.SingleSets += (end - i) - len(multiSets)

				multiStart := time.Now()
				err = processor.ProcessMultiSets(multiSets)
				if err != nil {
					logf("다중 집합 처리 실패: %v\n", err)
					return
				} else {
					fmt.Println("다중 집합 처리 성공")
				}
				multiTime := time.Since(multiStart)
				testResult.MultiSetTime += multiTime
				testResult.MultiSets += len(multiSets)

				logf("  %d/%d 집합 처리 완료 (단일: %v, 다중: %v)\n",
					end, len(allSets), singleTime, multiTime)

				// 메모리 사용량 체크
				currentMemory := GetMemoryUsage()
				if currentMemory > maxMemoryUsage {
					maxMemoryUsage = currentMemory
				}
			}

			processingTime := time.Since(startProcessing)
			logf("집합 처리 완료! (소요 시간: %v)\n", processingTime)
			testResult.TotalProcessingTime = processingTime
		}
	}

	// Plan B: 파일로 나누어 처리
	if !planA {
		logf("Plan B: 파일로 나누어 처리\n")

		// 테스트 데이터 생성 및 저장
		dataDir := filepath.Join(TestResultsDir, "sets")
		err := os.MkdirAll(dataDir, 0755)
		if err != nil {
			logf("데이터 디렉토리 생성 실패: %v\n", err)
			return
		}

		startGenerate := time.Now()
		totalGenerated, err := GenerateAndSaveSets(totalSets, rangeValue, dataDir)
		if err != nil {
			logf("집합 생성 및 저장 실패: %v\n", err)
			return
		}
		generateTime := time.Since(startGenerate)
		logf("집합 생성 및 저장 완료! (소요 시간: %v, 총 %d개 집합)\n",
			generateTime, totalGenerated)

		// 파일에서 데이터 로드하여 처리
		files, err := filepath.Glob(filepath.Join(dataDir, "chunk_*.gob"))
		if err != nil {
			logf("데이터 파일 찾기 실패: %v\n", err)
			return
		}

		startProcessing := time.Now()
		processed := 0

		for _, file := range files {
			logf("파일 %s 처리 중...\n", filepath.Base(file))

			// 파일에서 집합 로드
			sets, err := LoadSetsFromFile(file)
			if err != nil {
				logf("파일 로드 실패: %v\n", err)
				continue
			}

			// 배치 처리
			batchSize := 1_000_000 // 백만 개씩 처리

			for i := 0; i < len(sets); i += batchSize {
				end := i + batchSize
				if end > len(sets) {
					end = len(sets)
				}

				// 단일 집합과 다중 집합 분리 처리 시간 측정
				singleStart := time.Now()
				err, multiSets := processor.ProcessSingleSets(sets[i:end])
				if err != nil {
					logf("단일 집합 처리 실패: %v\n", err)
					return
				}
				singleTime := time.Since(singleStart)
				testResult.SingleSetTime += singleTime
				testResult.SingleSets += (end - i) - len(multiSets)

				multiStart := time.Now()
				err = processor.ProcessMultiSets(multiSets)
				if err != nil {
					logf("다중 집합 처리 실패: %v\n", err)
					return
				}
				multiTime := time.Since(multiStart)
				testResult.MultiSetTime += multiTime
				testResult.MultiSets += len(multiSets)

				processed += (end - i)
				logf("  %d/%d 집합 처리 완료 (전체 진행: %d/%d, 단일: %v, 다중: %v)\n",
					end-i, len(sets), processed, totalGenerated, singleTime, multiTime)

				// 메모리 사용량 체크
				currentMemory := GetMemoryUsage()
				if currentMemory > maxMemoryUsage {
					maxMemoryUsage = currentMemory
				}
			}

			// 처리 후 메모리 정리
			sets = nil
			runtime.GC()
		}

		processingTime := time.Since(startProcessing)
		logf("집합 처리 완료! (소요 시간: %v)\n", processingTime)
		testResult.TotalProcessingTime = processingTime
	}

	// 3. 검증 단계
	logf("\n3. 결과 검증 중...\n")

	// 랜덤 샘플 확인
	logf("랜덤 샘플 %d개 확인 중...\n", SampleSetCount)
	err = VerifyRandomSamples(processor, SampleSetCount)
	if err != nil {
		logf("샘플 확인 실패: %v\n", err)
	}

	// 4. 결과 요약
	testResult.MaxMemoryUsage = maxMemoryUsage

	logf("\n=== 테스트 결과 요약 ===\n")
	logf("총 집합 수: %d\n", testResult.TotalSets)
	logf("단일 집합 수: %d\n", testResult.SingleSets)
	logf("다중 집합 수: %d\n", testResult.MultiSets)
	logf("총 처리 시간: %v\n", testResult.TotalProcessingTime)
	logf("단일 집합 처리 시간: %v\n", testResult.SingleSetTime)
	logf("다중 집합 처리 시간: %v\n", testResult.MultiSetTime)
	logf("최대 메모리 사용량: %d MB\n", testResult.MaxMemoryUsage)
	logf("종료 시간: %s\n", time.Now().Format("2006-01-02 15:04:05"))

	// 5. 성능 통계
	if testResult.TotalSets > 0 {
		setsPerSecond := float64(testResult.TotalSets) / testResult.TotalProcessingTime.Seconds()
		logf("초당 처리 집합 수: %.2f sets/sec\n", setsPerSecond)
	}

	if testResult.SingleSets > 0 {
		singleSetsPerSecond := float64(testResult.SingleSets) / testResult.SingleSetTime.Seconds()
		logf("초당 단일 집합 처리 수: %.2f sets/sec\n", singleSetsPerSecond)
	}

	if testResult.MultiSets > 0 {
		multiSetsPerSecond := float64(testResult.MultiSets) / testResult.MultiSetTime.Seconds()
		logf("초당 다중 집합 처리 수: %.2f sets/sec\n", multiSetsPerSecond)
	}

	logf("\n테스트 완료!\n")
}
