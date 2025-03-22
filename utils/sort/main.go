package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"time"
)

const (
	// 데이터 설정
	TotalElements = 2_100_000_000 // 21억 원소

	// 청크 설정
	NumChunks        = 100                       // 100개 청크로 분할
	ElementsPerChunk = TotalElements / NumChunks // 청크당 원소 수 (2100만)

	// 메모리 제한 (한 번에 처리할 수 있는 최대 원소 수)
	MaxElementsInMemory = 100_000_000 // 1억 원소(약 400MB)

	// 임시 파일 설정
	TempDir           = "temp_sort"
	ChunkFileTemplate = "chunk_%d.bin"
	MergeFileTemplate = "merge_%d_%d.bin"
	FinalSortedFile   = "sorted_data.bin"

	// 검증 설정
	ValidateSampleSize = 10_000_000 // 1천만 개 샘플로 검증
)

// 메모리 사용량 출력
func printMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("메모리 사용량: %d MB (Alloc: %d MB, Sys: %d MB)\n",
		m.Alloc/1024/1024, m.Alloc/1024/1024, m.Sys/1024/1024)
}

// 임시 디렉토리 생성
func createTempDir() error {
	if _, err := os.Stat(TempDir); os.IsNotExist(err) {
		return os.Mkdir(TempDir, 0755)
	}

	// 기존 파일 정리
	files, err := os.ReadDir(TempDir)
	if err != nil {
		return err
	}

	for _, file := range files {
		err := os.Remove(filepath.Join(TempDir, file.Name()))
		if err != nil {
			return err
		}
	}

	return nil
}

// 1. 랜덤 데이터 생성 및 청크별 정렬
func generateAndSortChunks() error {
	fmt.Println("1. 랜덤 데이터 생성 및 청크별 정렬 시작...")
	start := time.Now()

	// 병렬 처리를 위한 워커 풀
	numWorkers := runtime.NumCPU()
	var wg sync.WaitGroup

	// 청크 분배
	chunksPerWorker := NumChunks / numWorkers
	if NumChunks%numWorkers != 0 {
		chunksPerWorker++
	}

	// 에러 수집
	errChan := make(chan error, numWorkers)

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)

		startChunk := w * chunksPerWorker
		endChunk := (w + 1) * chunksPerWorker
		if endChunk > NumChunks {
			endChunk = NumChunks
		}

		go func(workerID, start, end int) {
			defer wg.Done()

			// 시드 분리로 각 워커가 다른 난수 생성
			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))

			for chunkID := start; chunkID < end; chunkID++ {
				// 진행상황 출력
				fmt.Printf("워커 %d: 청크 %d/%d 처리 중...\n", workerID, chunkID+1, end-start)

				// 청크 데이터 생성 (메모리에 들어갈 수 있는 크기)
				chunkData := make([]int32, ElementsPerChunk)

				// 1~21억 범위의 랜덤 값 생성
				for i := 0; i < ElementsPerChunk; i++ {
					chunkData[i] = r.Int31n(TotalElements) + 1
				}

				// 메모리 내 정렬
				slices.Sort(chunkData)

				// 정렬된 데이터 파일에 쓰기
				chunkFile := filepath.Join(TempDir, fmt.Sprintf(ChunkFileTemplate, chunkID))
				f, err := os.Create(chunkFile)
				if err != nil {
					errChan <- fmt.Errorf("청크 파일 생성 실패: %v", err)
					return
				}

				writer := bufio.NewWriter(f)
				for _, val := range chunkData {
					binary.Write(writer, binary.LittleEndian, val)
				}

				writer.Flush()
				f.Close()

				// 메모리 확보를 위해 명시적으로 참조 해제
				chunkData = nil
				runtime.GC()
			}

			fmt.Printf("워커 %d: 모든 청크 처리 완료\n", workerID)
		}(w, startChunk, endChunk)
	}

	// 모든 워커 완료 대기
	wg.Wait()
	close(errChan)

	// 에러 확인
	for err := range errChan {
		if err != nil {
			return err
		}
	}

	fmt.Printf("랜덤 데이터 생성 및 청크별 정렬 완료. 소요시간: %v\n", time.Since(start))
	printMemUsage()
	return nil
}

// 2. 청크 병합 (k-way merge)
func mergeChunks() error {
	fmt.Println("2. 청크 병합 시작...")
	start := time.Now()

	// 메모리 제한에 따라 한 번에 병합할 수 있는 청크 수 계산
	maxChunksPerMerge := MaxElementsInMemory / ElementsPerChunk
	if maxChunksPerMerge < 2 {
		maxChunksPerMerge = 2 // 최소 2개 청크 병합
	}

	// 단계별 병합
	currentChunks := NumChunks
	mergeRound := 0

	for currentChunks > 1 {
		mergeRound++
		fmt.Printf("병합 라운드 %d: %d개 청크 병합 중...\n", mergeRound, currentChunks)

		// 이번 라운드 결과 청크 수 계산
		resultChunks := (currentChunks + maxChunksPerMerge - 1) / maxChunksPerMerge

		// 병렬 처리용 워커 풀
		numWorkers := runtime.NumCPU()
		if resultChunks < numWorkers {
			numWorkers = resultChunks
		}

		var wg sync.WaitGroup
		errChan := make(chan error, numWorkers)

		mergesPerWorker := (resultChunks + numWorkers - 1) / numWorkers

		for w := 0; w < numWorkers; w++ {
			wg.Add(1)

			startMerge := w * mergesPerWorker
			endMerge := (w + 1) * mergesPerWorker
			if endMerge > resultChunks {
				endMerge = resultChunks
			}

			go func(workerID, start, end int) {
				defer wg.Done()

				for mergeIdx := start; mergeIdx < end; mergeIdx++ {
					// 이 병합 그룹의 시작과 끝 청크 인덱스
					startChunkIdx := mergeIdx * maxChunksPerMerge
					endChunkIdx := startChunkIdx + maxChunksPerMerge
					if endChunkIdx > currentChunks {
						endChunkIdx = currentChunks
					}

					// 단일 청크만 남은 경우 파일명만 변경
					if startChunkIdx+1 == endChunkIdx {
						srcFile := ""
						if mergeRound == 1 {
							srcFile = filepath.Join(TempDir, fmt.Sprintf(ChunkFileTemplate, startChunkIdx))
						} else {
							srcFile = filepath.Join(TempDir, fmt.Sprintf(MergeFileTemplate, mergeRound-1, startChunkIdx))
						}

						dstFile := filepath.Join(TempDir, fmt.Sprintf(MergeFileTemplate, mergeRound, mergeIdx))

						err := os.Rename(srcFile, dstFile)
						if err != nil {
							errChan <- fmt.Errorf("파일 이름 변경 실패: %v", err)
							return
						}
						continue
					}

					// 여러 청크 병합
					fmt.Printf("워커 %d: 청크 %d~%d 병합 중...\n", workerID, startChunkIdx, endChunkIdx-1)

					// k-way 병합 수행
					err := kWayMerge(mergeRound, startChunkIdx, endChunkIdx, mergeIdx)
					if err != nil {
						errChan <- err
						return
					}
				}
			}(w, startMerge, endMerge)
		}

		// 모든 워커 완료 대기
		wg.Wait()
		close(errChan)

		// 에러 확인
		for err := range errChan {
			if err != nil {
				return err
			}
		}

		// 병합 후 청크 수 업데이트
		currentChunks = resultChunks

		// 메모리 정리
		runtime.GC()
		printMemUsage()
	}

	// 최종 결과 파일 이름 변경
	finalMergeFile := filepath.Join(TempDir, fmt.Sprintf(MergeFileTemplate, mergeRound, 0))
	finalFile := filepath.Join(TempDir, FinalSortedFile)

	err := os.Rename(finalMergeFile, finalFile)
	if err != nil {
		return fmt.Errorf("최종 파일 이름 변경 실패: %v", err)
	}

	fmt.Printf("청크 병합 완료. 소요시간: %v\n", time.Since(start))
	return nil
}

// k-way 병합 (여러 청크를 한 번에 병합)
func kWayMerge(round, startChunkIdx, endChunkIdx, resultIdx int) error {
	// 각 청크 파일 오픈
	chunkCount := endChunkIdx - startChunkIdx
	files := make([]*os.File, chunkCount)
	readers := make([]*bufio.Reader, chunkCount)
	buffers := make([]int32, chunkCount)
	valid := make([]bool, chunkCount)

	// 초기화
	for i := 0; i < chunkCount; i++ {
		var filePath string
		if round == 1 {
			// 첫 번째 라운드는 원본 청크 파일 사용
			filePath = filepath.Join(TempDir, fmt.Sprintf(ChunkFileTemplate, startChunkIdx+i))
		} else {
			// 이후 라운드는 이전 병합 결과 사용
			filePath = filepath.Join(TempDir, fmt.Sprintf(MergeFileTemplate, round-1, startChunkIdx+i))
		}

		f, err := os.Open(filePath)
		if err != nil {
			return fmt.Errorf("청크 파일 열기 실패: %v", err)
		}
		defer f.Close()

		files[i] = f
		readers[i] = bufio.NewReader(f)

		// 첫 원소 읽기
		err = binary.Read(readers[i], binary.LittleEndian, &buffers[i])
		if err == nil {
			valid[i] = true
		} else if err != io.EOF {
			return fmt.Errorf("청크 데이터 읽기 실패: %v", err)
		}
	}

	// 결과 파일 생성
	resultFile := filepath.Join(TempDir, fmt.Sprintf(MergeFileTemplate, round, resultIdx))
	out, err := os.Create(resultFile)
	if err != nil {
		return fmt.Errorf("결과 파일 생성 실패: %v", err)
	}
	defer out.Close()

	writer := bufio.NewWriter(out)
	defer writer.Flush()

	// 버퍼 크기 조정 (성능 향상)
	const bufferSize = 4 * 1024 * 1024 // 4MB
	outBuf := make([]int32, 0, bufferSize/4)

	// k-way 병합 수행
	for {
		// 모든 청크를 다 처리했는지 확인
		active := false
		for i := 0; i < chunkCount; i++ {
			if valid[i] {
				active = true
				break
			}
		}

		if !active {
			break
		}

		// 현재 가장 작은 값 찾기
		minIdx := -1
		var minVal int32

		for i := 0; i < chunkCount; i++ {
			if valid[i] && (minIdx == -1 || buffers[i] < minVal) {
				minIdx = i
				minVal = buffers[i]
			}
		}

		// 최소값을 결과에 쓰기
		outBuf = append(outBuf, minVal)

		// 버퍼가 가득 차면 디스크에 쓰기
		if len(outBuf) == cap(outBuf) {
			for _, val := range outBuf {
				binary.Write(writer, binary.LittleEndian, val)
			}
			outBuf = outBuf[:0]
		}

		// 다음 값 읽기
		var nextVal int32
		err := binary.Read(readers[minIdx], binary.LittleEndian, &nextVal)
		if err == nil {
			buffers[minIdx] = nextVal
		} else if err == io.EOF {
			valid[minIdx] = false
		} else {
			return fmt.Errorf("청크 데이터 읽기 실패: %v", err)
		}
	}

	// 남은 버퍼 쓰기
	for _, val := range outBuf {
		binary.Write(writer, binary.LittleEndian, val)
	}

	return nil
}

// 3. 정렬 결과 검증
func validateSorting() error {
	fmt.Println("3. 정렬 결과 검증 중...")
	start := time.Now()

	// 정렬된 파일 열기
	sortedFile := filepath.Join(TempDir, FinalSortedFile)
	file, err := os.Open(sortedFile)
	if err != nil {
		return fmt.Errorf("정렬 파일 열기 실패: %v", err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	// 검증 (샘플링)
	samplingInterval := TotalElements / ValidateSampleSize
	if samplingInterval < 1 {
		samplingInterval = 1
	}

	var prev int32
	var current int32
	var count int64
	var samplesChecked int

	err = binary.Read(reader, binary.LittleEndian, &prev)
	if err != nil {
		return fmt.Errorf("첫 번째 원소 읽기 실패: %v", err)
	}
	count = 1
	samplesChecked = 1

	// 첫 번째 값은 1 이상이어야 함
	if prev < 1 {
		return fmt.Errorf("유효하지 않은 값 발견: %d (인덱스: 0)", prev)
	}

	fmt.Println("첫 번째 값:", prev)

	for {
		// 원소 읽기
		err = binary.Read(reader, binary.LittleEndian, &current)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("데이터 읽기 실패: %v", err)
		}

		count++

		// 샘플링 간격으로 검증
		if count%int64(samplingInterval) == 0 {
			samplesChecked++

			// 검증: 현재 값은 이전 값보다 크거나 같아야 함
			if current < prev {
				return fmt.Errorf("정렬 오류 발견: %d > %d (인덱스: %d)", prev, current, count-1)
			}

			// 검증: 값은 1~21억 범위 내여야 함
			if current < 1 || current > TotalElements {
				return fmt.Errorf("유효하지 않은 값 발견: %d (인덱스: %d)", current, count-1)
			}

			prev = current

			// 진행 상황 출력
			if samplesChecked%1000000 == 0 {
				fmt.Printf("검증 진행 중: %d/%d 샘플 검사 (현재 값: %d)\n",
					samplesChecked, ValidateSampleSize, current)
			}
		}
	}

	fmt.Printf("마지막 값: %d\n", prev)
	fmt.Printf("총 %d개 원소 중 %d개 샘플 검증 완료\n", count, samplesChecked)

	if count != TotalElements {
		return fmt.Errorf("원소 수 불일치: 예상 %d개, 실제 %d개", TotalElements, count)
	}

	fmt.Printf("정렬 검증 완료. 소요시간: %v\n", time.Since(start))
	return nil
}

func main() {
	// 프로그램 시작 시간
	startTime := time.Now()

	// 임시 디렉토리 생성
	fmt.Println("임시 디렉토리 생성...")
	if err := createTempDir(); err != nil {
		fmt.Printf("임시 디렉토리 생성 실패: %v\n", err)
		return
	}

	// 초기 메모리 상태
	fmt.Println("초기 메모리 상태:")
	printMemUsage()

	// 1. 랜덤 데이터 생성 및 청크별 정렬
	if err := generateAndSortChunks(); err != nil {
		fmt.Printf("데이터 생성 및 정렬 실패: %v\n", err)
		return
	}

	// 2. 청크 병합
	if err := mergeChunks(); err != nil {
		fmt.Printf("청크 병합 실패: %v\n", err)
		return
	}

	// 3. 정렬 결과 검증
	if err := validateSorting(); err != nil {
		fmt.Printf("정렬 검증 실패: %v\n", err)
		return
	}

	// 최종 결과 및 통계
	fmt.Println("\n======= 최종 결과 =======")
	fmt.Printf("총 원소 수: %d\n", TotalElements)
	fmt.Printf("총 소요 시간: %v\n", time.Since(startTime))

	// 최종 메모리 사용량
	fmt.Println("최종 메모리 사용량:")
	printMemUsage()

	fmt.Println("\n정렬된 파일 위치:", filepath.Join(TempDir, FinalSortedFile))
	fmt.Println("프로그램 완료!")
}
