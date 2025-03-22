package keyvalDB

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v3"
)

// 글로벌 카운터 키
var badgerCounterKey = []byte("global_counter")

// BadgerBenchmark 구조체는 BadgerDB 벤치마크를 위한 설정을 담고 있습니다
type BadgerBenchmark struct {
	// 문자열 개수 (기본 2억개)
	StringCount int
	// 문자열 크기 (기본 20바이트)
	StringSize int
	// 병렬 처리 워커 수 (기본 CPU 코어 수)
	NumWorkers int
	// DB 경로
	DBPath string
	// 로깅 여부
	Verbose bool
	// 글로벌 카운터에 대한 뮤텍스
	counterMutex sync.Mutex
}

// NewBadgerBenchmark는 기본 설정으로 벤치마크 인스턴스를 생성합니다
func NewBadgerBenchmark() *BadgerBenchmark {
	return &BadgerBenchmark{
		StringCount: 200_000_000, // 2억개
		StringSize:  20,
		NumWorkers:  runtime.NumCPU(),
		DBPath:      "./data/badger",
		Verbose:     true,
	}
}

// Run은 BadgerDB 벤치마크를 실행합니다
func (b *BadgerBenchmark) Run() error {
	// DB 경로가 존재하는지 확인하고 필요하면 생성
	if err := os.MkdirAll(b.DBPath, 0755); err != nil {
		return fmt.Errorf("DB 디렉토리 생성 실패: %w", err)
	}

	// 기존 DB 삭제
	if err := os.RemoveAll(b.DBPath); err != nil {
		return fmt.Errorf("기존 DB 삭제 실패: %w", err)
	}
	if err := os.MkdirAll(b.DBPath, 0755); err != nil {
		return fmt.Errorf("DB 디렉토리 생성 실패: %w", err)
	}

	// BadgerDB 옵션 설정 (성능 최적화)
	opts := badger.DefaultOptions(b.DBPath)
	opts.Logger = nil                 // 로깅 비활성화
	opts.SyncWrites = false           // 동기화 비활성화로 성능 향상
	opts.NumVersionsToKeep = 1        // 이전 버전 유지 안함
	opts.NumLevelZeroTables = 8       // 레벨 0 테이블 수 증가
	opts.NumLevelZeroTablesStall = 16 // 병렬 쓰기를 위한 스톨 임계값 증가
	opts.ValueLogFileSize = 1 << 28   // 256MB 값 로그 파일
	opts.NumMemtables = 8             // 메모리 테이블 수 증가
	opts.BlockSize = 16 * 1024        // 디스크 I/O 최적화
	opts.BlockCacheSize = 1 << 30     // 1GB 블록 캐시
	opts.IndexCacheSize = 200 << 20   // 200MB 인덱스 캐시
	opts.ValueThreshold = 256         // 값 임계값 설정
	opts.Compression = 1
	// 트랜잭션 충돌 관련 설정 개선
	opts.DetectConflicts = false          // 충돌 감지 비활성화로 성능 향상 (우리는 직접 뮤텍스로 제어)
	opts.NumCompactors = runtime.NumCPU() // 압축기 수 증가

	// BadgerDB 열기
	db, err := badger.Open(opts)
	if err != nil {
		return fmt.Errorf("BadgerDB 열기 실패: %w", err)
	}
	defer db.Close()

	// 글로벌 카운터 초기화
	if err := initializeCounter(db); err != nil {
		return fmt.Errorf("글로벌 카운터 초기화 실패: %w", err)
	}

	if b.Verbose {
		fmt.Printf("BadgerDB 벤치마크 시작 (문자열 수: %d, 워커 수: %d)\n", b.StringCount, b.NumWorkers)
	}

	// 1단계: 벤치마크 시작 (시간 측정)
	startTime := time.Now()

	// 더 작은 배치 크기로 조정하여 충돌 가능성 감소
	batchSize := 10000 // 기존 10000에서 5000으로 감소
	numBatches := (b.StringCount + batchSize - 1) / batchSize

	// 워커 수 제한 (너무 많은 워커가 동시에 돌면 충돌 가능성 증가)
	actualWorkers := b.NumWorkers
	if actualWorkers > 16 {
		actualWorkers = 16 // 최대 16개로 제한
		if b.Verbose {
			fmt.Printf("워커 수를 16개로 제한합니다 (원래: %d)\n", b.NumWorkers)
		}
	}

	// 병렬 처리를 위한 워커 풀 생성
	var wg sync.WaitGroup
	errChan := make(chan error, numBatches)
	semaphore := make(chan struct{}, actualWorkers)
	var successCount, failCount int64

	// 2단계: 문자열 생성 및 ID 부여 (병렬 처리)
	for batchIdx := 0; batchIdx < numBatches; batchIdx++ {
		wg.Add(1)
		semaphore <- struct{}{} // 세마포어 획득

		go func(idx int) {
			defer wg.Done()
			defer func() { <-semaphore }() // 세마포어 반환

			startOffset := idx * batchSize
			endOffset := (idx + 1) * batchSize
			if endOffset > b.StringCount {
				endOffset = b.StringCount
			}
			count := endOffset - startOffset

			// 각 키 생성
			keys := make([][]byte, 0, count)
			for i := 0; i < count; i++ {
				stringIdx := startOffset + i
				key := generateKey(stringIdx, b.StringSize)
				keys = append(keys, key)
			}

			// 현재 배치의 성공, 실패 카운트
			var batchSuccess, batchFail int64

			// 트랜잭션 재시도 횟수와 지수 백오프 설정 추가
			maxRetries := 10 // 최대 재시도 횟수 증가
			baseDelay := 20 * time.Millisecond

			for retry := 0; retry < maxRetries; retry++ {
				// 지수 백오프로 대기 시간 계산 (첫 시도는 대기 없음)
				if retry > 0 {
					retryDelay := baseDelay * time.Duration(1<<uint(retry-1))
					// 최대 3초까지만 대기
					if retryDelay > 3*time.Second {
						retryDelay = 3 * time.Second
					}
					time.Sleep(retryDelay)
				}

				// 1. 읽기 트랜잭션으로 존재하지 않는 키만 찾기
				newKeys := make([][]byte, 0, count)
				existingKeys := make(map[string]uint64)

				err := db.View(func(txn *badger.Txn) error {
					for _, key := range keys {
						item, err := txn.Get(key)
						if err == badger.ErrKeyNotFound {
							// 키가 존재하지 않음
							newKeys = append(newKeys, key)
						} else if err != nil {
							return fmt.Errorf("키 조회 실패: %w", err)
						} else {
							// 키가 존재하면 ID 획득
							err = item.Value(func(val []byte) error {
								if len(val) != 8 {
									return fmt.Errorf("잘못된 ID 크기: %d", len(val))
								}
								existingKeys[string(key)] = binary.BigEndian.Uint64(val)
								return nil
							})
							if err != nil {
								return err
							}
						}
					}
					return nil
				})

				if err != nil {
					if retry == maxRetries-1 {
						errChan <- fmt.Errorf("배치 %d: 키 조회 최대 재시도 횟수 초과: %w", idx, err)
						atomic.AddInt64(&failCount, int64(count))
						return
					}
					continue // 재시도
				}

				// 이미 존재하는 키는 성공으로 카운트
				atomic.AddInt64(&batchSuccess, int64(len(existingKeys)))

				// 2. 새 키가 있다면 ID 할당 및 저장
				if len(newKeys) > 0 {
					// 글로벌 카운터에 대한 경쟁 상태를 방지하기 위해 글로벌 뮤텍스 사용
					// 로컬 뮤텍스(batchMutex)가 아닌 벤치마크 인스턴스의 뮤텍스를 사용
					b.counterMutex.Lock()

					// 필요한 ID 수만큼 카운터 증가 및 시작 ID 획득
					startID, err := getNextBatchIDs(db, uint64(len(newKeys)))

					// ID 할당 완료 후 즉시 뮤텍스 해제 (다른 고루틴이 빨리 진행할 수 있도록)
					b.counterMutex.Unlock()

					if err != nil {
						if retry == maxRetries-1 {
							errChan <- fmt.Errorf("배치 %d: ID 할당 최대 재시도 횟수 초과: %w", idx, err)
							atomic.AddInt64(&failCount, int64(len(newKeys)))
							return
						}
						continue // 재시도
					}

					// 배치 쓰기
					wb := db.NewWriteBatch()
					defer wb.Cancel() // 자원 누수 방지

					// 쓰기 성공한 키 수
					successKeysCount := int64(0)

					for i, key := range newKeys {
						id := startID + uint64(i)
						idBytes := make([]byte, 8)
						binary.BigEndian.PutUint64(idBytes, id)

						// 키->ID 저장
						if err := wb.Set(key, idBytes); err != nil {
							errChan <- fmt.Errorf("배치 %d: 키 저장 실패: %w", idx, err)
							continue
						}

						// ID->키 저장 (역방향 조회를 위해)
						idKey := make([]byte, 9)
						idKey[0] = 'i' // ID 키임을 표시
						binary.BigEndian.PutUint64(idKey[1:], id)
						if err := wb.Set(idKey, key); err != nil {
							errChan <- fmt.Errorf("배치 %d: ID 키 저장 실패: %w", idx, err)
							continue
						}

						successKeysCount++
					}

					// 배치 쓰기 실행
					if err := wb.Flush(); err != nil {
						// badger.ErrConflict 또는 다른 트랜잭션 충돌 에러 검사
						if retry == maxRetries-1 {
							errChan <- fmt.Errorf("배치 %d: 배치 쓰기 최대 재시도 횟수 초과: %w", idx, err)
							atomic.AddInt64(&failCount, int64(len(newKeys)))
							return
						}
						continue // 재시도
					}

					// 성공한 키 수 추가
					atomic.AddInt64(&batchSuccess, successKeysCount)
					// 실패한 키 수 추가
					atomic.AddInt64(&batchFail, int64(len(newKeys))-successKeysCount)
				}

				// 모든 작업이 성공적으로 완료됨
				break
			}

			// 현재 배치의 성공/실패 카운트를 전체 카운트에 반영
			atomic.AddInt64(&successCount, batchSuccess)
			atomic.AddInt64(&failCount, batchFail)

			// 진행 상황 출력 (100개 배치마다)
			if b.Verbose && idx%100 == 0 {
				fmt.Printf("배치 %d/%d 완료 (%.2f%%)\n", idx, numBatches, float64(idx*100)/float64(numBatches))
			}
		}(batchIdx)
	}

	// 모든 쓰기 작업 완료 대기
	wg.Wait()
	close(errChan)

	// 에러 확인
	errCount := 0
	for err := range errChan {
		if errCount < 10 { // 처음 10개 에러만 출력
			fmt.Printf("에러: %v\n", err)
		}
		errCount++
	}
	if errCount > 10 {
		fmt.Printf("... 그 외 %d개 에러\n", errCount-10)
	}

	writeTime := time.Since(startTime)
	if b.Verbose {
		fmt.Printf("쓰기 작업 완료: %v (성공: %d, 실패: %d)\n", writeTime, successCount, failCount)
	}

	// 3단계: 인코딩-디코딩 확인
	startVerify := time.Now()
	var verifySuccess, verifyFail int64

	// 검증을 위한 워커 풀 생성
	for batchIdx := 0; batchIdx < numBatches; batchIdx++ {
		wg.Add(1)
		semaphore <- struct{}{} // 세마포어 획득

		go func(idx int) {
			defer wg.Done()
			defer func() { <-semaphore }() // 세마포어 반환

			startOffset := idx * batchSize
			endOffset := (idx + 1) * batchSize
			if endOffset > b.StringCount {
				endOffset = b.StringCount
			}
			count := endOffset - startOffset

			// 각 키에 대해 검증
			err := db.View(func(txn *badger.Txn) error {
				for i := 0; i < count; i++ {
					stringIdx := startOffset + i
					key := generateKey(stringIdx, b.StringSize)

					// 키->ID 검증
					item, err := txn.Get(key)
					if err == badger.ErrKeyNotFound {
						// 이 키는 등록되지 않았으므로 검증 불필요
						continue
					} else if err != nil {
						atomic.AddInt64(&verifyFail, 1)
						continue
					}

					var id uint64
					err = item.Value(func(val []byte) error {
						if len(val) != 8 {
							return fmt.Errorf("잘못된 ID 크기: %d", len(val))
						}
						id = binary.BigEndian.Uint64(val)
						return nil
					})

					if err != nil {
						atomic.AddInt64(&verifyFail, 1)
						continue
					}

					// ID->키 검증
					idKey := make([]byte, 9)
					idKey[0] = 'i'
					binary.BigEndian.PutUint64(idKey[1:], id)

					item, err = txn.Get(idKey)
					if err != nil {
						atomic.AddInt64(&verifyFail, 1)
						continue
					}

					err = item.Value(func(val []byte) error {
						if string(val) != string(key) {
							return fmt.Errorf("키 불일치")
						}
						return nil
					})

					if err != nil {
						atomic.AddInt64(&verifyFail, 1)
					} else {
						atomic.AddInt64(&verifySuccess, 1)
					}
				}
				return nil
			})

			if err != nil {
				fmt.Printf("검증 트랜잭션 에러 (batch=%d): %v\n", idx, err)
			}

			// 진행 상황 출력 (100개 배치마다)
			if b.Verbose && idx%100 == 0 {
				fmt.Printf("검증 배치 %d/%d 완료 (%.2f%%)\n", idx, numBatches, float64(idx*100)/float64(numBatches))
			}
		}(batchIdx)
	}

	// 모든 검증 작업 완료 대기
	wg.Wait()

	verifyTime := time.Since(startVerify)
	totalTime := time.Since(startTime)

	// DB 크기 계산
	dbSize := getDirSize(b.DBPath)

	// 결과 출력
	fmt.Println("\n===== BadgerDB 벤치마크 결과 =====")
	fmt.Printf("총 실행 시간: %v\n", totalTime)
	fmt.Printf("쓰기 시간: %v (%.2f 초당 작업수)\n", writeTime, float64(successCount)/writeTime.Seconds())
	fmt.Printf("검증 시간: %v (%.2f 초당 작업수)\n", verifyTime, float64(verifySuccess)/verifyTime.Seconds())
	fmt.Printf("성공한 쓰기: %d (%.2f%%)\n", successCount, float64(successCount*100)/float64(b.StringCount))
	fmt.Printf("성공한 검증: %d (%.2f%%)\n", verifySuccess, float64(verifySuccess*100)/float64(b.StringCount))
	fmt.Printf("DB 크기: %.2f GB\n", float64(dbSize)/(1024*1024*1024))

	return nil
}

// 글로벌 카운터 초기화 함수
func initializeCounter(db *badger.DB) error {
	return db.Update(func(txn *badger.Txn) error {
		// 카운터가 존재하는지 확인
		_, err := txn.Get(badgerCounterKey)
		if err == badger.ErrKeyNotFound {
			// 카운터 초기화 (0부터 시작)
			valueBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(valueBytes, 0)
			return txn.Set(badgerCounterKey, valueBytes)
		}
		return err // 다른 에러가 있으면 반환
	})
}

// 다음 ID 배치를 가져오는 함수 (원자적으로 증가, 재시도 로직 포함)
func getNextBatchIDs(db *badger.DB, count uint64) (uint64, error) {
	var startID uint64
	maxRetries := 5
	retryDelay := 10 * time.Millisecond

	for i := 0; i < maxRetries; i++ {
		err := db.Update(func(txn *badger.Txn) error {
			item, err := txn.Get(badgerCounterKey)
			if err != nil {
				return fmt.Errorf("카운터 조회 실패: %w", err)
			}

			err = item.Value(func(val []byte) error {
				if len(val) != 8 {
					return fmt.Errorf("잘못된 카운터 값 크기: %d", len(val))
				}
				startID = binary.BigEndian.Uint64(val)
				return nil
			})
			if err != nil {
				return fmt.Errorf("카운터 값 읽기 실패: %w", err)
			}

			// count만큼 카운터 증가
			nextID := startID + count
			valueBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(valueBytes, nextID)

			return txn.Set(badgerCounterKey, valueBytes)
		})

		if err == nil {
			return startID, nil // 성공
		}

		if i < maxRetries-1 {
			// 충돌이 발생했다면 잠시 대기 후 재시도
			time.Sleep(retryDelay)
			// 지수 백오프
			retryDelay *= 2
		} else {
			return 0, fmt.Errorf("ID 할당 최대 재시도 횟수 초과: %w", err)
		}
	}

	return 0, fmt.Errorf("ID 할당 실패")
}

// 키 생성 함수
func generateKey(idx int, size int) []byte {
	key := make([]byte, size)
	s := fmt.Sprintf("0x%x", idx)
	copy(key, s)
	return key
}

// 디렉토리 크기 계산 함수
func getDirSize(path string) int64 {
	var size int64
	filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size
}

// 벤치마크 실행 함수
func RunBadgerDBBenchmark() {
	benchmark := NewBadgerBenchmark()

	// 개발 모드에서는 작은 데이터셋 사용
	devMode := os.Getenv("DEV_MODE") == "true"
	if devMode {
		benchmark.StringCount = 1_000_000 // 100만개
		fmt.Println("개발 모드: 작은 데이터셋 사용 (100만개)")
	}

	// 워커 수 설정 (환경변수)
	if workerEnv := os.Getenv("NUM_WORKERS"); workerEnv != "" {
		var workers int
		fmt.Sscanf(workerEnv, "%d", &workers)
		if workers > 0 {
			benchmark.NumWorkers = workers
		}
	}

	fmt.Printf("BadgerDB 벤치마크 시작 (문자열: %d개, 워커: %d개)\n",
		benchmark.StringCount, benchmark.NumWorkers)

	err := benchmark.Run()
	if err != nil {
		fmt.Printf("벤치마크 실패: %v\n", err)
		os.Exit(1)
	}
}
