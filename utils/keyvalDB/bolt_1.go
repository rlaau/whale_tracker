package keyvalDB

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/bbolt"
)

// 버킷 이름 정의
var (
	keyToBucket    = []byte("keyToID")        // 키->ID 매핑을 저장하는 버킷
	idToBucket     = []byte("idToKey")        // ID->키 매핑을 저장하는 버킷
	metadataBucket = []byte("metadata")       // 메타데이터(글로벌 카운터 등) 저장 버킷
	counterKey     = []byte("global_counter") // 글로벌 카운터 키
)

// BoltBenchmark 구조체는 BoltDB 벤치마크를 위한 설정을 담고 있습니다
type BoltBenchmark struct {
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

// NewBoltBenchmark는 기본 설정으로 벤치마크 인스턴스를 생성합니다
func NewBoltBenchmark() *BoltBenchmark {
	return &BoltBenchmark{
		StringCount: 200_000_000, // 2억개
		StringSize:  20,
		NumWorkers:  runtime.NumCPU(),
		DBPath:      "./data/bolt/bolt.db",
		Verbose:     true,
	}
}

// Run은 BoltDB 벤치마크를 실행합니다
func (b *BoltBenchmark) Run() error {
	// DB 경로가 존재하는지 확인하고 필요하면 생성
	if err := os.MkdirAll(filepath.Dir(b.DBPath), 0755); err != nil {
		return fmt.Errorf("DB 디렉토리 생성 실패: %w", err)
	}

	// 기존 DB 삭제
	if err := os.RemoveAll(b.DBPath); err != nil {
		return fmt.Errorf("기존 DB 삭제 실패: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(b.DBPath), 0755); err != nil {
		return fmt.Errorf("DB 디렉토리 생성 실패: %w", err)
	}

	// BoltDB 옵션 설정 (성능 최적화)
	options := &bbolt.Options{
		NoSync:       true,                  // 동기화 비활성화로 성능 향상
		NoGrowSync:   true,                  // 성능을 위해 비활성화
		FreelistType: bbolt.FreelistMapType, // 맵 기반 프리리스트 (더 빠르지만 메모리 사용 증가)
		Timeout:      5 * time.Second,       // 트랜잭션 타임아웃
	}

	// BoltDB 열기
	db, err := bbolt.Open(b.DBPath, 0666, options)
	if err != nil {
		return fmt.Errorf("BoltDB 열기 실패: %w", err)
	}
	defer db.Close()

	// 버킷 생성 및 초기화
	err = db.Update(func(tx *bbolt.Tx) error {
		// 키->ID 버킷 생성
		if _, err := tx.CreateBucketIfNotExists(keyToBucket); err != nil {
			return fmt.Errorf("키->ID 버킷 생성 실패: %w", err)
		}

		// ID->키 버킷 생성
		if _, err := tx.CreateBucketIfNotExists(idToBucket); err != nil {
			return fmt.Errorf("ID->키 버킷 생성 실패: %w", err)
		}

		// 메타데이터 버킷 생성
		metaBucket, err := tx.CreateBucketIfNotExists(metadataBucket)
		if err != nil {
			return fmt.Errorf("메타데이터 버킷 생성 실패: %w", err)
		}

		// 글로벌 카운터 초기화
		valueBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(valueBytes, 0)
		if err := metaBucket.Put(counterKey, valueBytes); err != nil {
			return fmt.Errorf("글로벌 카운터 초기화 실패: %w", err)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("버킷 초기화 실패: %w", err)
	}

	if b.Verbose {
		fmt.Printf("BoltDB 벤치마크 시작 (문자열 수: %d, 워커 수: %d)\n", b.StringCount, b.NumWorkers)
	}

	// 1단계: 벤치마크 시작 (시간 측정)
	startTime := time.Now()

	// 병렬 처리를 위한 워커 풀 생성
	var wg sync.WaitGroup
	batchSize := 10000 // 배치 크기
	numBatches := (b.StringCount + batchSize - 1) / batchSize
	errChan := make(chan error, numBatches)
	semaphore := make(chan struct{}, b.NumWorkers)
	var successCount, failCount int64

	// BoltDB는 동시 쓰기가 불가능하므로 키 생성과 ID 검색을 병렬로 하고
	// 실제 쓰기는 큐를 통해 직렬화합니다.
	type writeTask struct {
		keys     [][]byte
		batchIdx int
	}
	writeChan := make(chan writeTask, b.NumWorkers*2)

	// 쓰기 워커 생성 (BoltDB는 하나의 쓰기 트랜잭션만 허용하므로 단일 쓰기 워커)
	go func() {
		for task := range writeChan {
			keys := task.keys
			idx := task.batchIdx

			// 트랜잭션 재시도 횟수
			maxRetries := 3

			var _, failInBatch int64

			for retry := 0; retry < maxRetries; retry++ {
				// 읽기 트랜잭션으로 존재하지 않는 키 확인
				newKeys := make([][]byte, 0, len(keys))
				existingKeys := make(map[string]uint64)

				err := db.View(func(tx *bbolt.Tx) error {
					bucket := tx.Bucket(keyToBucket)
					if bucket == nil {
						return fmt.Errorf("키->ID 버킷이 존재하지 않음")
					}

					for _, key := range keys {
						value := bucket.Get(key)
						if value == nil {
							// 키가 존재하지 않음
							newKeys = append(newKeys, key)
						} else {
							// 키가 존재하면 ID 획득
							if len(value) != 8 {
								return fmt.Errorf("잘못된 ID 크기: %d", len(value))
							}
							id := binary.BigEndian.Uint64(value)
							existingKeys[string(key)] = id
						}
					}
					return nil
				})

				if err != nil {
					if retry == maxRetries-1 {
						errChan <- err
						atomic.AddInt64(&failCount, int64(len(keys)))
						break
					}
					// 재시도
					time.Sleep(time.Duration(retry*50) * time.Millisecond)
					continue
				}

				// 이미 존재하는 키는 성공으로 카운트
				atomic.AddInt64(&successCount, int64(len(existingKeys)))

				// 새 키가 있다면 ID 할당 및 저장
				if len(newKeys) > 0 {
					var startID uint64
					// 글로벌 뮤텍스를 사용하여 ID 할당을 보호
					b.counterMutex.Lock()

					// 쓰기 트랜잭션 시작
					err = db.Update(func(tx *bbolt.Tx) error {
						// 글로벌 카운터 읽기
						metaBucket := tx.Bucket(metadataBucket)
						if metaBucket == nil {
							return fmt.Errorf("메타데이터 버킷이 존재하지 않음")
						}

						counterValue := metaBucket.Get(counterKey)
						if counterValue == nil {
							return fmt.Errorf("글로벌 카운터가 존재하지 않음")
						}

						startID = binary.BigEndian.Uint64(counterValue)
						nextID := startID + uint64(len(newKeys))

						// 글로벌 카운터 업데이트
						valueBytes := make([]byte, 8)
						binary.BigEndian.PutUint64(valueBytes, nextID)
						if err := metaBucket.Put(counterKey, valueBytes); err != nil {
							return fmt.Errorf("글로벌 카운터 업데이트 실패: %w", err)
						}

						return nil
					})
					b.counterMutex.Unlock() // ID 할당 완료 후 뮤텍스 해제

					if err != nil {
						if retry == maxRetries-1 {
							errChan <- fmt.Errorf("ID 할당 실패: %w", err)
							atomic.AddInt64(&failCount, int64(len(newKeys)))
							break
						}
						// 재시도
						time.Sleep(time.Duration(retry*50) * time.Millisecond)
						continue
					}

					// 새 키에 대한 상태 맵 - ID 할당 추적
					keyStatus := make(map[string]bool, len(newKeys))

					// 키-ID 매핑 저장
					err = db.Update(func(tx *bbolt.Tx) error {
						keyBucket := tx.Bucket(keyToBucket)
						idBucket := tx.Bucket(idToBucket)

						for i, key := range newKeys {
							id := startID + uint64(i)
							idBytes := make([]byte, 8)
							binary.BigEndian.PutUint64(idBytes, id)

							// 키->ID 저장
							if err := keyBucket.Put(key, idBytes); err != nil {
								keyStatus[string(key)] = false
								return fmt.Errorf("키 저장 실패: %w", err)
							}

							// ID->키 저장 (역방향 조회를 위해)
							idKey := make([]byte, 9)
							idKey[0] = 'i' // ID 키임을 표시
							binary.BigEndian.PutUint64(idKey[1:], id)
							if err := idBucket.Put(idKey, key); err != nil {
								keyStatus[string(key)] = false
								return fmt.Errorf("ID 키 저장 실패: %w", err)
							}

							keyStatus[string(key)] = true
						}

						return nil
					})

					if err != nil {
						if retry == maxRetries-1 {
							errChan <- fmt.Errorf("배치 쓰기 실패 (batch=%d): %w", idx, err)
							// 성공하지 못한 키들을 실패로 카운트
							failureCount := 0
							for _, success := range keyStatus {
								if !success {
									failureCount++
								}
							}
							atomic.AddInt64(&failInBatch, int64(failureCount))
							break
						}
						// 재시도
						time.Sleep(time.Duration(retry*50) * time.Millisecond)
						continue
					}

					// 성공한 키 수 카운트
					successKeysCount := 0
					for _, success := range keyStatus {
						if success {
							successKeysCount++
						}
					}
					atomic.AddInt64(&successCount, int64(successKeysCount))
					atomic.AddInt64(&failCount, int64(len(newKeys)-successKeysCount))
				}

				// 진행 상황 출력 (100개 배치마다)
				if b.Verbose && idx%100 == 0 {
					fmt.Printf("배치 %d/%d 완료 (%.2f%%)\n", idx, numBatches, float64(idx*100)/float64(numBatches))
				}

				break // 성공적으로 처리됨
			}
		}
	}()

	// 2단계: 문자열 생성 및 ID 부여 (병렬로 키 생성, 쓰기는 직렬화)
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

			// 쓰기 작업을 쓰기 채널에 전송
			writeChan <- writeTask{
				keys:     keys,
				batchIdx: idx,
			}
		}(batchIdx)
	}

	// 모든 키 생성 작업 완료 대기
	wg.Wait()
	close(writeChan) // 쓰기 채널 닫기 (쓰기 워커가 종료됨)

	// 에러 확인
	close(errChan)
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

	// 검증을 위한 워커 풀 생성 (BoltDB는 읽기 작업은 병렬 가능)
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
			err := db.View(func(tx *bbolt.Tx) error {
				keyBucket := tx.Bucket(keyToBucket)
				idBucket := tx.Bucket(idToBucket)

				if keyBucket == nil || idBucket == nil {
					return fmt.Errorf("버킷이 존재하지 않음")
				}

				for i := 0; i < count; i++ {
					stringIdx := startOffset + i
					key := generateKey(stringIdx, b.StringSize)

					// 키->ID 검증
					idBytes := keyBucket.Get(key)
					if idBytes == nil {
						// 이 키는 등록되지 않았으므로 검증 불필요
						continue
					}

					if len(idBytes) != 8 {
						atomic.AddInt64(&verifyFail, 1)
						continue
					}

					id := binary.BigEndian.Uint64(idBytes)

					// ID->키 검증
					idKey := make([]byte, 9)
					idKey[0] = 'i'
					binary.BigEndian.PutUint64(idKey[1:], id)

					storedKey := idBucket.Get(idKey)
					if storedKey == nil {
						atomic.AddInt64(&verifyFail, 1)
						continue
					}

					if string(storedKey) != string(key) {
						atomic.AddInt64(&verifyFail, 1)
						continue
					}

					atomic.AddInt64(&verifySuccess, 1)
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
	fmt.Println("\n===== BoltDB 벤치마크 결과 =====")
	fmt.Printf("총 실행 시간: %v\n", totalTime)
	fmt.Printf("쓰기 시간: %v (%.2f 초당 작업수)\n", writeTime, float64(successCount)/writeTime.Seconds())
	fmt.Printf("검증 시간: %v (%.2f 초당 작업수)\n", verifyTime, float64(verifySuccess)/verifyTime.Seconds())
	fmt.Printf("성공한 쓰기: %d (%.2f%%)\n", successCount, float64(successCount*100)/float64(b.StringCount))
	fmt.Printf("성공한 검증: %d (%.2f%%)\n", verifySuccess, float64(verifySuccess*100)/float64(b.StringCount))
	fmt.Printf("DB 크기: %.2f GB\n", float64(dbSize)/(1024*1024*1024))

	return nil
}

// 벤치마크 실행 함수
func RunBoltDBBenchmark() {
	benchmark := NewBoltBenchmark()

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

	fmt.Printf("BoltDB 벤치마크 시작 (문자열: %d개, 워커: %d개)\n",
		benchmark.StringCount, benchmark.NumWorkers)

	err := benchmark.Run()
	if err != nil {
		fmt.Printf("벤치마크 실패: %v\n", err)
		os.Exit(1)
	}
}

// ShardedBoltBenchmark는 샤딩을 적용한 BoltDB 벤치마크를 위한 구조체입니다
type ShardedBoltBenchmark struct {
	// 문자열 개수 (기본 2억개)
	StringCount int
	// 문자열 크기 (기본 20바이트)
	StringSize int
	// 병렬 처리 워커 수 (기본 CPU 코어 수)
	NumWorkers int
	// DB 디렉토리 경로
	DBDir string
	// 로깅 여부
	Verbose bool
	// 샤드 수 (기본 CPU 코어 수)
	ShardCount int
	// 배치 크기
	BatchSize int
	// 샤드별 DB 파일
	shards []*bbolt.DB
	// 샤드별 뮤텍스
	shardMutexes []sync.Mutex
	// 버킷 이름
	buckets struct {
		keyToID    []byte
		idToKey    []byte
		metadata   []byte
		counterKey []byte
	}
}

// NewShardedBoltBenchmark는 기본 설정으로 샤딩 벤치마크 인스턴스를 생성합니다
func NewShardedBoltBenchmark() *ShardedBoltBenchmark {
	b := &ShardedBoltBenchmark{
		StringCount: 200_000_00, // 2억개
		StringSize:  20,
		NumWorkers:  runtime.NumCPU(),
		DBDir:       "./data/boltmini",
		Verbose:     true,
		ShardCount:  runtime.NumCPU(), // 샤드 수는 CPU 코어 수와 동일하게 설정
		BatchSize:   10000,
	}

	// 버킷 이름 설정
	b.buckets.keyToID = []byte("keyToID")
	b.buckets.idToKey = []byte("idToKey")
	b.buckets.metadata = []byte("metadata")
	b.buckets.counterKey = []byte("global_counter")

	return b
}

// Setup은 벤치마크 실행 전 환경을 준비합니다
func (b *ShardedBoltBenchmark) Setup() error {
	// DB 디렉토리 생성
	if err := os.MkdirAll(b.DBDir, 0755); err != nil {
		return fmt.Errorf("DB 디렉토리 생성 실패: %w", err)
	}

	// 기존 DB 파일 삭제
	files, err := filepath.Glob(filepath.Join(b.DBDir, "shard_*.db"))
	if err != nil {
		return fmt.Errorf("기존 DB 파일 검색 실패: %w", err)
	}
	for _, file := range files {
		if err := os.Remove(file); err != nil {
			return fmt.Errorf("DB 파일 삭제 실패 (%s): %w", file, err)
		}
	}

	// 샤드 초기화
	b.shards = make([]*bbolt.DB, b.ShardCount)
	b.shardMutexes = make([]sync.Mutex, b.ShardCount)

	// BoltDB 옵션 설정 (성능 최적화)
	options := &bbolt.Options{
		NoSync:       true,                  // 동기화 비활성화로 성능 향상
		NoGrowSync:   true,                  // 성능을 위해 비활성화
		FreelistType: bbolt.FreelistMapType, // 맵 기반 프리리스트 (더 빠르지만 메모리 사용 증가)
		Timeout:      5 * time.Second,       // 트랜잭션 타임아웃
	}

	// 각 샤드별 DB 파일 생성 및 초기화
	for i := 0; i < b.ShardCount; i++ {
		dbPath := filepath.Join(b.DBDir, fmt.Sprintf("shard_%d.db", i))

		// DB 열기
		db, err := bbolt.Open(dbPath, 0666, options)
		if err != nil {
			// 이미 열린 샤드들을 닫기
			for j := 0; j < i; j++ {
				b.shards[j].Close()
			}
			return fmt.Errorf("샤드 %d DB 열기 실패: %w", i, err)
		}
		b.shards[i] = db

		// 버킷 생성 및 초기화
		err = db.Update(func(tx *bbolt.Tx) error {
			// 키->ID 버킷 생성
			if _, err := tx.CreateBucketIfNotExists(b.buckets.keyToID); err != nil {
				return fmt.Errorf("키->ID 버킷 생성 실패: %w", err)
			}

			// ID->키 버킷 생성
			if _, err := tx.CreateBucketIfNotExists(b.buckets.idToKey); err != nil {
				return fmt.Errorf("ID->키 버킷 생성 실패: %w", err)
			}

			// 메타데이터 버킷 생성
			metaBucket, err := tx.CreateBucketIfNotExists(b.buckets.metadata)
			if err != nil {
				return fmt.Errorf("메타데이터 버킷 생성 실패: %w", err)
			}

			// 글로벌 카운터 초기화
			valueBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(valueBytes, 0)
			if err := metaBucket.Put(b.buckets.counterKey, valueBytes); err != nil {
				return fmt.Errorf("글로벌 카운터 초기화 실패: %w", err)
			}

			return nil
		})
		if err != nil {
			// 이미 열린 샤드들을 닫기
			for j := 0; j <= i; j++ {
				b.shards[j].Close()
			}
			return fmt.Errorf("샤드 %d 버킷 초기화 실패: %w", i, err)
		}
	}

	if b.Verbose {
		fmt.Printf("샤딩된 BoltDB 벤치마크 준비 완료 (샤드 수: %d)\n", b.ShardCount)
	}

	return nil
}

// Cleanup은 벤치마크 실행 후 리소스를 정리합니다
func (b *ShardedBoltBenchmark) Cleanup() error {
	// 모든 샤드 DB 닫기
	for i, db := range b.shards {
		if db != nil {
			if err := db.Close(); err != nil {
				return fmt.Errorf("샤드 %d DB 닫기 실패: %w", i, err)
			}
		}
	}
	return nil
}

// Run은 샤딩된 BoltDB 벤치마크를 실행합니다
func (b *ShardedBoltBenchmark) Run() error {
	if b.Verbose {
		fmt.Printf("샤딩된 BoltDB 벤치마크 시작 (문자열 수: %d, 워커 수: %d, 샤드 수: %d)\n",
			b.StringCount, b.NumWorkers, b.ShardCount)
	}

	// 1단계: 벤치마크 시작 (시간 측정)
	startTime := time.Now()

	// 병렬 처리를 위한 워커 풀 생성
	var wg sync.WaitGroup
	numBatches := (b.StringCount + b.BatchSize - 1) / b.BatchSize
	errChan := make(chan error, numBatches)
	semaphore := make(chan struct{}, b.NumWorkers)
	var successCount, failCount int64

	// 2단계: 문자열 생성 및 ID 부여 (병렬 처리)
	for batchIdx := 0; batchIdx < numBatches; batchIdx++ {
		wg.Add(1)
		semaphore <- struct{}{} // 세마포어 획득

		go func(idx int) {
			defer wg.Done()
			defer func() { <-semaphore }() // 세마포어 반환

			startOffset := idx * b.BatchSize
			endOffset := (idx + 1) * b.BatchSize
			if endOffset > b.StringCount {
				endOffset = b.StringCount
			}
			count := endOffset - startOffset

			// 각 키 생성 및 샤드별로 분리
			shardedKeys := make(map[int][][]byte)
			for i := 0; i < count; i++ {
				stringIdx := startOffset + i
				key := b.generateKey(stringIdx)

				// 키를 해시하여 샤드 결정
				shardIdx := b.getShardIndex(key)

				// 샤드별 키 맵에 추가
				shardedKeys[shardIdx] = append(shardedKeys[shardIdx], key)
			}

			// 각 샤드별로 처리
			for shardIdx, keys := range shardedKeys {
				// 샤드에 해당하는 DB 가져오기
				db := b.shards[shardIdx]

				// 트랜잭션 재시도 횟수
				maxRetries := 3

				for retry := 0; retry < maxRetries; retry++ {
					// 1. 읽기 트랜잭션으로 존재하지 않는 키만 찾기
					newKeys := make([][]byte, 0, len(keys))
					existingKeys := make(map[string]uint64)

					err := db.View(func(tx *bbolt.Tx) error {
						bucket := tx.Bucket(b.buckets.keyToID)
						if bucket == nil {
							return fmt.Errorf("키->ID 버킷이 존재하지 않음")
						}

						for _, key := range keys {
							value := bucket.Get(key)
							if value == nil {
								// 키가 존재하지 않음
								newKeys = append(newKeys, key)
							} else {
								// 키가 존재하면 ID 획득
								if len(value) != 8 {
									return fmt.Errorf("잘못된 ID 크기: %d", len(value))
								}
								id := binary.BigEndian.Uint64(value)
								existingKeys[string(key)] = id
							}
						}
						return nil
					})

					if err != nil {
						if retry == maxRetries-1 {
							errChan <- fmt.Errorf("샤드 %d 키 조회 실패: %w", shardIdx, err)
							atomic.AddInt64(&failCount, int64(len(keys)))
							break
						}
						// 재시도
						time.Sleep(time.Duration(retry*50) * time.Millisecond)
						continue
					}

					// 이미 존재하는 키는 성공으로 카운트
					atomic.AddInt64(&successCount, int64(len(existingKeys)))

					// 2. 새 키가 있다면 ID 할당 및 저장
					if len(newKeys) > 0 {
						var startID uint64

						// 샤드별 뮤텍스를 사용하여 ID 할당을 보호
						b.shardMutexes[shardIdx].Lock()

						// 쓰기 트랜잭션 시작
						err = db.Update(func(tx *bbolt.Tx) error {
							// 글로벌 카운터 읽기
							metaBucket := tx.Bucket(b.buckets.metadata)
							if metaBucket == nil {
								return fmt.Errorf("메타데이터 버킷이 존재하지 않음")
							}

							counterValue := metaBucket.Get(b.buckets.counterKey)
							if counterValue == nil {
								return fmt.Errorf("글로벌 카운터가 존재하지 않음")
							}

							startID = binary.BigEndian.Uint64(counterValue)
							nextID := startID + uint64(len(newKeys))

							// 글로벌 카운터 업데이트
							valueBytes := make([]byte, 8)
							binary.BigEndian.PutUint64(valueBytes, nextID)
							if err := metaBucket.Put(b.buckets.counterKey, valueBytes); err != nil {
								return fmt.Errorf("글로벌 카운터 업데이트 실패: %w", err)
							}

							return nil
						})
						b.shardMutexes[shardIdx].Unlock() // ID 할당 완료 후 뮤텍스 해제

						if err != nil {
							if retry == maxRetries-1 {
								errChan <- fmt.Errorf("샤드 %d ID 할당 실패: %w", shardIdx, err)
								atomic.AddInt64(&failCount, int64(len(newKeys)))
								break
							}
							// 재시도
							time.Sleep(time.Duration(retry*50) * time.Millisecond)
							continue
						}

						// 키-ID 매핑 저장
						successKeysCount := 0

						err = db.Update(func(tx *bbolt.Tx) error {
							keyBucket := tx.Bucket(b.buckets.keyToID)
							idBucket := tx.Bucket(b.buckets.idToKey)

							for i, key := range newKeys {
								id := startID + uint64(i)
								idBytes := make([]byte, 8)
								binary.BigEndian.PutUint64(idBytes, id)

								// 키->ID 저장
								if err := keyBucket.Put(key, idBytes); err != nil {
									return fmt.Errorf("키 저장 실패: %w", err)
								}

								// ID->키 저장 (역방향 조회를 위해)
								idKey := make([]byte, 9)
								idKey[0] = 'i' // ID 키임을 표시
								binary.BigEndian.PutUint64(idKey[1:], id)
								if err := idBucket.Put(idKey, key); err != nil {
									return fmt.Errorf("ID 키 저장 실패: %w", err)
								}

								successKeysCount++
							}

							return nil
						})

						if err != nil {
							if retry == maxRetries-1 {
								errChan <- fmt.Errorf("샤드 %d 배치 쓰기 실패: %w", shardIdx, err)
								atomic.AddInt64(&failCount, int64(len(newKeys)))
								break
							}
							// 재시도
							time.Sleep(time.Duration(retry*50) * time.Millisecond)
							continue
						}

						// 성공한 키 수 카운트
						atomic.AddInt64(&successCount, int64(successKeysCount))
						atomic.AddInt64(&failCount, int64(len(newKeys)-successKeysCount))
					}

					break // 샤드 처리 성공
				}
			}

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

			startOffset := idx * b.BatchSize
			endOffset := (idx + 1) * b.BatchSize
			if endOffset > b.StringCount {
				endOffset = b.StringCount
			}
			count := endOffset - startOffset

			// 키 생성 및 검증
			for i := 0; i < count; i++ {
				stringIdx := startOffset + i
				key := b.generateKey(stringIdx)

				// 키에 해당하는 샤드 결정
				shardIdx := b.getShardIndex(key)
				db := b.shards[shardIdx]

				// 검증
				err := db.View(func(tx *bbolt.Tx) error {
					keyBucket := tx.Bucket(b.buckets.keyToID)
					idBucket := tx.Bucket(b.buckets.idToKey)

					if keyBucket == nil || idBucket == nil {
						return fmt.Errorf("버킷이 존재하지 않음")
					}

					// 키->ID 검증
					idBytes := keyBucket.Get(key)
					if idBytes == nil {
						// 이 키는 등록되지 않았으므로 검증 불필요
						return nil
					}

					if len(idBytes) != 8 {
						atomic.AddInt64(&verifyFail, 1)
						return nil
					}

					id := binary.BigEndian.Uint64(idBytes)

					// ID->키 검증
					idKey := make([]byte, 9)
					idKey[0] = 'i'
					binary.BigEndian.PutUint64(idKey[1:], id)

					storedKey := idBucket.Get(idKey)
					if storedKey == nil {
						atomic.AddInt64(&verifyFail, 1)
						return nil
					}

					if string(storedKey) != string(key) {
						atomic.AddInt64(&verifyFail, 1)
						return nil
					}

					atomic.AddInt64(&verifySuccess, 1)
					return nil
				})

				if err != nil {
					fmt.Printf("샤드 %d 검증 트랜잭션 에러: %v\n", shardIdx, err)
				}
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
	var dbSize int64
	for i := 0; i < b.ShardCount; i++ {
		dbPath := filepath.Join(b.DBDir, fmt.Sprintf("shard_%d.db", i))
		if fi, err := os.Stat(dbPath); err == nil {
			dbSize += fi.Size()
		}
	}

	// 결과 출력
	fmt.Println("\n===== 샤딩된 BoltDB 벤치마크 결과 =====")
	fmt.Printf("총 실행 시간: %v\n", totalTime)
	fmt.Printf("쓰기 시간: %v (%.2f 초당 작업수)\n", writeTime, float64(successCount)/writeTime.Seconds())
	fmt.Printf("검증 시간: %v (%.2f 초당 작업수)\n", verifyTime, float64(verifySuccess)/verifyTime.Seconds())
	fmt.Printf("성공한 쓰기: %d (%.2f%%)\n", successCount, float64(successCount*100)/float64(b.StringCount))
	fmt.Printf("성공한 검증: %d (%.2f%%)\n", verifySuccess, float64(verifySuccess*100)/float64(b.StringCount))
	fmt.Printf("DB 크기: %.2f GB\n", float64(dbSize)/(1024*1024*1024))
	fmt.Printf("샤드 수: %d\n", b.ShardCount)

	return nil
}

// generateKey는 인덱스에 해당하는 키를 생성합니다
func (b *ShardedBoltBenchmark) generateKey(idx int) []byte {
	key := make([]byte, b.StringSize)
	s := fmt.Sprintf("0x%x", idx)
	copy(key, s)
	return key
}

// getShardIndex는 키에 대한 샤드 인덱스를 결정합니다
func (b *ShardedBoltBenchmark) getShardIndex(key []byte) int {
	h := fnv.New32a()
	h.Write(key)
	return int(h.Sum32() % uint32(b.ShardCount))
}

// RunShardedBoltBenchmark는 샤딩된 BoltDB 벤치마크를 실행합니다
func RunShardedBoltBenchmark() {
	benchmark := NewShardedBoltBenchmark()

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

	// 샤드 수 설정 (환경변수)
	if shardEnv := os.Getenv("SHARD_COUNT"); shardEnv != "" {
		var shards int
		fmt.Sscanf(shardEnv, "%d", &shards)
		if shards > 0 {
			benchmark.ShardCount = shards
		}
	}

	fmt.Printf("샤딩된 BoltDB 벤치마크 시작 (문자열: %d개, 워커: %d개, 샤드: %d개)\n",
		benchmark.StringCount, benchmark.NumWorkers, benchmark.ShardCount)

	if err := benchmark.Setup(); err != nil {
		fmt.Printf("벤치마크 설정 실패: %v\n", err)
		os.Exit(1)
	}

	err := benchmark.Run()
	if err != nil {
		fmt.Printf("벤치마크 실행 실패: %v\n", err)
	}

	if err := benchmark.Cleanup(); err != nil {
		fmt.Printf("벤치마크 정리 실패: %v\n", err)
	}
}
func RunBenchmark() {
	// 원래 BoltDB 벤치마크
	fmt.Println("\n===== 원래 BoltDB 벤치마크 =====")
	RunBoltDBBenchmark()

	// 개선된 샤딩 BoltDB 벤치마크
	fmt.Println("\n===== 샤딩된 BoltDB 벤치마크 =====")
	RunShardedBoltBenchmark()
}
