package keyvalDB

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v3"
)

// Badger2Benchmark 구조체는 두 번째 벤치마크 스위트를 위한 설정을 담고 있습니다
type Badger2Benchmark struct {
	// 기존 키 작업 수 (기본 5천만개)
	ExistingOpCount int
	// 새 키 작업 수 (기본 5천만개)
	NewOpCount int
	// 병렬 워커 수 (기본 CPU 코어 수)
	NumWorkers int
	// 기존 DB 경로
	DBPath string
	// 이전 벤치마크에서 생성한 최대 ID 값
	MaxExistingID int
	// 문자열 크기 (바이트)
	StringSize int
	// 상세 로깅 여부
	Verbose bool
	// 재현 가능한 테스트를 위한 랜덤 시드
	RandomSeed int64
}

// NewBadger2Benchmark는 기본 설정으로 벤치마크 인스턴스를 생성합니다
func NewBadger2Benchmark() *Badger2Benchmark {
	return &Badger2Benchmark{
		ExistingOpCount: 50_000_000, // 5천만개 (기존 키)
		NewOpCount:      50_000_000, // 5천만개 (새 키)
		NumWorkers:      runtime.NumCPU(),
		DBPath:          "./data/badger",
		MaxExistingID:   250_000_000, // 2억개 (기존 벤치마크에서 생성한 수)
		StringSize:      20,
		Verbose:         true,
		RandomSeed:      time.Now().UnixNano(), // 기본값은 현재 시간 기준 시드
	}
}

// Run은 BadgerDB 두 번째 벤치마크 스위트를 실행합니다
func (b *Badger2Benchmark) Run() error {
	// DB 경로 존재 여부 확인
	if _, err := os.Stat(b.DBPath); os.IsNotExist(err) {
		return fmt.Errorf("DB 경로가 존재하지 않습니다: %s", b.DBPath)
	}

	// BadgerDB 최적화 설정
	opts := badger.DefaultOptions(b.DBPath)
	opts.Logger = nil                 // 로깅 비활성화
	opts.SyncWrites = false           // 동기화 비활성화로 성능 향상
	opts.NumVersionsToKeep = 1        // 버전 관리 최소화
	opts.NumLevelZeroTables = 8       // L0 테이블 수 증가
	opts.NumLevelZeroTablesStall = 16 // 병렬 쓰기 임계값 증가
	opts.ValueLogFileSize = 1 << 28   // 256MB 값 로그 파일
	opts.NumMemtables = 8             // 메모리 테이블 수 증가
	opts.BlockSize = 16 * 1024        // 블록 크기 최적화
	opts.BlockCacheSize = 1 << 30     // 1GB 블록 캐시
	opts.IndexCacheSize = 200 << 20   // 200MB 인덱스 캐시
	opts.ValueThreshold = 256         // 값 임계값 설정
	opts.Compression = 1              // 압축 활성화

	// 데이터베이스 열기
	db, err := badger.Open(opts)
	if err != nil {
		return fmt.Errorf("BadgerDB 열기 실패: %w", err)
	}
	defer db.Close()

	// 총 작업 수 계산
	totalOpCount := b.ExistingOpCount + b.NewOpCount

	// 벤치마크 시작 정보 출력
	if b.Verbose {
		fmt.Printf("BadgerDB 두 번째 벤치마크 시작 (기존 키: %d개, 새 키: %d개, 총: %d개, 워커 수: %d)\n",
			b.ExistingOpCount, b.NewOpCount, totalOpCount, b.NumWorkers)
		fmt.Printf("랜덤 시드: %d (재현 가능한 벤치마크를 위해)\n", b.RandomSeed)
	}

	// 난수 생성기 초기화
	rng := rand.New(rand.NewSource(b.RandomSeed))

	// 테스트 데이터 생성: 기존 키 + 새 키
	allIDs := make([]uint64, totalOpCount)

	// 앞부분은 기존 ID (0 ~ MaxExistingID-1 범위에서 랜덤 선택)
	for i := 0; i < b.ExistingOpCount; i++ {
		allIDs[i] = uint64(rng.Intn(b.MaxExistingID))
	}

	// 뒷부분은 새 ID (MaxExistingID부터 시작하여 순차 생성)
	for i := 0; i < b.NewOpCount; i++ {
		allIDs[b.ExistingOpCount+i] = uint64(b.MaxExistingID + i)
	}

	// ID 배열 섞기 (DB는 키 존재 여부를 모르는 상태 시뮬레이션)
	rng.Shuffle(len(allIDs), func(i, j int) {
		allIDs[i], allIDs[j] = allIDs[j], allIDs[i]
	})

	// 성능 측정용 카운터
	var (
		// 1단계: 키 존재 여부 확인
		existsCount    atomic.Int64 // 이미 존재하는 키 수
		notExistsCount atomic.Int64 // 존재하지 않는 키 수
		checkTime      atomic.Int64 // 키 확인 시간 (나노초)

		// 2단계: 키가 존재하지 않으면 쓰기
		writeSuccess atomic.Int64 // 쓰기 성공 횟수
		writeFail    atomic.Int64 // 쓰기 실패 횟수
		writeTime    atomic.Int64 // 쓰기 시간 (나노초)

		// 3단계: 인코딩 테스트
		encodeSuccess atomic.Int64 // 인코딩 성공 횟수
		encodeFail    atomic.Int64 // 인코딩 실패 횟수
		encodeTime    atomic.Int64 // 인코딩 시간 (나노초)

		// 4단계: 디코딩 테스트
		decodeSuccess atomic.Int64 // 디코딩 성공 횟수
		decodeFail    atomic.Int64 // 디코딩 실패 횟수
		decodeTime    atomic.Int64 // 디코딩 시간 (나노초)
	)

	// 벤치마크 시작 시간
	startTime := time.Now()

	// 병렬 처리를 위한 워커 풀 설정
	var wg sync.WaitGroup
	batchSize := 10000 // 배치 크기
	numBatches := (len(allIDs) + batchSize - 1) / batchSize
	semaphore := make(chan struct{}, b.NumWorkers)

	// 배치 단위로 병렬 처리
	for batchIdx := 0; batchIdx < numBatches; batchIdx++ {
		wg.Add(1)
		semaphore <- struct{}{} // 세마포어 획득

		go func(idx int) {
			defer wg.Done()
			defer func() { <-semaphore }() // 세마포어 반환

			// 현재 배치의 범위 계산
			startOffset := idx * batchSize
			endOffset := (idx + 1) * batchSize
			if endOffset > len(allIDs) {
				endOffset = len(allIDs)
			}
			count := endOffset - startOffset

			// 배치 단위 트랜잭션 준비
			readTxn := db.NewTransaction(false) // 읽기용 트랜잭션
			defer readTxn.Discard()

			// 쓰기 배치 준비
			wb := db.NewWriteBatch()
			defer wb.Cancel()

			// 이 배치에 쓰기 작업이 있는지 추적
			hasWrites := false

			// 배치 내 각 ID 처리
			for i := 0; i < count; i++ {
				id := allIDs[startOffset+i]
				key := generateKey(int(id), b.StringSize)

				// ===== 1단계: 키 존재 여부 확인 =====
				checkStart := time.Now()

				// 트랜잭션이 커지면 새로 생성 - 트랜잭션 재활용 최적화
				if i > 0 && i%1000 == 0 {
					readTxn.Discard()
					readTxn = db.NewTransaction(false)
				}

				// 키 조회 (BadgerDB의 블룸 필터 자동 활용)
				_, err := readTxn.Get(key)

				// 키 확인 시간 측정
				checkDuration := time.Since(checkStart)
				checkTime.Add(checkDuration.Nanoseconds())

				// 키 존재 여부에 따른 처리
				exists := err == nil
				if exists {
					existsCount.Add(1)
				} else {
					notExistsCount.Add(1)

					// ===== 2단계: 키가 존재하지 않으면 쓰기 =====
					writeStart := time.Now()

					// 키->ID 매핑 생성 및 쓰기
					idBytes := make([]byte, 8)
					binary.BigEndian.PutUint64(idBytes, id)
					errWrite := wb.Set(key, idBytes)

					// ID->키 매핑 생성 및 쓰기
					if errWrite == nil {
						idKey := make([]byte, 9)
						idKey[0] = 'i' // ID 키 접두사
						binary.BigEndian.PutUint64(idKey[1:], id)
						errWrite = wb.Set(idKey, key)
					}

					// 쓰기 시간 측정
					writeDuration := time.Since(writeStart)
					writeTime.Add(writeDuration.Nanoseconds())

					// 쓰기 결과 추적
					if errWrite == nil {
						writeSuccess.Add(1)
						hasWrites = true
					} else {
						writeFail.Add(1)
					}
				}

				// ===== 3단계: 인코딩 테스트 (String→ID) - 모든 키에 대해 수행 =====
				encodeStart := time.Now()

				// 인코딩 테스트를 위한 새 트랜잭션 (쓰기 결과가 반영되도록)
				encodeTxn := db.NewTransaction(false)

				// 키로 ID 조회 (인코딩 테스트)
				var foundID uint64
				encodeErr := error(nil)

				item, err := encodeTxn.Get(key)
				if err == nil {
					err = item.Value(func(val []byte) error {
						if len(val) != 8 {
							return fmt.Errorf("잘못된 ID 크기")
						}
						foundID = binary.BigEndian.Uint64(val)
						if foundID != id {
							return fmt.Errorf("ID 불일치: %d != %d", foundID, id)
						}
						return nil
					})

					if err != nil {
						encodeErr = err
					}
				} else {
					encodeErr = err
				}

				encodeTxn.Discard() // 트랜잭션 종료

				// 인코딩 시간 측정
				encodeDuration := time.Since(encodeStart)
				encodeTime.Add(encodeDuration.Nanoseconds())

				// 인코딩 결과 추적
				if encodeErr == nil {
					encodeSuccess.Add(1)
				} else {
					encodeFail.Add(1)
				}

				// ===== 4단계: 디코딩 테스트 (ID→String) - 모든 키에 대해 수행 =====
				decodeStart := time.Now()

				// 디코딩 테스트를 위한 새 트랜잭션
				decodeTxn := db.NewTransaction(false)

				// ID로 키 조회 (디코딩 테스트)
				idKey := make([]byte, 9)
				idKey[0] = 'i'
				binary.BigEndian.PutUint64(idKey[1:], id)

				decodeErr := error(nil)
				item, err = decodeTxn.Get(idKey)
				if err == nil {
					err = item.Value(func(val []byte) error {
						decodedKey := string(val)
						expectedKey := string(generateKey(int(id), b.StringSize))
						if decodedKey != expectedKey {
							return fmt.Errorf("키 불일치")
						}
						return nil
					})

					if err != nil {
						decodeErr = err
					}
				} else {
					decodeErr = err
				}

				decodeTxn.Discard() // 트랜잭션 종료

				// 디코딩 시간 측정
				decodeDuration := time.Since(decodeStart)
				decodeTime.Add(decodeDuration.Nanoseconds())

				// 디코딩 결과 추적
				if decodeErr == nil {
					decodeSuccess.Add(1)
				} else {
					decodeFail.Add(1)
				}
			}

			// 배치 쓰기 수행
			if hasWrites {
				if err := wb.Flush(); err != nil && b.Verbose {
					fmt.Printf("배치 쓰기 실패 (batch=%d): %v\n", idx, err)
				}
			}

			// 진행 상황 출력
			if b.Verbose && idx%100 == 0 {
				fmt.Printf("배치 처리: %d/%d 완료 (%.2f%%)\n",
					idx, numBatches, float64(idx*100)/float64(numBatches))
			}
		}(batchIdx)
	}

	// 모든 작업 완료 대기
	wg.Wait()

	// 총 실행 시간 계산
	totalTime := time.Since(startTime)

	// 측정된 시간 변환
	checkTimeD := time.Duration(checkTime.Load())
	writeTimeD := time.Duration(writeTime.Load())
	encodeTimeD := time.Duration(encodeTime.Load())
	decodeTimeD := time.Duration(decodeTime.Load())

	// 총 작업 수 저장
	totalOps := b.ExistingOpCount + b.NewOpCount

	// 평균 시간 계산
	avgCheckTime := safeDiv2(checkTimeD.Nanoseconds(), int64(totalOps))
	avgWriteTime := safeDiv2(writeTimeD.Nanoseconds(), notExistsCount.Load())
	avgEncodeTime := safeDiv2(encodeTimeD.Nanoseconds(), int64(totalOps))
	avgDecodeTime := safeDiv2(decodeTimeD.Nanoseconds(), int64(totalOps))

	// 결과 출력
	fmt.Println("\n===== BadgerDB 두 번째 벤치마크 결과 =====")
	fmt.Printf("총 실행 시간: %v\n", totalTime)
	fmt.Printf("총 작업 수: %d (기존 키: %d, 새 키: %d)\n",
		totalOps, existsCount.Load(), notExistsCount.Load())
	fmt.Printf("실제로 없어서 추가한 횟수: %d (성공: %d, 실패: %d)\n",
		notExistsCount.Load(), writeSuccess.Load(), writeFail.Load())

	// 1단계: 키 확인 결과
	fmt.Printf("\n1. 키 존재 여부 확인 (블룸 필터 활용):\n")
	fmt.Printf("   - 총 시간: %v (평균: %v/작업)\n",
		checkTimeD, time.Duration(avgCheckTime))
	fmt.Printf("   - 키 존재: %d, 키 없음: %d\n",
		existsCount.Load(), notExistsCount.Load())
	fmt.Printf("   - 초당 작업: %.2f\n",
		safeDiv(float64(totalOps), checkTimeD.Seconds()))

	// 2단계: 쓰기 결과
	fmt.Printf("\n2. 키 쓰기 (존재하지 않는 키만):\n")
	fmt.Printf("   - 총 시간: %v (평균: %v/작업)\n",
		writeTimeD, time.Duration(avgWriteTime))
	fmt.Printf("   - 시도 횟수: %d (실제 없어서 쓰기 시도)\n",
		notExistsCount.Load())
	fmt.Printf("   - 성공: %d, 실패: %d (성공률: %.2f%%)\n",
		writeSuccess.Load(), writeFail.Load(),
		safePercentage(writeSuccess.Load(), notExistsCount.Load()))
	fmt.Printf("   - 초당 작업: %.2f\n",
		safeDiv(float64(notExistsCount.Load()), writeTimeD.Seconds()))

	// 3단계: 인코딩 결과
	fmt.Printf("\n3. 인코딩 테스트 (String→ID, 모든 키):\n")
	fmt.Printf("   - 총 시간: %v (평균: %v/작업)\n",
		encodeTimeD, time.Duration(avgEncodeTime))
	fmt.Printf("   - 성공: %d, 실패: %d (성공률: %.2f%%)\n",
		encodeSuccess.Load(), encodeFail.Load(),
		safePercentage(encodeSuccess.Load(), int64(totalOps)))
	fmt.Printf("   - 초당 작업: %.2f\n",
		safeDiv(float64(totalOps), encodeTimeD.Seconds()))

	// 4단계: 디코딩 결과
	fmt.Printf("\n4. 디코딩 테스트 (ID→String, 모든 키):\n")
	fmt.Printf("   - 총 시간: %v (평균: %v/작업)\n",
		decodeTimeD, time.Duration(avgDecodeTime))
	fmt.Printf("   - 성공: %d, 실패: %d (성공률: %.2f%%)\n",
		decodeSuccess.Load(), decodeFail.Load(),
		safePercentage(decodeSuccess.Load(), int64(totalOps)))
	fmt.Printf("   - 초당 작업: %.2f\n",
		safeDiv(float64(totalOps), decodeTimeD.Seconds()))

	return nil
}

// RunBadgerDB2Benchmark 함수는 두 번째 벤치마크 스위트를 실행합니다
func RunBadgerDB2Benchmark() {
	// 기본 설정으로 벤치마크 인스턴스 생성
	benchmark := NewBadger2Benchmark()

	// 개발 모드에서는 더 작은 데이터셋 사용 (빠른 테스트용)
	devMode := os.Getenv("DEV_MODE") == "true"
	if devMode {
		benchmark.ExistingOpCount = 500_000 // 50만개 (기존 키)
		benchmark.NewOpCount = 500_000      // 50만개 (새 키)
		benchmark.MaxExistingID = 1_000_000 // 첫번째 벤치마크의 개발 모드 값
		fmt.Println("개발 모드: 작은 데이터셋 사용 (총 100만개, 기존 키 50만개, 새 키 50만개)")
	}

	// 환경 변수를 통한 워커 수 설정 (시스템 최적화용)
	if workerEnv := os.Getenv("NUM_WORKERS"); workerEnv != "" {
		var workers int
		fmt.Sscanf(workerEnv, "%d", &workers)
		if workers > 0 {
			benchmark.NumWorkers = workers
		}
	}

	// 환경 변수를 통한 랜덤 시드 설정 (재현성용)
	if seedEnv := os.Getenv("RANDOM_SEED"); seedEnv != "" {
		var seed int64
		fmt.Sscanf(seedEnv, "%d", &seed)
		if seed > 0 {
			benchmark.RandomSeed = seed
		}
	}

	// 벤치마크 실행 정보 출력
	fmt.Printf("BadgerDB 두 번째 벤치마크 시작 (기존 키: %d개, 새 키: %d개, 총: %d개, 워커: %d개, 랜덤 시드: %d)\n",
		benchmark.ExistingOpCount, benchmark.NewOpCount,
		benchmark.ExistingOpCount+benchmark.NewOpCount, benchmark.NumWorkers, benchmark.RandomSeed)

	// 벤치마크 실행
	err := benchmark.Run()
	if err != nil {
		fmt.Printf("벤치마크 실패: %v\n", err)
		os.Exit(1)
	}
}

// safeDiv는 분모가 0일 때 0을 반환하는 안전한 나눗셈 함수
func safeDiv(numerator, denominator float64) float64 {
	if denominator == 0 {
		return 0
	}
	return numerator / denominator
}

// safeDiv2는 정수형을 위한 안전한 나눗셈 함수
func safeDiv2(numerator int64, denominator int64) int64 {
	if denominator == 0 {
		return 0
	}
	return numerator / denominator
}

// safePercentage는 분모가 0일 때 0을 반환하는 안전한 백분율 계산 함수
func safePercentage(numerator, denominator int64) float64 {
	if denominator == 0 {
		return 0
	}
	return float64(numerator*100) / float64(denominator)
}
