package main

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/edsrzf/mmap-go"
)

const (
	// 메모리 관리 설정
	MaxMemoryUsage     = 12 * 1024 * 1024 * 1024 // 최대 12GB 메모리 사용
	MaxDepositsPerUser = 1000                    // 한 사용자가 최대로 가질 수 있는 입금 주소 수 (최적화용)
	ConcurrentWorkers  = 8                       // 병렬 작업자 수 (CPU 코어 수에 맞게 조정)
)

// 이더리움 주소 타입
type EthAddress [20]byte

// Dual 구조체 - 입금 주소와 관련 사용자 주소들
type Dual struct {
	Deposit EthAddress   // 입금 주소
	Users   []EthAddress // 사용자 주소들
}

// 병합된 Dual 구조체 - 여러 입금 주소와 사용자 주소들
type UnionedDual struct {
	DepositsMap map[EthAddress]bool // 모든 입금 주소들 (맵으로 중복 방지)
	UsersMap    map[EthAddress]bool // 모든 사용자 주소들 (맵으로 중복 방지)
	Root        EthAddress          // 대표(루트) 주소
	Size        uint32              // 총 노드 수
}

// UnionFind 분석기
type UnionFindAnalyzer struct {
	// 원본 데이터
	duals []Dual

	// Union-Find 데이터 구조
	parent     []uint32              // 부모 인덱스
	size       []uint32              // 집합 크기
	addrToIdx  map[EthAddress]uint32 // 주소 -> 인덱스 매핑
	idxToAddr  []EthAddress          // 인덱스 -> 주소 매핑
	totalNodes uint32                // 총 노드 수

	// 분석 결과
	rootStats     map[uint32]uint32       // 루트 별 크기 통계
	unionHistory  []UnionHistoryEntry     // 유니온 히스토리
	unionedDuals  map[uint32]*UnionedDual // 루트 인덱스 -> 병합된 Dual
	depositToRoot map[EthAddress]uint32   // 입금 주소 -> 루트 인덱스 매핑

	// 동기화 관련
	unionMutex sync.Mutex // Union 연산 동기화를 위한 뮤텍스
}

// 유니온 히스토리 엔트리
type UnionHistoryEntry struct {
	Addr1     EthAddress // 첫 번째 주소
	Addr2     EthAddress // 두 번째 주소
	NewRoot   EthAddress // 새 루트 주소
	NewSize   uint32     // 새 집합 크기
	Timestamp time.Time  // 타임스탬프
}

// 주소를 16진수 문자열로 변환
func (addr EthAddress) String() string {
	return "0x" + hex.EncodeToString(addr[:])
}

// 16진수 문자열을 주소로 변환
func ParseEthAddress(addr string) (EthAddress, error) {
	var result EthAddress
	addr = strings.TrimSpace(strings.ToLower(addr))
	addr = strings.TrimPrefix(addr, "0x")

	if len(addr) != 40 {
		return result, fmt.Errorf("invalid address length: %s", addr)
	}

	b, err := hex.DecodeString(addr)
	if err != nil {
		return result, fmt.Errorf("invalid hex string: %s", err)
	}

	copy(result[:], b)
	return result, nil
}

// Dual 데이터 파일 로드 (최적화된 mmap 직접 접근 사용)
func LoadDualFile(filename string) ([]Dual, error) {
	// 파일 열기
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("파일 열기 실패: %w", err)
	}
	defer file.Close()

	// 메모리 매핑
	mmapData, err := mmap.Map(file, mmap.RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("메모리 매핑 실패: %w", err)
	}
	defer mmapData.Unmap()

	// 안전하게 슬라이스 변환
	data := unsafe.Slice((*byte)(unsafe.Pointer(&mmapData[0])), len(mmapData))

	// 결과 슬라이스의 예상 크기를 미리 할당 (메모리 효율성 증가)
	estimatedCount := len(data) / 100 // 평균 Dual 크기를 100바이트로 가정
	duals := make([]Dual, 0, estimatedCount)

	offset := 0

	// 파일 끝까지 읽기
	for offset < len(data) {
		// 바이트 배열 끝에 도달하면 중단
		if offset+4 > len(data) {
			break
		}

		// 사용자 수 읽기
		userCount := binary.LittleEndian.Uint32(data[offset : offset+4])
		offset += 4

		// 입금 주소 읽기
		if offset+20 > len(data) {
			break
		}
		var deposit EthAddress
		copy(deposit[:], data[offset:offset+20])
		offset += 20

		// 사용자 주소 읽기
		usersSize := int(userCount) * 20
		if offset+usersSize > len(data) {
			break
		}

		users := make([]EthAddress, userCount)
		for i := uint32(0); i < userCount; i++ {
			copy(users[i][:], data[offset:offset+20])
			offset += 20
		}

		// Dual 추가
		duals = append(duals, Dual{
			Deposit: deposit,
			Users:   users,
		})
	}

	return duals, nil
}

// Dual 크기 분포 분석 (수정된 범위)
func AnalyzeDualSizeDistribution(duals []Dual) map[string]int {
	// 크기 범위 정의 (수정됨)
	sizeRanges := map[string]func(int) bool{
		"크기 1":       func(size int) bool { return size == 1 },
		"크기 2":       func(size int) bool { return size == 2 },
		"크기 3~6":     func(size int) bool { return size >= 3 && size <= 6 },
		"크기 7~8":     func(size int) bool { return size >= 7 && size <= 8 },
		"크기 9~16":    func(size int) bool { return size >= 9 && size <= 16 },
		"크기 17~32":   func(size int) bool { return size >= 17 && size <= 32 },
		"크기 33~64":   func(size int) bool { return size >= 33 && size <= 64 },
		"크기 65~127":  func(size int) bool { return size >= 65 && size <= 127 },
		"크기 128~256": func(size int) bool { return size >= 128 && size <= 256 },
		"크기 257+":    func(size int) bool { return size >= 257 },
	}

	// 분포 계산
	distribution := make(map[string]int)
	for rangeName := range sizeRanges {
		distribution[rangeName] = 0
	}

	// 구체적인 크기별 분포도 추적
	exactSizes := make(map[int]int)

	for _, dual := range duals {
		size := len(dual.Users)
		exactSizes[size]++

		for rangeName, checkFunc := range sizeRanges {
			if checkFunc(size) {
				distribution[rangeName]++
				break
			}
		}
	}

	// 상세 크기 분포 (처음 20개만)
	fmt.Println("\n상세 크기 분포 (상위 20개):")

	// 크기별로 정렬
	type sizeCount struct {
		size  int
		count int
	}

	var sizeCountList []sizeCount
	for size, count := range exactSizes {
		sizeCountList = append(sizeCountList, sizeCount{size, count})
	}

	sort.Slice(sizeCountList, func(i, j int) bool {
		return sizeCountList[i].count > sizeCountList[j].count
	})

	for i, sc := range sizeCountList {
		if i >= 20 {
			break
		}
		fmt.Printf("크기 %d: %d개 (%.2f%%)\n",
			sc.size, sc.count, float64(sc.count)/float64(len(duals))*100)
	}

	return distribution
}

// 새 UnionFindAnalyzer 생성
func NewUnionFindAnalyzer(duals []Dual) *UnionFindAnalyzer {
	analyzer := &UnionFindAnalyzer{
		duals:         duals,
		addrToIdx:     make(map[EthAddress]uint32),
		rootStats:     make(map[uint32]uint32),
		totalNodes:    0,
		unionedDuals:  make(map[uint32]*UnionedDual),
		depositToRoot: make(map[EthAddress]uint32),
	}

	// 모든 고유 주소 수집 (deposit + users)
	uniqueAddrs := make(map[EthAddress]bool)

	for _, dual := range duals {
		uniqueAddrs[dual.Deposit] = true
		for _, user := range dual.Users {
			uniqueAddrs[user] = true
		}
	}

	// 인덱스 할당
	analyzer.totalNodes = uint32(len(uniqueAddrs))
	analyzer.idxToAddr = make([]EthAddress, analyzer.totalNodes)

	idx := uint32(0)
	for addr := range uniqueAddrs {
		analyzer.addrToIdx[addr] = idx
		analyzer.idxToAddr[idx] = addr
		idx++
	}

	// 유니온-파인드 데이터 구조 초기화
	analyzer.parent = make([]uint32, analyzer.totalNodes)
	analyzer.size = make([]uint32, analyzer.totalNodes)

	// 초기 상태: 각 노드는 자신이 루트
	for i := uint32(0); i < analyzer.totalNodes; i++ {
		analyzer.parent[i] = i
		analyzer.size[i] = 1
	}

	// 각 Dual의 deposit을 초기 UnionedDual로 설정
	for _, dual := range duals {
		idx, ok := analyzer.addrToIdx[dual.Deposit]
		if !ok {
			continue
		}

		analyzer.unionedDuals[idx] = &UnionedDual{
			DepositsMap: map[EthAddress]bool{dual.Deposit: true},
			UsersMap:    make(map[EthAddress]bool),
			Root:        dual.Deposit,
			Size:        uint32(len(dual.Users) + 1), // deposit + users
		}

		// 사용자 추가
		for _, user := range dual.Users {
			analyzer.unionedDuals[idx].UsersMap[user] = true
		}

		analyzer.depositToRoot[dual.Deposit] = idx
	}

	return analyzer
}

// 최적화된 Find 연산 (경로 압축 구현 개선)
func (a *UnionFindAnalyzer) Find(idx uint32) uint32 {
	if a.parent[idx] != idx {
		a.parent[idx] = a.Find(a.parent[idx]) // 재귀적 경로 압축
	}
	return a.parent[idx]
}

// 스레드 안전한 Find 연산 (병렬 처리에 사용)
func (a *UnionFindAnalyzer) FindSafe(idx uint32) uint32 {
	// 경로 정보를 먼저 수집
	var path []uint32
	current := idx

	// 루트 찾기
	for current != a.parent[current] {
		path = append(path, current)
		current = a.parent[current]
	}

	// 경로 압축 (로컬 변경만)
	root := current
	for _, node := range path {
		a.parent[node] = root
	}

	return root
}

// Union 연산 (UnionedDual 병합 로직 포함) - 최적화 및 스레드 안전성 추가
func (a *UnionFindAnalyzer) Union(idx1, idx2 uint32) {
	// 각 인덱스의 루트 찾기
	root1 := a.Find(idx1)
	root2 := a.Find(idx2)

	// 이미 같은 집합에 있는 경우
	if root1 == root2 {
		return
	}

	// 스레드 안전을 위해 락 획득
	a.unionMutex.Lock()
	defer a.unionMutex.Unlock()

	// 다시 한번 루트 확인 (락 획득 후)
	root1 = a.Find(idx1)
	root2 = a.Find(idx2)

	if root1 == root2 {
		return
	}

	var newRoot, oldRoot uint32

	// 작은 트리를 큰 트리에 붙이기 (크기를 기준으로)
	if a.size[root1] < a.size[root2] {
		a.parent[root1] = root2
		a.size[root2] += a.size[root1]
		newRoot = root2
		oldRoot = root1
	} else {
		a.parent[root2] = root1
		a.size[root1] += a.size[root2]
		newRoot = root1
		oldRoot = root2
	}

	// UnionedDual 병합 - 최적화된 버전 호출
	a.mergeUnionedDualsOptimized(newRoot, oldRoot)

	// 모든 히스토리 저장
	historyEntry := UnionHistoryEntry{
		Addr1:     a.idxToAddr[idx1],
		Addr2:     a.idxToAddr[idx2],
		NewRoot:   a.idxToAddr[newRoot],
		NewSize:   a.size[newRoot],
		Timestamp: time.Now(),
	}
	a.unionHistory = append(a.unionHistory, historyEntry)
}

// 최적화된 UnionedDual 병합 (맵 사용으로 O(1) 시간 복잡도)
func (a *UnionFindAnalyzer) mergeUnionedDualsOptimized(newRoot, oldRoot uint32) {
	newDual, newExists := a.unionedDuals[newRoot]
	oldDual, oldExists := a.unionedDuals[oldRoot]

	// 둘 다 없으면 새로 생성
	if !newExists && !oldExists {
		a.unionedDuals[newRoot] = &UnionedDual{
			DepositsMap: make(map[EthAddress]bool),
			UsersMap:    make(map[EthAddress]bool),
			Root:        a.idxToAddr[newRoot],
			Size:        a.size[newRoot],
		}
		return
	}

	// 새 루트에 UnionedDual이 없으면 생성
	if !newExists {
		a.unionedDuals[newRoot] = &UnionedDual{
			DepositsMap: make(map[EthAddress]bool),
			UsersMap:    make(map[EthAddress]bool),
			Root:        a.idxToAddr[newRoot],
			Size:        a.size[newRoot],
		}
		newDual = a.unionedDuals[newRoot]
	}

	// 이전 루트에 UnionedDual이 없으면 무시
	if !oldExists {
		return
	}

	// 맵을 사용하여 효율적으로 병합 (O(1) 시간 복잡도로 중복 검사)
	for deposit := range oldDual.DepositsMap {
		newDual.DepositsMap[deposit] = true
		a.depositToRoot[deposit] = newRoot
	}

	for user := range oldDual.UsersMap {
		newDual.UsersMap[user] = true
	}

	newDual.Size = a.size[newRoot]

	// 이전 UnionedDual 삭제
	delete(a.unionedDuals, oldRoot)
}

// 주소로 Union 연산 수행
func (a *UnionFindAnalyzer) UnionByAddress(addr1, addr2 EthAddress) {
	idx1, ok1 := a.addrToIdx[addr1]
	idx2, ok2 := a.addrToIdx[addr2]

	if !ok1 || !ok2 {
		log.Printf("⚠️ 알 수 없는 주소: %s 또는 %s", addr1.String(), addr2.String())
		return
	}

	a.Union(idx1, idx2)
}

// ProcessDuals 함수 최적화 및 병렬 처리 개선
func (a *UnionFindAnalyzer) ProcessDuals() {
	startTime := time.Now()

	log.Println("1단계: Dual 내부 연결 처리 중...")
	processedCount := int64(0)
	totalDuals := int64(len(a.duals))

	// 진행 상황 모니터링을 위한 별도 고루틴
	done := make(chan bool)
	go func() {
		lastReportTime := time.Now()
		for {
			select {
			case <-done:
				return
			default:
				if time.Since(lastReportTime) > time.Second {
					currentProcessed := atomic.LoadInt64(&processedCount)
					elapsed := time.Since(startTime)
					remaining := time.Duration(float64(elapsed) / float64(currentProcessed) * float64(totalDuals-currentProcessed))
					log.Printf("진행 중... %d/%d Dual 처리 (%.2f%%), 예상 남은 시간: %v",
						currentProcessed, totalDuals, float64(currentProcessed)*100/float64(totalDuals),
						remaining.Round(time.Second))
					lastReportTime = time.Now()
				}
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()

	// 1단계: Dual 내부 연결
	// 이 단계는 병렬화가 안전함 (각 Dual 내부 연결은 독립적임)
	chunkSize := len(a.duals) / ConcurrentWorkers
	if chunkSize < 1 {
		chunkSize = 1
	}

	var wg sync.WaitGroup

	// 작업자 함수
	processDualRange := func(startIdx, endIdx int) {
		defer wg.Done()

		for i := startIdx; i < endIdx; i++ {
			dual := a.duals[i]

			// 입금 주소 인덱스 가져오기
			depositIdx, ok := a.addrToIdx[dual.Deposit]
			if !ok {
				atomic.AddInt64(&processedCount, 1)
				continue
			}

			// 사용자들을 입금 주소와 연결
			for _, user := range dual.Users {
				userIdx, ok := a.addrToIdx[user]
				if !ok {
					continue
				}

				// 유저와 입금 주소 연결
				a.Union(depositIdx, userIdx)
			}

			atomic.AddInt64(&processedCount, 1)
		}
	}

	// 작업 분배
	for i := 0; i < ConcurrentWorkers; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if i == ConcurrentWorkers-1 {
			end = len(a.duals)
		}

		wg.Add(1)
		go processDualRange(start, end)
	}

	// 작업 완료 대기
	wg.Wait()
	close(done) // 모니터링 고루틴 종료

	log.Printf("1단계 완료: %d개 Dual 처리 (소요 시간: %v)", len(a.duals), time.Since(startTime))

	// 1단계 완료 후 클러스터 상태 확인
	tempClusters := make(map[uint32]bool)
	for i := uint32(0); i < a.totalNodes; i++ {
		root := a.Find(i)
		tempClusters[root] = true
	}
	log.Printf("1단계 완료 후 클러스터 수: %d", len(tempClusters))

	// 2단계: 공통 사용자를 가진 Dual 간 연결
	log.Println("2단계: 공통 사용자를 가진 Dual 간 연결 처리 중...")
	stage2Start := time.Now()

	// 사용자 주소 -> 해당 사용자가 속한 입금 주소들 매핑
	userToDeposits := make(map[EthAddress][]EthAddress)

	// 매핑 구성
	for _, dual := range a.duals {
		for _, user := range dual.Users {
			// 배열 재할당 최소화를 위한 용량 관리
			if deposits, exists := userToDeposits[user]; exists {
				if len(deposits) < cap(deposits) {
					userToDeposits[user] = append(deposits, dual.Deposit)
				} else {
					// 배열 용량이 부족하면 더 큰 용량으로 재할당
					newDeposits := make([]EthAddress, len(deposits), len(deposits)*2)
					copy(newDeposits, deposits)
					userToDeposits[user] = append(newDeposits, dual.Deposit)
				}
			} else {
				// 처음 생성 시 작은 용량으로 시작
				userToDeposits[user] = []EthAddress{dual.Deposit}
			}
		}
	}

	// 사용자가 너무 많은 입금 주소를 가진 경우 최적화
	for user, deposits := range userToDeposits {
		if len(deposits) > MaxDepositsPerUser {
			log.Printf("최적화: 사용자 %s이(가) %d개의 입금 주소를 가짐 -> %d개로 제한",
				user.String(), len(deposits), MaxDepositsPerUser)
			userToDeposits[user] = deposits[:MaxDepositsPerUser]
		}
	}

	// 병렬 처리를 위해 작업 분할
	usersList := make([]EthAddress, 0, len(userToDeposits))
	for user := range userToDeposits {
		if len(userToDeposits[user]) > 1 { // 여러 입금 주소를 가진 사용자만 포함
			usersList = append(usersList, user)
		}
	}

	log.Printf("총 %d명의 사용자가 다수의 입금 주소 보유", len(usersList))

	// 작업 분할
	userChunkSize := len(usersList) / ConcurrentWorkers
	if userChunkSize < 1 {
		userChunkSize = 1
	}

	// 연결 카운터
	connCount := int64(0)

	// 진행 상황 모니터링 고루틴
	done2 := make(chan bool)
	go func() {
		lastReportTime := time.Now()
		for {
			select {
			case <-done2:
				return
			default:
				if time.Since(lastReportTime) > time.Second {
					currentConn := atomic.LoadInt64(&connCount)
					log.Printf("공통 사용자 연결 중... %d개 연결 완료", currentConn)
					lastReportTime = time.Now()
				}
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()

	// 작업자 함수
	processUserRange := func(users []EthAddress) {
		defer wg.Done()

		localConnCount := 0
		sameRootCount := 0 // 이미 같은 루트인 경우 카운트

		for _, user := range users {
			deposits := userToDeposits[user]

			// 모든 입금 주소 쌍을 연결
			for i := 0; i < len(deposits); i++ {
				for j := i + 1; j < len(deposits); j++ {
					// 두 입금 주소의 인덱스 가져오기
					idx1, ok1 := a.addrToIdx[deposits[i]]
					idx2, ok2 := a.addrToIdx[deposits[j]]

					if !ok1 || !ok2 {
						continue
					}

					// 이미 같은 클러스터에 있는지 확인
					root1 := a.Find(idx1)
					root2 := a.Find(idx2)

					if root1 == root2 {
						sameRootCount++ // 이미 같은 루트인 경우
					} else {
						// 서로 다른 클러스터면 병합
						a.Union(idx1, idx2)
						localConnCount++
					}
				}
			}

			// 주기적으로 카운터 업데이트 (더 자주 업데이트)
			if localConnCount > 10 {
				atomic.AddInt64(&connCount, int64(localConnCount))
				localConnCount = 0
			}
		}

		// 남은 카운터 업데이트 - 매우 중요! 누락 방지
		atomic.AddInt64(&connCount, int64(localConnCount))

		// 디버깅용 통계
		log.Printf("작업자 통계: 새 연결 %d개, 이미 같은 루트 %d개",
			localConnCount, sameRootCount)
	}

	// 작업 분배
	wg.Add(ConcurrentWorkers)
	for i := 0; i < ConcurrentWorkers; i++ {
		start := i * userChunkSize
		end := start + userChunkSize
		if i == ConcurrentWorkers-1 {
			end = len(usersList)
		}

		if start < end {
			go processUserRange(usersList[start:end])
		} else {
			wg.Done() // 작업이 없는 경우
		}
	}

	// 작업 완료 대기
	wg.Wait()
	close(done2) // 모니터링 고루틴 종료

	log.Printf("총 %d개의 공통 사용자 기반 연결 완료 (소요 시간: %v)",
		atomic.LoadInt64(&connCount), time.Since(stage2Start))
}

// 병합 과정에서 사용자 주소가 제대로 병합되는지 확인하는 함수 (디버깅용)
func (a *UnionFindAnalyzer) CheckUsersMerge(idx1, idx2 uint32) {
	root1 := a.Find(idx1)
	root2 := a.Find(idx2)

	ud1, exists1 := a.unionedDuals[root1]
	ud2, exists2 := a.unionedDuals[root2]

	if !exists1 || !exists2 {
		return
	}

	fmt.Printf("병합 전: 주소1(users: %d), 주소2(users: %d)\n",
		len(ud1.UsersMap), len(ud2.UsersMap))

	// 현재 Union 로직 호출 (실제 병합 수행)
	a.Union(idx1, idx2)

	// 병합 후 결과 확인
	newRoot := a.Find(idx1) // 둘 중 아무거나 사용 가능 (같은 루트를 가짐)
	newUd, newExists := a.unionedDuals[newRoot]

	if !newExists {
		return
	}

	fmt.Printf("병합 후: 새 루트(users: %d)\n", len(newUd.UsersMap))
}

// 디버깅용 - 병합 시 사용자 주소가 제대로 복사되는지 확인
func (a *UnionFindAnalyzer) CheckMergeLogic() {
	// 두 개의 UnionedDual 객체 생성
	ud1 := &UnionedDual{
		DepositsMap: map[EthAddress]bool{},
		UsersMap:    map[EthAddress]bool{},
	}

	ud2 := &UnionedDual{
		DepositsMap: map[EthAddress]bool{},
		UsersMap:    map[EthAddress]bool{},
	}

	// 임의의 주소 생성
	addr1 := EthAddress{1, 2, 3}
	addr2 := EthAddress{4, 5, 6}
	addr3 := EthAddress{7, 8, 9}
	addr4 := EthAddress{10, 11, 12}

	// 첫 번째 객체에 주소 추가
	ud1.DepositsMap[addr1] = true
	ud1.UsersMap[addr2] = true

	// 두 번째 객체에 주소 추가
	ud2.DepositsMap[addr3] = true
	ud2.UsersMap[addr4] = true

	// 병합 전 출력
	fmt.Println("병합 전:")
	fmt.Printf("UD1: deposits=%d, users=%d\n", len(ud1.DepositsMap), len(ud1.UsersMap))
	fmt.Printf("UD2: deposits=%d, users=%d\n", len(ud2.DepositsMap), len(ud2.UsersMap))

	// ud2를 ud1으로 병합
	for deposit := range ud2.DepositsMap {
		ud1.DepositsMap[deposit] = true
	}

	for user := range ud2.UsersMap {
		ud1.UsersMap[user] = true
	}

	// 병합 후 출력
	fmt.Println("병합 후:")
	fmt.Printf("UD1: deposits=%d, users=%d\n", len(ud1.DepositsMap), len(ud1.UsersMap))
}

// Union-Find 결과 분석
func (a *UnionFindAnalyzer) AnalyzeResults() {
	// 1. 클러스터 (연결 요소) 통계 계산
	clusters := make(map[uint32][]uint32)   // 루트 -> 집합의 모든 노드
	clusterSizes := make(map[uint32]uint32) // 루트 -> 집합 크기

	// 각 노드의 루트 찾아서 클러스터 구성
	for i := uint32(0); i < a.totalNodes; i++ {
		root := a.Find(i)
		clusters[root] = append(clusters[root], i)
		clusterSizes[root] = a.size[root]
	}

	// UnionedDual 정보 최종 정리
	for root := range clusters {
		if _, exists := a.unionedDuals[root]; !exists {
			// 루트에 해당하는 UnionedDual이 없으면 생성
			a.unionedDuals[root] = &UnionedDual{
				DepositsMap: make(map[EthAddress]bool),
				UsersMap:    make(map[EthAddress]bool),
				Root:        a.idxToAddr[root],
				Size:        clusterSizes[root],
			}
		}
	}

	// 클러스터 크기 분포 계산
	sizeDistribution := make(map[string]int)
	sizeRanges := map[string]func(uint32) bool{
		"크기 1":       func(size uint32) bool { return size == 1 },
		"크기 2":       func(size uint32) bool { return size == 2 },
		"크기 3~6":     func(size uint32) bool { return size >= 3 && size <= 6 },
		"크기 7~8":     func(size uint32) bool { return size >= 7 && size <= 8 },
		"크기 9~16":    func(size uint32) bool { return size >= 9 && size <= 16 },
		"크기 17~32":   func(size uint32) bool { return size >= 17 && size <= 32 },
		"크기 33~64":   func(size uint32) bool { return size >= 33 && size <= 64 },
		"크기 65~127":  func(size uint32) bool { return size >= 65 && size <= 127 },
		"크기 128~256": func(size uint32) bool { return size >= 128 && size <= 256 },
		"크기 257+":    func(size uint32) bool { return size >= 257 },
	}

	for rangeName := range sizeRanges {
		sizeDistribution[rangeName] = 0
	}

	for _, size := range clusterSizes {
		for rangeName, checkFunc := range sizeRanges {
			if checkFunc(size) {
				sizeDistribution[rangeName]++
				break
			}
		}
	}

	// 통계 출력
	fmt.Println("\n유니언-파인드 후 클러스터 분석 결과:")
	fmt.Printf("총 노드 수: %d\n", a.totalNodes)
	fmt.Printf("총 클러스터 수: %d\n", len(clusters))
	fmt.Printf("총 병합 히스토리 개수: %d (전체 저장)\n",
		len(a.unionHistory))

	// 사용자 주소 통계 정보 추가
	fmt.Println("\n사용자 주소 통계:")

	// 사용자 주소 수 분포 계산
	userCountMap := make(map[int]int)
	maxUserCount := 0
	totalUserCount := 0

	for _, ud := range a.unionedDuals {
		userCount := len(ud.UsersMap)
		userCountMap[userCount]++
		totalUserCount += userCount

		if userCount > maxUserCount {
			maxUserCount = userCount
		}
	}

	fmt.Printf("총 클러스터 수: %d\n", len(a.unionedDuals))
	fmt.Printf("총 사용자 주소 수: %d\n", totalUserCount)
	fmt.Printf("최대 사용자 주소 수: %d\n", maxUserCount)

	// 사용자 주소 수 분포 출력
	fmt.Println("\n사용자 주소 수 분포 (상위 10개):")

	type userCountPair struct {
		count int
		freq  int
	}

	var userCounts []userCountPair
	for count, freq := range userCountMap {
		userCounts = append(userCounts, userCountPair{count, freq})
	}

	sort.Slice(userCounts, func(i, j int) bool {
		return userCounts[i].freq > userCounts[j].freq
	})

	for i, pair := range userCounts {
		if i >= 10 {
			break
		}
		fmt.Printf("사용자 주소 %d개: %d개 클러스터 (%.2f%%)\n",
			pair.count, pair.freq, float64(pair.freq)/float64(len(a.unionedDuals))*100)
	}

	// 사용자 주소가 많은 클러스터 출력
	fmt.Println("\n사용자 주소가 가장 많은 클러스터 (상위 5개):")

	var userRichRoots []uint32
	for root := range a.unionedDuals {
		userRichRoots = append(userRichRoots, root)
	}

	sort.Slice(userRichRoots, func(i, j int) bool {
		return len(a.unionedDuals[userRichRoots[i]].UsersMap) > len(a.unionedDuals[userRichRoots[j]].UsersMap)
	})

	for i := 0; i < min(5, len(userRichRoots)); i++ {
		root := userRichRoots[i]
		ud := a.unionedDuals[root]
		fmt.Printf("클러스터 %d: 사용자 주소 %d개, 입금 주소 %d개, 크기 %d\n",
			i+1, len(ud.UsersMap), len(ud.DepositsMap), ud.Size)

		// 입금 주소 샘플 출력
		if len(ud.DepositsMap) > 0 {
			fmt.Printf("  입금 주소 샘플: ")
			count := 0
			for deposit := range ud.DepositsMap {
				if count >= 3 {
					fmt.Printf("외 %d개\n", len(ud.DepositsMap)-3)
					break
				}
				fmt.Printf("%s ", deposit.String())
				count++
			}
			if count <= 3 {
				fmt.Println()
			}
		}

		// 사용자 주소 샘플 출력
		if len(ud.UsersMap) > 0 {
			fmt.Printf("  사용자 주소 샘플: ")
			count := 0
			for user := range ud.UsersMap {
				if count >= 3 {
					fmt.Printf("외 %d개\n", len(ud.UsersMap)-3)
					break
				}
				fmt.Printf("%s ", user.String())
				count++
			}
			if count <= 3 {
				fmt.Println()
			}
		}
	}

	// 클러스터 크기 분포 출력
	fmt.Println("\n클러스터 크기 분포:")

	// 범위를 정렬된 순서로 출력
	ranges := []string{
		"크기 1", "크기 2", "크기 3~6", "크기 7~8",
		"크기 9~16", "크기 17~32", "크기 33~64",
		"크기 65~127", "크기 128~256", "크기 257+",
	}

	for _, r := range ranges {
		count := sizeDistribution[r]
		percent := float64(count) / float64(len(clusters)) * 100
		fmt.Printf("%s: %d개 (%.2f%%)\n", r, count, percent)
	}

	// 상위 10개 클러스터 출력
	var sortedRoots []uint32
	for root := range clusters {
		sortedRoots = append(sortedRoots, root)
	}

	sort.Slice(sortedRoots, func(i, j int) bool {
		return clusterSizes[sortedRoots[i]] > clusterSizes[sortedRoots[j]]
	})

	fmt.Println("\n상위 10개 클러스터:")
	for i := 0; i < min(10, len(sortedRoots)); i++ {
		root := sortedRoots[i]
		ud := a.unionedDuals[root]
		fmt.Printf("클러스터 %d: 크기 %d, 루트 주소: %s\n",
			i+1, clusterSizes[root], a.idxToAddr[root].String())

		// 입금 주소 및 사용자 주소 개수 출력
		depositsCount := len(ud.DepositsMap)
		usersCount := len(ud.UsersMap)
		fmt.Printf("  입금 주소 수: %d, 사용자 주소 수: %d\n", depositsCount, usersCount)

		// 입금 주소 샘플 출력 (최대 5개)
		if depositsCount > 0 {
			fmt.Printf("  입금 주소 샘플: ")
			count := 0
			for deposit := range ud.DepositsMap {
				if count >= 5 {
					fmt.Printf("외 %d개\n", depositsCount-5)
					break
				}
				fmt.Printf("%s ", deposit.String())
				count++
			}
			if count <= 5 {
				fmt.Println()
			}
		}
	}

	// 유니온 히스토리 요약
	fmt.Printf("\n총 유니온 연산 수: %d (전체 저장)\n",
		len(a.unionHistory))
	if len(a.unionHistory) > 0 {
		fmt.Println("\n유니온 히스토리 샘플 (처음 5개):")
		for i, entry := range a.unionHistory {
			if i >= 5 {
				break
			}
			fmt.Printf("병합: %s + %s -> 루트: %s (크기: %d)\n",
				entry.Addr1.String(), entry.Addr2.String(),
				entry.NewRoot.String(), entry.NewSize)
		}
	}

	// 각 크기별 클러스터 출력 시 추가 정보 표시
	fmt.Println("\n각 크기별 UnionedDual 분석:")

	// 각 크기별 최대 사용자 수 계산
	for size := 3; size <= 6; size++ {
		var sizeRoots []uint32
		for root, clusterSize := range clusterSizes {
			if clusterSize == uint32(size) {
				sizeRoots = append(sizeRoots, root)
			}
		}

		if len(sizeRoots) == 0 {
			continue
		}

		// 사용자 수가 가장 많은 클러스터 찾기
		maxUsers := 0
		for _, root := range sizeRoots {
			ud, exists := a.unionedDuals[root]
			if exists && len(ud.UsersMap) > maxUsers {
				maxUsers = len(ud.UsersMap)
			}
		}

		// 결과 출력
		fmt.Printf("\n크기 %d: 총 %d개 클러스터, 최대 사용자 수: %d\n",
			size, len(sizeRoots), maxUsers)
	}

	// 각 크기별로 10개씩 출력
	printClustersBySize(a, clusters, clusterSizes, 3, 6)
}

// 특정 크기 범위의 클러스터를 출력하는 함수
func printClustersBySize(a *UnionFindAnalyzer, clusters map[uint32][]uint32, clusterSizes map[uint32]uint32, minSize, maxSize int) {
	fmt.Println("\n크기별 UnionedDual 샘플:")

	for size := minSize; size <= maxSize; size++ {
		// 해당 크기의 클러스터 찾기
		var sizeRoots []uint32
		for root, clusterSize := range clusterSizes {
			if clusterSize == uint32(size) {
				sizeRoots = append(sizeRoots, root)
			}
		}

		// 정렬 기준 추가 - 양쪽 모두 고려
		sort.Slice(sizeRoots, func(i, j int) bool {
			ud1, exists1 := a.unionedDuals[sizeRoots[i]]
			ud2, exists2 := a.unionedDuals[sizeRoots[j]]

			if !exists1 || !exists2 {
				return false
			}

			// 입금 주소와 사용자 주소 수의 합으로 정렬 (다양성 증가)
			totalAddrs1 := len(ud1.DepositsMap) + len(ud1.UsersMap)
			totalAddrs2 := len(ud2.DepositsMap) + len(ud2.UsersMap)
			return totalAddrs1 > totalAddrs2
		})

		fmt.Printf("\n크기 %d인 UnionedDual (최대 10개):\n", size)
		for i := 0; i < min(10, len(sizeRoots)); i++ {
			root := sizeRoots[i]
			ud, exists := a.unionedDuals[root]

			if !exists {
				continue
			}

			// 입금 주소와 사용자 주소 맵에서 슬라이스로 변환
			deposits := make([]EthAddress, 0, len(ud.DepositsMap))
			for deposit := range ud.DepositsMap {
				deposits = append(deposits, deposit)
			}

			users := make([]EthAddress, 0, len(ud.UsersMap))
			for user := range ud.UsersMap {
				users = append(users, user)
			}

			fmt.Printf("UnionedDual %d: 크기 %d, 입금 주소 수 %d, 사용자 주소 수 %d\n",
				i+1, size, len(deposits), len(users))

			// 입금 주소 샘플 출력
			if len(deposits) > 0 {
				fmt.Printf("  입금 주소: ")
				for j, deposit := range deposits {
					if j >= 5 {
						fmt.Printf("외 %d개", len(deposits)-5)
						break
					}
					fmt.Printf("%s ", deposit.String())
				}
				fmt.Println()
			}

			// 사용자 주소 샘플 출력
			if len(users) > 0 {
				fmt.Printf("  사용자 주소: ")
				for j, user := range users {
					if j >= 5 {
						fmt.Printf("외 %d개", len(users)-5)
						break
					}
					fmt.Printf("%s ", user.String())
				}
				fmt.Println()
			}
		}

		if len(sizeRoots) > 10 {
			fmt.Printf("... 외 %d개 더 존재\n", len(sizeRoots)-10)
		} else if len(sizeRoots) == 0 {
			fmt.Println("해당 크기의 클러스터가 없습니다.")
		}
	}
}

// 메모리 사용량 모니터링
func monitorMemory() {
	for {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)

		// 최대 메모리 초과 시 경고 및 GC 실행
		if m.Alloc > MaxMemoryUsage {
			log.Printf("⚠️ 메모리 사용량 경고: %.2f GB 사용 중, GC 실행",
				float64(m.Alloc)/(1024*1024*1024))
			runtime.GC()
		}

		time.Sleep(5 * time.Second)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func main() {
	// 메모리 모니터링 시작
	go monitorMemory()

	// 난수 초기화
	rand.Seed(time.Now().UnixNano())

	// 변수 초기화
	dualFile := "./output/dual_09_to_12.bin"

	// Dual 파일 로드
	log.Println("Dual 파일 로드 중...")
	startTime := time.Now()
	duals, err := LoadDualFile(dualFile)
	if err != nil {
		log.Fatalf("Dual 파일 로드 실패: %v", err)
	}
	log.Printf("총 %d개의 Dual 로드됨 (소요 시간: %v)", len(duals), time.Since(startTime))

	// 크기 분포 분석
	log.Println("Dual 크기 분포 분석 중...")
	distribution := AnalyzeDualSizeDistribution(duals)

	// 분포 출력
	fmt.Println("\nDual 크기 분포:")
	var totalDuals int
	for _, count := range distribution {
		totalDuals += count
	}

	// 분포를 정렬된 순서로 출력
	ranges := []string{
		"크기 1", "크기 2", "크기 3~6", "크기 7~8", "크기 9~16",
		"크기 17~32", "크기 33~64", "크기 65~127", "크기 128~256", "크기 257+",
	}

	for _, r := range ranges {
		count := distribution[r]
		percent := float64(count) / float64(totalDuals) * 100
		fmt.Printf("%s: %d개 (%.2f%%)\n", r, count, percent)
	}

	// Union-Find 분석기 생성
	log.Println("Union-Find 분석기 초기화 중...")
	analyzer := NewUnionFindAnalyzer(duals)
	log.Printf("분석기 초기화 완료 (%d개 노드)", analyzer.totalNodes)

	// 유니언-파인드 처리
	log.Println("Union-Find 연산 시작...")
	startTime = time.Now()
	analyzer.ProcessDuals()
	log.Printf("Union-Find 연산 완료 (소요 시간: %v)", time.Since(startTime))

	// 결과 분석 및 출력
	log.Println("결과 분석 중...")
	analyzer.AnalyzeResults()

	log.Println("모든 작업 완료!")
}
