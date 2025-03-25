package main

// import (
// 	"fmt"
// 	"math/rand"
// 	"runtime"
// 	"sync"
// 	"time"
// )

// // 배열 기반 Union-Find 구현
// type ArrayUnionFind struct {
// 	parent []uint64 // 부모 배열
// 	rank   []uint8  // 랭크 배열 (트리 높이 관리용)
// 	size   []uint8  // 집합 크기 배열 (1~4로 제한)
// 	count  int      // 서로소 집합의 수
// }

// // 새 Union-Find 생성
// func NewArrayUnionFind(n int) *ArrayUnionFind {
// 	parent := make([]uint64, n)
// 	rank := make([]uint8, n)
// 	size := make([]uint8, n)

// 	// 모든 원소를 자기 자신을 가리키도록 초기화
// 	for i := 0; i < n; i++ {
// 		parent[i] = uint64(i)
// 		size[i] = 1 // 초기 크기는 1
// 	}

// 	return &ArrayUnionFind{
// 		parent: parent,
// 		rank:   rank,
// 		size:   size,
// 		count:  n, // 초기 서로소 집합 수는 n
// 	}
// }

// // Find: 원소의 루트를 찾는 함수 (경로 압축 적용)
// func (uf *ArrayUnionFind) Find(x uint64) uint64 {
// 	if uf.parent[x] != x {
// 		uf.parent[x] = uf.Find(uf.parent[x]) // 경로 압축
// 	}
// 	return uf.parent[x]
// }

// // Union: 두 원소를 합치는 함수 (랭크 기반 최적화 적용)
// func (uf *ArrayUnionFind) Union(x, y uint64) bool {
// 	rootX := uf.Find(x)
// 	rootY := uf.Find(y)

// 	// 이미 같은 집합에 속함
// 	if rootX == rootY {
// 		return false
// 	}

// 	// 랭크에 따라 합치기 (작은 트리를 큰 트리 아래로)
// 	if uf.rank[rootX] < uf.rank[rootY] {
// 		uf.parent[rootX] = rootY
// 		// 집합 크기 업데이트 (최대 4)
// 		newSize := uf.size[rootX] + uf.size[rootY]

// 		uf.size[rootY] = newSize
// 	} else if uf.rank[rootX] > uf.rank[rootY] {
// 		uf.parent[rootY] = rootX
// 		// 집합 크기 업데이트 (최대 4)
// 		newSize := uf.size[rootX] + uf.size[rootY]

// 		uf.size[rootX] = newSize
// 	} else {
// 		uf.parent[rootY] = rootX
// 		uf.rank[rootX]++
// 		// 집합 크기 업데이트 (최대 4)
// 		newSize := uf.size[rootX] + uf.size[rootY]

// 		uf.size[rootX] = newSize
// 	}

// 	// 서로소 집합 수 감소
// 	uf.count--
// 	return true
// }

// // GetCount: 현재 서로소 집합 수 반환
// func (uf *ArrayUnionFind) GetCount() int {
// 	return uf.count
// }

// // 메모리 사용량 구조체
// type MemoryStats struct {
// 	Alloc      uint64
// 	TotalAlloc uint64
// 	Sys        uint64
// 	NumGC      uint32
// }

// // 메모리 사용량 측정
// func getMemoryStats() MemoryStats {
// 	var m runtime.MemStats
// 	runtime.ReadMemStats(&m)
// 	return MemoryStats{
// 		Alloc:      m.Alloc,
// 		TotalAlloc: m.TotalAlloc,
// 		Sys:        m.Sys,
// 		NumGC:      m.NumGC,
// 	}
// }

// // 메모리 사용량 출력
// func printMemoryUsage(stage string, stats MemoryStats) {
// 	fmt.Printf("===== %s =====\n", stage)
// 	fmt.Printf("현재 할당: %.2f MB\n", float64(stats.Alloc)/1024/1024)
// 	fmt.Printf("총 할당: %.2f MB\n", float64(stats.TotalAlloc)/1024/1024)
// 	fmt.Printf("시스템 메모리: %.2f MB\n", float64(stats.Sys)/1024/1024)
// 	fmt.Printf("GC 실행 횟수: %d\n", stats.NumGC)
// 	fmt.Println()
// }

// func main() {
// 	// 시작 시간
// 	startTime := time.Now()

// 	// 원소 수
// 	const numElements = 1_000_000_000 // 1억

// 	fmt.Printf("배열 기반 Union-Find 테스트 (원소 %d개)\n", numElements)
// 	fmt.Println("----------------------------------------------")

// 	// 초기 메모리 상태
// 	initialStats := getMemoryStats()
// 	printMemoryUsage("초기 상태", initialStats)

// 	// 1. 유니언 파인드 초기화
// 	fmt.Println("1. Union-Find 초기화 중...")
// 	initStart := time.Now()

// 	uf := NewArrayUnionFind(numElements)

// 	initDuration := time.Since(initStart)
// 	fmt.Printf("초기화 완료: %v\n", initDuration)

// 	initStats := getMemoryStats()
// 	printMemoryUsage("초기화 후", initStats)

// 	// 2. Union 연산 수행
// 	fmt.Println("2. Union 연산 수행 중...")

// 	// 20%의 원소가 크기 2~4인 집합에 속하도록 설정
// 	numToMerge := int(float64(numElements) * 0.2)
// 	fmt.Printf("병합할 원소 수: %d (전체의 20%%)\n", numToMerge)

// 	unionStart := time.Now()

// 	// 원소들을 크기별로 할당 (2, 3, 4)
// 	targetSets := make(map[int][]uint64)
// 	for size := 2; size <= 4; size++ {
// 		targetSets[size] = make([]uint64, 0, numToMerge/3)
// 	}

// 	// 무작위로 원소를 크기별 집합에 할당
// 	for i := 0; i < numToMerge; i++ {
// 		elem := uint64(rand.Intn(numElements))
// 		size := rand.Intn(3) + 2 // 2, 3, 4 중 하나 선택
// 		targetSets[size] = append(targetSets[size], elem)
// 	}

// 	// 병합 작업 수행
// 	mergedCount := 0
// 	for size, elems := range targetSets {
// 		fmt.Printf("크기 %d인 집합 생성 중...\n", size)

// 		numSets := len(elems) / size
// 		for i := 0; i < numSets; i++ {
// 			baseElem := elems[i*size]

// 			for j := 1; j < size; j++ {
// 				if i*size+j < len(elems) {
// 					otherElem := elems[i*size+j]
// 					uf.Union(baseElem, otherElem)
// 					mergedCount++
// 				}
// 			}

// 			// 진행 상황 출력
// 			if (i+1)%(numSets/10) == 0 && i > 0 {
// 				fmt.Printf("크기 %d인 집합 진행률: %d/%d (%.1f%%)\n",
// 					size, i+1, numSets, float64(i+1)/float64(numSets)*100)
// 				unionStats := getMemoryStats()
// 				printMemoryUsage(fmt.Sprintf("크기 %d 집합 진행중", size), unionStats)
// 			}
// 		}
// 	}

// 	unionDuration := time.Since(unionStart)
// 	fmt.Printf("Union 연산 완료: %v (병합 %d개)\n", unionDuration, mergedCount)

// 	unionStats := getMemoryStats()
// 	printMemoryUsage("Union 후", unionStats)

// 	// 3. 집합 크기 분포 계산
// 	fmt.Println("3. 집합 크기 분포 계산 중...")
// 	distributionStart := time.Now()

// 	// 병렬 처리를 위한 설정
// 	numWorkers := runtime.NumCPU()
// 	workerSize := numElements / numWorkers
// 	sizeDist := make([]map[uint8]int, numWorkers)

// 	var wg sync.WaitGroup

// 	for w := 0; w < numWorkers; w++ {
// 		wg.Add(1)
// 		go func(id int) {
// 			defer wg.Done()
// 			start := id * workerSize
// 			end := (id + 1) * workerSize
// 			if id == numWorkers-1 {
// 				end = numElements
// 			}

// 			// 각 워커의 로컬 크기 분포
// 			dist := make(map[uint8]int)

// 			for i := start; i < end; i++ {
// 				root := uf.Find(uint64(i))
// 				size := uf.size[root]
// 				dist[size]++
// 			}

// 			sizeDist[id] = dist
// 			fmt.Printf("워커 %d: %d-%d 범위 처리 완료\n", id, start, end)
// 		}(w)
// 	}

// 	wg.Wait()

// 	// 모든 워커의 결과를 합산
// 	finalSizeDist := make(map[uint8]int)

// 	for _, dist := range sizeDist {
// 		for size, count := range dist {
// 			finalSizeDist[size] += count
// 		}
// 	}

// 	distributionDuration := time.Since(distributionStart)
// 	fmt.Printf("집합 크기 분포 계산 완료: %v\n", distributionDuration)

// 	// 집합 크기 분포 출력
// 	fmt.Println("최종 집합 크기 분포:")
// 	for size := uint8(1); size <= 100; size++ {
// 		count := finalSizeDist[size]
// 		percent := float64(count) / float64(numElements) * 100
// 		fmt.Printf("크기 %d인 집합: %d개 (%.2f%%)\n", size, count, percent)
// 	}

// 	distStats := getMemoryStats()
// 	printMemoryUsage("크기 분포 계산 후", distStats)

// 	// 전체 실행 시간
// 	totalDuration := time.Since(startTime)
// 	fmt.Printf("\n전체 실행 시간: %v\n", totalDuration)

// 	// 서로소 집합 수 출력
// 	fmt.Printf("최종 서로소 집합 수: %d\n", uf.GetCount())

// 	// 최종 메모리 통계
// 	finalStats := getMemoryStats()
// 	printMemoryUsage("최종 상태", finalStats)
// }
