package main

// import (
// 	"fmt"
// 	"math/rand"
// 	"runtime"
// 	"sync"
// 	"time"
// )

// // Node 구조체 - Union-Find 트리의 개별 노드
// type Node struct {
// 	Value  uint64 // 원소 값
// 	Parent *Node  // 부모 노드 참조
// 	Rank   uint8  // 트리 높이 (Union-by-rank 최적화용)
// 	Size   uint8  // 집합 크기 (1~4로 제한)
// }

// // 노드 기반 Union-Find 구현
// type NodeUnionFind struct {
// 	Nodes map[uint64]*Node // 값 -> 노드 매핑
// 	Count int              // 서로소 집합의 수
// 	mu    sync.RWMutex     // 병렬 접근을 위한 뮤텍스
// }

// // 새 Union-Find 생성
// func NewNodeUnionFind() *NodeUnionFind {
// 	return &NodeUnionFind{
// 		Nodes: make(map[uint64]*Node),
// 		Count: 0,
// 	}
// }

// // MakeSet: 새 원소 추가
// func (uf *NodeUnionFind) MakeSet(x uint64) *Node {
// 	uf.mu.Lock()
// 	defer uf.mu.Unlock()

// 	// 이미 존재하는 원소인지 확인
// 	if node, exists := uf.Nodes[x]; exists {
// 		return node
// 	}

// 	// 새 노드 생성
// 	node := &Node{
// 		Value: x,
// 		Rank:  0,
// 		Size:  1, // 초기 크기는 1
// 	}
// 	node.Parent = node // 자기 자신을 가리킴

// 	uf.Nodes[x] = node
// 	uf.Count++

// 	return node
// }

// // Find: 원소의 루트를 찾는 함수 (경로 압축 적용)
// func (uf *NodeUnionFind) Find(x uint64) *Node {
// 	uf.mu.RLock()
// 	node, exists := uf.Nodes[x]
// 	uf.mu.RUnlock()

// 	if !exists {
// 		return nil
// 	}

// 	return uf.FindNode(node)
// }

// // FindNode: 노드의 루트를 찾는 함수 (경로 압축 적용)
// func (uf *NodeUnionFind) FindNode(node *Node) *Node {
// 	if node.Parent != node {
// 		node.Parent = uf.FindNode(node.Parent) // 경로 압축
// 	}
// 	return node.Parent
// }

// // Union: 두 원소를 합치는 함수 (랭크 기반 최적화 적용)
// func (uf *NodeUnionFind) Union(x, y uint64) bool {
// 	rootX := uf.Find(x)
// 	rootY := uf.Find(y)

// 	// 존재하지 않는 원소이거나 이미 같은 집합
// 	if rootX == nil || rootY == nil || rootX == rootY {
// 		return false
// 	}

// 	uf.mu.Lock()
// 	defer uf.mu.Unlock()

// 	// 랭크에 따라 합치기 (작은 트리를 큰 트리 아래로)
// 	if rootX.Rank < rootY.Rank {
// 		rootX.Parent = rootY
// 		// 집합 크기 업데이트 (최대 4)
// 		newSize := rootX.Size + rootY.Size
// 		if newSize > 4 {
// 			newSize = 4
// 		}
// 		rootY.Size = newSize
// 	} else if rootX.Rank > rootY.Rank {
// 		rootY.Parent = rootX
// 		// 집합 크기 업데이트 (최대 4)
// 		newSize := rootX.Size + rootY.Size
// 		if newSize > 4 {
// 			newSize = 4
// 		}
// 		rootX.Size = newSize
// 	} else {
// 		rootY.Parent = rootX
// 		rootX.Rank++
// 		// 집합 크기 업데이트 (최대 4)
// 		newSize := rootX.Size + rootY.Size
// 		if newSize > 4 {
// 			newSize = 4
// 		}
// 		rootX.Size = newSize
// 	}

// 	// 서로소 집합 수 감소
// 	uf.Count--
// 	return true
// }

// // GetCount: 현재 서로소 집합 수 반환
// func (uf *NodeUnionFind) GetCount() int {
// 	uf.mu.RLock()
// 	defer uf.mu.RUnlock()
// 	return uf.Count
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
// 	const numElements = 100_000_000 // 1억

// 	fmt.Printf("노드 기반 Union-Find 테스트 (원소 %d개)\n", numElements)
// 	fmt.Println("----------------------------------------------")

// 	// 초기 메모리 상태
// 	initialStats := getMemoryStats()
// 	printMemoryUsage("초기 상태", initialStats)

// 	// 1. 유니언 파인드 초기화
// 	fmt.Println("1. Union-Find 초기화 중...")
// 	initStart := time.Now()

// 	uf := NewNodeUnionFind()

// 	// 병렬 처리로 초기화
// 	numWorkers := runtime.NumCPU()
// 	workerSize := numElements / numWorkers

// 	fmt.Printf("병렬 초기화: %d개 워커 사용\n", numWorkers)

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

// 			for i := start; i < end; i++ {
// 				uf.MakeSet(uint64(i))

// 				// 진행 상황 출력
// 				if (i-start+1)%(workerSize/10) == 0 {
// 					fmt.Printf("워커 %d: %d/%d 원소 초기화 (%.1f%%)\n",
// 						id, i-start+1, end-start, float64(i-start+1)/float64(end-start)*100)
// 				}
// 			}
// 		}(w)
// 	}

// 	wg.Wait()

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

// 	sizeDist := make(map[uint8]int)

// 	// 샘플링 간격 (1억 원소 전체를 검사하는 대신 샘플링)
// 	const samplingInterval = 100
// 	samplesCount := 0

// 	for i := 0; i < numElements; i += samplingInterval {
// 		root := uf.Find(uint64(i))
// 		if root != nil {
// 			sizeDist[root.Size]++
// 			samplesCount++
// 		}

// 		// 진행 상황 출력
// 		if (i+samplingInterval)%(numElements/10) < samplingInterval {
// 			fmt.Printf("크기 분포 계산 진행률: %.1f%%\n", float64(i+samplingInterval)/float64(numElements)*100)
// 		}
// 	}

// 	distributionDuration := time.Since(distributionStart)
// 	fmt.Printf("집합 크기 분포 계산 완료: %v (샘플 %d개)\n", distributionDuration, samplesCount)

// 	// 샘플링 결과에서 전체 분포 추정
// 	fmt.Println("최종 집합 크기 분포 (샘플링 기반 추정):")
// 	for size := uint8(1); size <= 4; size++ {
// 		count := sizeDist[size]
// 		percent := float64(count) / float64(samplesCount) * 100
// 		fmt.Printf("크기 %d인 집합: %d개 (%.2f%%)\n", size, count, percent)
// 	}

// 	distStats := getMemoryStats()
// 	printMemoryUsage("크기 분포 계산 후", distStats)

// 	// 전체 실행 시간
// 	totalDuration := time.Since(startTime)
// 	fmt.Printf("\n전체 실행 시간: %v\n", totalDuration)
// }
