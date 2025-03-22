package main

import (
	"fmt"
	"sort"
)

// DisjointSet는 분리 집합을 관리하는 유니온 파인드 구조체입니다.
type DisjointSet struct {
	parent map[uint64]uint64 // 각 원소의 부모를 저장
	rank   map[uint64]int    // 트리의 높이를 저장 (최적화용)
	size   map[uint64]int    // 각 집합의 크기를 저장
}

// NewDisjointSet은 새로운 DisjointSet 인스턴스를 생성합니다.
func NewDisjointSet() *DisjointSet {
	return &DisjointSet{
		parent: make(map[uint64]uint64),
		rank:   make(map[uint64]int),
		size:   make(map[uint64]int),
	}
}

// MakeSet은 새로운 원소를 집합에 추가합니다.
func (ds *DisjointSet) MakeSet(x uint64) {
	if _, exists := ds.parent[x]; exists {
		return // 이미 존재하는 원소는 무시
	}

	ds.parent[x] = x // 자기 자신을 부모로 설정
	ds.rank[x] = 0   // 초기 랭크는 0
	ds.size[x] = 1   // 초기 크기는 1
}

// Find는 원소 x가 속한 집합의 대표(루트)를 찾습니다.
// 경로 압축 최적화를 적용합니다.
func (ds *DisjointSet) Find(x uint64) uint64 {
	if _, exists := ds.parent[x]; !exists {
		ds.MakeSet(x)
	}

	if ds.parent[x] != x {
		ds.parent[x] = ds.Find(ds.parent[x]) // 경로 압축
	}
	return ds.parent[x]
}

// Union은 두 원소가 속한 집합을 합칩니다.
// 랭크에 따른 병합 최적화를 적용합니다.
func (ds *DisjointSet) Union(x, y uint64) {
	rootX := ds.Find(x)
	rootY := ds.Find(y)

	if rootX == rootY {
		return // 이미 같은 집합에 속해 있음
	}

	// 랭크에 따라 병합 (랭크가 작은 트리를 랭크가 큰 트리에 붙임)
	if ds.rank[rootX] < ds.rank[rootY] {
		ds.parent[rootX] = rootY
		ds.size[rootY] += ds.size[rootX] // rootY가 새로운 루트가 되므로 크기 업데이트
	} else {
		ds.parent[rootY] = rootX
		ds.size[rootX] += ds.size[rootY] // rootX가 새로운 루트가 되므로 크기 업데이트

		// 두 트리의 랭크가 같으면 결과 트리의 랭크를 증가
		if ds.rank[rootX] == ds.rank[rootY] {
			ds.rank[rootX]++
		}
	}
}

// GetSize는 원소 x가 속한 집합의 크기를 반환합니다.
func (ds *DisjointSet) GetSize(x uint64) int {
	root := ds.Find(x)
	return ds.size[root]
}

// GetSets는 현재 분리된 모든 집합들을 반환합니다.
func (ds *DisjointSet) GetSets() map[uint64][]uint64 {
	result := make(map[uint64][]uint64)

	// 모든 요소를 순회하며 집합을 계산
	for element := range ds.parent {
		root := ds.Find(element) // 경로 압축이 여기서도 발생

		if _, exists := result[root]; !exists {
			result[root] = []uint64{}
		}
		result[root] = append(result[root], element)
	}

	// 각 집합 내부를 정렬
	for root := range result {
		sort.Slice(result[root], func(i, j int) bool {
			return result[root][i] < result[root][j]
		})
	}

	return result
}

// PrintSets는 현재 모든 분리 집합과 그 원소들을 출력합니다.
func (ds *DisjointSet) PrintSets() {
	sets := ds.GetSets()

	fmt.Println("현재 분리 집합 상태:")

	// 루트 기준으로 정렬하여 일관된 순서로 출력
	roots := make([]uint64, 0, len(sets))
	for root := range sets {
		roots = append(roots, root)
	}
	sort.Slice(roots, func(i, j int) bool {
		return roots[i] < roots[j]
	})

	for _, root := range roots {
		fmt.Printf("집합 %d의 원소들: %v (크기: %d)\n", root, sets[root], ds.size[root])
	}
}

func main() {
	// 유니온 파인드 인스턴스 생성
	uf := NewDisjointSet()

	// 집합 1 생성 (1, 2, 3, 4, 5)
	uf.MakeSet(1)
	for _, val := range []uint64{2, 3, 4, 5} {
		uf.MakeSet(val)
		uf.Union(1, val) // 1을 대표로 하여 모든 요소를 하나의 집합으로 연결
	}

	// 집합 2 생성 (10, 11, 12, 13)
	uf.MakeSet(10)
	for _, val := range []uint64{11, 12, 13} {
		uf.MakeSet(val)
		uf.Union(10, val) // 10을 대표로 하여 모든 요소를 하나의 집합으로 연결
	}

	// 집합 3 생성 (20, 21, 22)
	uf.MakeSet(20)
	for _, val := range []uint64{21, 22} {
		uf.MakeSet(val)
		uf.Union(20, val) // 20을 대표로 하여 모든 요소를 하나의 집합으로 연결
	}

	// 집합 4 생성 (30, 31, 32, 33, 34)
	uf.MakeSet(30)
	for _, val := range []uint64{31, 32, 33, 34} {
		uf.MakeSet(val)
		uf.Union(30, val) // 30을 대표로 하여 모든 요소를 하나의 집합으로 연결
	}

	// 집합 5 생성 (40, 41, 42)
	uf.MakeSet(40)
	for _, val := range []uint64{41, 42} {
		uf.MakeSet(val)
		uf.Union(40, val) // 40을 대표로 하여 모든 요소를 하나의 집합으로 연결
	}

	// 집합 6 생성 (50, 51, 52, 53)
	uf.MakeSet(50)
	for _, val := range []uint64{51, 52, 53} {
		uf.MakeSet(val)
		uf.Union(50, val) // 50을 대표로 하여 모든 요소를 하나의 집합으로 연결
	}

	// 초기 상태 출력
	fmt.Println("초기 상태 (6개의 분리 집합):")
	uf.PrintSets()

	// 6개의 집합을 3개로 합치기
	fmt.Println("\n6개의 집합을 3개로 합치는 중...")

	// 집합 1과 집합 4 합치기
	uf.Union(1, 30)

	// 집합 2와 집합 5 합치기
	uf.Union(10, 40)

	// 집합 3과 집합 6 합치기
	uf.Union(20, 50)

	// 최종 결과 출력
	fmt.Println("\n최종 상태 (3개의 분리 집합):")
	uf.PrintSets()
}
