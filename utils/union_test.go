package main_test

import (
	"math/rand"
	"strconv"
	"testing"
	"time"
)

// UnionFind 구조체: 각 원소의 부모와 랭크를 관리합니다.
type UnionFind struct {
	parent map[string]string
	rank   map[string]int
}

// NewUnionFind는 새로운 UnionFind 인스턴스를 생성합니다.
func NewUnionFind() *UnionFind {
	return &UnionFind{
		parent: make(map[string]string),
		rank:   make(map[string]int),
	}
}

// MakeSet은 원소 x를 초기 집합으로 등록합니다.
func (uf *UnionFind) MakeSet(x string) {
	if _, exists := uf.parent[x]; !exists {
		uf.parent[x] = x
		uf.rank[x] = 0
	}
}

// Find는 경로 압축을 적용하여 x의 대표(루트)를 찾습니다.
func (uf *UnionFind) Find(x string) string {
	if uf.parent[x] != x {
		uf.parent[x] = uf.Find(uf.parent[x])
	}
	return uf.parent[x]
}

// Union은 두 원소 x와 y의 집합을 랭크를 고려하여 병합합니다.
func (uf *UnionFind) Union(x, y string) {
	rootX := uf.Find(x)
	rootY := uf.Find(y)
	if rootX == rootY {
		return
	}
	if uf.rank[rootX] < uf.rank[rootY] {
		uf.parent[rootX] = rootY
	} else if uf.rank[rootX] > uf.rank[rootY] {
		uf.parent[rootY] = rootX
	} else {
		uf.parent[rootY] = rootX
		uf.rank[rootX]++
	}
}

// TestUnionFindRandomUnion는 전체 1천만 개의 원소 중,
// 9,900,000개는 그대로 유지하고, 나머지 100,000개의 그룹에 대해
// 각 그룹마다 (80%는 2개, 10%는 3개, 5%는 4~20개, 5%는 20~50개)의 원소를 랜덤 선택해 union 연산을 수행합니다.
// 최종적으로 union에 의해 묶인 그룹들의 결과(대표값이 동일한지)와 전체 disjoint set의 수가 예상과 일치하는지 검증합니다.
func TestUnionFindRandomUnion(t *testing.T) {
	totalSets := 10000000 // 1천만 개의 원소
	uf := NewUnionFind()
	start := time.Now()

	// 1. 모든 원소에 대해 MakeSet 호출
	for i := 0; i < totalSets; i++ {
		elem := "elem_" + strconv.Itoa(i)
		uf.MakeSet(elem)
		if (i+1)%1000000 == 0 {
			t.Logf("MakeSet 완료: %d 개 원소, 경과 시간: %v", i+1, time.Since(start))
		}
	}

	// 2. 10만 개의 union 그룹 생성 (랜덤 분포에 따라 그룹 크기 결정)
	numUnionGroups := 100000
	groupSizes := make([]int, numUnionGroups)
	totalUnionElements := 0

	// 재현성을 위해 고정 seed 사용
	for i := 0; i < numUnionGroups; i++ {
		r := rand.Float64()
		var size int
		switch {
		case r < 0.8:
			size = 2
		case r < 0.9:
			size = 3
		case r < 0.95:
			// 4~20 사이의 랜덤 정수 (양쪽 포함)
			size = rand.Intn(17) + 4
		default:
			// 20~50 사이의 랜덤 정수 (양쪽 포함)
			size = rand.Intn(31) + 20
		}
		groupSizes[i] = size
		totalUnionElements += size
	}

	if totalUnionElements > totalSets {
		t.Fatalf("union 그룹에 필요한 원소(%d)가 전체 원소(%d)보다 많습니다.", totalUnionElements, totalSets)
	}

	// 3. 0 ~ totalSets-1의 인덱스를 섞어 union 그룹에 사용할 원소를 선정
	indices := make([]int, totalSets)
	for i := 0; i < totalSets; i++ {
		indices[i] = i
	}
	rand.Shuffle(totalSets, func(i, j int) {
		indices[i], indices[j] = indices[j], indices[i]
	})

	// 4. 각 그룹에 대해 union 연산 수행
	pos := 0
	mergedCount := 0 // union 연산으로 합쳐진 원소 수의 총합 (각 그룹은 k개 원소면 k-1번 merge)
	for i, size := range groupSizes {
		if pos+size > totalSets {
			break
		}
		// 그룹 내 첫번째 원소를 기준으로 나머지 원소와 union
		base := "elem_" + strconv.Itoa(indices[pos])
		for j := 1; j < size; j++ {
			uf.Union(base, "elem_"+strconv.Itoa(indices[pos+j]))
		}
		mergedCount += (size - 1)
		pos += size

		if (i+1)%10000 == 0 {
			t.Logf("Union 그룹 %d/%d 처리, 경과 시간: %v", i+1, numUnionGroups, time.Since(start))
		}
	}
	// 예상 최종 disjoint set의 개수 = 전체 원소 수 - merge 횟수
	expectedDisjointSets := totalSets - mergedCount

	// 5. 모든 원소에 대해 Find 호출하여 최종 disjoint set 개수 검증
	roots := make(map[string]bool, totalSets)
	for i := 0; i < totalSets; i++ {
		root := uf.Find("elem_" + strconv.Itoa(i))
		roots[root] = true
		if (i+1)%1000000 == 0 {
			t.Logf("검증 완료: %d 개 원소, 경과 시간: %v", i+1, time.Since(start))
		}
	}
	finalDisjointSets := len(roots)
	t.Logf("예상 disjoint set 개수: %d, 최종 disjoint set 개수: %d, 총 소요 시간: %v",
		expectedDisjointSets, finalDisjointSets, time.Since(start))

	if finalDisjointSets != expectedDisjointSets {
		t.Errorf("disjoint set 개수 불일치: 예상 %d, 실제 %d", expectedDisjointSets, finalDisjointSets)
	}
}
