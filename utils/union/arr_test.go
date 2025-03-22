// filename: unionfind_test.go
package main_test

// import (
// 	"math/rand"
// 	"testing"
// 	"time"
// 	. "whale_tracker/utils/union"
// )

// func TestUnionFindBasic(t *testing.T) {
// 	uf := NewArrayUnionFind(10)
// 	// 초기에 0~9까지 10개 원소가 각자 자기 집합.

// 	if uf.GetCount() != 10 {
// 		t.Errorf("초기 서로소 집합 수가 10이 아닙니다. 실제: %d", uf.GetCount())
// 	}

// 	// 1) Union(1,2)
// 	uf.Union(1, 2)
// 	if uf.Find(1) != uf.Find(2) {
// 		t.Errorf("1과 2는 동일 루트를 가져야 합니다.")
// 	}
// 	if uf.GetCount() != 9 {
// 		t.Errorf("서로소 집합 수가 9여야 합니다. 실제: %d", uf.GetCount())
// 	}

// 	// 2) Union(2,3) -> 결국 (1,2,3)은 한 집합
// 	uf.Union(2, 3)
// 	if uf.Find(1) != uf.Find(3) {
// 		t.Errorf("1, 2, 3은 동일 루트를 가져야 합니다.")
// 	}
// 	if uf.GetCount() != 8 {
// 		t.Errorf("서로소 집합 수가 8여야 합니다. 실제: %d", uf.GetCount())
// 	}

// 	// 3) Union(4,5), Union(5,6) -> (4,5,6)은 한 집합
// 	uf.Union(4, 5)
// 	uf.Union(5, 6)
// 	if uf.Find(4) != uf.Find(6) {
// 		t.Errorf("4,5,6은 동일 루트를 가져야 합니다.")
// 	}
// 	if uf.GetCount() != 6 {
// 		t.Errorf("서로소 집합 수가 6여야 합니다. 실제: %d", uf.GetCount())
// 	}

// 	// 4) 아직 합쳐지지 않은 것들(7,8,9)은 각각 자기 루트
// 	if uf.Find(7) == uf.Find(1) {
// 		t.Errorf("7과 1은 다른 집합이어야 합니다.")
// 	}
// 	if uf.Find(8) == uf.Find(4) {
// 		t.Errorf("8과 4는 다른 집합이어야 합니다.")
// 	}
// 	if uf.GetCount() != 6 {
// 		t.Errorf("서로소 집합 수가 6이어야 합니다. 실제: %d", uf.GetCount())
// 	}

// 	// 5) 추가로 (1, 4)를 Union하면 (1,2,3)과 (4,5,6)이 결국 하나의 집합이 됨
// 	uf.Union(1, 4)
// 	if uf.Find(2) != uf.Find(6) {
// 		t.Errorf("2와 6은 이제 동일 집합이어야 합니다.")
// 	}
// 	if uf.GetCount() != 5 {
// 		t.Errorf("서로소 집합 수가 5여야 합니다. 실제: %d", uf.GetCount())
// 	}
// }

// // 간단히 그래프를 인접 리스트로 표현하기 위한 자료구조
// type Graph struct {
// 	adj [][]int
// }

// func newGraph(n int) *Graph {
// 	return &Graph{adj: make([][]int, n)}
// }

// func (g *Graph) addEdge(u, v int) {
// 	g.adj[u] = append(g.adj[u], v)
// 	g.adj[v] = append(g.adj[v], u)
// }

// func TestUnionFindRandom(t *testing.T) {
// 	rand.Seed(time.Now().UnixNano())

// 	n := 50 // 필요에 따라 500, 1000 등으로 늘려볼 수 있음
// 	uf := NewArrayUnionFind(n)
// 	g := newGraph(n)

// 	// 대략 M번 정도 무작위 연결
// 	M := 100
// 	for i := 0; i < M; i++ {
// 		x := rand.Intn(n)
// 		y := rand.Intn(n)
// 		uf.Union(uint64(x), uint64(y))
// 		g.addEdge(x, y)
// 	}

// 	// 이제 그래프의 연결 컴포넌트(connected component)를 찾는다
// 	visited := make([]bool, n)
// 	compID := make([]int, n) // 각 노드가 속한 컴포넌트 번호
// 	currentComp := 0

// 	// DFS 함수
// 	var dfs func(int)
// 	dfs = func(start int) {
// 		stack := []int{start}
// 		visited[start] = true
// 		compID[start] = currentComp

// 		for len(stack) > 0 {
// 			top := stack[len(stack)-1]
// 			stack = stack[:len(stack)-1]

// 			for _, nxt := range g.adj[top] {
// 				if !visited[nxt] {
// 					visited[nxt] = true
// 					compID[nxt] = currentComp
// 					stack = append(stack, nxt)
// 				}
// 			}
// 		}
// 	}

// 	// 모든 노드에 대해 컴포넌트 탐색
// 	for i := 0; i < n; i++ {
// 		if !visited[i] {
// 			dfs(i)
// 			currentComp++
// 		}
// 	}

// 	// 이제 각 컴포넌트별로 Union-Find의 루트가 동일한지 확인
// 	for i := 0; i < n; i++ {
// 		for j := i + 1; j < n; j++ {
// 			sameComp := (compID[i] == compID[j])
// 			sameRoot := (uf.Find(uint64(i)) == uf.Find(uint64(j)))

// 			if sameComp && !sameRoot {
// 				t.Errorf("그래프 상 %d와 %d는 같은 컴포넌트인데, UF 루트가 다릅니다.", i, j)
// 			}
// 			if !sameComp && sameRoot {
// 				t.Errorf("그래프 상 %d와 %d는 다른 컴포넌트인데, UF 루트가 같습니다.", i, j)
// 			}
// 		}
// 	}
// }
