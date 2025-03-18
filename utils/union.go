package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"reflect"
	"time"
	"unsafe"

	"github.com/edsrzf/mmap-go"
)

// MMapUnionFind는 메모리 매핑된 파일을 사용하여 대규모 union-find를 구현합니다.
// parent는 int32 배열(각 원소에 대해 4바이트)이며, rank는 int8 배열입니다.
type MMapUnionFind struct {
	n           int32
	parentFile  string
	rankFile    string
	parentMMap  mmap.MMap
	rankMMap    mmap.MMap
	parentSlice []int32
	rankSlice   []int8
}

// NewMMapUnionFind는 n개의 원소(0~n-1)를 초기화하고, parentFile과 rankFile 이름의 파일을 MMAP합니다.
func NewMMapUnionFind(n int32, parentFile, rankFile string) (*MMapUnionFind, error) {
	uf := &MMapUnionFind{
		n:          n,
		parentFile: parentFile,
		rankFile:   rankFile,
	}

	// parent 배열: n * 4 bytes
	parentSize := int(n * 4)
	fParent, err := os.OpenFile(parentFile, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	defer fParent.Close()

	if err := fParent.Truncate(int64(parentSize)); err != nil {
		return nil, err
	}

	parentMap, err := mmap.Map(fParent, mmap.RDWR, 0)
	if err != nil {
		return nil, err
	}
	uf.parentMMap = parentMap

	// rank 배열: n * 1 byte
	rankSize := int(n)
	fRank, err := os.OpenFile(rankFile, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	defer fRank.Close()

	if err := fRank.Truncate(int64(rankSize)); err != nil {
		return nil, err
	}

	rankMap, err := mmap.Map(fRank, mmap.RDWR, 0)
	if err != nil {
		return nil, err
	}
	uf.rankMMap = rankMap

	// 메모리 매핑된 byte slice를 int32 슬라이스로 변환 (parent)
	hdrParent := (*reflect.SliceHeader)(unsafe.Pointer(&uf.parentSlice))
	hdrParent.Data = uintptr(unsafe.Pointer(&uf.parentMMap[0]))
	hdrParent.Len = parentSize / 4
	hdrParent.Cap = parentSize / 4

	// 메모리 매핑된 byte slice를 int8 슬라이스로 변환 (rank)
	hdrRank := (*reflect.SliceHeader)(unsafe.Pointer(&uf.rankSlice))
	hdrRank.Data = uintptr(unsafe.Pointer(&uf.rankMMap[0]))
	hdrRank.Len = rankSize
	hdrRank.Cap = rankSize

	// 초기화: parent[i] = i, rank[i] = 0
	for i := int32(0); i < n; i++ {
		uf.parentSlice[i] = i
		uf.rankSlice[i] = 0
	}

	return uf, nil
}

// Close는 메모리 매핑을 해제합니다.
func (uf *MMapUnionFind) Close() error {
	if err := uf.parentMMap.Unmap(); err != nil {
		return err
	}
	if err := uf.rankMMap.Unmap(); err != nil {
		return err
	}
	return nil
}

// Find는 경로 압축을 적용하여 x의 대표(root)를 찾습니다.
func (uf *MMapUnionFind) Find(x int32) int32 {
	if uf.parentSlice[x] != x {
		uf.parentSlice[x] = uf.Find(uf.parentSlice[x])
	}
	return uf.parentSlice[x]
}

// Union은 두 원소 x와 y의 집합을 병합합니다.
func (uf *MMapUnionFind) Union(x, y int32) {
	rootX := uf.Find(x)
	rootY := uf.Find(y)
	if rootX == rootY {
		return
	}
	if uf.rankSlice[rootX] < uf.rankSlice[rootY] {
		uf.parentSlice[rootX] = rootY
	} else if uf.rankSlice[rootX] > uf.rankSlice[rootY] {
		uf.parentSlice[rootY] = rootX
	} else {
		uf.parentSlice[rootY] = rootX
		uf.rankSlice[rootX]++
	}
}

func main() {
	// n값이 매우 큰 경우를 가정하지만, 노트북 스펙(32GB RAM)에서는
	// 실제 테스트 시 n을 너무 크게 잡으면 OOM 문제가 발생하므로, 여기서는 테스트용으로 10,000,000을 사용합니다.
	var n int32 = 10_000_000 // 프로덕션에서는 2_000_000_000(20억) 이상의 값을 사용
	numUnions := n / 20      // 전체 원소의 5% 정도가 union 연산에 참여

	parentFile := "parent.dat"
	rankFile := "rank.dat"

	fmt.Printf("n = %d 원소에 대해 MMAP 기반 union-find 초기화 중...\n", n)
	start := time.Now()
	uf, err := NewMMapUnionFind(n, parentFile, rankFile)
	if err != nil {
		log.Fatalf("Error initializing MMapUnionFind: %v", err)
	}
	defer func() {
		if err := uf.Close(); err != nil {
			log.Fatalf("Error closing MMapUnionFind: %v", err)
		}
	}()
	fmt.Printf("초기화 완료 (소요 시간: %v)\n", time.Since(start))

	// union 연산 수행 (진행 상황 1,000만 단위로 출력)
	rand.Seed(time.Now().UnixNano())
	startUnion := time.Now()
	for i := int32(0); i < numUnions; i++ {
		base := rand.Int31n(n)
		unionSize := rand.Int31n(99) + 2 // 2~100개 원소
		for j := int32(1); j < unionSize; j++ {
			other := rand.Int31n(n)
			uf.Union(base, other)
		}
		if (i+1)%1000000 == 0 {
			fmt.Printf("Union 진행: %d / %d, 경과 시간: %v\n", i+1, numUnions, time.Since(startUnion))
		}
	}
	fmt.Printf("Union 연산 완료 (소요 시간: %v)\n", time.Since(startUnion))

	// 모든 원소에 대해 Find를 호출해 최종 그룹(대표) 수를 계산
	groupCountMap := make(map[int32]int32)
	startCount := time.Now()
	for i := int32(0); i < n; i++ {
		root := uf.Find(i)
		groupCountMap[root]++
		if (i+1)%10000000 == 0 {
			fmt.Printf("Find 진행: %d / %d, 경과 시간: %v\n", i+1, n, time.Since(startCount))
		}
	}
	groupCount := len(groupCountMap)
	fmt.Printf("전체 원소 수: %d\n", n)
	fmt.Printf("최종 disjoint 집합 수: %d\n", groupCount)
	fmt.Printf("전체 처리 소요 시간: %v\n", time.Since(start))

	// 필요에 따라 임시 파일 삭제 (테스트 완료 후)
	// os.Remove(parentFile)
	// os.Remove(rankFile)
}
