// package main_test

// import (
// 	"encoding/binary"
// 	"fmt"
// 	"io"
// 	"os"
// 	"path/filepath"
// 	"reflect"
// 	"runtime"
// 	"sync"
// 	"testing"
// 	"time"
// 	"unsafe"

// 	"github.com/edsrzf/mmap-go"
// )

// const (
// 	maxDepth          = 20
// 	chunkSize         = 50_000_000 // 파티션 크기 (n=2e8 → 대략 4개 파티션)
// 	binHeaderLen      = 4
// 	chunkBridgePasses = 3
// )

// // ChunkUnionFind: 부분매핑 + 브리지
// type ChunkUnionFind struct {
// 	n          uint32
// 	parentFile string
// 	rankFile   string
// 	rootFile   string

// 	fParent *os.File
// 	fRank   *os.File
// 	fRoot   *os.File
// }

// // * CHANGED: n값을 작게 해서 테스트 (예: 2,000,000) -> 필요 시 원래대로 2e8로
// const testN = 2_000_000 //**

// // NewChunkUnionFind
// func NewChunkUnionFind(n uint32, pf, rf, rf2 string) (*ChunkUnionFind, error) {
// 	parentSize := int64(n) * 4
// 	rankSize := int64(n)
// 	rootSize := int64(n) * 4

// 	fmt.Printf("//* DEBUG: Creating files: parent=%s(%.2fMB), rank=%s(%.2fMB), root=%s(%.2fMB)\n",
// 		pf, float64(parentSize)/(1024*1024),
// 		rf, float64(rankSize)/(1024*1024),
// 		rf2, float64(rootSize)/(1024*1024))

// 	fP, err := os.OpenFile(pf, os.O_RDWR|os.O_CREATE, 0644)
// 	if err != nil {
// 		return nil, fmt.Errorf("open parentFile fail: %w", err)
// 	}
// 	if err := fP.Truncate(parentSize); err != nil {
// 		fP.Close()
// 		return nil, fmt.Errorf("truncate parentFile fail: %w", err)
// 	}

// 	fR, err2 := os.OpenFile(rf, os.O_RDWR|os.O_CREATE, 0644)
// 	if err2 != nil {
// 		fP.Close()
// 		return nil, fmt.Errorf("open rankFile fail: %w", err2)
// 	}
// 	if err := fR.Truncate(rankSize); err != nil {
// 		fR.Close()
// 		fP.Close()
// 		return nil, fmt.Errorf("truncate rankFile fail: %w", err)
// 	}

// 	fRt, err3 := os.OpenFile(rf2, os.O_RDWR|os.O_CREATE, 0644)
// 	if err3 != nil {
// 		fR.Close()
// 		fP.Close()
// 		return nil, fmt.Errorf("open rootFile fail: %w", err3)
// 	}
// 	if err := fRt.Truncate(rootSize); err != nil {
// 		fRt.Close()
// 		fR.Close()
// 		fP.Close()
// 		return nil, fmt.Errorf("truncate rootFile fail: %w", err)
// 	}

// 	return &ChunkUnionFind{
// 		n:          n,
// 		parentFile: pf,
// 		rankFile:   rf,
// 		rootFile:   rf2,
// 		fParent:    fP,
// 		fRank:      fR,
// 		fRoot:      fRt,
// 	}, nil
// }

// func (cu *ChunkUnionFind) partitionCount() int {
// 	pc := int(cu.n / chunkSize)
// 	if cu.n%chunkSize != 0 {
// 		pc++
// 	}
// 	return pc
// }
// func (cu *ChunkUnionFind) partitionRange(idx int) (uint32, uint32) {
// 	start := uint32(idx) * chunkSize
// 	end := start + chunkSize
// 	if end > cu.n {
// 		end = cu.n
// 	}
// 	return start, end
// }

// // * DEBUG: MMapPartition 로깅
// func (cu *ChunkUnionFind) MMapPartition(start, end uint32) (
// 	mmap.MMap, mmap.MMap, mmap.MMap,
// 	[]uint32, []uint8, []uint32, error,
// ) {
// 	if start >= end {
// 		fmt.Printf("//* DEBUG: MMapPartition skip: partition empty. start=%d end=%d\n", start, end)
// 		return nil, nil, nil, nil, nil, nil, nil
// 	}
// 	lengthP := int(end-start) * 4
// 	offP := int64(start) * 4
// 	fmt.Printf("//* DEBUG: MMap parent offset=%d length=%d\n", offP, lengthP)
// 	mmP, er := mmap.MapRegion(cu.fParent, lengthP, mmap.RDWR, 0, offP)
// 	if er != nil {
// 		return nil, nil, nil, nil, nil, nil, fmt.Errorf("mmap parent fail offset=%d len=%d => %w", offP, lengthP, er)
// 	}

// 	lengthR := int(end - start)
// 	offR := int64(start)
// 	fmt.Printf("//* DEBUG: MMap rank offset=%d length=%d\n", offR, lengthR)
// 	mmR, er2 := mmap.MapRegion(cu.fRank, lengthR, mmap.RDWR, 0, offR)
// 	if er2 != nil {
// 		mmP.Unmap()
// 		return nil, nil, nil, nil, nil, nil, fmt.Errorf("mmap rank fail offset=%d len=%d => %w", offR, lengthR, er2)
// 	}

// 	lengthRt := int(end-start) * 4
// 	offRt := int64(start) * 4
// 	fmt.Printf("//* DEBUG: MMap root offset=%d length=%d\n", offRt, lengthRt)
// 	mmRt, er3 := mmap.MapRegion(cu.fRoot, lengthRt, mmap.RDWR, 0, offRt)
// 	if er3 != nil {
// 		mmP.Unmap()
// 		mmR.Unmap()
// 		return nil, nil, nil, nil, nil, nil, fmt.Errorf("mmap root fail offset=%d len=%d => %w", offRt, lengthRt, er3)
// 	}

// 	pSlice := mapUint32(mmP, end-start)
// 	rSlice := mapUint8(mmR, end-start)
// 	rtSlice := mapUint32(mmRt, end-start)

// 	return mmP, mmR, mmRt, pSlice, rSlice, rtSlice, nil
// }

// func mapUint32(mm []byte, count uint32) []uint32 {
// 	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&mm[0]))
// 	hdr.Len = int(count)
// 	hdr.Cap = int(count)
// 	return *(*[]uint32)(unsafe.Pointer(hdr))
// }
// func mapUint8(mm []byte, count uint32) []uint8 {
// 	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&mm[0]))
// 	hdr.Len = int(count)
// 	hdr.Cap = int(count)
// 	return *(*[]uint8)(unsafe.Pointer(hdr))
// }

// // Close
// func (cu *ChunkUnionFind) Close(deleteFiles bool) {
// 	if cu.fParent != nil {
// 		cu.fParent.Close()
// 	}
// 	if cu.fRank != nil {
// 		cu.fRank.Close()
// 	}
// 	if cu.fRoot != nil {
// 		cu.fRoot.Close()
// 	}
// 	if deleteFiles {
// 		os.Remove(cu.parentFile)
// 		os.Remove(cu.rankFile)
// 		os.Remove(cu.rootFile)
// 	}
// }

// func (cu *ChunkUnionFind) InitPartition(idx int) error {
// 	start, end := cu.partitionRange(idx)
// 	mmP, mmR, mmRt, pSl, rSl, rtSl, er := cu.MMapPartition(start, end)
// 	if er != nil {
// 		return fmt.Errorf("InitPartition(%d) fail: %w", idx, er)
// 	}
// 	if mmP == nil {
// 		fmt.Printf("//* DEBUG: partition %d empty => no init.\n", idx)
// 		return nil
// 	}
// 	defer mmP.Unmap()
// 	defer mmR.Unmap()
// 	defer mmRt.Unmap()

// 	for i := uint32(0); i < (end - start); i++ {
// 		pSl[i] = i
// 		rSl[i] = 0
// 		rtSl[i] = 0xFFFFFFFF
// 	}
// 	return nil
// }

// // ----------------------------------------------
// // Find/Union
// // ----------------------------------------------
// func FindWithDepth(p []uint32, r []uint8, x, depth uint32) (uint32, bool) {
// 	if depth >= maxDepth {
// 		return x, false
// 	}
// 	if p[x] != x {
// 		rt, ok := FindWithDepth(p, r, p[x], depth+1)
// 		if !ok {
// 			return x, false
// 		}
// 		p[x] = rt
// 		return rt, true
// 	}
// 	return x, true
// }
// func UnionWithDepth(p []uint32, r []uint8, x, y uint32) {
// 	rx, okx := FindWithDepth(p, r, x, 0)
// 	ry, oky := FindWithDepth(p, r, y, 0)
// 	if !okx || !oky {
// 		return
// 	}
// 	if rx == ry {
// 		return
// 	}
// 	rrX := r[rx]
// 	rrY := r[ry]
// 	if rrX < rrY {
// 		p[rx] = ry
// 	} else if rrX > rrY {
// 		p[ry] = rx
// 	} else {
// 		p[ry] = rx
// 		r[rx]++
// 	}
// }

// // ----------------------------------------------
// // ProcessPartition
// // ----------------------------------------------
// func (cu *ChunkUnionFind) ProcessPartition(idx int, binFile string, bridgeF *os.File) error {
// 	start, end := cu.partitionRange(idx)
// 	mmP, mmR, mmRt, pSl, rSl, _, er := cu.MMapPartition(start, end)
// 	if er != nil {
// 		return fmt.Errorf("ProcessPartition(%d) fail: %w", idx, er)
// 	}
// 	if mmP == nil {
// 		fmt.Printf("//* DEBUG: partition %d empty => skip ProcessPartition\n", idx)
// 		return nil
// 	}
// 	defer mmP.Unmap()
// 	defer mmR.Unmap()
// 	defer mmRt.Unmap()

// 	f, er2 := os.Open(binFile)
// 	if er2 != nil {
// 		return fmt.Errorf("open binFile fail: %w", er2)
// 	}
// 	defer f.Close()

// 	for {
// 		hdr := make([]byte, 4)
// 		_, err3 := io.ReadFull(f, hdr)
// 		if err3 == io.EOF {
// 			break
// 		} else if err3 != nil {
// 			return nil
// 		}
// 		sz := binary.LittleEndian.Uint32(hdr)
// 		setBuf := make([]byte, 4*sz)
// 		if _, err4 := io.ReadFull(f, setBuf); err4 != nil {
// 			return nil
// 		}

// 		arr := make([]uint32, sz)
// 		off := 0
// 		for i := uint32(0); i < sz; i++ {
// 			arr[i] = binary.LittleEndian.Uint32(setBuf[off:])
// 			off += 4
// 		}
// 		allIn := true
// 		for _, v := range arr {
// 			if v < start || v >= end {
// 				allIn = false
// 				break
// 			}
// 		}
// 		if allIn {
// 			if len(arr) > 1 {
// 				base := arr[0] - start
// 				for _, v2 := range arr[1:] {
// 					UnionWithDepth(pSl, rSl, base, v2-start)
// 				}
// 			}
// 		} else {
// 			bb := make([]byte, 4+4*len(arr))
// 			binary.LittleEndian.PutUint32(bb[0:], sz)
// 			of2 := 4
// 			for _, vv := range arr {
// 				binary.LittleEndian.PutUint32(bb[of2:], vv)
// 				of2 += 4
// 			}
// 			bridgeF.Write(bb)
// 		}
// 	}
// 	return nil
// }

// // ----------------------------------------------
// // BridgeAdjacent
// // ----------------------------------------------
// func (cu *ChunkUnionFind) BridgeAdjacent(i int, bfPath string) error {
// 	startA, endA := cu.partitionRange(i)
// 	startB, endB := cu.partitionRange(i + 1)
// 	if startA == endA || startB == endB {
// 		return nil
// 	}

// 	mmPA, mmRA, mmRtA, pA, rA, _, er := cu.MMapPartition(startA, endA)
// 	if er != nil {
// 		return fmt.Errorf("bridge partA fail: %w", er)
// 	}
// 	if mmPA == nil {
// 		return nil
// 	}
// 	defer mmPA.Unmap()
// 	defer mmRA.Unmap()
// 	defer mmRtA.Unmap()

// 	mmPB, mmRB, mmRtB, pB, rB, _, er2 := cu.MMapPartition(startB, endB)
// 	if er2 != nil {
// 		return fmt.Errorf("bridge partB fail: %w", er2)
// 	}
// 	if mmPB == nil {
// 		return nil
// 	}
// 	defer mmPB.Unmap()
// 	defer mmRB.Unmap()
// 	defer mmRtB.Unmap()

// 	bf, er3 := os.Open(bfPath)
// 	if er3 != nil {
// 		return fmt.Errorf("open bridge file fail:%w", er3)
// 	}
// 	defer bf.Close()

// 	for {
// 		hdr := make([]byte, 4)
// 		_, er4 := io.ReadFull(bf, hdr)
// 		if er4 == io.EOF {
// 			break
// 		}
// 		if er4 != nil {
// 			return nil
// 		}
// 		sz := binary.LittleEndian.Uint32(hdr)
// 		buf := make([]byte, 4*sz)
// 		if _, er5 := io.ReadFull(bf, buf); er5 != nil {
// 			return nil
// 		}
// 		arr := make([]uint32, sz)
// 		off := 0
// 		for i2 := uint32(0); i2 < sz; i2++ {
// 			arr[i2] = binary.LittleEndian.Uint32(buf[off:])
// 			off += 4
// 		}

// 		var basePart = -1
// 		var baseOff uint32
// 		for _, v := range arr {
// 			switch {
// 			case v >= startA && v < endA:
// 				if basePart < 0 {
// 					basePart = i
// 					baseOff = v - startA
// 				} else {
// 					if basePart == i {
// 						UnionWithDepth(pA, rA, baseOff, v-startA)
// 					} else {
// 						unifyCross(pB, rB, baseOff, pA, rA, v-startA)
// 					}
// 				}
// 			case v >= startB && v < endB:
// 				if basePart < 0 {
// 					basePart = i + 1
// 					baseOff = v - startB
// 				} else {
// 					if basePart == i+1 {
// 						UnionWithDepth(pB, rB, baseOff, v-startB)
// 					} else {
// 						unifyCross(pA, rA, baseOff, pB, rB, v-startB)
// 					}
// 				}
// 			}
// 		}
// 	}
// 	return nil
// }
// func unifyCross(pA []uint32, rA []uint8, offA uint32,
// 	pB []uint32, rB []uint8, offB uint32) {
// 	rootA, okA := FindWithDepth(pA, rA, offA, 0)
// 	rootB, okB := FindWithDepth(pB, rB, offB, 0)
// 	if !okA || !okB {
// 		return
// 	}
// 	if rootA == rootB {
// 		return
// 	}
// 	pA[rootA] = offB
// }

// // ----------------------------------------------
// // ParallelFindAndRoot
// // ----------------------------------------------
// func (cu *ChunkUnionFind) ParallelFindAndRoot() error {
// 	pc := cu.partitionCount()
// 	startTime := time.Now()
// 	for idx := 0; idx < pc; idx++ {
// 		st, ed := cu.partitionRange(idx)
// 		mmP, mmR, mmRt, pSl, rSl, rtSl, er := cu.MMapPartition(st, ed)
// 		if er != nil {
// 			return fmt.Errorf("ParallelFind(%d) fail: %w", idx, er)
// 		}
// 		if mmP == nil {
// 			fmt.Printf("//* DEBUG: partition %d empty => skip find\n", idx)
// 			continue
// 		}
// 		chunkLen := ed - st
// 		numWorkers := runtime.NumCPU()
// 		chunkPerWorker := int(chunkLen) / numWorkers
// 		var wg sync.WaitGroup
// 		for w := 0; w < numWorkers; w++ {
// 			wstart := w * chunkPerWorker
// 			wend := wstart + chunkPerWorker
// 			if w == numWorkers-1 {
// 				wend = int(chunkLen)
// 			}
// 			wg.Add(1)
// 			go func(s2, e2 int) {
// 				defer wg.Done()
// 				for i2 := s2; i2 < e2; i2++ {
// 					root, ok := FindWithDepth(pSl, rSl, uint32(i2), 0)
// 					if ok {
// 						rtSl[i2] = root
// 					} else {
// 						rtSl[i2] = 0xFFFFFFFF
// 					}
// 				}
// 			}(wstart, wend)
// 		}
// 		wg.Wait()

// 		mmP.Unmap()
// 		mmR.Unmap()
// 		mmRt.Unmap()
// 	}
// 	fmt.Printf("ParallelFindAndRoot done. took=%v\n", time.Since(startTime))
// 	return nil
// }

// // ---------------------------
// // 외부 정렬 & 유니크
// // ---------------------------
// func quickSortUint32(a []uint32) {
// 	if len(a) < 2 {
// 		return
// 	}
// 	pivot := a[len(a)/2]
// 	a[len(a)/2], a[0] = a[0], pivot
// 	left, right := 1, len(a)-1
// 	for left <= right {
// 		for left <= right && a[left] < pivot {
// 			left++
// 		}
// 		for left <= right && a[right] >= pivot {
// 			right--
// 		}
// 		if left <= right {
// 			a[left], a[right] = a[right], a[left]
// 			left++
// 			right--
// 		}
// 	}
// 	a[0], a[right] = a[right], pivot
// 	quickSortUint32(a[0:right])
// 	quickSortUint32(a[right+1:])
// }
// func writeUint32Slice(fname string, data []uint32) error              { /* same */ return nil }
// func readAllUint32(fname string) ([]uint32, error)                    { /* same */ return nil, nil }
// func mergeTwoFiles(f1, f2, out string) error                          { /* same*/ return nil }
// func kWayMerge(chunks []string, outFile string) error                 { /* same*/ return nil }
// func externalSortRoot(rootFile string, n uint32, tmpDir string) error { /* same*/ return nil }
// func countUniqueRoots(sortedFile string) (uint64, error)              { /* same*/ return 0, nil }

// // --------------------------------------------------------------------------------
// // 최종 Test
// // --------------------------------------------------------------------------------
// func TestBigUnionChunk(t *testing.T) {
// 	//* CHANGED: n값 작은 testN으로 대체
// 	var n uint32 = testN

// 	pc := int(n / chunkSize)
// 	if n%chunkSize != 0 {
// 		pc++
// 	}

// 	wd, _ := os.Getwd()
// 	t.Logf("//* DEBUG: WD=%s, n=%d => partitions=%d", wd, n, pc)

// 	cu, err := NewChunkUnionFind(n, "parent.dat", "rank.dat", "root.dat")
// 	if err != nil {
// 		t.Fatalf("NewChunkUnionFind fail:%v", err)
// 	}
// 	defer cu.Close(false)

// 	// init
// 	for i := 0; i < pc; i++ {
// 		if er2 := cu.InitPartition(i); er2 != nil {
// 			t.Fatalf("InitPartition(%d) fail:%v", i, er2)
// 		}
// 	}

// 	// sets_bin/*.bin => partition => ...
// 	if err := os.MkdirAll("bridge", 0755); err != nil {
// 		t.Fatalf("mkdir bridge fail:%v", err)
// 	}
// 	binFiles, _ := filepath.Glob("sets_bin/*.bin")
// 	for i := 0; i < pc; i++ {
// 		bfName := fmt.Sprintf("bridge/bridge_part%d.bin", i)
// 		bf, er3 := os.Create(bfName)
// 		if er3 != nil {
// 			t.Fatalf("create bf fail:%v", er3)
// 		}
// 		for _, binF := range binFiles {
// 			if er4 := cu.ProcessPartition(i, binF, bf); er4 != nil {
// 				t.Fatalf("ProcessPartition(%d) fail:%v", i, er4)
// 			}
// 		}
// 		bf.Close()
// 	}

// 	// bridging
// 	passCount := 3
// 	for pass := 1; pass <= passCount; pass++ {
// 		t.Logf("//* DEBUG: bridging pass #%d", pass)
// 		for i := 0; i < pc-1; i++ {
// 			bfName := fmt.Sprintf("bridge/bridge_part%d.bin", i)
// 			if err2 := cu.BridgeAdjacent(i, bfName); err2 != nil {
// 				t.Fatalf("BridgeAdjacent(%d) fail:%v", i, err2)
// 			}
// 		}
// 	}

// 	// parallel find
// 	if err2 := cu.ParallelFindAndRoot(); err2 != nil {
// 		t.Fatalf("ParallelFindAndRoot:%v", err2)
// 	}

// 	// external sort => uniq
// 	if err2 := externalSortRoot("root.dat", n, "tmp_sort_dir"); err2 != nil {
// 		t.Fatalf("externalSortRoot fail:%v", err2)
// 	}
// 	c2, erC := countUniqueRoots("root.dat.sorted")
// 	if erC != nil {
// 		t.Fatalf("countUniqueRoots fail:%v", erC)
// 	}
// 	t.Logf("//* RESULT: disjoint sets = %d", c2)
// }

package main_test

// import (
// 	"fmt"
// 	"math/rand"
// 	"os"
// 	"reflect"
// 	"runtime"
// 	"sync"
// 	"testing"
// 	"time"
// 	"unsafe"

// 	"github.com/edsrzf/mmap-go"
// )

// // 최대 재귀 깊이 제한
// const maxDepth = 20

// // MMapUnionFind는 매우 큰 원소 집합을 MMAP로 관리하는 구조체입니다.
// //   - parentSlice, rankSlice: union-find 알고리즘용 parent/rank 배열
// //   - rootSlice: 각 인덱스별 최종 Find 결과(루트)를 저장할 별도의 mmap 영역
// type MMapUnionFind struct {
// 	n          int32
// 	parentFile string
// 	rankFile   string
// 	rootFile   string

// 	parentMMap mmap.MMap
// 	rankMMap   mmap.MMap
// 	rootMMap   mmap.MMap

// 	parentSlice []int32 // parent 배열 (mmapped)
// 	rankSlice   []int8  // rank 배열 (mmapped)
// 	rootSlice   []int32 // 각 인덱스의 최종 루트를 기록하는 배열 (mmapped)
// }

// // createLargeFile: 주어진 크기로 파일을 생성하거나 Truncate합니다.
// func createLargeFile(fileName string, size int64) (*os.File, error) {
// 	f, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0644)
// 	if err != nil {
// 		return nil, err
// 	}
// 	err = f.Truncate(size)
// 	if err != nil {
// 		// sparse 파일로 fallback
// 		_, seekErr := f.Seek(size-1, 0)
// 		if seekErr != nil {
// 			f.Close()
// 			return nil, seekErr
// 		}
// 		_, writeErr := f.Write([]byte{0})
// 		if writeErr != nil {
// 			f.Close()
// 			return nil, writeErr
// 		}
// 	}
// 	return f, nil
// }

// // NewMMapUnionFind: n개의 원소(0~n-1)를 관리할 parent/rank/root 파일들을 생성하고 MMAP합니다.
// func NewMMapUnionFind(n int32, parentFile, rankFile, rootFile string) (*MMapUnionFind, error) {
// 	uf := &MMapUnionFind{
// 		n:          n,
// 		parentFile: parentFile,
// 		rankFile:   rankFile,
// 		rootFile:   rootFile,
// 	}

// 	//--------------------------------
// 	// 1) parent.dat (n * 4 바이트)
// 	//--------------------------------
// 	parentSize := int64(n) * 4
// 	fParent, err := createLargeFile(parentFile, parentSize)
// 	if err != nil {
// 		return nil, err
// 	}
// 	parentMap, err := mmap.Map(fParent, mmap.RDWR, 0)
// 	if err != nil {
// 		fParent.Close()
// 		return nil, err
// 	}
// 	fParent.Close()
// 	uf.parentMMap = parentMap

// 	//--------------------------------
// 	// 2) rank.dat (n * 1 바이트)
// 	//--------------------------------
// 	rankSize := int64(n)
// 	fRank, err := createLargeFile(rankFile, rankSize)
// 	if err != nil {
// 		return nil, err
// 	}
// 	rankMap, err := mmap.Map(fRank, mmap.RDWR, 0)
// 	if err != nil {
// 		fRank.Close()
// 		return nil, err
// 	}
// 	fRank.Close()
// 	uf.rankMMap = rankMap

// 	//--------------------------------
// 	// 3) root.dat (n * 4 바이트) - 각 원소의 최종 루트를 기록
// 	//--------------------------------
// 	rootSize := int64(n) * 4
// 	fRoot, err := createLargeFile(rootFile, rootSize)
// 	if err != nil {
// 		return nil, err
// 	}
// 	rootMap, err := mmap.Map(fRoot, mmap.RDWR, 0)
// 	if err != nil {
// 		fRoot.Close()
// 		return nil, err
// 	}
// 	fRoot.Close()
// 	uf.rootMMap = rootMap

// 	//--------------------------------
// 	// 슬라이스 재해석
// 	//--------------------------------
// 	{
// 		hdrParent := (*reflect.SliceHeader)(unsafe.Pointer(&uf.parentSlice))
// 		hdrParent.Data = uintptr(unsafe.Pointer(&uf.parentMMap[0]))
// 		hdrParent.Len = int(parentSize / 4)
// 		hdrParent.Cap = int(parentSize / 4)
// 	}
// 	{
// 		hdrRank := (*reflect.SliceHeader)(unsafe.Pointer(&uf.rankSlice))
// 		hdrRank.Data = uintptr(unsafe.Pointer(&uf.rankMMap[0]))
// 		hdrRank.Len = int(rankSize)
// 		hdrRank.Cap = int(rankSize)
// 	}
// 	{
// 		hdrRoot := (*reflect.SliceHeader)(unsafe.Pointer(&uf.rootSlice))
// 		hdrRoot.Data = uintptr(unsafe.Pointer(&uf.rootMMap[0]))
// 		hdrRoot.Len = int(rootSize / 4)
// 		hdrRoot.Cap = int(rootSize / 4)
// 	}

// 	//--------------------------------
// 	// 병렬 초기화
// 	//--------------------------------
// 	numWorkers := runtime.NumCPU()
// 	chunkSize := n / int32(numWorkers)
// 	var wg sync.WaitGroup
// 	start := time.Now()
// 	for w := 0; w < numWorkers; w++ {
// 		startIdx := int32(w) * chunkSize
// 		endIdx := startIdx + chunkSize
// 		if w == numWorkers-1 {
// 			endIdx = n
// 		}
// 		wg.Add(1)
// 		go func(s, e int32) {
// 			defer wg.Done()
// 			for i := s; i < e; i++ {
// 				uf.parentSlice[i] = i
// 				uf.rankSlice[i] = 0
// 				uf.rootSlice[i] = -1 // 아직 Find 결과 없음
// 				if (i+1)%100000000 == 0 {
// 					fmt.Printf("초기화 진행: %d / %d 원소 처리\n", i+1, n)
// 				}
// 			}
// 		}(startIdx, endIdx)
// 	}
// 	wg.Wait()
// 	fmt.Printf("초기화 완료 (소요 시간: %v)\n", time.Since(start))

// 	return uf, nil
// }

// // CloseAndCleanup: MMAP 해제 후, (옵션) 파일 삭제
// func (uf *MMapUnionFind) CloseAndCleanup(deleteFiles bool) error {
// 	if err := uf.parentMMap.Unmap(); err != nil {
// 		return err
// 	}
// 	if err := uf.rankMMap.Unmap(); err != nil {
// 		return err
// 	}
// 	if err := uf.rootMMap.Unmap(); err != nil {
// 		return err
// 	}
// 	if deleteFiles {
// 		os.Remove(uf.parentFile)
// 		os.Remove(uf.rankFile)
// 		os.Remove(uf.rootFile)
// 	}
// 	return nil
// }

// // FindWithDepth: 재귀 깊이가 maxDepth 초과 시 false 반환
// func (uf *MMapUnionFind) FindWithDepth(x int32, depth int) (int32, bool) {
// 	if depth >= maxDepth {
// 		return x, false
// 	}
// 	if uf.parentSlice[x] != x {
// 		root, ok := uf.FindWithDepth(uf.parentSlice[x], depth+1)
// 		if !ok {
// 			return x, false
// 		}
// 		uf.parentSlice[x] = root
// 		return root, true
// 	}
// 	return x, true
// }

// // UnionWithDepth: 두 원소 x,y를 병합 (재귀 깊이 초과 시 스킵)
// func (uf *MMapUnionFind) UnionWithDepth(x, y int32) bool {
// 	rootX, okX := uf.FindWithDepth(x, 0)
// 	rootY, okY := uf.FindWithDepth(y, 0)
// 	if !okX || !okY {
// 		return false
// 	}
// 	if rootX == rootY {
// 		return true
// 	}
// 	if uf.rankSlice[rootX] < uf.rankSlice[rootY] {
// 		uf.parentSlice[rootX] = rootY
// 	} else if uf.rankSlice[rootX] > uf.rankSlice[rootY] {
// 		uf.parentSlice[rootY] = rootX
// 	} else {
// 		uf.parentSlice[rootY] = rootX
// 		uf.rankSlice[rootX]++
// 	}
// 	return true
// }

// // TestMMapUnionFindLargeFileBased: 20억 개 원소에 대해
// // 1) parent/rank/root.dat 3개 파일로 관리
// // 2) union은 재귀 깊이 제한 적용
// // 3) 최종 Find 결과(루트)를 rootSlice에 직접 기록 (map 사용 안 함)
// func TestMMapUnionFindLargeFileBased(t *testing.T) {
// 	if testing.Short() {
// 		t.Skip("대규모 MMAP union-find 테스트는 -short 모드에서 스킵합니다.")
// 	}

// 	var n int32 = 2000000000 // 20억
// 	numUnions := n / 100

// 	parentFile := "parent.dat"
// 	rankFile := "rank.dat"
// 	rootFile := "root.dat"

// 	t.Logf("n = %d 원소에 대해 MMAP union-find 시작...", n)
// 	start := time.Now()

// 	// 1) 초기화
// 	uf, err := NewMMapUnionFind(n, parentFile, rankFile, rootFile)
// 	if err != nil {
// 		t.Fatalf("초기화 오류: %v", err)
// 	}
// 	defer func() {
// 		// 테스트가 끝나면 mmap 해제 및 파일 삭제
// 		if err := uf.CloseAndCleanup(true); err != nil {
// 			t.Fatalf("CloseAndCleanup 오류: %v", err)
// 		}
// 	}()

// 	t.Logf("초기화 완료 (소요시간: %v)", time.Since(start))

// 	// 2) union 연산
// 	startUnion := time.Now()
// 	rand.Seed(time.Now().UnixNano())
// 	for i := int32(0); i < numUnions; i++ {
// 		base := rand.Int31n(n)
// 		unionSize := rand.Int31n(49) + 2
// 		for j := int32(1); j < unionSize; j++ {
// 			other := rand.Int31n(n)
// 			uf.UnionWithDepth(base, other)
// 		}
// 		if (i+1)%1000000 == 0 {
// 			t.Logf("Union 진행: %d / %d, 경과: %v", i+1, numUnions, time.Since(startUnion))
// 		}
// 	}
// 	t.Logf("Union 연산 완료 (소요 시간: %v)", time.Since(startUnion))

// 	// 3) 병렬 Find: 각 인덱스별 rootSlice[i] = 루트
// 	numWorkers := runtime.NumCPU()
// 	chunkSize := n / int32(numWorkers)
// 	var wg sync.WaitGroup
// 	startFind := time.Now()

// 	for w := 0; w < numWorkers; w++ {
// 		startIdx := int32(w) * chunkSize
// 		endIdx := startIdx + chunkSize
// 		if w == numWorkers-1 {
// 			endIdx = n
// 		}
// 		wg.Add(1)
// 		go func(s, e int32) {
// 			defer wg.Done()
// 			for i := s; i < e; i++ {
// 				root, ok := uf.FindWithDepth(i, 0)
// 				if !ok {
// 					// 재귀 너무 깊으면 스킵 (rootSlice[i]는 -1 유지)
// 					continue
// 				}
// 				uf.rootSlice[i] = root
// 				if (i+1-s)%10000000 == 0 {
// 					fmt.Printf("Worker[%d-%d] Find 진행: %d\n", s, e, i+1-s)
// 				}
// 			}
// 		}(startIdx, endIdx)
// 	}
// 	wg.Wait()
// 	t.Logf("Find 연산 완료 (소요 시간: %v)", time.Since(startFind))

// 	totalTime := time.Since(start)
// 	t.Logf("전체 처리 소요 시간: %v", totalTime)

// 	// 주의: rootSlice에 모든 i의 루트가 기록되었으나,
// 	// 몇 개가 disjoint인지 등은 별도의 외부 정렬/처리를 통해 확인해야 함.
// 	// 여기서는 map을 쓰지 않고, 파일(root.dat)에 각 인덱스별 루트를 기록하는 것으로 마무리.
// }

// package unionfind_test

// import (
// 	"fmt"
// 	"math/rand"
// 	"os"
// 	"reflect"
// 	"runtime"
// 	"sync"
// 	"testing"
// 	"time"
// 	"unsafe"

// 	"github.com/edsrzf/mmap-go"
// )

// // MMapUnionFind는 매우 큰 원소 집합을 MMAP를 이용해 관리하는 union‑find 자료구조입니다.
// // parent: 각 원소의 부모 인덱스를 저장하는 int32 배열 (각 원소 4바이트)
// // rank: 각 집합의 트리 깊이를 저장하는 int8 배열 (각 원소 1바이트)
// // 이 방식은 메모리 내 배열 대신 디스크 파일을 매핑함으로써, n이 매우 클 때 물리 메모리 부담을 줄입니다.
// type MMapUnionFind struct {
// 	n           int32     // 전체 원소 수
// 	parentFile  string    // parent 배열을 저장할 파일 이름
// 	rankFile    string    // rank 배열을 저장할 파일 이름
// 	parentMMap  mmap.MMap // parent 배열에 대한 메모리 매핑
// 	rankMMap    mmap.MMap // rank 배열에 대한 메모리 매핑
// 	parentSlice []int32   // MMAP된 byte slice를 int32 슬라이스로 재해석한 것
// 	rankSlice   []int8    // MMAP된 byte slice를 int8 슬라이스로 재해석한 것
// }

// const maxDepth = 20 // 최대 재귀 깊이 제한

// // createLargeFile는 지정한 파일을 원하는 크기로 할당합니다.
// // Truncate에 실패하면 sparse 파일 방식으로 할당을 시도합니다.
// func createLargeFile(fileName string, size int64) (*os.File, error) {
// 	f, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0644)
// 	if err != nil {
// 		return nil, err
// 	}
// 	err = f.Truncate(size)
// 	if err != nil {
// 		_, seekErr := f.Seek(size-1, 0)
// 		if seekErr != nil {
// 			f.Close()
// 			return nil, seekErr
// 		}
// 		_, writeErr := f.Write([]byte{0})
// 		if writeErr != nil {
// 			f.Close()
// 			return nil, writeErr
// 		}
// 	}
// 	return f, nil
// }

// // NewMMapUnionFind는 n개의 원소(0~n-1)를 초기화하고, parentFile과 rankFile을 MMAP하는 함수입니다.
// func NewMMapUnionFind(n int32, parentFile, rankFile string) (*MMapUnionFind, error) {
// 	uf := &MMapUnionFind{
// 		n:          n,
// 		parentFile: parentFile,
// 		rankFile:   rankFile,
// 	}

// 	// ----- parent 배열 설정 -----
// 	// parent 배열: n * 4바이트 (int32)
// 	parentSize := int64(n) * 4
// 	fParent, err := createLargeFile(parentFile, parentSize)
// 	if err != nil {
// 		return nil, err
// 	}
// 	parentMap, err := mmap.Map(fParent, mmap.RDWR, 0)
// 	if err != nil {
// 		fParent.Close()
// 		return nil, err
// 	}
// 	fParent.Close()
// 	uf.parentMMap = parentMap

// 	// ----- rank 배열 설정 -----
// 	// rank 배열: n * 1바이트 (int8)
// 	rankSize := int64(n)
// 	fRank, err := createLargeFile(rankFile, rankSize)
// 	if err != nil {
// 		return nil, err
// 	}
// 	rankMap, err := mmap.Map(fRank, mmap.RDWR, 0)
// 	if err != nil {
// 		fRank.Close()
// 		return nil, err
// 	}
// 	fRank.Close()
// 	uf.rankMMap = rankMap

// 	// ----- 슬라이스 재해석 -----
// 	hdrParent := (*reflect.SliceHeader)(unsafe.Pointer(&uf.parentSlice))
// 	hdrParent.Data = uintptr(unsafe.Pointer(&uf.parentMMap[0]))
// 	hdrParent.Len = int(parentSize / 4)
// 	hdrParent.Cap = int(parentSize / 4)

// 	hdrRank := (*reflect.SliceHeader)(unsafe.Pointer(&uf.rankSlice))
// 	hdrRank.Data = uintptr(unsafe.Pointer(&uf.rankMMap[0]))
// 	hdrRank.Len = int(rankSize)
// 	hdrRank.Cap = int(rankSize)

// 	// ----- 병렬 초기화 -----
// 	numWorkers := runtime.NumCPU()
// 	chunkSize := n / int32(numWorkers)
// 	var wg sync.WaitGroup
// 	initStart := time.Now()
// 	for w := 0; w < numWorkers; w++ {
// 		startIdx := int32(w) * chunkSize
// 		endIdx := startIdx + chunkSize
// 		if w == numWorkers-1 {
// 			endIdx = n
// 		}
// 		wg.Add(1)
// 		go func(start, end int32) {
// 			defer wg.Done()
// 			for i := start; i < end; i++ {
// 				uf.parentSlice[i] = i
// 				uf.rankSlice[i] = 0
// 				// 진행 상황 로깅: 1억 개 단위마다 한 번만 출력
// 				if (i+1)%100000000 == 0 {
// 					fmt.Printf("초기화 진행: %d / %d 원소 처리\n", i+1, n)
// 				}
// 			}
// 		}(startIdx, endIdx)
// 	}
// 	wg.Wait()
// 	totalInitTime := time.Since(initStart)
// 	fmt.Printf("초기화 완료 (전체 소요 시간: %v)\n", totalInitTime)

// 	return uf, nil
// }

// // Close는 MMAP된 파일들을 해제합니다.
// func (uf *MMapUnionFind) Close() error {
// 	if err := uf.parentMMap.Unmap(); err != nil {
// 		return err
// 	}
// 	if err := uf.rankMMap.Unmap(); err != nil {
// 		return err
// 	}
// 	return nil
// }

// // FindWithDepth는 재귀 깊이를 추적하여, 최대 maxDepth를 초과하면 false를 반환합니다.
// func (uf *MMapUnionFind) FindWithDepth(x int32, depth int) (int32, bool) {
// 	if depth >= maxDepth {
// 		return x, false // 깊이 제한 초과
// 	}
// 	if uf.parentSlice[x] != x {
// 		root, ok := uf.FindWithDepth(uf.parentSlice[x], depth+1)
// 		if !ok {
// 			return x, false
// 		}
// 		uf.parentSlice[x] = root
// 		return root, true
// 	}
// 	return x, true
// }

// // UnionWithDepth는 FindWithDepth를 사용하여 union을 수행합니다.
// // 만약 재귀 깊이가 maxDepth에 도달하면 union 연산을 스킵합니다.
// func (uf *MMapUnionFind) UnionWithDepth(x, y int32) bool {
// 	rootX, okX := uf.FindWithDepth(x, 0)
// 	rootY, okY := uf.FindWithDepth(y, 0)
// 	if !okX || !okY {
// 		return false // 깊이 제한에 도달했으므로 union 스킵
// 	}
// 	if rootX == rootY {
// 		return true
// 	}
// 	if uf.rankSlice[rootX] < uf.rankSlice[rootY] {
// 		uf.parentSlice[rootX] = rootY
// 	} else if uf.rankSlice[rootX] > uf.rankSlice[rootY] {
// 		uf.parentSlice[rootY] = rootX
// 	} else {
// 		uf.parentSlice[rootY] = rootX
// 		uf.rankSlice[rootX]++
// 	}
// 	return true
// }

// // FindIterative는 반복문을 사용한 Find 함수입니다.
// func (uf *MMapUnionFind) FindIterative(x int32) int32 {
// 	for uf.parentSlice[x] != x {
// 		uf.parentSlice[x] = uf.parentSlice[uf.parentSlice[x]]
// 		x = uf.parentSlice[x]
// 	}
// 	return x
// }

// // TestMMapUnionFindLarge는 20억 원소에 대해 MMAP 기반 union-find의 동작을 검증하는 테스트입니다.
// // 1) union 및 find 연산에서 재귀 깊이가 1000 이상이면 해당 union 연산을 스킵하도록 하고,
// // 2) disjoint 집합은 최대 10억 원소까지 감당할 수 있도록 함,
// // 3) find 연산은 병렬 처리하여 전체 처리를 개선합니다.
// func TestMMapUnionFindLarge(t *testing.T) {
// 	if testing.Short() {
// 		t.Skip("대규모 MMAP union-find 테스트는 -short 모드에서 스킵합니다.")
// 	}

// 	var n int32 = 100000000 // 20억 원소
// 	numUnions := n / 100    // union 연산 횟수를 1%로 제한
// 	parentFile := "parent.dat"
// 	rankFile := "rank.dat"

// 	t.Logf("n = %d 원소에 대해 MMAP 기반 union-find 초기화 시작...", n)
// 	start := time.Now()
// 	uf, err := NewMMapUnionFind(n, parentFile, rankFile)
// 	if err != nil {
// 		t.Fatalf("MMapUnionFind 초기화 오류: %v", err)
// 	}
// 	defer func() {
// 		if err := uf.Close(); err != nil {
// 			t.Fatalf("MMapUnionFind 종료 오류: %v", err)
// 		}
// 	}()
// 	t.Logf("초기화 완료 (소요 시간: %v)", time.Since(start))

// 	// union 연산: 재귀 깊이 제한을 적용한 UnionWithDepth 사용
// 	startUnion := time.Now()
// 	rand.Seed(time.Now().UnixNano())
// 	for i := int32(0); i < numUnions; i++ {
// 		base := rand.Int31n(n)
// 		unionSize := rand.Int31n(49) + 2 // 2 ~ 50개 원소
// 		for j := int32(1); j < unionSize; j++ {
// 			other := rand.Int31n(n)
// 			uf.UnionWithDepth(base, other) // 깊이 제한 적용
// 		}
// 		if (i+1)%1000000 == 0 {
// 			t.Logf("Union 진행: %d / %d, 경과 시간: %v", i+1, numUnions, time.Since(startUnion))
// 		}
// 	}
// 	t.Logf("Union 연산 완료 (소요 시간: %v)", time.Since(startUnion))

// 	// 병렬 Find 연산: 전체 원소를 CPU 코어 수만큼 청크로 나누어 처리
// 	numWorkers := runtime.NumCPU()
// 	chunkSize := n / int32(numWorkers)
// 	var wg sync.WaitGroup
// 	globalMap := make(map[int32]int32)
// 	var mu sync.Mutex

// 	for w := 0; w < numWorkers; w++ {
// 		startIdx := int32(w) * chunkSize
// 		endIdx := startIdx + chunkSize
// 		if w == numWorkers-1 {
// 			endIdx = n
// 		}
// 		wg.Add(1)
// 		go func(start, end int32) {
// 			defer wg.Done()
// 			localMap := make(map[int32]int32)
// 			for i := start; i < end; i++ {
// 				root, ok := uf.FindWithDepth(i, 0)
// 				// 만약 깊이 제한에 걸렸다면, 재귀가 너무 깊은 구간은 건너뜁니다.
// 				if !ok {
// 					continue
// 				}
// 				localMap[root]++
// 				if (i+1-start)%10000000 == 0 {
// 					fmt.Printf("Worker [%d-%d] Find 진행: %d 원소 처리\n", start, end, i+1-start)
// 				}
// 			}
// 			mu.Lock()
// 			for k, v := range localMap {
// 				globalMap[k] += v
// 			}
// 			mu.Unlock()
// 		}(startIdx, endIdx)
// 	}
// 	wg.Wait()
// 	groupCount := len(globalMap)
// 	totalTime := time.Since(start)

// 	t.Logf("전체 원소 수: %d", n)
// 	t.Logf("최종 disjoint 집합 수: %d", groupCount)
// 	t.Logf("전체 처리 소요 시간: %v", totalTime)
// }

// 추가 검증 로직은 필요에 따라 삽입 가능

// package unionfind_test

// import (
// 	"fmt"
// 	"math/rand"
// 	"os"
// 	"reflect"
// 	"runtime"
// 	"sync"
// 	"testing"
// 	"time"
// 	"unsafe"

// 	"github.com/edsrzf/mmap-go"
// )

// //* 결과적으로 === RUN   TestMMapUnionFindLarge
//     union_test.go:192: n = 200000000 원소에 대해 MMAP 기반 union-find 초기화 시작...
// 	초기화 진행: 200000000 / 200000000 원소 처리
// 	초기화 진행: 100000000 / 200000000 원소 처리
// 	초기화 완료 (전체 소요 시간: 104.43875ms)
// 		union_test.go:203: 초기화 완료 (소요 시간: 690.210283ms)
// 		union_test.go:217: Union 진행: 1000000 / 4000000, 경과 시간: 3.532798202s
// 		union_test.go:217: Union 진행: 2000000 / 4000000, 경과 시간: 7.217292347s
// 		union_test.go:217: Union 진행: 3000000 / 4000000, 경과 시간: 10.667734828s
// 		union_test.go:217: Union 진행: 4000000 / 4000000, 경과 시간: 14.019732897s
// 		union_test.go:220: Union 연산 완료 (소요 시간: 14.019798775s)
// 		union_test.go:229: Find 진행: 10000000 / 200000000, 경과 시간: 1.149942889s
// 		union_test.go:229: Find 진행: 20000000 / 200000000, 경과 시간: 2.456217414s
// 		union_test.go:229: Find 진행: 30000000 / 200000000, 경과 시간: 4.023066709s
// 		union_test.go:229: Find 진행: 40000000 / 200000000, 경과 시간: 5.023278172s
// 		union_test.go:229: Find 진행: 50000000 / 200000000, 경과 시간: 7.39629201s
// 		union_test.go:229: Find 진행: 60000000 / 200000000, 경과 시간: 8.291852085s
// 		union_test.go:229: Find 진행: 70000000 / 200000000, 경과 시간: 9.254942323s
// 		union_test.go:229: Find 진행: 80000000 / 200000000, 경과 시간: 10.377010481s
// 		union_test.go:229: Find 진행: 90000000 / 200000000, 경과 시간: 11.716553224s
// 		union_test.go:229: Find 진행: 100000000 / 200000000, 경과 시간: 16.999413761s
// 		union_test.go:229: Find 진행: 110000000 / 200000000, 경과 시간: 18.254709502s
// 		union_test.go:229: Find 진행: 120000000 / 200000000, 경과 시간: 19.383204422s
// 		union_test.go:229: Find 진행: 130000000 / 200000000, 경과 시간: 20.481133089s
// 		union_test.go:229: Find 진행: 140000000 / 200000000, 경과 시간: 21.630763988s
// 		union_test.go:229: Find 진행: 150000000 / 200000000, 경과 시간: 22.91845431s
// 		union_test.go:229: Find 진행: 160000000 / 200000000, 경과 시간: 24.391462657s
// 		union_test.go:229: Find 진행: 170000000 / 200000000, 경과 시간: 25.975153447s
// 		union_test.go:229: Find 진행: 180000000 / 200000000, 경과 시간: 27.575320363s
// 		union_test.go:229: Find 진행: 190000000 / 200000000, 경과 시간: 33.805587871s
// 		union_test.go:229: Find 진행: 200000000 / 200000000, 경과 시간: 37.155565143s
// 		union_test.go:235: 전체 원소 수: 200000000
// 		union_test.go:236: 최종 disjoint 집합 수: 118994553
// 		union_test.go:237: 전체 처리 소요 시간: 51.865667278s
// 	--- PASS: TestMMapUnionFindLarge (51.89s)
// 	PASS
// 	ok      whale_tracker/utils     52.159s
// // MMapUnionFind는 매우 큰 원소 집합을 MMAP를 이용해 관리하는 union-find 자료구조입니다.
// // - parent: 각 원소의 부모 인덱스를 저장하는 int32 배열 (각 원소 4바이트)
// // - rank: 각 집합의 트리 깊이를 저장하는 int8 배열 (각 원소 1바이트)
// // 이 방식은 메모리 내 배열 대신 디스크 파일을 매핑함으로써, n이 매우 클 때 물리 메모리 부담을 줄입니다.
// type MMapUnionFind struct {
// 	n           int32     // 전체 원소 수
// 	parentFile  string    // parent 배열을 저장할 파일 이름
// 	rankFile    string    // rank 배열을 저장할 파일 이름
// 	parentMMap  mmap.MMap // parent 배열에 대한 메모리 매핑
// 	rankMMap    mmap.MMap // rank 배열에 대한 메모리 매핑
// 	parentSlice []int32   // MMAP된 byte slice를 int32 슬라이스로 재해석한 것
// 	rankSlice   []int8    // MMAP된 byte slice를 int8 슬라이스로 재해석한 것
// }

// // createLargeFile는 지정한 파일을 원하는 크기로 할당합니다.
// // Truncate에 실패하면 sparse 파일 방식으로 할당을 시도합니다.
// func createLargeFile(fileName string, size int64) (*os.File, error) {
// 	f, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0644)
// 	if err != nil {
// 		return nil, err
// 	}
// 	// Truncate로 파일 크기 설정 시도
// 	err = f.Truncate(size)
// 	if err != nil {
// 		// 실패 시, sparse 파일 생성 방식으로 시도: size-1 위치로 이동 후 0 바이트 기록
// 		_, seekErr := f.Seek(size-1, 0)
// 		if seekErr != nil {
// 			f.Close()
// 			return nil, seekErr
// 		}
// 		_, writeErr := f.Write([]byte{0})
// 		if writeErr != nil {
// 			f.Close()
// 			return nil, writeErr
// 		}
// 	}
// 	return f, nil
// }

// // NewMMapUnionFind는 n개의 원소(0~n-1)를 초기화하고, parentFile과 rankFile을 MMAP하는 함수입니다.
// func NewMMapUnionFind(n int32, parentFile, rankFile string) (*MMapUnionFind, error) {
// 	uf := &MMapUnionFind{
// 		n:          n,
// 		parentFile: parentFile,
// 		rankFile:   rankFile,
// 	}

// 	// ----- parent 배열 설정 -----
// 	// parent 배열: n * 4바이트 (int32)
// 	parentSize := int64(n) * 4
// 	fParent, err := createLargeFile(parentFile, parentSize)
// 	if err != nil {
// 		return nil, err
// 	}
// 	parentMap, err := mmap.Map(fParent, mmap.RDWR, 0)
// 	if err != nil {
// 		fParent.Close()
// 		return nil, err
// 	}
// 	fParent.Close()
// 	uf.parentMMap = parentMap

// 	// ----- rank 배열 설정 -----
// 	// rank 배열: n * 1바이트 (int8)
// 	rankSize := int64(n)
// 	fRank, err := createLargeFile(rankFile, rankSize)
// 	if err != nil {
// 		return nil, err
// 	}
// 	rankMap, err := mmap.Map(fRank, mmap.RDWR, 0)
// 	if err != nil {
// 		fRank.Close()
// 		return nil, err
// 	}
// 	fRank.Close()
// 	uf.rankMMap = rankMap

// 	// ----- 슬라이스 재해석 -----
// 	// parentMMap을 int32 슬라이스로 변환
// 	hdrParent := (*reflect.SliceHeader)(unsafe.Pointer(&uf.parentSlice))
// 	hdrParent.Data = uintptr(unsafe.Pointer(&uf.parentMMap[0]))
// 	hdrParent.Len = int(parentSize / 4)
// 	hdrParent.Cap = int(parentSize / 4)

// 	// rankMMap을 int8 슬라이스로 변환
// 	hdrRank := (*reflect.SliceHeader)(unsafe.Pointer(&uf.rankSlice))
// 	hdrRank.Data = uintptr(unsafe.Pointer(&uf.rankMMap[0]))
// 	hdrRank.Len = int(rankSize)
// 	hdrRank.Cap = int(rankSize)

// 	// ----- 병렬 초기화 -----
// 	// 전체 원소를 여러 청크로 나누어 여러 고루틴에서 동시에 초기화합니다.
// 	numWorkers := runtime.NumCPU() // CPU 코어 수 만큼 고루틴 사용
// 	chunkSize := n / int32(numWorkers)
// 	var wg sync.WaitGroup
// 	initStart := time.Now()
// 	for w := 0; w < numWorkers; w++ {
// 		startIdx := int32(w) * chunkSize
// 		// 마지막 고루틴는 나머지 원소까지 처리
// 		endIdx := startIdx + chunkSize
// 		if w == numWorkers-1 {
// 			endIdx = n
// 		}
// 		wg.Add(1)
// 		go func(start, end int32) {
// 			defer wg.Done()
// 			for i := start; i < end; i++ {
// 				uf.parentSlice[i] = i
// 				uf.rankSlice[i] = 0
// 				// 진행 상황 로깅은 1억 개 단위마다 한 번만 출력
// 				if (i+1)%100000000 == 0 {
// 					fmt.Printf("초기화 진행: %d / %d 원소 처리\n", i+1, n)
// 				}
// 			}
// 		}(startIdx, endIdx)
// 	}
// 	wg.Wait()
// 	totalInitTime := time.Since(initStart)
// 	fmt.Printf("초기화 완료 (전체 소요 시간: %v)\n", totalInitTime)

// 	return uf, nil
// }

// // Close는 MMAP된 파일들을 해제합니다.
// func (uf *MMapUnionFind) Close() error {
// 	if err := uf.parentMMap.Unmap(); err != nil {
// 		return err
// 	}
// 	if err := uf.rankMMap.Unmap(); err != nil {
// 		return err
// 	}
// 	return nil
// }

// // Find는 경로 압축(Path Compression)을 적용하여 x의 대표(root)를 찾습니다.
// func (uf *MMapUnionFind) Find(x int32) int32 {
// 	if uf.parentSlice[x] != x {
// 		uf.parentSlice[x] = uf.Find(uf.parentSlice[x])
// 	}
// 	return uf.parentSlice[x]
// }

// // Union은 두 원소 x와 y의 집합을 병합합니다.
// // 낮은 랭크의 트리를 높은 랭크의 트리에 붙여 트리 높이를 최소화합니다.
// func (uf *MMapUnionFind) Union(x, y int32) {
// 	rootX := uf.Find(x)
// 	rootY := uf.Find(y)
// 	if rootX == rootY {
// 		return
// 	}
// 	if uf.rankSlice[rootX] < uf.rankSlice[rootY] {
// 		uf.parentSlice[rootX] = rootY
// 	} else if uf.rankSlice[rootX] > uf.rankSlice[rootY] {
// 		uf.parentSlice[rootY] = rootX
// 	} else {
// 		uf.parentSlice[rootY] = rootX
// 		uf.rankSlice[rootX]++
// 	}
// }

// // TestMMapUnionFindLarge는 20억(2,000,000,000) 원소에 대해 MMAP 기반 union-find의 동작을 검증하는 테스트입니다.
// // 시스템 자원이 충분하지 않다면 -short 모드에서 이 테스트를 스킵합니다.
// func TestMMapUnionFindLarge(t *testing.T) {
// 	if testing.Short() {
// 		t.Skip("대규모 MMAP union-find 테스트는 -short 모드에서 스킵합니다.")
// 	}

// 	// 전체 원소 수 n: 2억
// 	var n int32 = 200000000
// 	// union 연산 횟수를 기존 5% (n/20)에서 1% (n/100)로 줄여 부하를 완화합니다.
// 	numUnions := n / 50

// 	parentFile := "parent.dat"
// 	rankFile := "rank.dat"

// 	t.Logf("n = %d 원소에 대해 MMAP 기반 union-find 초기화 시작...", n)
// 	start := time.Now()
// 	uf, err := NewMMapUnionFind(n, parentFile, rankFile)
// 	if err != nil {
// 		t.Fatalf("MMapUnionFind 초기화 오류: %v", err)
// 	}
// 	defer func() {
// 		if err := uf.Close(); err != nil {
// 			t.Fatalf("MMapUnionFind 종료 오류: %v", err)
// 		}
// 	}()
// 	t.Logf("초기화 완료 (소요 시간: %v)", time.Since(start))

// 	// union 연산 수행: union 횟수를 줄여 부하를 완화

// 	startUnion := time.Now()
// 	for i := int32(0); i < numUnions; i++ {
// 		base := rand.Int31n(n)
// 		// 2 ~ 100개의 원소 대신 2 ~ 50개 원소로 union 수행
// 		unionSize := rand.Int31n(49) + 2
// 		for j := int32(1); j < unionSize; j++ {
// 			other := rand.Int31n(n)
// 			uf.Union(base, other)
// 		}
// 		if (i+1)%1000000 == 0 {
// 			t.Logf("Union 진행: %d / %d, 경과 시간: %v", i+1, numUnions, time.Since(startUnion))
// 		}
// 	}
// 	t.Logf("Union 연산 완료 (소요 시간: %v)", time.Since(startUnion))

// 	// 모든 원소에 대해 Find 연산을 수행하여 최종 disjoint 집합 수를 계산합니다.
// 	groupCountMap := make(map[int32]int32)
// 	startFind := time.Now()
// 	for i := int32(0); i < n; i++ {
// 		root := uf.Find(i)
// 		groupCountMap[root]++
// 		if (i+1)%10000000 == 0 {
// 			t.Logf("Find 진행: %d / %d, 경과 시간: %v", i+1, n, time.Since(startFind))
// 		}
// 	}
// 	groupCount := len(groupCountMap)
// 	totalTime := time.Since(start)

// 	t.Logf("전체 원소 수: %d", n)
// 	t.Logf("최종 disjoint 집합 수: %d", groupCount)
// 	t.Logf("전체 처리 소요 시간: %v", totalTime)

// 	// 추가 검증 로직은 필요에 따라 삽입 가능
// }
