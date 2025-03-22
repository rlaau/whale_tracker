// filename: main_test.go
package main_test

import (
	"math/rand"
	"os"
	"runtime"
	"testing"
	. "whale_tracker/utils/union"
)

var TestBatchSize int
var TestTotalBatches int

// GlobalBackup 구조체: 테스트 전후로 대규모 상수를 백업/복원하기 위한 예시
type GlobalBackup struct {
	oldBatchSize    int
	oldTotalBatches int
	oldDataDir      string
}

// 백업 함수
func backupGlobals() *GlobalBackup {
	return &GlobalBackup{
		oldBatchSize:    BatchSize,
		oldTotalBatches: TotalBatches,
		oldDataDir:      "unionfind_data", // 디폴트 디렉토리
	}
}

// 복원 함수
func (gb *GlobalBackup) restore() {
	TestBatchSize = gb.oldBatchSize
	TestTotalBatches = gb.oldTotalBatches
	// dataDir도 본인이 관리하는 방식에 따라 복원
}

// 간단 테스트용으로 상수를 축소하고, 작은 규모 테스트를 수행
func TestMMapUnionFind_SmallScale(t *testing.T) {
	// 1. 전역 상수 백업
	backup := backupGlobals()

	// 2. 전역 상수 일시적으로 축소
	//    예) BatchSize = 50, TotalBatches = 2 (총 100개 원소)
	TestBatchSize = 50
	TestTotalBatches = 2
	dataDir := "testdata_unionfind"

	// 테스트 전에 혹시 같은 디렉토리가 남아있으면 삭제
	os.RemoveAll(dataDir)

	// 3. 테스트 시작
	defer func() {
		// 마지막에 디렉토리 정리 (필요 시 주석 처리)
		os.RemoveAll(dataDir)

		// 전역 상수 복원
		backup.restore()
	}()

	// 4. 실제 로직 실행
	//    main() 함수를 직접 호출하는 대신,
	//    core 로직을 테스트하기 위해 "점증적 처리기"를 직접 사용해 본다.
	processor, err := NewIncrementalProcessor(dataDir)
	if err != nil {
		t.Fatalf("NewIncrementalProcessor 실패: %v", err)
	}

	// 작은 배치 2개를 순차 처리
	for batchID := 0; batchID < TotalBatches; batchID++ {
		t.Logf("===== 배치 %d 시작 =====", batchID)

		// 4-1) StartNewBatch()
		err = processor.StartNewBatch()
		if err != nil {
			t.Fatalf("배치 %d StartNewBatch() 실패: %v", batchID, err)
		}

		// 4-2) 무작위로 집합들(2~4개짜리)을 만들어서 병합
		//      (원래 generateTestBatch()와 유사한 로직)
		numToMerge := int(float64(BatchSize) * 0.2)
		used := make(map[uint64]bool)
		for i := 0; i < numToMerge; i++ {
			size := rand.Intn(3) + 2 // 2,3,4 크기 중 하나
			setElems := make([]uint64, 0, size)

			for j := 0; j < size; j++ {
				for {
					elem := uint64(rand.Intn(BatchSize) + batchID*BatchSize)
					if !used[elem] {
						used[elem] = true
						setElems = append(setElems, elem)
						break
					}
				}
			}

			// 실제 병합
			err = processor.ProcessSet(setElems)
			if err != nil {
				t.Errorf("ProcessSet 오류 (batch=%d, set=%v): %v", batchID, setElems, err)
			}
		}

		// 4-3) batch마다 강제 GC
		runtime.GC()
	}

	// 5. 아카이브 간 병합 계산
	err = MergeArchives(processor)
	if err != nil {
		t.Fatalf("mergeArchives 실패: %v", err)
	}

	// 6. 최종 결과를 한번 더 체크 (샘플링)
	disjointSets, sizeDist, err := processor.CalculateGlobalState()
	if err != nil {
		t.Fatalf("CalculateGlobalState 실패: %v", err)
	}

	t.Logf("최종 서로소 집합 수: %d", disjointSets)
	t.Log("집합 크기 분포 (샘플):")
	for k, v := range sizeDist {
		// 너무 많은 크기(1~120) 모두 찍긴 하므로, 여기서는 일부만
		if v > 0 {
			t.Logf("  size=%d -> count=%d", k, v)
		}
	}

	// 간단 검증:
	// 작은 데이터이므로, 만약 모든 원소가 다 1개 집합으로만 남아있다면
	// disjointSets 가 BatchSize*TotalBatches(=100) 근처일 것이고,
	// 많은 병합이 일어났다면 disjointSets 가 좀 줄어 있을 것임.
	if disjointSets >= int64(BatchSize*TotalBatches) {
		t.Errorf("병합이 거의 안 일어난 것 같습니다(집합 수가 너무 큼). disjointSets=%d", disjointSets)
	}
}

// 조금 더 단순화해서, "하나의 배치"만 테스트해보고 싶다면:
func TestSingleBatchOnly(t *testing.T) {
	// 전역 상수 백업 & 축소
	backup := backupGlobals()
	defer backup.restore()

	TestBatchSize = 20
	TestTotalBatches = 1
	dataDir := "testdata_singlebatch"
	os.RemoveAll(dataDir)
	defer os.RemoveAll(dataDir)

	processor, err := NewIncrementalProcessor(dataDir)
	if err != nil {
		t.Fatalf("NewIncrementalProcessor 실패: %v", err)
	}

	// 1개 배치만 테스트
	err = processor.StartNewBatch()
	if err != nil {
		t.Fatalf("StartNewBatch() 실패: %v", err)
	}

	// 작은 병합 시나리오
	// 예) [0,1], [2,3,4], [15,16], [5], ...
	sets := [][]uint64{
		{0, 1},
		{2, 3, 4},
		{15, 16},
		{7, 8, 9, 10}, // 4개
		{19},          // 단일 원소 (병합X)
	}

	for _, s := range sets {
		// batchOffset이 0이므로 그냥 그대로
		err := processor.ProcessSet(s)
		if err != nil {
			t.Errorf("ProcessSet(%v) 실패: %v", s, err)
		}
	}

	// 아카이브 병합 계산
	err = MergeArchives(processor)
	if err != nil {
		t.Errorf("mergeArchives 실패: %v", err)
	}

	// 결과 확인
	ds, dist, err := processor.CalculateGlobalState()
	if err != nil {
		t.Errorf("CalculateGlobalState 실패: %v", err)
	}
	t.Logf("서로소 집합 수: %d", ds)
	t.Logf("크기 분포 샘플: %v", dist)

	// [0,1] / [2,3,4] / [7,8,9,10] / [15,16] / [19], 그리고 합쳐지지 않은 나머지
	// 총 20개 중, 위 5개 그룹 + 나머지 9개(단일) = 14개의 disjoint set일 것(이상적인 경우)
	if ds != 14 {
		t.Errorf("기대하는 disjoint set 수=14, 실제=%d (병합 로직 이상?)", ds)
	}
}
