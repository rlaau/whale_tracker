package main

import (
	"fmt"
	"os"
	"time"
)

// 메인 함수 - 대규모 Union-Find 테스트 수행
func main() {
	startTime := time.Now()

	// 파일 경로 및 최대 원소 수 설정
	dataFile := "large_disjoint_set.bin"

	inMemoryMode := true // 메모리 우선 모드 활성화

	// 파일이 이미 존재하면 삭제 (새로 시작)
	if _, err := os.Stat(dataFile); err == nil {
		fmt.Println("기존 파일을 삭제합니다...")
		os.Remove(dataFile)
	}

	// MmapDisjointSet 인스턴스 생성
	fmt.Println("Disjoint Set 초기화 중...")
	ds, err := NewMmapDisjointSet(dataFile, MaxElement, inMemoryMode)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	defer ds.Close() // 리소스 정리

	fmt.Println("초기화 완료:", time.Since(startTime))

	// 첫 번째 단계: 2억 개 원소 처리
	processPhase(ds, 1)

	// 두 번째 단계: 추가 2억 개 원소 처리
	processPhase(ds, 2)

	// 최종 통계 정보 출력
	fmt.Println("\n=== 최종 통계 정보 ===")
	stats := ds.GetStats()
	for k, v := range stats {
		fmt.Printf("%s: %v\n", k, v)
	}

	fmt.Printf("\n총 실행 시간: %v\n", time.Since(startTime))
}
