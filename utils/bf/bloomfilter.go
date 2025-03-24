package main

import (
	"fmt"
	"math"
)

// 블룸 필터 구조체
type BloomFilter struct {
	// Bloom Filter 크기
	size uint64
	// Bloom Filter 배열
	bf []uint64
	// 해시 함수 개수
	hashFunctions uint64
}

// 블룸 필터 생성 함수 - 메모리 사용량 최대 2-4GB 허용
func NewBloomFilter(expectedElements uint64, falsePositiveRate float64) *BloomFilter {
	// 블룸 필터 적정 크기 계산 (m = -n*ln(p)/(ln(2)^2))
	// 여기서 n은 예상 요소 수, p는 허용 오탐률
	size := uint64(float64(expectedElements) * (-1.0) * math.Log(falsePositiveRate) / (math.Log(2) * math.Log(2)))

	// 크기 제한 (2-4GB 사이로 조정)
	const minSize = 2 * 1024 * 1024 * 1024 * 8 // 2GB (비트 단위)
	const maxSize = 4 * 1024 * 1024 * 1024 * 8 // 4GB (비트 단위)

	if size < minSize {
		size = minSize
	}
	if size > maxSize {
		size = maxSize
	}

	// 최적 해시 함수 개수 계산 (k = m/n*ln(2))
	hashFunctions := uint64(float64(size) / float64(expectedElements) * math.Log(2))
	if hashFunctions < 8 {
		hashFunctions = 8 // 최소 8개
	}
	if hashFunctions > 16 {
		hashFunctions = 16 // 최대 16개
	}

	// 사이즈를 64bit 단위로 올림 정렬
	arraySize := (size + 63) / 64

	fmt.Printf("블룸 필터 생성: 크기 %.2f GB, 해시 함수 %d개, 예상 오탐률 %.4f%%\n",
		float64(arraySize*8)/(1024*1024*1024), hashFunctions, falsePositiveRate*100)

	return &BloomFilter{
		size:          size,
		bf:            make([]uint64, arraySize),
		hashFunctions: hashFunctions,
	}
}

// 블룸 필터에 요소 추가
func (bf *BloomFilter) Add(element uint64) {
	for i := uint64(0); i < bf.hashFunctions; i++ {
		// 다양한 해시값 생성을 위한 두 개의 서로 다른 해시 함수 사용
		// FNV-1a와 유사한 해시 알고리즘 사용
		h1 := element
		h1 = (h1 ^ (h1 >> 30)) * 0xbf58476d1ce4e5b9
		h1 = (h1 ^ (h1 >> 27)) * 0x94d049bb133111eb
		h1 = h1 ^ (h1 >> 31)

		// 또 다른 해시 방식
		h2 := element
		h2 = ((h2 >> 16) ^ h2) * 0x45d9f3b
		h2 = ((h2 >> 16) ^ h2) * 0x45d9f3b
		h2 = (h2 >> 16) ^ h2

		// 두 해시를 조합해 i번째 해시 생성
		hash := (h1 + i*h2) % bf.size
		index := hash / 64
		bit := hash % 64

		bf.bf[index] |= (1 << bit)
	}
}

// 블룸 필터에 요소 있는지 확인
func (bf *BloomFilter) MayContain(element uint64) bool {
	for i := uint64(0); i < bf.hashFunctions; i++ {
		// 추가 시와 동일한 해시 알고리즘 사용
		h1 := element
		h1 = (h1 ^ (h1 >> 30)) * 0xbf58476d1ce4e5b9
		h1 = (h1 ^ (h1 >> 27)) * 0x94d049bb133111eb
		h1 = h1 ^ (h1 >> 31)

		h2 := element
		h2 = ((h2 >> 16) ^ h2) * 0x45d9f3b
		h2 = ((h2 >> 16) ^ h2) * 0x45d9f3b
		h2 = (h2 >> 16) ^ h2

		hash := (h1 + i*h2) % bf.size
		index := hash / 64
		bit := hash % 64

		if (bf.bf[index] & (1 << bit)) == 0 {
			return false
		}
	}
	return true
}
