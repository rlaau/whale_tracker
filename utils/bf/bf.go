package main

import (
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/edsrzf/mmap-go"
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

// 사용자 집합 구조체
type UserSet struct {
	// 집합 크기
	size uint16
	// 집합 원소
	elements []uint64
}

// 점진적 유니온 파인드 프로세서
type IncrementalUnionFindProcessor struct {
	dataDir string // 데이터 디렉토리

	// 파일 핸들러
	parentFile *os.File
	rankFile   *os.File
	sizeFile   *os.File

	// mmap 영역 (영구적인 데이터)
	parentArchive mmap.MMap
	rankArchive   mmap.MMap
	sizeArchive   mmap.MMap
	// 현재 윈도우 데이터
	parentSlice []uint64
	rankSlice   []uint8
	sizeSlice   []uint16
	// 윈도우 정보
	windowStart uint64
	windowEnd   uint64
	// 블룸 필터
	//오탐률 0.5%정도, 해시함수 8개.
	totalBloomFilter *BloomFilter // 전체 원소
	multiBloomFilter *BloomFilter // 다중 원소 집합

	// 총 원소 수 및 파일 용량
	totalElements uint64
	fileCapacity  uint64
}

*참고사항 시작*
mmap시 해당 알고리즘은 기본적으로 다량의 임의접근을 필연적으로 하게 됨.
MADV_RANDOM: 임의 접근 패턴 힌트
때문에 초기화시 unix.Madvise(addr, uintptr(length), unix.MADV_RANDOM) 같은 패턴을 적절히 써주면 좋을 듯.
또한, 초기 파일의 크기는 40억개의 엔트리가 들어온다 가정하고 작성하면 될듯. 
*참고사항 끝*

func (i *IncrementalUnionFindProcessor) ProcessSets(userSets []UserSet) (error, []UserSet) {
	1. 해당 함수는, 집합의 리스트를 받아서, IncrementalUnionFindProcessor의 아카이브를 업데이트 한다.
	1. mulitSets이란 값에 i.ProccesSingleSets(userSets)를 대입한다.

}
func (i *IncrementalUnionFindProcessor) ProcessSingleSets(userSets []UserSet) (error, []UserSet) {
	1. 해당 함수는 단일 셋을 골라서 그것을 통해 ncrementalUnionFindProcessor의 아카이브를 업데이트 한다.
	1. multiSets, batchBuffer를 초기화한다.
	2. userSets를 순회한다.
	3. 만약 userSets의 userSet의 size가 2 이상이면 이를 multiSets에 어펜드한다.
	4. 만약 UserSets의의 userSet의 size가 1이면 ReflectSingleSet(userSet, batchBuffer)를 호출한다.i
	5. 순회를 끝낸다.
	6. batchBuffer를 순회하며, 다음 과정을 거친다.
	6-1 : i의 totalBloomfilter에서 batchBuffer값이 있는지 확인한다.
	6-2: 토탈 블룸필터가 있다고 나오면 우선 그 값을 "needToCheck"에 추가하며, 없다고 나오면 "needToBatch"에 추가한다. 
	6-3: "needToCheck"어레이에서, uint64값을 1억 단위 청크로 잘라서(0~99_999_999사이의 값을 한 청크로 하기), 1억 단위로 슬라이스해온 parentSlice값과 비교하여, 그 값이 이미 존재하는지 확인하다.
	6-3: 이는, parentArchive를 다 로드할 수 없으므로, 1억개씩 슬라이스 청크를 가져와서 확인하기 위함이다.
	6-3: 만약 진짜로 있다고 판명된 경우 스킵하고,실제론 없다고 판명된 경우 이는 needToBatch에 추가한다. 
	6-4: needToBatch어레이 역시, 그 값을 1억 단위 청크로 잘라서 순회한다. 각 요소는 1억짜리 parentSlice청크에 자기자신을 루트로 등록한다. 랭크 등도 같이 0으로 초기화한다다. 즉, 유니언파인드의 셋을 자기자신 셋으로 초기화하는 것이다.
	6-4: 이러한 배칭(1억개의 청크 기반)연산은 BatchSingleSet을 통해 수행한다.
	6-4: 이러한 배칭을 통해 페이지폴트를 1억번 마다 한번씩 일어나도록 줄일 수 있다.
	7. multeSets를 리턴한다. 
	7. 즉, 해당 함수를 통해 "단일 크기 집합"에 대한 처리를 완료한 후, 복합 크기 집합을 리턴하는 것이다.

}
func (i *IncrementalUnionFindProcessor) BatchSingleSet(batchBuffer []uint64) error {
	1. 해당 함수는 단일 크기 집합을 청크 단위로 IncrementalUnionFindProcessor의 아카이브에 업데이트한다.
	1. batchBuffer를 순회하면서 각 값을 totalBloomFilter에 추가한다.
	2: parsedByChunk 딕셔너리를 생성 후 ParseByChunk(batchBuffer)를 대입한다.
	3: 이후 parsedByChunk 딕셔너리를 순회하며 알맞은 슬라이스를 호출하여 parentSlice를 통해 parentArhive를업데이트 한다.
}

func ParseByChunck (elements []uint64) map[int][]uint64 {
	1. 해당 함수는, uint64배열을 받아서, 그 값을 1억을 기준으로 청킹한다.
	2. parsedByChunk맵을 초기화
	3. elements를 순회한다.
	4. 0~99_999_999사이의 값은 parsedByChunk[0]에 어펜드, 100_000_000~199_999_999 사이의 앖은 parsedByChunk[1]에 어펜드 하는으로 맵을 업데이트
	5. parsedBychunk를 리턴한다. 
}

func (i *IncrementalUnionFindProcessor) ProcessMultiSets(userSets []UserSet) (error, []UserSet) {
	1. 해당 함수는 크기가 2이상은 집합들을 IncrementalUnionFindProcessor의 아카이브에 반영한다.




	1. 일단 지들끼리 유니언파인드시키기
	2. 여기서 조금 특별한 구조 이용
	3. 이후 루트 딱지 때고, 앵커 개수 기반 업뎃이냐 머지냐 구분해서 또 시도하기
	4. 머지 시 히스토리 기록. (임시 버퍼)
	5. 히스토리뿐 아니라, 이후 mongoDB변환시엔 rootSlice가 유용할지도...
	6. 또한, 유니언파인드는 size도 기록해서 항상 1000체킹
	7. 일단 fuf읽기
	7. 이후 그걸 여기 기본구조, 알고리즘에 반영하기. 타당할 시에
	7. 특히 ReaderAt과 oneFile로 하는게 좀 괜찮게 보임. 물론 페이지폴트는 일어남.
	7. => 임시 유니언 파인드 뿐 아니라, 최종적으로 아카이브에서 꺼내오는 로직까지 마스터. 
	=> 병렬 ReadAt도 가능
	7. 캐시는 끄기
	8. 다만 그건 추후로 미루기.








	1. needToCheck ([]UserSet) 생성. 
	2. userSets를 us개체로 삼아 순회
	3. 순회문 내부 : us의 elements에서, elements의 el순회하면서 해당 el을 i의 muiltBloomfilter에 넣음.
	3-1: 순회문 내부: 만약 전부 없다고 나올 시-> 해당 set을 "즉시"(동기적으로 시퀀셜하게 수행되어야 함.) AddMultiSet처리.
	3-1: 순회문 내부: 만약 한 원소가 있다고 나온 경우, 그 원소가 진짜로 archive에 있는지 작은 슬라이스 가져와서 확인
	3-1: 순회문 내부: 만약 있다고 했는데 없으면 이 역시 AddMultiSet처리.
	3-1: 순회문 내부: 만약 진짜로 있는 경우, 동기적으로 MergeMultiSet처리.
	3-1: 순회문 내부: finally하게 해당 el을 i의 totalBloomFilter,muiltBloomfilter에 추가한다.


}
func (i *IncrementalUnionFindProcessor) AddMultiSet(userSet UserSet) error {
	1. 해당 함수는 다른 집합과 머지할 필요 없는, 닫힌 집합을 IncrementalUnionFindProcessor의 아카이브에 반영한다.
    1. userSet.elements 중 첫 원소를 루트원소로 잡아서 유니언한다.
	2. parentMap, rankMap을 만들어 element와 그 유니언, 랭크를 저장한다
	2. parsedByChunk 딕셔너리를 만들어 ParseByChunck(userSet.elements)를 대입한다.
	3. 해당 parseByChunk를 순회하며, parenMap,rankMap을 참고하여, 1억 단위로 받아온 슬라이스에 정보를 추가한다. 


}
func (i *IncrementalUnionFindProcessor) MergeMultiSet(userSet UserSet) error {}


// 받아온 멀티셋들을 임시로 클러스터링해두는 유니언파인드 구조체.

// DisjointSet는 분리 집합을 관리하는 유니온 파인드 구조체입니다.
type DisjointSet struct {
	parent map[uint64]uint64 // 각 원소의 부모를 저장
	rank   map[uint64]int    // 트리의 높이를 저장 (최적화용)
}

// NewDisjointSet은 새로운 DisjointSet 인스턴스를 생성합니다.
func NewDisjointSet() *DisjointSet {
	return &DisjointSet{
		parent: make(map[uint64]uint64),
		rank:   make(map[uint64]int),
	}
}

// MakeSet은 새로운 원소를 집합에 추가합니다.
func (ds *DisjointSet) MakeSet(x uint64) {
	if _, exists := ds.parent[x]; exists {
		return // 이미 존재하는 원소는 무시
	}

	ds.parent[x] = x // 자기 자신을 부모로 설정
	ds.rank[x] = 0   // 초기 랭크는 0
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
	} else {
		ds.parent[rootY] = rootX

		// 두 트리의 랭크가 같으면 결과 트리의 랭크를 증가
		if ds.rank[rootX] == ds.rank[rootY] {
			ds.rank[rootX]++
		}
	}
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

	for _, set := range sets {
		fmt.Printf("집합의 원소들: %v\n", set)
	}
}


// 테스트케이스 생성기
// 분포에 따라 모든 집합 생성
func GenerateAllSets(numSets uint64, rangeValue uint64) []UserSet {
	// 분포에 따른 집합 수 계산
	singleCount := int(float64(numSets) * 0.90)   // 90%
	twoCount := int(float64(numSets) * 0.07)      // 7%
	threeCount := int(float64(numSets) * 0.01)    // 1%
	fourCount := int(float64(numSets) * 0.005)    // 0.5%
	fiveCount := int(float64(numSets) * 0.0025)   // 0.25%
	sixCount := int(float64(numSets) * 0.0025)    // 0.25%
	sevenCount := int(float64(numSets) * 0.0025)  // 0.25%
	eightCount := int(float64(numSets) * 0.0025)  // 0.25%
	nineCount := int(float64(numSets) * 0.001)    // 0.1%
	tenCount := int(float64(numSets) * 0.001)     // 0.1%
	elevenCount := int(float64(numSets) * 0.0005) // 0.05%
	twelveCount := int(float64(numSets) * 0.0005) // 0.05%
	twentyCount := int(float64(numSets) * 0.0003) // 0.03%
	thirtyCount := int(float64(numSets) * 0.0001) // 0.01%
	fiftyCount := int(float64(numSets) * 0.0001)  // 0.01%

	// 결과 슬라이스
	allSets := make([]UserSet, 0, numSets)

	// 각 크기별 집합 생성
	fmt.Println("1개 원소 집합 생성 중...")
	allSets = append(allSets, GenerateSets(rangeValue, 1)[:singleCount]...)

	fmt.Println("2개 원소 집합 생성 중...")
	allSets = append(allSets, GenerateSets(rangeValue, 2)[:twoCount]...)

	fmt.Println("3개 원소 집합 생성 중...")
	allSets = append(allSets, GenerateSets(rangeValue, 3)[:threeCount]...)

	fmt.Println("4-12개 원소 집합 생성 중...")
	allSets = append(allSets, GenerateSets(rangeValue, 4)[:fourCount]...)
	allSets = append(allSets, GenerateSets(rangeValue, 5)[:fiveCount]...)
	allSets = append(allSets, GenerateSets(rangeValue, 6)[:sixCount]...)
	allSets = append(allSets, GenerateSets(rangeValue, 7)[:sevenCount]...)
	allSets = append(allSets, GenerateSets(rangeValue, 8)[:eightCount]...)
	allSets = append(allSets, GenerateSets(rangeValue, 9)[:nineCount]...)
	allSets = append(allSets, GenerateSets(rangeValue, 10)[:tenCount]...)
	allSets = append(allSets, GenerateSets(rangeValue, 11)[:elevenCount]...)
	allSets = append(allSets, GenerateSets(rangeValue, 12)[:twelveCount]...)

	fmt.Println("다수 원소 집합 생성 중...")
	allSets = append(allSets, GenerateSets(rangeValue, 20)[:twentyCount]...)
	allSets = append(allSets, GenerateSets(rangeValue, 30)[:thirtyCount]...)
	allSets = append(allSets, GenerateSets(rangeValue, 50)[:fiftyCount]...)

	// 랜덤하게 섞기
	fmt.Println("집합 섞는 중...")
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(allSets), func(i, j int) {
		allSets[i], allSets[j] = allSets[j], allSets[i]
	})

	fmt.Printf("총 %d개 집합 생성 완료\n", len(allSets))
	return allSets
}
