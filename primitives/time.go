package primitives

// TimeFrame은 타임프레임.
// exlusive하게 작동
type TimeFrame struct {
	startTxId        TxId
	startBlockNumber uint64

	endTxId        TxId
	endBlockNumber uint64

	//*그 외의 여러 경제지표도 재미삼아? 넣어보기
}

const EntityClusterPeriodPerDay = 123                               //4개월
const ComputeStrategyPeriodPerWeek = EntityClusterPeriodPerDay / 41 // 3일

type TimeManager struct {
}
