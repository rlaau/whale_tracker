package primitives

// TimeFrame은 타임프레임.
// exlusive하게 작동
type TimeFrame struct {
	startTxId TxId
	endTxId   TxId
}

const EntityClusterPeriodPerWeek = 12
const ComputeStrategyPeriodPerWeek = 2

type TimeManager struct {
}

