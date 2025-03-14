package primitives

type TxId [32]byte

type Transaction struct {
	TxID        TxId
	BlockNumber uint64
	From        Address
	To          Address
	Value       uint64
}
