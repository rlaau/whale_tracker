package adapters

import (
	"encoding/hex"
	"whale_tracker/primitives"
)

// ✅ MongoDB 저장용 구조체
type MongoTransaction struct {
	TxID        string `bson:"tx_id"`
	TxSyntax    string `bson:"tx_syntax"`
	BlockNumber uint64 `bson:"block_number"`
	From        string `bson:"from"`
	To          string `bson:"to"`
	Value       string `bson:"value"`
	GasLimit    string `bson:"gas_limit"`
	Input       string `bson:"input"`
}

// ✅ 공통 Transaction → MongoDB 변환
func NewMongoTransaction(tx primitives.Transaction) MongoTransaction {
	return MongoTransaction{
		TxID:        hex.EncodeToString(tx.TxID[:]), // ✅ 0x 제거한 상태로 저장
		TxSyntax:    tx.TxSyntax,
		BlockNumber: tx.BlockNumber,
		From:        hex.EncodeToString(tx.From[:]), // ✅ 0x 제거한 상태로 저장
		To:          hex.EncodeToString(tx.To[:]),   // ✅ 0x 제거한 상태로 저장
		Value:       tx.Value.String(),              // ✅ 문자열로 변환
		GasLimit:    tx.GasLimit.String(),           // ✅ 문자열로 변환
		Input:       tx.Input,
	}
}

// ✅ MongoDB → 공통 Transaction 변환
func (m *MongoTransaction) ToTransaction() primitives.Transaction {
	var tx primitives.Transaction
	tx.TxSyntax = m.TxSyntax
	tx.BlockNumber = m.BlockNumber
	tx.Input = m.Input

	// ✅ 바이트 변환 (hex 디코딩)
	hex.Decode(tx.TxID[:], []byte(m.TxID))
	hex.Decode(tx.From[:], []byte(m.From))
	hex.Decode(tx.To[:], []byte(m.To))

	// ✅ BigInt 변환 (10진수)
	tx.Value = primitives.NewBigInt("0")
	tx.Value.SetString(m.Value, 10)

	tx.GasLimit = primitives.NewBigInt("0")
	tx.GasLimit.SetString(m.GasLimit, 10)

	return tx
}
