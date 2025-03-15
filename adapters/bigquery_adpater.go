package adapters

import (
	"encoding/hex"
	"whale_tracker/primitives"
)

// ✅ BigQuery 저장용 구조체
type BigQueryTransaction struct {
	TxID        string `bigquery:"tx_id"`
	TxSyntax    string `bigquery:"tx_syntax"`
	BlockNumber uint64 `bigquery:"block_number"`
	From        string `bigquery:"from"`
	To          string `bigquery:"to"`
	Value       string `bigquery:"value"`
	GasLimit    string `bigquery:"gas_limit"`
	Input       string `bigquery:"input"`
}

// ✅ 공통 Transaction → BigQuery 변환
func NewBigQueryTransaction(tx primitives.Transaction) BigQueryTransaction {
	return BigQueryTransaction{
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

// ✅ BigQuery → 공통 Transaction 변환
func (b *BigQueryTransaction) ToTransaction() primitives.Transaction {
	var tx primitives.Transaction
	tx.TxSyntax = b.TxSyntax
	tx.BlockNumber = b.BlockNumber
	tx.Input = b.Input

	// ✅ 바이트 변환 (hex 디코딩)
	hex.Decode(tx.TxID[:], []byte(b.TxID))
	hex.Decode(tx.From[:], []byte(b.From))
	hex.Decode(tx.To[:], []byte(b.To))

	// ✅ BigInt 변환 (10진수)
	tx.Value = primitives.NewBigInt("0")
	tx.Value.SetString(b.Value, 10)

	tx.GasLimit = primitives.NewBigInt("0")
	tx.GasLimit.SetString(b.GasLimit, 10)

	return tx
}
