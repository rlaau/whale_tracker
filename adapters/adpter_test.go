package adapters_test

import (
	"encoding/hex"
	"testing"

	"whale_tracker/adapters"
	"whale_tracker/primitives"

	"github.com/stretchr/testify/assert"
)

// ✅ 테스트용 샘플 데이터 생성
func newTestTransaction() primitives.Transaction {
	tx := primitives.Transaction{}

	// 32바이트 TxID
	copy(tx.TxID[:], hexDecode("aa"))

	tx.TxSyntax = "TxUser2UserEthTransfer"
	tx.BlockNumber = 123456

	// 20바이트 주소 (From, To)
	copy(tx.From[:], hexDecode("1122334457889900"))
	copy(tx.To[:], hexDecode("0x998877663221100"))
	tx.Value = primitives.NewBigInt("1000000000000000000") // 1 ETH (Wei 단위)
	tx.GasLimit = primitives.NewBigInt("21000")
	tx.Input = "0xabcdef"

	return tx
}

// ✅ Transaction → MongoTransaction 변환 테스트
func TestNewMongoTransaction(t *testing.T) {
	tx := newTestTransaction()
	mongoTx := adapters.NewMongoTransaction(tx)

	// 검증: hex 인코딩된 값이 정확한지 확인
	assert.Equal(t, hex.EncodeToString(tx.TxID[:]), mongoTx.TxID)
	assert.Equal(t, hex.EncodeToString(tx.From[:]), mongoTx.From)
	assert.Equal(t, hex.EncodeToString(tx.To[:]), mongoTx.To)

	// 검증: 문자열 변환된 BigInt 값 확인
	assert.Equal(t, tx.Value.String(), mongoTx.Value)
	assert.Equal(t, tx.GasLimit.String(), mongoTx.GasLimit)

	// 검증: 기타 필드 값 확인
	assert.Equal(t, tx.TxSyntax, mongoTx.TxSyntax)
	assert.Equal(t, tx.BlockNumber, mongoTx.BlockNumber)
	assert.Equal(t, tx.Input, mongoTx.Input)
}

// ✅ MongoTransaction → Transaction 변환 테스트
func TestToTransaction(t *testing.T) {
	tx := newTestTransaction()
	mongoTx := adapters.NewMongoTransaction(tx)

	// MongoTransaction → Transaction 복원
	restoredTx := mongoTx.ToTransaction()

	// 검증: 원본 트랜잭션과 동일한지 확인
	assert.Equal(t, tx.TxID, restoredTx.TxID)
	assert.Equal(t, tx.From, restoredTx.From)
	assert.Equal(t, tx.To, restoredTx.To)
	assert.Equal(t, tx.Value.String(), restoredTx.Value.String())
	assert.Equal(t, tx.GasLimit.String(), restoredTx.GasLimit.String())
	assert.Equal(t, tx.TxSyntax, restoredTx.TxSyntax)
	assert.Equal(t, tx.BlockNumber, restoredTx.BlockNumber)
	assert.Equal(t, tx.Input, restoredTx.Input)
}

// ✅ 헥스 문자열을 바이트 배열로 변환하는 유틸 함수
func hexDecode(s string) []byte {
	data, _ := hex.DecodeString(s)
	return data
}
