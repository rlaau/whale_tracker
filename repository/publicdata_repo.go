package repository

import (
	"context"
	"log"

	"whale_tracker/adapters"
	"whale_tracker/database"
	"whale_tracker/primitives"

	"google.golang.org/api/iterator"
)

// ✅ BigQuery 퍼블릭 데이터 트랜잭션 저장소
type PublicDataTransactionRepository struct {
	db database.PublicDataDB
}

// ✅ 저장소 인스턴스 생성
func NewPublicDataTransactionRepository(db database.PublicDataDB) *PublicDataTransactionRepository {
	return &PublicDataTransactionRepository{db: db}
}

// ✅ 최신 100개 트랜잭션 중 `value >= 1000` 인 것만 필터링
func (r *PublicDataTransactionRepository) GetLatestTransactions(ctx context.Context) ([]primitives.Transaction, error) {
	// ✅ 모킹 환경에서는 테스트 데이터셋 사용
	tableName := "bigquery-public-data.crypto_ethereum.transactions"
	if r.db.IsMock() {
		tableName = "test_project.blockchain.transactions"
	}

	query := `
		SELECT 
			transaction_hash AS tx_id, 
			block_number, 
			from_address AS from_address, 
			to_address AS to_address, 
			value, 
			gas_limit, 
			input
		FROM ` + "`" + tableName + "`" + `
		ORDER BY block_number DESC 
		LIMIT 100
	`

	// ✅ BigQuery 쿼리 실행
	q := r.db.Query(query)
	it, err := q.Read(ctx)
	if err != nil {
		log.Printf("[ERROR] BigQuery Query Execution Error: %v", err)
		return nil, err
	}

	// ✅ 결과 변환
	var transactions []primitives.Transaction
	for {
		var row adapters.PublicDataTransaction

		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Printf("[ERROR] Query Result Parsing Error: %v", err)
			return nil, err
		}

		// ✅ `value >= 1000` 필터링
		val := primitives.NewBigInt(row.Value)
		if val.Cmp(primitives.NewBigInt("1000")) >= 0 {
			tx := row.ToTransaction()
			transactions = append(transactions, tx)
		}
	}

	log.Printf("[INFO] Retrieved %d transactions (value >= 1000)", len(transactions))
	return transactions, nil
}
