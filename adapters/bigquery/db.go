package bigquery

import (
	"context"
	"encoding/hex"

	"whale_tracker/adapters"
	"whale_tracker/primitives"

	"cloud.google.com/go/bigquery"
)

type BigqueryTransaction struct {
	primitives.Transaction
	TxID string `bigquery:"tx_id"`
	From string `bigquery:"from"`
	To   string `bigquery:"to"`
}

// BigQuery용 생성자
func NewBigqueryTransaction(tx primitives.Transaction) BigqueryTransaction {
	return BigqueryTransaction{
		Transaction: tx,
		TxID:        hex.EncodeToString(tx.TxID[:]),
		From:        hex.EncodeToString(tx.From[:]),
		To:          hex.EncodeToString(tx.To[:]),
	}
}

type BigqueryTransactionStorage struct {
	client    *bigquery.Client
	tableName string
}

func NewBigqueryTransactionStorage(client *bigquery.Client, tableName string) adapters.TransactionStorage {
	return &BigqueryTransactionStorage{client: client, tableName: tableName}
}

func (bts *BigqueryTransactionStorage) InsertTransaction(ctx context.Context, tx primitives.Transaction) error {
	bqTx := NewBigqueryTransaction(tx)
	inserter := bts.client.Dataset("dataset_name").Table(bts.tableName).Inserter()
	return inserter.Put(ctx, bqTx)
}

func (bts *BigqueryTransactionStorage) FindTransaction(ctx context.Context, txID primitives.TxId) (primitives.Transaction, error) {
	// BigQuery는 주로 분석용이라 단건조회는 비용이 높음.
	// 추후 findByCond등 만들어서 배칭으로 크게하기
	return primitives.Transaction{}, nil
}
