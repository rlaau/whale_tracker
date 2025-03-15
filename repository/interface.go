package repository

import (
	"context"
	"whale_tracker/primitives"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// ✅ MongoDB 트랜잭션 저장소 인터페이스
type TransactionRepository interface {
	InsertTransaction(ctx context.Context, tx primitives.Transaction) error
	InsertManyTransactions(ctx context.Context, txs []primitives.Transaction, batchSize int) error
	FindTransactions(ctx context.Context, filter bson.M, limit int) ([]primitives.Transaction, error)
	UpdateTransactions(ctx context.Context, filter bson.M, update bson.M) error
	BulkUpdateTransactions(ctx context.Context, updates []mongo.WriteModel) error
	AggregateTransactions(ctx context.Context, pipeline mongo.Pipeline) ([]bson.M, error)
}
