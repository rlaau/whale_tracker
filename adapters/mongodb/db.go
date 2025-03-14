package mongodb

import (
	"context"
	"encoding/hex"

	"whale_tracker/adapters"
	"whale_tracker/primitives"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type MongoTransaction struct {
	primitives.Transaction `bson:"inline"` // 원본 타입의 필드들 그대로 임베딩 (inline으로 풀어줌)
	TxID                   string          `bson:"tx_id"` // string으로 저장하기 위한 필드
	From                   string          `bson:"from"`
	To                     string          `bson:"to"`
}

// 생성자 함수로 변환 로직 작성
func NewMongoTransaction(tx primitives.Transaction) MongoTransaction {
	return MongoTransaction{
		Transaction: tx,
		TxID:        hex.EncodeToString(tx.TxID[:]),
		From:        hex.EncodeToString(tx.From[:]),
		To:          hex.EncodeToString(tx.To[:]),
	}
}

type MongoTransactionStorage struct {
	collection *mongo.Collection
}

func NewMongoTransactionStorage(col *mongo.Collection) adapters.TransactionStorage {
	return &MongoTransactionStorage{collection: col}
}

func (mts *MongoTransactionStorage) InsertTransaction(ctx context.Context, tx primitives.Transaction) error {
	mongoTx := NewMongoTransaction(tx)
	_, err := mts.collection.InsertOne(ctx, mongoTx)
	return err
}

func (mts *MongoTransactionStorage) FindTransaction(ctx context.Context, txID primitives.TxId) (primitives.Transaction, error) {
	var mongoTx MongoTransaction
	idStr := hex.EncodeToString(txID[:])
	err := mts.collection.FindOne(ctx, bson.M{"tx_id": idStr}).Decode(&mongoTx)
	if err != nil {
		return primitives.Transaction{}, err
	}
	return mongoTx.Transaction, nil
}
