package repository

import (
	"context"
	"log"
	"sync"

	"whale_tracker/adapters"
	"whale_tracker/database"
	"whale_tracker/primitives"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// ✅ MongoDB 트랜잭션 저장소 구조체
type MongoTransactionRepository struct {
	Collection *mongo.Collection
}

// ✅ 트랜잭션 저장소 인스턴스 생성
func NewTransactionRepository(db database.MongoDB) TransactionRepository {
	return &MongoTransactionRepository{Collection: db.GetCollection("transactions")}
}

// ✅ InsertOne: 단일 트랜잭션 저장
func (r *MongoTransactionRepository) InsertTransaction(ctx context.Context, tx primitives.Transaction) error {
	mongoTx := adapters.NewMongoTransaction(tx) // ✅ 변환 적용
	_, err := r.Collection.InsertOne(ctx, mongoTx)
	return err
}

// ✅ InsertMany: 병렬 배칭 삽입 (배치 크기 지정 가능)
// ✅ InsertMany: 병렬 배칭 삽입 (어댑터 적용)
func (r *MongoTransactionRepository) InsertManyTransactions(ctx context.Context, txs []primitives.Transaction, batchSize int) error {
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, 10) // 동시 실행 제한 (최대 10개 고루틴)

	for i := 0; i < len(txs); i += batchSize {
		wg.Add(1)
		go func(start int) {
			defer wg.Done()
			semaphore <- struct{}{}

			end := start + batchSize
			if end > len(txs) {
				end = len(txs)
			}

			// ✅ primitives.Transaction → adapters.MongoTransaction 변환
			var mongoTxs []interface{} // InsertMany는 []interface{} 필요
			for _, tx := range txs[start:end] {
				mongoTxs = append(mongoTxs, adapters.NewMongoTransaction(tx))
			}

			_, err := r.Collection.InsertMany(ctx, mongoTxs, options.InsertMany().SetOrdered(false))
			if err != nil {
				log.Printf("Batch Insert Error: %v", err)
			}

			<-semaphore // 고루틴 해제
		}(i)
	}

	wg.Wait()
	return nil
}

// ✅ FindMany: 조건 기반 트랜잭션 검색 (어댑터 적용)
func (r *MongoTransactionRepository) FindTransactions(ctx context.Context, filter bson.M, limit int) ([]primitives.Transaction, error) {
	opts := options.Find().SetLimit(int64(limit))
	cursor, err := r.Collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	// ✅ MongoDB에서 가져온 데이터는 `MongoTransaction` 형태로 변환해야 함
	var mongoTxs []adapters.MongoTransaction
	if err := cursor.All(ctx, &mongoTxs); err != nil {
		return nil, err
	}

	// ✅ adapters.MongoTransaction → primitives.Transaction 변환
	var transactions []primitives.Transaction
	for _, mtx := range mongoTxs {
		transactions = append(transactions, mtx.ToTransaction())
	}

	return transactions, nil
}

// ✅ UpdateMany: 대량 업데이트 (MongoDB 저장 형식 유지)
func (r *MongoTransactionRepository) UpdateTransactions(ctx context.Context, filter bson.M, update bson.M) error {
	_, err := r.Collection.UpdateMany(ctx, filter, bson.M{"$set": update})
	return err
}

// ✅ BulkUpdate: 대규모 병렬 업데이트 (MongoDB 저장 형식 유지)
func (r *MongoTransactionRepository) BulkUpdateTransactions(ctx context.Context, updates []mongo.WriteModel) error {
	opts := options.BulkWrite().SetOrdered(false) // 비순차 실행
	_, err := r.Collection.BulkWrite(ctx, updates, opts)
	return err
}

// ✅ Aggregate: 복잡한 쿼리 처리 (MongoDB 저장 형식 유지)
func (r *MongoTransactionRepository) AggregateTransactions(ctx context.Context, pipeline mongo.Pipeline) ([]bson.M, error) {
	cursor, err := r.Collection.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var results []bson.M
	if err := cursor.All(ctx, &results); err != nil {
		return nil, err
	}
	return results, nil
}
