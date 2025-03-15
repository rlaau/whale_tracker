package service

import (
	"context"
	"fmt"
	"log"

	"whale_tracker/repository"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// ✅ 최근 50개 트랜잭션 중 Value가 2 이상인 것 찾기
func FindHighValueTransactions(repo repository.TransactionRepository) {
	pipeline := mongo.Pipeline{
		{{Key: "$sort", Value: bson.D{{Key: "block_number", Value: -1}}}},                        // 최신 블록 순 정렬
		{{Key: "$limit", Value: 50}},                                                             // 최근 50개 가져오기
		{{Key: "$match", Value: bson.D{{Key: "value", Value: bson.D{{Key: "$gte", Value: 2}}}}}}, // value ≥ 2 필터
	}

	results, err := repo.AggregateTransactions(context.TODO(), pipeline)
	if err != nil {
		log.Fatal(err)
	}

	// ✅ 결과 출력
	for _, result := range results {
		fmt.Println(result)
	}
}
