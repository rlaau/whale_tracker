package database

import (
	"context"
	"log"
	"sync"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// ✅ MongoDB 인터페이스 (Mocking 가능)
type MongoDB interface {
	GetCollection(name string) *mongo.Collection
}

// 싱글톤 인스턴스 및 동기화 객체
var (
	mongoOnce     sync.Once
	mongoInstance *MongoClient
)

// ✅ 싱글톤 MongoDB 클라이언트 구조체
type MongoClient struct {
	client *mongo.Client
}

// ✅ 싱글톤 인스턴스 반환
func GetMongoInstance() MongoDB {
	mongoOnce.Do(func() {
		clientOptions := options.Client().ApplyURI("mongodb://localhost:27017")
		client, err := mongo.Connect(context.TODO(), clientOptions)
		if err != nil {
			log.Fatal(err)
		}
		mongoInstance = &MongoClient{client: client}
	})
	return mongoInstance
}

// ✅ 다중 컬렉션 지원 (트랜잭션, 유저, 엔티티 등)
func (m *MongoClient) GetCollection(name string) *mongo.Collection {
	return m.client.Database("blockchain").Collection(name)
}
