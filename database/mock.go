package database

import "go.mongodb.org/mongo-driver/mongo"

// ✅ 테스트용 Mock 구조체
type MockMongoClient struct{}

// ✅ Mock 구현
func (m *MockMongoClient) GetCollection(name string) *mongo.Collection {
	return nil // 🚀 테스트에서는 실제 DB를 사용하지 않음
}
