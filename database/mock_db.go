package database

import (
	"sync"

	"go.mongodb.org/mongo-driver/bson"
)

// ✅ MockMongoClient: 테스트용 MongoDB Mock
type MockMongoClient struct {
	collections map[string]*MockCollection // 🔹 Mock 컬렉션 저장소
	mu          sync.Mutex
}

// ✅ MockMongoClient 생성자
func NewMockMongoClient() *MockMongoClient {
	return &MockMongoClient{
		collections: make(map[string]*MockCollection),
	}
}

// ✅ Mock 컬렉션 가져오기
func (m *MockMongoClient) GetCollection(name string) *MockCollection {
	m.mu.Lock()
	defer m.mu.Unlock()

	// ✅ 존재하지 않으면 새로 생성
	if _, exists := m.collections[name]; !exists {
		m.collections[name] = &MockCollection{data: []bson.M{}}
	}

	// 🚀 MockCollection을 반환 (실제 MongoDB 컬렉션처럼 동작)
	return m.collections[name]
}
