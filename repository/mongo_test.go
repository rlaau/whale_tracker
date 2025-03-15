package repository

import (
	"context"
	"testing"
	"whale_tracker/database"

	"go.mongodb.org/mongo-driver/bson"
)

// ✅ 트랜잭션 조회 테스트
func TestFindRecentHighValueTransactions(t *testing.T) {
	mockDB := &database.MockMongoClient{} // 🚀 Mock DB 사용
	repo := NewTransactionRepository(mockDB)

	// Mock에서는 실제 데이터가 없으므로 nil인지 체크
	_, err := repo.FindTransactions(context.TODO(), bson.M{}, 10)
	if err == nil {
		t.Errorf("Mock에서는 데이터가 없어야 함")
	}
}
