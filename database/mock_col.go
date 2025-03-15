package database

import (
	"context"
	"errors"
	"sync"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// ✅ Mock 컬렉션 구조체 (MongoDB와 동일한 인터페이스를 유지)
type MockCollection struct {
	data []bson.M
	mu   sync.Mutex
}

// ✅ MockCollection의 Find() 메서드 구현 (MongoDB와 동일한 인터페이스 유지)
func (mc *MockCollection) Find(ctx context.Context, filter interface{}, opts ...*options.FindOptions) (*MockCursor, error) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	filterMap, ok := filter.(bson.M)
	if !ok {
		return nil, errors.New("invalid filter type")
	}

	var results []bson.M
	for _, doc := range mc.data {
		match := true
		for key, value := range filterMap {
			if doc[key] != value {
				match = false
				break
			}
		}
		if match {
			results = append(results, doc)
		}
	}

	// 🚨 MongoDB의 Find()는 Cursor를 반환하지만, 여기서는 데이터를 한 번에 반환
	return NewMockCursor(results), nil
}

// ✅ Mock 데이터 삽입
func (mc *MockCollection) InsertMany(ctx context.Context, docs []interface{}) (*mongo.InsertManyResult, error) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	for _, doc := range docs {
		if bsonDoc, ok := doc.(bson.M); ok {
			mc.data = append(mc.data, bsonDoc)
		} else {
			return nil, errors.New("invalid document format")
		}
	}
	return &mongo.InsertManyResult{}, nil
}

// ✅ MockCollection → mongo.Cursor를 흉내 내는 구조체
type MockCursor struct {
	data  []bson.M
	index int
}

// ✅ MongoDB Cursor 인터페이스를 따르도록 구현
func NewMockCursor(data []bson.M) *MockCursor {
	return &MockCursor{data: data, index: -1} // 시작 인덱스 조정
}

func (c *MockCursor) Next(ctx context.Context) bool {
	if c.index+1 < len(c.data) {
		c.index++
		return true
	}
	return false
}

func (c *MockCursor) Decode(val interface{}) error {
	if c.index >= len(c.data) || c.index < 0 {
		return errors.New("out of range")
	}
	*val.(*bson.M) = c.data[c.index]
	return nil
}

func (c *MockCursor) Close(ctx context.Context) error {
	return nil
}

func (c *MockCursor) All(ctx context.Context, results interface{}) error {
	if bsonResults, ok := results.(*[]bson.M); ok {
		*bsonResults = c.data
		return nil
	}
	return errors.New("invalid result type")
}
