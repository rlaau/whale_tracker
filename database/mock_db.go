package database

import (
	"path/filepath"
	"sync"

	"cloud.google.com/go/bigquery"
	"github.com/goccy/bigquery-emulator/server"
	bigquery_mock "github.com/yuseferi/bigquery-mock"
	"go.mongodb.org/mongo-driver/bson"
)

// ✅ BigQuery Mock 인터페이스 정의
type MockPublicDataDB struct {
	client *bigquery.Client
}

// ✅ MockPublicDataDB 생성자 (YAML 파일을 로드하여 BigQuery 모킹)
func NewMockPublicDataDB(yamlPath string) (*MockPublicDataDB, error) {
	bqClient, err := bigquery_mock.MockBigQuery("test_project", server.YAMLSource(filepath.Join(yamlPath)))
	if err != nil {
		return nil, err
	}
	return &MockPublicDataDB{client: bqClient}, nil
}
func (m *MockPublicDataDB) IsMock() bool {
	return true
}

// ✅ `GetTable()` 모킹
func (m *MockPublicDataDB) GetTable(name string) *bigquery.Table {
	return m.client.Dataset("blockchain").Table(name)
}

// ✅ `Query()` 모킹 (BigQuery 쿼리 실행)
func (m *MockPublicDataDB) Query(query string) *bigquery.Query {
	return m.client.Query(query)
}

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
