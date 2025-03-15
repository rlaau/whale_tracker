package database

import (
	"context"
	"log"
	"sync"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/option"
)

// ✅ BigQuery 인터페이스 정의
type PublicDataDB interface {
	GetTable(name string) *bigquery.Table
	Query(query string) *bigquery.Query
	IsMock() bool
}

// ✅ 싱글톤 패턴으로 BigQuery 클라이언트 생성
var (
	pblcDataOnce sync.Once
	pblcInstance *PublicDataClient
)

// ✅ BigQuery 클라이언트 구조체
type PublicDataClient struct {
	client *bigquery.Client
}

// ✅ 싱글톤 인스턴스 반환
func GetPublicDataInstance(projectID, credentialsPath string) PublicDataDB {
	pblcDataOnce.Do(func() {
		ctx := context.Background()
		client, err := bigquery.NewClient(ctx, projectID, option.WithCredentialsFile(credentialsPath))
		if err != nil {
			log.Fatalf("Failed to create BigQuery client: %v", err)
		}
		pblcInstance = &PublicDataClient{client: client}
	})
	return pblcInstance
}

func (b *PublicDataClient) IsMock() bool {
	return false
}

// ✅ BigQuery 테이블 반환
func (b *PublicDataClient) GetTable(name string) *bigquery.Table {
	return b.client.Dataset("blockchain").Table(name)
}

// ✅ BigQuery 쿼리 실행 (모킹 가능)
func (b *PublicDataClient) Query(query string) *bigquery.Query {
	return b.client.Query(query)
}
