package database

import (
	"context"
	"log"
	"sync"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/option"
)

var (
	clientOnce sync.Once
	clientInst *bigquery.Client
)

func GetBigQueryClient(ctx context.Context, projectID, credentialsPath string) *bigquery.Client {
	clientOnce.Do(func() {
		client, err := bigquery.NewClient(ctx, projectID, option.WithCredentialsFile(credentialsPath))
		if err != nil {
			log.Fatalf("BigQuery client 생성 실패: %v", err)
		}
		clientInst = client
	})
	return clientInst
}
