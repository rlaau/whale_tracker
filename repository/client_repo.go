package repository

import (
	"context"
	"log"
	"whale_tracker/primitives"

	"encoding/hex"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
)

type TxRaw struct {
	From primitives.Address
	To   primitives.Address
}

func FetchTransactions(ctx context.Context, client *bigquery.Client) ([]TxRaw, error) {
	query := `
	SELECT from_address, to_address
	FROM ` + "`bigquery-public-data.crypto_ethereum.transactions`" + `
	WHERE block_timestamp >= "2023-01-01"
	ORDER BY to_address ASC -- 클러스터링-정렬
	LIMIT 100000000
	`

	it, err := client.Query(query).Read(ctx)
	if err != nil {
		return nil, err
	}

	var transactions []TxRaw
	count := 0

	for {
		var row struct {
			From string `bigquery:"from_address"`
			To   string `bigquery:"to_address"`
		}
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Printf("에러: %v", err)
			continue
		}

		transactions = append(transactions, TxRaw{
			From: decodeHexAddress(row.From),
			To:   decodeHexAddress(row.To),
		})
		count++
		if count%1000000 == 0 {
			log.Printf("불러온 트랜잭션 수: %d\n", count)
		}
	}

	log.Printf("총 불러온 트랜잭션 수: %d\n", count)
	return transactions, nil
}

func decodeHexAddress(addr string) primitives.Address {
	var result primitives.Address
	b, err := hex.DecodeString(addr[2:])
	if err != nil || len(b) != 20 {
		return result
	}
	copy(result[:], b)
	return result
}
