package adapters

import (
	"context"
	"whale_tracker/primitives"
)

type TransactionStorage interface {
	InsertTransaction(ctx context.Context, tx primitives.Transaction) error
	FindTransaction(ctx context.Context, txID primitives.TxId) (primitives.Transaction, error)
}
