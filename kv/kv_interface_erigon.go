//go:build erigon

package kv

import (
	"github.com/uncommoncorrelation/go-mdbx-db/kv/iter"
	"github.com/uncommoncorrelation/go-mdbx-db/kv/order"
)

// ---- Temporal part

type (
	Domain      string
	History     string
	InvertedIdx string
)

type TemporalTx interface {
	Tx
	DomainGet(name Domain, k, k2 []byte) (v []byte, ok bool, err error)
	DomainGetAsOf(name Domain, k, k2 []byte, ts uint64) (v []byte, ok bool, err error)
	HistoryGet(name History, k []byte, ts uint64) (v []byte, ok bool, err error)

	// IndexRange - return iterator over range of inverted index for given key `k`
	// Asc semantic:  [from, to) AND from > to
	// Desc semantic: [from, to) AND from < to
	// Limit -1 means Unlimited
	// from -1, to -1 means unbounded (StartOfTable, EndOfTable)
	// Example: IndexRange("IndexName", 10, 5, order.Desc, -1)
	// Example: IndexRange("IndexName", -1, -1, order.Asc, 10)
	IndexRange(name InvertedIdx, k []byte, fromTs, toTs int, asc order.By, limit int) (timestamps iter.U64, err error)
	HistoryRange(name History, fromTs, toTs int, asc order.By, limit int) (it iter.KV, err error)
	DomainRange(name Domain, fromKey, toKey []byte, ts uint64, asc order.By, limit int) (it iter.KV, err error)
}
