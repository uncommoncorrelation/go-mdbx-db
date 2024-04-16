//go:build erigon

/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package iter_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/uncommoncorrelation/go-mdbx-db/kv"
	"github.com/uncommoncorrelation/go-mdbx-db/kv/iter"
	"github.com/uncommoncorrelation/go-mdbx-db/kv/memdb"
)

func TestUnionPairs(t *testing.T) {
	db := memdb.NewTestDB(t, kv.ChaindataTablesCfg)
	ctx := context.Background()
	t.Run("simple", func(t *testing.T) {
		require := require.New(t)
		tx, _ := db.BeginRw(ctx)
		defer tx.Rollback()
		_ = tx.Put(kv.E2AccountsHistory, []byte{1}, []byte{1})
		_ = tx.Put(kv.E2AccountsHistory, []byte{3}, []byte{1})
		_ = tx.Put(kv.E2AccountsHistory, []byte{4}, []byte{1})
		_ = tx.Put(kv.PlainState, []byte{2}, []byte{9})
		_ = tx.Put(kv.PlainState, []byte{3}, []byte{9})
		it, _ := tx.Range(kv.E2AccountsHistory, nil, nil)
		it2, _ := tx.Range(kv.PlainState, nil, nil)
		keys, values, err := iter.ToKVArray(iter.UnionKV(it, it2, -1))
		require.NoError(err)
		require.Equal([][]byte{{1}, {2}, {3}, {4}}, keys)
		require.Equal([][]byte{{1}, {9}, {1}, {1}}, values)
	})
	t.Run("empty 1st", func(t *testing.T) {
		require := require.New(t)
		tx, _ := db.BeginRw(ctx)
		defer tx.Rollback()
		_ = tx.Put(kv.PlainState, []byte{2}, []byte{9})
		_ = tx.Put(kv.PlainState, []byte{3}, []byte{9})
		it, _ := tx.Range(kv.E2AccountsHistory, nil, nil)
		it2, _ := tx.Range(kv.PlainState, nil, nil)
		keys, _, err := iter.ToKVArray(iter.UnionKV(it, it2, -1))
		require.NoError(err)
		require.Equal([][]byte{{2}, {3}}, keys)
	})
	t.Run("empty 2nd", func(t *testing.T) {
		require := require.New(t)
		tx, _ := db.BeginRw(ctx)
		defer tx.Rollback()
		_ = tx.Put(kv.E2AccountsHistory, []byte{1}, []byte{1})
		_ = tx.Put(kv.E2AccountsHistory, []byte{3}, []byte{1})
		_ = tx.Put(kv.E2AccountsHistory, []byte{4}, []byte{1})
		it, _ := tx.Range(kv.E2AccountsHistory, nil, nil)
		it2, _ := tx.Range(kv.PlainState, nil, nil)
		keys, _, err := iter.ToKVArray(iter.UnionKV(it, it2, -1))
		require.NoError(err)
		require.Equal([][]byte{{1}, {3}, {4}}, keys)
	})
	t.Run("empty both", func(t *testing.T) {
		require := require.New(t)
		tx, _ := db.BeginRw(ctx)
		defer tx.Rollback()
		it, _ := tx.Range(kv.E2AccountsHistory, nil, nil)
		it2, _ := tx.Range(kv.PlainState, nil, nil)
		m := iter.UnionKV(it, it2, -1)
		require.False(m.HasNext())
	})
	t.Run("error handling", func(t *testing.T) {
		require := require.New(t)
		tx, _ := db.BeginRw(ctx)
		defer tx.Rollback()
		it := iter.PairsWithError(10)
		it2 := iter.PairsWithError(12)
		keys, _, err := iter.ToKVArray(iter.UnionKV(it, it2, -1))
		require.Equal("expected error at iteration: 10", err.Error())
		require.Equal(10, len(keys))
	})
}
