//go:build !windows && erigon

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
package mdbx_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/uncommoncorrelation/go-mdbx-db/kv"
	"github.com/uncommoncorrelation/go-mdbx-db/kv/mdbx"
	"github.com/uncommoncorrelation/go-mdbx-db/kv/memdb"
)

func TestBucketCRUD(t *testing.T) {
	require := require.New(t)
	db, tx := memdb.NewTestTx(t, kv.ChaindataTablesCfg)

	normalBucket := kv.ChaindataTables[15]
	deprecatedBucket := kv.ChaindataDeprecatedTables[0]
	migrator := tx

	// check thad buckets have unique DBI's
	uniquness := map[kv.DBI]bool{}
	castedKv, ok := db.(*mdbx.MdbxKV)
	if !ok {
		t.Skip()
	}
	for _, dbi := range castedKv.AllDBI() {
		if dbi == mdbx.NonExistingDBI {
			continue
		}
		_, ok := uniquness[dbi]
		require.False(ok)
		uniquness[dbi] = true
	}

	require.True(migrator.ExistsBucket(normalBucket))
	require.True(errors.Is(migrator.DropBucket(normalBucket), kv.ErrAttemptToDeleteNonDeprecatedBucket))

	require.False(migrator.ExistsBucket(deprecatedBucket))
	require.NoError(migrator.CreateBucket(deprecatedBucket))
	require.True(migrator.ExistsBucket(deprecatedBucket))

	require.NoError(migrator.DropBucket(deprecatedBucket))
	require.False(migrator.ExistsBucket(deprecatedBucket))

	require.NoError(migrator.CreateBucket(deprecatedBucket))
	require.True(migrator.ExistsBucket(deprecatedBucket))

	c, err := tx.RwCursor(deprecatedBucket)
	require.NoError(err)
	err = c.Put([]byte{1}, []byte{1})
	require.NoError(err)
	v, err := tx.GetOne(deprecatedBucket, []byte{1})
	require.NoError(err)
	require.Equal([]byte{1}, v)

	buckets, err := migrator.ListBuckets()
	require.NoError(err)
	require.True(len(buckets) > 10)

	// check thad buckets have unique DBI's
	uniquness = map[kv.DBI]bool{}
	for _, dbi := range castedKv.AllDBI() {
		if dbi == mdbx.NonExistingDBI {
			continue
		}
		_, ok := uniquness[dbi]
		require.False(ok)
		uniquness[dbi] = true
	}
}
