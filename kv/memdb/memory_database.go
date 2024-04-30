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

package memdb

import (
	"context"
	"testing"

	"github.com/uncommoncorrelation/go-mdbx-db/kv"
	"github.com/uncommoncorrelation/go-mdbx-db/kv/mdbx"
	"github.com/uncommoncorrelation/go-mdbx-db/log"
)

func New(tmpDir string, tblConfig kv.TableCfg) kv.RwDB {
	return mdbx.NewMDBX(log.Noop()).InMem(tmpDir).WithTableCfg(tblConfig).MustOpen()
}

func NewTestDB(tb testing.TB, tblConfig kv.TableCfg) kv.RwDB {
	tb.Helper()
	tmpDir := tb.TempDir()
	tb.Helper()
	db := New(tmpDir, tblConfig)
	tb.Cleanup(db.Close)
	return db
}

func BeginRw(tb testing.TB, db kv.RwDB) kv.RwTx {
	tb.Helper()
	tx, err := db.BeginRw(context.Background())
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(tx.Rollback)
	return tx
}

func BeginRo(tb testing.TB, db kv.RoDB) kv.Tx {
	tb.Helper()
	tx, err := db.BeginRo(context.Background())
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(tx.Rollback)
	return tx
}

func NewTestTx(tb testing.TB, tblConfig kv.TableCfg) (kv.RwDB, kv.RwTx) {
	tb.Helper()
	tmpDir := tb.TempDir()
	db := New(tmpDir, tblConfig)
	tb.Cleanup(db.Close)
	tx, err := db.BeginRw(context.Background())
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(tx.Rollback)
	return db, tx
}
