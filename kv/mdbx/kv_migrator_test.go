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
	"context"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/require"

	"github.com/uncommoncorrelation/go-mdbx-db/kv"
	"github.com/uncommoncorrelation/go-mdbx-db/kv/mdbx"
	"github.com/uncommoncorrelation/go-mdbx-db/log"
)

func TestReadOnlyMode(t *testing.T) {
	path := t.TempDir()
	db1 := mdbx.NewMDBX(log.NewNoop()).Path(path).MapSize(16 * datasize.MB).WithTableCfg(kv.TableCfg{
		kv.Headers: kv.TableCfgItem{},
	}).MustOpen()
	db1.Close()
	time.Sleep(10 * time.Millisecond) // win sometime need time to close file

	db2 := mdbx.NewMDBX(log.NewNoop()).Readonly().Path(path).MapSize(16 * datasize.MB).WithTableCfg(kv.TableCfg{
		kv.Headers: kv.TableCfgItem{},
	}).MustOpen()
	defer db2.Close()

	tx, err := db2.BeginRo(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	c, err := tx.Cursor(kv.Headers)
	require.NoError(t, err)
	defer c.Close()
	_, _, err = c.Seek([]byte("some prefix"))
	require.NoError(t, err)
}
