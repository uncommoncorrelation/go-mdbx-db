//go:build erigon

package mdbx

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/uncommoncorrelation/go-mdbx-db/kv"
)

func baseAutoConversion(t *testing.T) (kv.RwDB, kv.RwTx, kv.RwCursor) {
	t.Helper()
	path := t.TempDir()
	logger := log.New()
	db := NewMDBX(logger).InMem(path).WithTableCfg(kv.ChaindataTablesCfg).MustOpen()

	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)

	c, err := tx.RwCursor(kv.PlainState)
	require.NoError(t, err)

	// Insert some records
	require.NoError(t, c.Put([]byte("A"), []byte("0")))
	require.NoError(t, c.Put([]byte("A..........................._______________________________A"), []byte("1")))
	require.NoError(t, c.Put([]byte("A..........................._______________________________C"), []byte("2")))
	require.NoError(t, c.Put([]byte("B"), []byte("8")))
	require.NoError(t, c.Put([]byte("C"), []byte("9")))
	require.NoError(t, c.Put([]byte("D..........................._______________________________A"), []byte("3")))
	require.NoError(t, c.Put([]byte("D..........................._______________________________C"), []byte("4")))

	return db, tx, c
}

func TestAutoConversion(t *testing.T) {
	db, tx, c := baseAutoConversion(t)
	defer db.Close()
	defer tx.Rollback()
	defer c.Close()

	// key length conflict
	require.Error(t, c.Put([]byte("A..........................."), []byte("?")))

	require.NoError(t, c.Delete([]byte("A..........................._______________________________A")))
	require.NoError(t, c.Put([]byte("B"), []byte("7")))
	require.NoError(t, c.Delete([]byte("C")))
	require.NoError(t, c.Put([]byte("D..........................._______________________________C"), []byte("6")))
	require.NoError(t, c.Put([]byte("D..........................._______________________________E"), []byte("5")))

	k, v, err := c.First()
	require.NoError(t, err)
	assert.Equal(t, []byte("A"), k)
	assert.Equal(t, []byte("0"), v)

	k, v, err = c.Next()
	require.NoError(t, err)
	assert.Equal(t, []byte("A..........................._______________________________C"), k)
	assert.Equal(t, []byte("2"), v)

	k, v, err = c.Next()
	require.NoError(t, err)
	assert.Equal(t, []byte("B"), k)
	assert.Equal(t, []byte("7"), v)

	k, v, err = c.Next()
	require.NoError(t, err)
	assert.Equal(t, []byte("D..........................._______________________________A"), k)
	assert.Equal(t, []byte("3"), v)

	k, v, err = c.Next()
	require.NoError(t, err)
	assert.Equal(t, []byte("D..........................._______________________________C"), k)
	assert.Equal(t, []byte("6"), v)

	k, v, err = c.Next()
	require.NoError(t, err)
	assert.Equal(t, []byte("D..........................._______________________________E"), k)
	assert.Equal(t, []byte("5"), v)

	k, v, err = c.Next()
	require.NoError(t, err)
	assert.Nil(t, k)
	assert.Nil(t, v)
}

func TestAutoConversionSeekBothRange(t *testing.T) {
	db, tx, nonDupC := baseAutoConversion(t)
	nonDupC.Close()
	defer db.Close()
	defer tx.Rollback()

	c, err := tx.RwCursorDupSort(kv.PlainState)
	require.NoError(t, err)

	require.NoError(t, c.Delete([]byte("A..........................._______________________________A")))
	require.NoError(t, c.Put([]byte("D..........................._______________________________C"), []byte("6")))
	require.NoError(t, c.Put([]byte("D..........................._______________________________E"), []byte("5")))

	v, err := c.SeekBothRange([]byte("A..........................."), []byte("_______________________________A"))
	require.NoError(t, err)
	assert.Equal(t, []byte("_______________________________C2"), v)

	_, v, err = c.NextDup()
	require.NoError(t, err)
	assert.Nil(t, v)

	v, err = c.SeekBothRange([]byte("A..........................."), []byte("_______________________________X"))
	require.NoError(t, err)
	assert.Nil(t, v)

	v, err = c.SeekBothRange([]byte("B..........................."), []byte(""))
	require.NoError(t, err)
	assert.Nil(t, v)

	v, err = c.SeekBothRange([]byte("C..........................."), []byte(""))
	require.NoError(t, err)
	assert.Nil(t, v)

	v, err = c.SeekBothRange([]byte("D..........................."), []byte(""))
	require.NoError(t, err)
	assert.Equal(t, []byte("_______________________________A3"), v)

	_, v, err = c.NextDup()
	require.NoError(t, err)
	assert.Equal(t, []byte("_______________________________C6"), v)

	_, v, err = c.NextDup()
	require.NoError(t, err)
	assert.Equal(t, []byte("_______________________________E5"), v)

	_, v, err = c.NextDup()
	require.NoError(t, err)
	assert.Nil(t, v)

	v, err = c.SeekBothRange([]byte("X..........................."), []byte("_______________________________Y"))
	require.NoError(t, err)
	assert.Nil(t, v)
}

func TestBeginRoAfterClose(t *testing.T) {
	db := NewMDBX(log.New()).InMem(t.TempDir()).WithTableCfg(kv.ChaindataTablesCfg).MustOpen()
	db.Close()
	_, err := db.BeginRo(context.Background())
	require.ErrorContains(t, err, "closed")
}

func TestBeginRwAfterClose(t *testing.T) {
	db := NewMDBX(log.New()).InMem(t.TempDir()).WithTableCfg(kv.ChaindataTablesCfg).MustOpen()
	db.Close()
	_, err := db.BeginRw(context.Background())
	require.ErrorContains(t, err, "closed")
}

func TestBeginRoWithDoneContext(t *testing.T) {
	db := NewMDBX(log.New()).InMem(t.TempDir()).WithTableCfg(kv.ChaindataTablesCfg).MustOpen()
	defer db.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := db.BeginRo(ctx)
	require.ErrorIs(t, err, context.Canceled)
}

func TestBeginRwWithDoneContext(t *testing.T) {
	db := NewMDBX(log.New()).InMem(t.TempDir()).WithTableCfg(kv.ChaindataTablesCfg).MustOpen()
	defer db.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := db.BeginRw(ctx)
	require.ErrorIs(t, err, context.Canceled)
}

func testCloseWaitsAfterTxBegin(
	t *testing.T,
	count int,
	txBeginFunc func(kv.RwDB) (kv.StatelessReadTx, error),
	txEndFunc func(kv.StatelessReadTx) error,
) {
	t.Helper()
	db := NewMDBX(log.New()).InMem(t.TempDir()).WithTableCfg(kv.ChaindataTablesCfg).MustOpen()
	var txs []kv.StatelessReadTx
	for i := 0; i < count; i++ {
		tx, err := txBeginFunc(db)
		require.Nil(t, err)
		txs = append(txs, tx)
	}

	isClosed := &atomic.Bool{}
	closeDone := make(chan struct{})

	go func() {
		db.Close()
		isClosed.Store(true)
		close(closeDone)
	}()

	for _, tx := range txs {
		// arbitrary delay to give db.Close() a chance to exit prematurely
		time.Sleep(time.Millisecond * 20)
		assert.False(t, isClosed.Load())

		err := txEndFunc(tx)
		require.Nil(t, err)
	}

	<-closeDone
	assert.True(t, isClosed.Load())
}

func TestCloseWaitsAfterTxBegin(t *testing.T) {
	ctx := context.Background()
	t.Run("BeginRoAndCommit", func(t *testing.T) {
		testCloseWaitsAfterTxBegin(
			t,
			1,
			func(db kv.RwDB) (kv.StatelessReadTx, error) { return db.BeginRo(ctx) },
			func(tx kv.StatelessReadTx) error { return tx.Commit() },
		)
	})
	t.Run("BeginRoAndCommit3", func(t *testing.T) {
		testCloseWaitsAfterTxBegin(
			t,
			3,
			func(db kv.RwDB) (kv.StatelessReadTx, error) { return db.BeginRo(ctx) },
			func(tx kv.StatelessReadTx) error { return tx.Commit() },
		)
	})
	t.Run("BeginRoAndRollback", func(t *testing.T) {
		testCloseWaitsAfterTxBegin(
			t,
			1,
			func(db kv.RwDB) (kv.StatelessReadTx, error) { return db.BeginRo(ctx) },
			func(tx kv.StatelessReadTx) error { tx.Rollback(); return nil },
		)
	})
	t.Run("BeginRoAndRollback3", func(t *testing.T) {
		testCloseWaitsAfterTxBegin(
			t,
			3,
			func(db kv.RwDB) (kv.StatelessReadTx, error) { return db.BeginRo(ctx) },
			func(tx kv.StatelessReadTx) error { tx.Rollback(); return nil },
		)
	})
	t.Run("BeginRwAndCommit", func(t *testing.T) {
		testCloseWaitsAfterTxBegin(
			t,
			1,
			func(db kv.RwDB) (kv.StatelessReadTx, error) { return db.BeginRw(ctx) },
			func(tx kv.StatelessReadTx) error { return tx.Commit() },
		)
	})
	t.Run("BeginRwAndRollback", func(t *testing.T) {
		testCloseWaitsAfterTxBegin(
			t,
			1,
			func(db kv.RwDB) (kv.StatelessReadTx, error) { return db.BeginRw(ctx) },
			func(tx kv.StatelessReadTx) error { tx.Rollback(); return nil },
		)
	})
}
