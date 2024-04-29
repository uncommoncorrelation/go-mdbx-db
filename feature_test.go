package gomdbxdb

import (
	"context"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"

	"github.com/uncommoncorrelation/go-mdbx-db/kv"
	"github.com/uncommoncorrelation/go-mdbx-db/kv/mdbx"
)

func TestIncrementSequence(t *testing.T) {
	path := t.TempDir()
	logger := log.New()
	db := mdbx.NewMDBX(logger).InMem(path).WithTableCfg(kv.DefaultTableCfg).MapSize(128 * datasize.MB).MustOpen()
	t.Cleanup(db.Close)

	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)

	_, err = tx.IncrementSequence("sequenceTest", 1)
	require.Nil(t, err)

	chaV, err := tx.ReadSequence("sequenceTest")
	require.Nil(t, err)
	require.Equal(t, chaV, uint64(1))
}

func BenchmarkIncrementSequence(b *testing.B) {
	b.StopTimer()
	path := b.TempDir()
	logger := log.New()
	db := mdbx.NewMDBX(logger).InMem(path).WithTableCfg(kv.DefaultTableCfg).MapSize(128 * datasize.MB).MustOpen()
	b.Cleanup(db.Close)

	tx, err := db.BeginRw(context.Background())
	require.NoError(b, err)
	b.Cleanup(tx.Rollback)

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_, err = tx.IncrementSequence("sequenceTest", 1)
		if err != nil {
			b.Fatal(err)
		}
	}
	err = tx.Commit()
	if err != nil {
		b.Fatal(err)
	}
}

func BenchmarkConstantPut(b *testing.B) {
	b.StopTimer()
	path := b.TempDir()
	logger := log.New()
	db := mdbx.NewMDBX(logger).InMem(path).WithTableCfg(kv.TableCfg{
		"putTest": {},
	}).MapSize(256 * datasize.MB).MustOpen()
	b.Cleanup(db.Close)

	tx, err := db.BeginRw(context.Background())
	require.NoError(b, err)
	b.Cleanup(tx.Rollback)

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		err = tx.Put("putTest", []byte{byte(i)}, []byte("value"))
		if err != nil {
			b.Fatal(err)
		}
	}
	err = tx.Commit()
	if err != nil {
		b.Fatal(err)
	}
}
