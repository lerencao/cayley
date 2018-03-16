package tikv

import (
	"context"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/kv"
	kv2 "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
	"unsafe"
)

const Type = "tikv"

func init() {
	kv.Register(Type, kv.Registration{
		NewFunc:      Create,
		InitFunc:     Create,
		IsPersistent: true,
	})
}

func Create(path string, opt graph.Options) (kv.BucketKV, error) {
	storage, err := tikv.Driver{}.Open(path)
	if err != nil {
		return nil, err
	}
	tikvStorage := storage.(tikv.Storage)

	return kv.FromFlat(newTiKV(tikvStorage)), nil
}

type TiKV struct {
	Store tikv.Storage
}

func newTiKV(store tikv.Storage) *TiKV {
	return &TiKV{
		Store: store,
	}
}

func (tikv *TiKV) Type() string {
	return Type
}

func (tikv *TiKV) Close() error {
	return tikv.Store.Close()
}

func (tikv *TiKV) Tx(update bool) (kv.FlatTx, error) {
	tx := &TiTx{
		update: update,
	}
	var err error
	if tx.update {
		tx.txn, err = tikv.Store.Begin()
	} else {
		var version kv2.Version
		version, err = tikv.Store.CurrentVersion()
		if err == nil {
			tx.sn, err = tikv.Store.GetSnapshot(version)
		}
	}
	if err != nil {
		return nil, err
	}
	return tx, nil
}

// TODO: merge txn and sn
type TiTx struct {
	update bool
	txn    kv2.Transaction
	sn     kv2.Snapshot
}

func (tx *TiTx) Commit(ctx context.Context) error {
	if tx.update {
		return tx.txn.Commit(ctx)
	}
	return nil
}

func (tx *TiTx) Rollback() error {
	if tx.update && tx.txn.Valid() {
		return tx.txn.Rollback()
	}
	return nil
}

//func (tx *TiTx) Bucket(name []byte) kv.Bucket {
//	return &bucket{
//		name: name,
//		tx:   tx,
//	}
//}

func (tx *TiTx) Get(ctx context.Context, keys [][]byte) ([][]byte, error) {
	snap := tx.sn
	if tx.update {
		snap = tx.txn.GetSnapshot()
	}
	// We want []Key instead of [][]byte, use some magic to save memory.
	origKeys := *(*[]kv2.Key)(unsafe.Pointer(&keys))
	m, err := snap.BatchGet(origKeys)

	vals := make([][]byte, len(keys))
	if m != nil {
		for i, k := range keys {
			mk := string(k)
			if m[mk] != nil {
				vals[i] = m[mk]
			}
		}
	}
	return vals, err
}

func (tx *TiTx) Put(k, v []byte) error {
	return tx.txn.Set(k, v)
}

func (tx *TiTx) Del(k []byte) error {
	return tx.txn.Delete(k)
}

func (tx *TiTx) Scan(pref []byte) kv.KVIterator {
	it, err := tx.txn.Seek(pref)
	// TODO: handle pref is nil case.
	// if err is not nil, the returned iterator will has err too.
	// And `Next` will return false.
	return &iterator{
		pref: pref,
		it:   it,
		err:  err,
	}
}

type iterator struct {
	pref []byte
	it   kv2.Iterator
	k, v []byte
	err  error
}

func (iter *iterator) Next(ctx context.Context) bool {
	// this should come first.
	if iter.err != nil {
		return false
	}

	if !iter.it.Valid() {
		return false
	}

	iter.err = iter.it.Next()
	if iter.err != nil {
		return false
	}
	key := iter.it.Key()

	// out of range
	if !key.HasPrefix(iter.pref) {
		return false
	}
	iter.k = key
	iter.v = iter.it.Value()
	return true
}

func (iter *iterator) Err() error {
	return iter.err
}
func (iter *iterator) Close() error {
	iter.it.Close()
	return nil
}

func (iter *iterator) Key() []byte {
	return iter.k
}
func (iter *iterator) Val() []byte {
	return iter.v
}
