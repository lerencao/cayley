package tikv

import (
	"context"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/kv"
	"github.com/juju/errors"
	kv2 "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
	"sync/atomic"
	"unsafe"
)

const Type = "tikv"

func init() {
	// TODO: a customized gc handler is needed
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
	closed uint32
	Store  tikv.Storage
}

func newTiKV(store tikv.Storage) *TiKV {
	kvStore := &TiKV{
		Store:  store,
		closed: 0,
	}
	return kvStore
}

func (t *TiKV) Type() string {
	return Type
}

func (t *TiKV) Close() error {
	// TODO: make close idempotent
	if atomic.CompareAndSwapUint32(&t.closed, 0, 1) {
		return t.Store.Close()
	}
	return nil
}

func (t *TiKV) Tx(update bool) (kv.FlatTx, error) {
	tx := &TiTx{update: update}
	txn, err := t.Store.Begin()
	if err != nil {
		return nil, err
	}
	tx.txn = txn
	return tx, nil
}

type TiTx struct {
	update bool
	txn    kv2.Transaction
}

func (tx *TiTx) Commit(ctx context.Context) error {
	if !tx.update {
		return ErrTxNotWritable
	}

	return tx.txn.Commit(ctx)
}

func (tx *TiTx) Rollback() error {
	if tx.update {
		return tx.txn.Rollback()
	}
	return nil
}

func (tx *TiTx) Get(ctx context.Context, keys [][]byte) ([][]byte, error) {
	vals := make([][]byte, len(keys))
	if !tx.update {
		// We want []Key instead of [][]byte, use some magic to save memory.
		origKeys := *(*[]kv2.Key)(unsafe.Pointer(&keys))
		storeVals, err := tx.txn.GetSnapshot().BatchGet(origKeys)
		if err != nil {
			return nil, errors.Trace(err)
		}

		for i, k := range keys {
			val, ok := storeVals[string(k)]
			if !ok {
				vals[i] = nil
			} else {
				vals[i] = val
			}
		}
		return vals, nil
	}

	// for update case, we need to find in buffer first.
	memBuf := tx.txn.GetMemBuffer()
	needHitStoreKeys := make([]kv2.Key, len(keys))
	needHitStoreKeyIndexesMap := make(map[string]int, len(keys))
	for i, k := range keys {
		val, err := memBuf.Get(k)
		vals[i] = val
		if kv2.IsErrNotFound(err) {
			vals[i] = nil
			needHitStoreKeys = append(needHitStoreKeys, k)
			needHitStoreKeyIndexesMap[string(k)] = i
		} else if err != nil {
			return nil, errors.Trace(err)
		}
	}
	storeVals, err := tx.txn.GetSnapshot().BatchGet(needHitStoreKeys)
	if err != nil {

		return nil, errors.Trace(err)
	}
	for k, v := range storeVals {
		idx, ok := needHitStoreKeyIndexesMap[k]
		if ok {
			if len(v) == 0 {
				vals[idx] = nil
			} else {
				vals[idx] = v
			}
		}
	}
	return vals, nil
}

func (tx *TiTx) Put(k, v []byte) error {
	if !tx.update {
		return ErrTxNotWritable
	}
	return tx.txn.Set(k, v)
}

func (tx *TiTx) Del(k []byte) error {
	if !tx.update {
		return ErrTxNotWritable
	}
	return tx.txn.Delete(k)
}

func (tx *TiTx) Scan(pref []byte) kv.KVIterator {
	return &iterator{
		pref:  pref,
		txn:   tx.txn,
		first: true,
	}
}

type iterator struct {
	pref      []byte
	txn       kv2.Transaction
	err       error
	first     bool
	innerIter kv2.Iterator
}

func (it *iterator) Next(ctx context.Context) bool {
	if it.first {
		it.first = false
		it.innerIter, it.err = it.txn.Iter(it.pref, nil)
		if it.err != nil {
			return false
		}
		key := it.innerIter.Key()
		// if we hit prefix key in first seek, return directly
		if key.HasPrefix(it.pref) {
			return true
		}
		// or else, we need to iter it, to find first prefix key
		return it.seekFirst(ctx)
	} else {
		if !it.innerIter.Valid() {
			return false
		}

		it.err = it.innerIter.Next()
		if it.err != nil {
			return false
		}

		key := it.innerIter.Key()
		if key.HasPrefix(it.pref) {
			return true
		} else {
			// key prefix not found anymore, make it invalid
			it.innerIter.Close()
			return false
		}
	}
}

func (it *iterator) Err() error {
	return it.err
}

func (it *iterator) Close() error {
	if it.innerIter != nil {
		it.innerIter.Close()
	}
	return nil
}

func (it *iterator) Key() []byte {
	return it.innerIter.Key()
}

func (it *iterator) Val() []byte {
	return it.innerIter.Value()
}

// seek to first key which starts with it.pref
func (it *iterator) seekFirst(ctx context.Context) bool {
	for it.innerIter.Valid() {
		it.err = it.innerIter.Next()
		if it.err != nil {
			return false
		}

		key := it.innerIter.Key()
		if key.HasPrefix(it.pref) {
			return true
		}
	}

	return false
}
