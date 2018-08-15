package tikv

import (
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/kv"
	"github.com/cayleygraph/cayley/graph/kv/kvtest"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/tikv"
	"testing"
)

func makeTiKV(t testing.TB) (kv.BucketKV, graph.Options, func()) {
	//store, err := tikv.NewTestTiKVStore(client, pdClient, nil, nil)
	store, err := mockstore.NewMockTikvStore()
	if err != nil {
		t.Fatalf("Could not create tikv client and pd client")
	}
	return kv.FromFlat(newTiKV(store.(tikv.Storage))), nil, func() {
		//store.Close()
	}
}

func TestTiKV(t *testing.T) {
	kvtest.TestAll(t, makeTiKV, nil)
}

func BenchmarkTiKV(b *testing.B) {
	kvtest.BenchmarkAll(b, makeTiKV, nil)
}
