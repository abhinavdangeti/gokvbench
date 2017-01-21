// The MIT License (MIT)

package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"unsafe"

	"github.com/couchbase/moss"
	"github.com/t3rm1n4l/nitro/skiplist"
	"github.com/t3rm1n4l/nitro/plasma"
	"strings"
)

type (
	benchF func(b *testing.B)
	stores struct {
		bench []benchF
		run   *bool
	}
)

func main() {
	testAll := flag.Bool("all", false, "Run all tests")
	tests := []*stores{
		&stores{
			bench: []benchF{
				testMossCreateBatch1,
				testMossCreateBatchNSize10000,
				testMossCreateBatchNSize1000,
				testMossReadBatch1,
				testMossReadBatchNSize10000,
			},
			run: flag.Bool("moss", false, ""),
		},
		&stores{
			bench: []benchF{
				testPlasmaPersist1,
				testPlasmaPersistNSize10000,
				testPlasmaPersistNSize1000,
				testPlasmaReadPersist1,
				testPlasmaReadPersistNSize10000,
			},
			run: flag.Bool("plasma", false, ""),
		},
	}
	flag.Parse()

	ran := false
	for _, test := range tests {
		if *test.run == false && *testAll == false {
			continue
		}
		ran = true
		for _, f := range test.bench {
			res := testing.Benchmark(f)
			fmt.Printf("%40s    %15s%15s\n", funcName(f), res.String(), res.MemString())
		}
	}
	if ran == false {
		fmt.Println("No benchmark executed! Try -h switch for help.")
	}
	fmt.Println("\nDone!")
}

// MOSS/STORE APIs

func mossWrite(b *testing.B, batchsize int) (d string, s *moss.Store, c moss.Collection) {
	return mossWriteEx(b, batchsize, false)
}

func mossWriteEx(b *testing.B, batchsize int, wantSync bool) (d string, s *moss.Store, c moss.Collection) {
	if batchsize > b.N {
		batchsize = b.N
	}

	dir := "./mossStore"
	os.Mkdir(dir, 0777)

	var err error
	var m sync.Mutex
	var waitingForCleanCh chan struct{}

	var store *moss.Store
	var coll moss.Collection

	co := moss.CollectionOptions{
		OnEvent: func(event moss.Event) {
			if event.Kind == moss.EventKindPersisterProgress {
				stats, err := coll.Stats()
				if err == nil &&
					stats.CurDirtyOps <= 0 &&
					stats.CurDirtyBytes <= 0 &&
					stats.CurDirtySegments <= 0 {
					m.Lock()
					if waitingForCleanCh != nil {
						waitingForCleanCh <- struct{}{}
						waitingForCleanCh = nil
					}
					m.Unlock()
				}
			}
		},
	}

	store, coll, err = moss.OpenStoreCollection(dir,
		moss.StoreOptions{
			CollectionOptions: co,
		},
		moss.StorePersistOptions{})
	if err != nil || store == nil {
		b.Error("expected OpenStoreCollection to work")
	}

	k_arr := make([][]byte, b.N)
	v_arr := make([][]byte, b.N)

	for i := 0; i < b.N; i++ {
		k, v := kv(i)
		k_arr[i] = k
		v_arr[i] = v
	}

	b.ResetTimer()

	cur := 0

	ch := make(chan struct{}, 1)

	for i := 0; i < b.N / batchsize; i++ {
		batch, err := coll.NewBatch(batchsize, batchsize*16)
		if err != nil {
			b.Error("expected ok")
		}

		for j := 0; j < batchsize; j++ {
			kbuf, _ := batch.Alloc(len(k_arr[cur]))
			copy(kbuf, k_arr[cur])
			batch.AllocSet(kbuf, nil)
			cur++
		}

		if wantSync {
			m.Lock()
			waitingForCleanCh = ch
			m.Unlock()
		}

		err = coll.ExecuteBatch(batch, moss.WriteOptions{})
		if err != nil {
			b.Error("expected exec batch ok")
		}

		if wantSync {
			<-ch
		}
	}

	return dir, store, coll
}

func testMossWrite(b *testing.B, batchsize int) {
	dir, store, coll := mossWrite(b, batchsize)
	defer os.RemoveAll(dir)
	defer store.Close()
	defer coll.Close()
}

func testMossRead(b *testing.B, batchsize int) {
	dir, store, coll := mossWrite(b, batchsize)
	defer os.RemoveAll(dir)
	defer store.Close()
	defer coll.Close()

	k_arr := make([][]byte, b.N)
	v_arr := make([][]byte, b.N)

	for i := 0; i < b.N; i++ {
		k, v := kv(i)
		k_arr[i] = k
		v_arr[i] = v
	}

    readOptions := moss.ReadOptions{NoCopyValue: true}

	b.ResetTimer()

	snapshot, _ := store.Snapshot()
	for i := 0; i < b.N; i++ {
		val, err := snapshot.Get(k_arr[i], readOptions)
		if err != nil {
			b.Error("expected read to pass")
		}
		if len(val) > 0 {
			b.Error("value should have been len 0")
		}
		/*
		if len(v_arr[i]) == len(val) {
			for j := range v_arr[i] {
				if v_arr[i][j] != val[j] {
					b.Error("value mismatch")
				}
			}
		} else {
			b.Error("value length mismatch")
		}
		*/
	}

	snapshot.Close()
}

func testMossCreateBatch1(b *testing.B) {
	testMossWrite(b, b.N)
}

func testMossCreateBatchNSize10000(b *testing.B) {
	testMossWrite(b, 10000)
}

func testMossCreateBatchNSize1000(b *testing.B) {
	testMossWrite(b, 1000)
}

func testMossReadBatch1(b *testing.B) {
	testMossRead(b, b.N)
}

func testMossReadBatchNSize10000(b *testing.B) {
	testMossRead(b, 10000)
}

// NITRO/PLASMA APIs

var plasmaCfg = plasma.Config{
	MaxDeltaChainLen: 200,
	MaxPageItems:     400,
	MinPageItems:     25,
	/*
	Compare:          plasma.compareBytes,
	ItemSize: func(itm unsafe.Pointer) uintptr {
		if itm == skiplist.MinItem || itm == skiplist.MaxItem {
			return 0
		}
		return uintptr((*plasma.item)(itm).Size())
	},
	*/
	Compare:          skiplist.CompareInt,
	ItemSize: func(unsafe.Pointer) uintptr {
		return unsafe.Sizeof(new(skiplist.IntKeyItem))
	},

	File:                "testplasma.data",
	FlushBufferSize:     1024 * 1024,
	LSSCleanerThreshold: 10,
	AutoLSSCleaning:     false,
}

//func plasmaWrite(b *testing.B) (st *plasma.Plasma, wr *plasma.Writer, arr [][]byte) {
func plasmaWrite(b *testing.B, batchsize int) (st *plasma.Plasma, wr *plasma.Writer) {
	s, err := plasma.New(plasmaCfg)
	if err != nil {
		b.Error("open failed")
	}

	/*
	k_arr := make([][]byte, b.N)
	v_arr := make([][]byte, b.N)

	for i := 0; i < b.N; i++ {
		k, v := kv(i)
		k_arr[i] = k
		v_arr[i] = v
	}
	*/

	w := s.NewWriter()

	b.ResetTimer()

	/*
	cur := 0

	for i := 0; i < b.N / batchsize; i++ {
		for j := 0; j < batchsize; j++ {
			w.InsertKV(k_arr[cur], v_arr[cur])
			cur++
		}
		s.PersistAll()
	}
	*/

	for i := 0; i < b.N; i++ {
		w.Insert(skiplist.NewIntKeyItem(i))

		if i % batchsize == 0 {
			s.PersistAll()
		}
	}
	s.PersistAll()

	return s, w
	//return s, w, k_arr
}

func testPlasmaWrite(b *testing.B, batchsize int) {
	//s, _, _ := plasmaWrite(b, batchsize)
	s, _ := plasmaWrite(b, batchsize)
	defer os.RemoveAll(plasmaCfg.File)
	defer s.Close()
}

func testPlasmaRead(b *testing.B, batchsize int) {
	//s, w, k_arr := plasmaWrite(b, batchsize)
	s, w := plasmaWrite(b, batchsize)
	defer os.RemoveAll(plasmaCfg.File)
	defer s.Close()

	s.EvictAll()

	b.ResetTimer()

	/*
	for i := 0; i < b.N; i++ {
		itm := skiplist.NewByteKeyItem(k_arr[i])
		w.Lookup(itm)
		//got, _ := w.Lookup(itm)
		//if plasma.compareBytes(itm, got) != 0 {
		//	b.Error("mismatch in item")
		//}
	}
	*/

	for i := 0; i < b.N; i++ {
		itm := skiplist.NewIntKeyItem(i)
		got, err := w.Lookup(itm)
		if err != nil {
			b.Errorf("lookup err: %v", err)
		}
		if skiplist.CompareInt(itm, got) != 0 {
			b.Error("mismatch in item")
		}
	}

}

func testPlasmaPersist1(b *testing.B) {
	testPlasmaWrite(b, b.N)
}

func testPlasmaPersistNSize10000(b *testing.B) {
	testPlasmaWrite(b, 10000)
}

func testPlasmaPersistNSize1000(b *testing.B) {
	testPlasmaWrite(b, 1000)
}

func testPlasmaReadPersist1(b *testing.B) {
	testPlasmaRead(b, b.N)
}

func testPlasmaReadPersistNSize10000(b *testing.B) {
	testPlasmaRead(b, 10000)
}

// HELPER APIs

func kv(i int) ([]byte, []byte) {
	k := []byte(strconv.Itoa(i))
	v := make([]byte, 8)
	binary.LittleEndian.PutUint64(v, uint64(i))
	return k, v
}

func kvs(i int) (string, []byte) {
	v := make([]byte, 8)
	binary.LittleEndian.PutUint64(v, uint64(i))
	return strconv.Itoa(i), v
}

func isDoh(err error) {
	if err != nil {
		panic(err)
	}
}

func funcName(i interface{}) string {
	fn := runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
	return strings.Replace(fn, "main.test", "Benchmark", -1)
}

func bc(b *testing.B, expected, actual []byte) {
	if bytes.Compare(expected, actual) != 0 {
		b.Fatal("Expected %s got %s", expected, actual)
	}
}
