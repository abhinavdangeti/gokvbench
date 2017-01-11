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
	"testing"

	"github.com/steveyen/gkvlite"
	"github.com/couchbase/moss"
	"github.com/t3rm1n4l/nitro"
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
				testGkvliteWrite,
				testGkvliteRead,
			},
			run: flag.Bool("gkvlite", false, ""),
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
			fmt.Printf("%s\t%s\t%s\n", funcName(f), res.String(), res.MemString())
		}
	}
	if ran == false {
		fmt.Println("No benchmark executed! Try -h switch for help.")
	}
	fmt.Println("\nDone!")
}

func gkvliteWrite(b *testing.B) (*os.File, *gkvlite.Store, *gkvlite.Collection) {
	os.Remove("test.gkvlite")
	f, err := os.Create("test.gkvlite")
	isDoh(err)

	s, err := gkvlite.NewStore(f)

	defer f.Sync()
	defer s.Flush()
	c := s.SetCollection("MAIN", nil)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k, v := kv(i)
		c.Set(k, v)
	}
	return f, s, c
}

func testGkvliteWrite(b *testing.B) {
	f, s, _ := gkvliteWrite(b)
	defer f.Close()
	defer s.Close()
}

func testGkvliteRead(b *testing.B) {
	f, s, c := gkvliteWrite(b)
	defer f.Close()
	defer s.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k, v := kv(i)
		v2, err := c.Get(k)
		if err != nil {
			b.Error(err)
		}
		bc(b, v, v2)
	}
}

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
