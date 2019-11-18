package cache

import (
	"fmt"
	"sync"
	"testing"
)

func TestNewRingPlain(t *testing.T) {
	r := NewRing(&RingOptions{
		Addrs: []string{
			"redis://127.0.0.1:6380/0",
			"redis://:@127.0.0.1:6380/1",
		},
		Marshal: "plain",
	})
	defer r.Close()

	r.Set("aa", "haha", 0)
	// r.Set("b", []byte("haha"), 0)

	d, _ := r.Get("aa")
	t.Logf("data: %s", d)
}

func TestNewRingMsgpack(t *testing.T) {
	r := NewRing(&RingOptions{
		Addrs: []string{
			"redis://127.0.0.1:6380/0",
			"redis://:@127.0.0.1:6380/1",
		},
	})
	defer r.Close()

	r.Set("aa", "haha", 0)
	// r.Set("b", []byte("haha"), 0)

	d, _ := r.Get("aa")
	t.Logf("data: %s", d)
}

func TestGetSetRace(t *testing.T) {
	r := NewRing(&RingOptions{
		Addrs: []string{
			"redis://127.0.0.1:6380/0",
			"redis://:@127.0.0.1:6380/1",
		},
	})
	defer r.Close()

	heihei := func(k string, f func()) {
		defer f()
		r.Set(k, 1, 0)
		v, _ := r.Get(k)
		t.Logf("%s: %s", k, v)
	}

	var wg sync.WaitGroup
	for i := 1; i <= 30; i++ {
		wg.Add(1)
		k := fmt.Sprintf("a%d", i)
		go heihei(k, wg.Done)
	}
	wg.Wait()
}
