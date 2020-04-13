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
	// r.Set("aa", 1, 0) // no pass

	d, _ := r.Get("aa")
	t.Logf("data: %s", d)
}

func TestGetSetRace(t *testing.T) {
	r := NewRing(&RingOptions{
		Addrs: []string{
			"redis://127.0.0.1:6380/0",
			"redis://:@127.0.0.1:6380/1",
		},
		MaxActive: 250,
		MaxIdle:   250,
	})
	defer r.Close()

	heihei := func(k string, f func()) {
		defer f()
		r.Set(k, "1", 0)
		v, err := r.Get(k)
		if err != nil {
			t.Errorf("err %s: %s", k, err)
		} else {
			t.Logf("%s: %s", k, v)
		}
	}

	var wg sync.WaitGroup
	for i := 1; i <= 200; i++ {
		wg.Add(1)
		k := fmt.Sprintf("a%d", i)
		go heihei(k, wg.Done)
	}
	wg.Wait()
}

func TestNewRingIncr(t *testing.T) {
	r := NewRing(&RingOptions{
		Addrs: []string{
			"redis://127.0.0.1:6380/1",
		},
		Marshal: "plain",
	})
	defer r.Close()

	var value int64
	value, _ = r.Incr("aa", 0)
	t.Log(value)
	value, _ = r.IncrBy("aa", "3", 10)
	t.Log(value)
}

func TestNewRingDecr(t *testing.T) {
	r := NewRing(&RingOptions{
		Addrs: []string{
			"redis://127.0.0.1:6380/1",
		},
		Marshal: "plain",
	})
	defer r.Close()

	var value int64
	value, _ = r.Decr("aa", 60)
	t.Log(value)
	value, _ = r.DecrBy("aa", "3", 0)
	t.Log(value)
}

func TestNewRingExpire(t *testing.T) {
	r := NewRing(&RingOptions{
		Addrs: []string{
			"redis://127.0.0.1:6380/1",
		},
		Marshal: "plain",
	})
	defer r.Close()

	value, _ := r.Decr("aa", 60)
	t.Log(value)
	_, _ = r.Expire("aa", 100)
}

func TestNewRingPanic(t *testing.T) {
	r := NewRing(&RingOptions{
		Addrs: []string{
			"127.0.0.1:6380/0",
			"127.0.0.1:6380/1",
		},
		Marshal: "plain",
	})
	defer func() {
		if r != nil {
			r.Close()
		}
	}()

	r.Set("aa", "haha", 0)

	// d, _ := r.Get("aa")
	// t.Logf("data: %s", d)
}
