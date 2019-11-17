package main

import "fmt"

func main() {
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
	fmt.Printf("data: %s\n", d)
}
