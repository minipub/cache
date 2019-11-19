package cache

import (
	"testing"
)

func TestGetRingNode(t *testing.T) {
	for _, addr := range []string{
		"redis://127.0.0.1",
		"redis://127.0.0.1:6379",
		"redis://127.0.0.1:6380/0",
		"redis://127.0.0.1/5",
		"redis://127.0.0.1/5,2",
		"redis://:abcdef@127.0.0.1:6380",
		"redis://:abcdef@127.0.0.1:6380/1,2",
	} {
		node := getRingNode(addr)
		t.Logf("addr: %s, node: %v\n", addr, node)
	}
}
