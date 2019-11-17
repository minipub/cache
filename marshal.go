package main

import (
	"github.com/vmihailenco/msgpack/v4"
)

type marshalModule interface {
	loads([]byte) ([]byte, error)
	dumps(interface{}) (interface{}, error)
}

type plainMarshal struct {
}

func (pm *plainMarshal) loads(
	b []byte) ([]byte, error) {
	return b, nil
}

func (pm *plainMarshal) dumps(
	v interface{}) (interface{}, error) {
	return v, nil
}

type msgpackMarshal struct {
}

func (mpm *msgpackMarshal) loads(
	b []byte) (v []byte, e error) {
	if e = msgpack.Unmarshal(b, &v); e != nil {
		return nil, e
	}
	return v, nil
}

func (mpm *msgpackMarshal) dumps(
	v interface{}) (b interface{}, e error) {
	if b, e = msgpack.Marshal(v); e != nil {
		return nil, e
	}
	return b, nil
}
