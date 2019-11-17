package main

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/kakiezhang/fgocache/hashring"
)

const (
	scheme = "redis://"
	colon  = ":"

	defaultPort = 6379
)

type Ring struct {
	mu      sync.RWMutex
	hash    *hashring.HashRing
	shards  map[string]*redis.Pool
	opt     *RingOptions
	marshal marshalModule
}

type RingOptions struct {
	Addrs       []string
	Nodes       map[string]*RingNode
	Weights     map[string]int
	MaxIdle     int
	MaxActive   int
	IdleTimeout time.Duration
	Marshal     string
}

type RingNode struct {
	server   string
	password string
	database int
	weight   int
}

func (opt *RingOptions) init() {
	for _, addr := range opt.Addrs {
		node := getRingNode(addr)
		opt.Nodes[addr] = node
		if node.weight > 0 {
			opt.Weights[addr] = node.weight
		}
	}

	if opt.MaxActive == 0 {
		opt.MaxActive = 500
	}

	if opt.MaxIdle == 0 {
		opt.MaxIdle = 50
	}

	if opt.IdleTimeout == 0 {
		opt.IdleTimeout = 60 * time.Second
	}
}

func getRingNode(addr string) *RingNode {
	if !strings.HasPrefix(addr, scheme) {
		panic("format error")
	}

	var arr []string
	var url string
	var db int
	var err error

	arr = strings.Split(addr[len(scheme):], "/")
	if len(arr) == 2 {
		url = arr[0]
		db, err = strconv.Atoi(arr[1])
		if err != nil {
			panic("db must be integer")
		}
	} else if len(arr) == 1 {
		url = arr[0]
	} else {
		panic("mm")
	}

	var url2 string
	var weight int

	arr = strings.Split(url, ",")
	if len(arr) == 2 {
		url2 = arr[0]
		weight, err = strconv.Atoi(arr[1])
		if err != nil {
			panic("weight must be integer")
		}
	} else if len(arr) == 1 {
		url2 = arr[0]
	} else {
		panic("mm")
	}

	var hostport, passwd string

	arr = strings.Split(url2, "@")
	if len(arr) == 2 {
		passwd, hostport = arr[0], arr[1]
		if !strings.HasPrefix(passwd, colon) {
			panic("hh")
		}
		passwd = passwd[len(colon):]
	} else if len(arr) == 1 {
		hostport = arr[0]
	} else {
		panic("nn")
	}

	arr = strings.Split(hostport, colon)
	if len(arr) == 2 {
		// do nothing
	} else if len(arr) == 1 {
		hostport = fmt.Sprintf("%s%s%d", arr[0], colon, defaultPort)
	} else {
		panic("nn")
	}

	return &RingNode{
		server:   hostport,
		password: passwd,
		database: db,
		weight:   weight,
	}
}

func NewRing(opt *RingOptions) *Ring {
	opt.Nodes = make(map[string]*RingNode)
	opt.Weights = make(map[string]int)
	opt.init()

	var mm marshalModule
	if opt.Marshal == "" || opt.Marshal == "msgpack" {
		mm = &msgpackMarshal{}
	} else if opt.Marshal == "plain" {
		mm = &plainMarshal{}
	} else {
		panic(fmt.Sprintf("no implement marshal[%s]", opt.Marshal))
	}

	var hash *hashring.HashRing
	if len(opt.Weights) > 0 {
		hash = hashring.NewWithWeights(opt.Weights)
	} else {
		hash = hashring.New(opt.Addrs)
	}

	r := &Ring{
		opt:     opt,
		hash:    hash,
		shards:  make(map[string]*redis.Pool),
		marshal: mm,
	}

	for addr, node := range opt.Nodes {
		r.shards[addr] = r.getRedisPool(node.server, node.password, node.database)
	}

	return r
}

func (r *Ring) getRedisPool(
	server, password string, database int) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     r.opt.MaxIdle,
		MaxActive:   r.opt.MaxActive,
		IdleTimeout: r.opt.IdleTimeout,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
			if err != nil {
				log.Printf("redis.Dial node(%s) failed", server)
				return nil, err
			}
			if len(password) > 0 {
				if _, err := c.Do("AUTH", password); err != nil {
					c.Close()
					log.Printf("redis.Auth node(%s) failed", server)
					return nil, err
				}
			}
			if database >= 0 && database < 16 {
				if _, err := c.Do("SELECT", database); err != nil {
					c.Close()
					log.Printf("redis.Select node(%s) db(%d) failed", server, database)
					return nil, err
				}
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
	}
}

func (r *Ring) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	var err error
	for _, shard := range r.shards {
		if err = shard.Close(); err != nil {
			break
		}
	}
	return err
}

func (r *Ring) keyToPool(key string) *redis.Pool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	node, ok := r.hash.GetNode(key)
	if !ok {
		log.Printf("not found key[%s]", key)
		return nil
	}

	return r.shards[node]
}

func (r *Ring) Get(key string) (data []byte, err error) {
	pool := r.keyToPool(key)
	conn := pool.Get()
	defer conn.Close()

	data, err = redis.Bytes(conn.Do("GET", key))
	if err != nil {
		if err == redis.ErrNil {
			return data, nil
		} else {
			return data, fmt.Errorf(
				"error getting key %s: %v", key, err)
		}
	}
	return r.marshal.loads(data)
}

func (r *Ring) Set(key string, value interface{}, expiry int) (err error) {
	pool := r.keyToPool(key)
	conn := pool.Get()
	defer conn.Close()

	var data interface{}
	data, err = r.marshal.dumps(value)
	if err != nil {
		return err
	}

	if expiry > 0 {
		_, err = conn.Do("SETEX", key, expiry, data)
	} else {
		_, err = conn.Do("SET", key, data)
	}

	if err != nil {
		v := data.(string)
		if len(v) > 15 {
			v = v[0:12] + "..."
		}
		return fmt.Errorf(
			"error setting key %s to %s: %v", key, v, err)
	}
	return err
}

func (r *Ring) Exists(key string) (bool, error) {
	pool := r.keyToPool(key)
	conn := pool.Get()
	defer conn.Close()

	ok, err := redis.Bool(conn.Do("EXISTS", key))
	if err != nil {
		return ok, fmt.Errorf(
			"error checking if key %s exists: %v", key, err)
	}
	return ok, err
}

func (r *Ring) Delete(key string) error {
	pool := r.keyToPool(key)
	conn := pool.Get()
	defer conn.Close()

	_, err := conn.Do("DEL", key)
	return err
}
