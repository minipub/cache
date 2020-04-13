package cache

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/minipub/cache/hashring"
)

type Ring struct {
	mu      sync.RWMutex
	hash    *hashring.HashRing
	shards  map[string]*redis.Pool
	opt     *RingOptions
	marshal marshalModule
}

type RingOptions struct {
	Addrs       []string // address may with weight
	MaxIdle     int
	MaxActive   int
	IdleTimeout time.Duration
	Marshal     string

	nodes         map[string]*RingNode
	weights       map[string]int
	addrsNoWeight []string // address without weight
}

func (opt *RingOptions) init() {
	for _, addr := range opt.Addrs {
		node := getRingNode(addr)
		opt.nodes[addr] = node
		opt.addrsNoWeight = append(opt.addrsNoWeight, node.addr)
		if node.weight > 0 {
			opt.weights[addr] = node.weight
		}
	}

	if opt.MaxActive == 0 {
		opt.MaxActive = 50
	}

	if opt.MaxIdle == 0 {
		opt.MaxIdle = 50
	}

	if opt.IdleTimeout == 0 {
		opt.IdleTimeout = 60 * time.Second
	}
}

func NewRing(opt *RingOptions) (r *Ring) {
	defer func() {
		if p := recover(); p != nil {
			fmt.Println(p)
		}
	}()

	opt.nodes = make(map[string]*RingNode)
	opt.weights = make(map[string]int)
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
	if len(opt.weights) > 0 {
		hash = hashring.NewWithWeights(opt.weights)
	} else {
		hash = hashring.New(opt.addrsNoWeight)
	}

	r = &Ring{
		opt:     opt,
		hash:    hash,
		shards:  make(map[string]*redis.Pool),
		marshal: mm,
	}

	for addr, node := range opt.nodes {
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
		return fmt.Errorf(
			"error setting key %s to %v: %v", key, data, err)
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

func (r *Ring) Incr(key string, expiry int) (int64, error) {
	pool := r.keyToPool(key)
	conn := pool.Get()
	defer conn.Close()

	value, err := conn.Do("INCR", key)
	if err != nil {
		return -1, fmt.Errorf(
			"error incr key %s: %v", key, err)
	}
	data := value.(int64)

	if expiry > 0 {
		if _, err := conn.Do("EXPIRE", key, expiry); err != nil {
			log.Printf("error expire key %s: %v", key, err)
		}
	}
	return data, nil
}

func (r *Ring) IncrBy(key, num string, expiry int) (int64, error) {
	pool := r.keyToPool(key)
	conn := pool.Get()
	defer conn.Close()

	value, err := conn.Do("INCRBY", key, num)
	if err != nil {
		return -1, fmt.Errorf(
			"error incrby key %s increment %s: %v", key, num, err)
	}
	data := value.(int64)

	if expiry > 0 {
		if _, err := conn.Do("EXPIRE", key, expiry); err != nil {
			log.Printf("error expire key %s: %v", key, err)
		}
	}
	return data, nil
}

func (r *Ring) Decr(key string, expiry int) (int64, error) {
	pool := r.keyToPool(key)
	conn := pool.Get()
	defer conn.Close()

	value, err := conn.Do("DECR", key)
	if err != nil {
		return -1, fmt.Errorf(
			"error decr key %s: %v", key, err)
	}
	data := value.(int64)

	if expiry > 0 {
		if _, err := conn.Do("EXPIRE", key, expiry); err != nil {
			log.Printf("error expire key %s: %v", key, err)
		}
	}
	return data, nil
}

func (r *Ring) DecrBy(key, num string, expiry int) (int64, error) {
	pool := r.keyToPool(key)
	conn := pool.Get()
	defer conn.Close()

	value, err := conn.Do("DECRBY", key, num)
	if err != nil {
		return -1, fmt.Errorf(
			"error decrby key %s decrement %s: %v", key, num, err)
	}
	data := value.(int64)

	if expiry > 0 {
		if _, err := conn.Do("EXPIRE", key, expiry); err != nil {
			log.Printf("error expire key %s: %v", key, err)
		}
	}
	return data, nil
}

func (r *Ring) Expire(key string, expiry int) (bool, error) {
	pool := r.keyToPool(key)
	conn := pool.Get()
	defer conn.Close()

	if _, err := conn.Do("EXPIRE", key, expiry); err != nil {
		return false, fmt.Errorf("error expire key %s: %v", key, err)
	}
	return true, nil
}
