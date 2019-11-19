package cache

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	scheme      = "redis://"
	colon       = ":"
	defaultPort = "6379"
)

type RingNode struct {
	addr     string // connnection url without weight
	server   string
	password string
	database int
	weight   int
}

func (rn *RingNode) String() string {
	return fmt.Sprintf(
		"addr[ %s ] server[ %s ] pwd[ %s ] db[ %d ] weight[ %d ]",
		rn.addr, rn.server, rn.password, rn.database, rn.weight)
}

type sepRule func(arg1, arg2 string) (rs1, rs2 interface{})

func getRingNode(addr string) *RingNode {
	if !strings.HasPrefix(addr, scheme) {
		panic("no scheme prefix addr")
	}

	var db, weight int
	var arg1, arg2 interface{}

	arg1, arg2 = splitOnceGeneric(
		addr[len(scheme):], ",", opAssignOnly1st, opParseInt2nd)

	urlNoWeight := fmt.Sprintf("%s", arg1)
	if arg2 != nil {
		weight = arg2.(int)
	}

	arg1, arg2 = splitOnceGeneric(
		urlNoWeight, "/", opAssignOnly1st, opParseInt2nd)

	url := fmt.Sprintf("%s", arg1)
	if arg2 != nil {
		db = arg2.(int)
	}

	var hostport, passwd string

	arg1, arg2 = splitOnceGeneric(
		url, "@", opAssign1stTo2nd, opRemovePwdColonPrefix)

	passwd = arg1.(string)
	hostport = arg2.(string)

	arg1, arg2 = splitOnceGeneric(
		hostport, colon, nil, nil)

	port := arg2.(string)
	if port == "" {
		hostport = arg1.(string)
		hostport = fmt.Sprintf("%s%s%s", hostport, colon, defaultPort)
	}

	return &RingNode{
		addr:     scheme + urlNoWeight,
		server:   hostport,
		password: passwd,
		database: db,
		weight:   weight,
	}
}

func opParseInt2nd(arg1, arg2 string) (rs1, rs2 interface{}) {
	p2, err := strconv.Atoi(arg2)
	if err != nil {
		panic(err)
	}
	return arg1, p2
}

func opAssignOnly1st(arg1, arg2 string) (rs1, rs2 interface{}) {
	return arg1, nil
}

func opRemovePwdColonPrefix(arg1, arg2 string) (rs1, rs2 interface{}) {
	if !strings.HasPrefix(arg1, colon) {
		panic("passwd need prefix a colon")
	}
	return arg1[len(colon):], arg2
}

func opAssign1stTo2nd(arg1, arg2 string) (rs1, rs2 interface{}) {
	return "", arg1
}

func splitOnceGeneric(s, sep string, f1, f2 sepRule) (rs1, rs2 interface{}) {
	arr := strings.Split(s, sep)
	if len(arr) == 2 {
		if f2 != nil {
			rs1, rs2 = f2(arr[0], arr[1])
		} else {
			rs1, rs2 = arr[0], arr[1]
		}
	} else if len(arr) == 1 {
		if f1 != nil {
			rs1, rs2 = f1(arr[0], "")
		} else {
			rs1, rs2 = arr[0], ""
		}
	} else {
		panic("impossible reach")
	}
	return rs1, rs2
}
