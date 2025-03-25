package main

import (
	"fmt"
	"net"
	"strings"
	"sync"
)

func processCommand(c string) (func(net.Conn, []string) error, error) {

	switch strings.ToLower(c) {
	case "ping":
		return ping, nil
	case "echo":
		return echo, nil
	case "set":
		return set, nil
	case "get":
		return get, nil
	default:
		fmt.Println("Default case triggered :: ", c)
		return nil, fmt.Errorf("not yet implemented")
	}
}

func ping(c net.Conn, args []string) error {
	c.Write([]byte("+PONG\r\n"))
	return nil
}

func echo(c net.Conn, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("ECHO requires an argument")
	}

	response := strings.Join(args, " ")
	resp := fmt.Sprintf("$%d\r\n%s\r\n", len(response), response)
	fmt.Println("resp :: ", resp)
	c.Write([]byte(resp))

	return nil
}

type Map struct {
	sync.Mutex
	Data map[string]string
}

var sessionData = &Map{
	Data: make(map[string]string),
}

func set(c net.Conn, args []string) error {

	if len(args) != 2 {
		return fmt.Errorf("ERR syntax error")
	}
	key := args[0]
	value := args[1]

	sessionData.Lock()
	defer sessionData.Unlock()
	sessionData.Data[key] = value

	c.Write([]byte("+OK\r\n"))

	return nil
}

func get(c net.Conn, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("GET requires an argument")
	}

	response, ok := sessionData.Data[args[0]]
	if ok {
		c.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(response), response)))
	} else {
		c.Write([]byte("$-1\r\n"))
	}

	return nil
}
