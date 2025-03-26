package main

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
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
	case "config":
		return config, nil
	default:
		fmt.Println("Default case triggered :: ", c)
		return nil, fmt.Errorf("not yet implemented")
	}
}

func config(c net.Conn, args []string) error {
	if len(args) != 2 || strings.ToLower(args[0]) != "get" {
		return fmt.Errorf("ERR not yet supported")
	}

	var resp string

	switch strings.ToLower(args[1]) {
	case "dir":
		dir := PERSISTENT_CONFIG["dir"]
		resp = fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len("dir"), "dir", len(dir), dir)
	case "dbfilename":
		dbFileName := PERSISTENT_CONFIG["dbFileName"]
		resp = fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len("dbFileName"), "dbFileName", len(dbFileName), dbFileName)
	}

	_, err := c.Write([]byte(resp))

	return err
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

	if len(args) < 2 {
		return fmt.Errorf("syntax error")
	}
	key := args[0]
	value := args[1]

	sessionData.Lock()
	defer sessionData.Unlock()
	sessionData.Data[key] = value

	// Handle PX (expiry in milliseconds)
	if len(args) > 2 {
		if len(args) != 4 || strings.ToLower(args[2]) != "px" {
			return fmt.Errorf("syntax error")
		}

		expiry, err := strconv.Atoi(args[3])
		fmt.Println("expiry :: ", expiry)
		if err != nil || expiry < 0 {
			return fmt.Errorf("invalid PX value")
		}

		// Launch expiration goroutine
		go func(key string, exp int) {

			time.Sleep(time.Duration(exp) * time.Millisecond)
			sessionData.Lock()
			delete(sessionData.Data, key)

			// Only delete if the key is still the same value
			if v, exists := sessionData.Data[key]; exists && v == value {
				delete(sessionData.Data, key)
			}
			sessionData.Unlock()
		}(key, expiry)
	}

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
