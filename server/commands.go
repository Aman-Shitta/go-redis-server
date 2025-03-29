package server

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/utils"
)

func (r *RedisServer) ProcessCommand(c string) (func(net.Conn, []string) error, error) {

	switch strings.ToLower(c) {
	case "ping":
		return r.ping, nil
	case "echo":
		return r.echo, nil
	case "set":
		return r.set, nil
	case "get":
		return r.get, nil
	case "config":
		return r.config, nil
	case "keys":
		return r.keys, nil
	default:
		utils.LogEntry("blink", "Default case triggered :: ", c)
		return nil, fmt.Errorf("not yet implemented")
	}
}

func (r *RedisServer) config(c net.Conn, args []string) error {
	if len(args) != 2 || strings.ToLower(args[0]) != "get" {
		return fmt.Errorf("ERR not yet supported")
	}

	var resp string

	switch strings.ToLower(args[1]) {
	case "dir":
		dir := r.Cnf.Dir
		resp = utils.ToArrayBulkString([]string{"dir", dir}...)

		// resp = fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len("dir"), "dir", len(dir), dir)
	case "dbfilename":
		dbFileName := r.Cnf.Dbfilename
		resp = utils.ToArrayBulkString([]string{"dbFileName", dbFileName}...)
		// resp = fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len("dbFileName"), "dbFileName", len(dbFileName), dbFileName)
	}

	_, err := c.Write([]byte(resp))

	return err
}

func (r *RedisServer) ping(c net.Conn, args []string) error {
	c.Write([]byte("+PONG\r\n"))
	return nil
}

func (r *RedisServer) echo(c net.Conn, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("ECHO requires an argument")
	}

	response := strings.Join(args, " ")
	resp := utils.ToSimpleString(response, "OK")
	// resp := fmt.Sprintf("$%d\r\n%s\r\n", len(response), response)
	fmt.Println("resp :: ", resp)
	c.Write([]byte(resp))

	return nil
}

func (r *RedisServer) set(c net.Conn, args []string) error {

	if len(args) < 2 {
		return fmt.Errorf("syntax error")
	}
	key := args[0]
	value := args[1]

	SessionStore.Lock()
	defer SessionStore.Unlock()
	SessionStore.Data[key] = value

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
			SessionStore.Lock()
			delete(SessionStore.Data, key)

			// Only delete if the key is still the same value
			if v, exists := SessionStore.Data[key]; exists && v == value {
				delete(SessionStore.Data, key)
			}
			SessionStore.Unlock()
		}(key, expiry)
	}

	c.Write([]byte(utils.ToSimpleString("OK", "OK")))

	return nil
}

func (r *RedisServer) get(c net.Conn, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("GET requires an argument")
	}

	response, ok := SessionStore.Data[args[0]]
	if ok && time.Now().Compare(ExpKeys[response]) <= 0 {
		resp := utils.ToBulkString([]string{response}...)
		// c.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(response), response)))
		c.Write([]byte(resp))
	} else {
		c.Write([]byte("$-1\r\n"))
	}

	return nil
}

func (r *RedisServer) keys(c net.Conn, args []string) error {

	if len(args) == 0 {
		return fmt.Errorf("ERR not yet supported")
	}

	allKeys := make([]string, 0, len(SessionStore.Data))
	for k := range SessionStore.Data {
		allKeys = append(allKeys, k)
	}
	var resp string
	if args[0] == "*" {
		resp = utils.ToArrayBulkString(allKeys...)
	} else {
		filteredKeys, err := utils.MatchPatternKeys(allKeys, args[0])
		if err != nil {
			return err
		}
		resp = utils.ToArrayBulkString(filteredKeys...)
	}

	_, err := c.Write([]byte(resp))
	return err
}
