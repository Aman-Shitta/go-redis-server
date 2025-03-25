package main

import (
	"fmt"
	"net"
	"strings"
)

func processCommand(c string) (func(net.Conn, []string) error, error) {

	switch strings.ToLower(c) {
	case "ping":
		return ping, nil
	case "echo":
		return echo, nil
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
