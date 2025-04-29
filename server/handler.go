package server

import (
	"fmt"
	"net"
)

type CommandHandler interface {
	Execute(args ...any) (string, error)
}

type ArgHandler func(args []string) (string, error)
type MultiHandler func(conn net.Conn, args []string) (string, error)

type argHandlerWrapper struct {
	handler ArgHandler
}

func (a argHandlerWrapper) Execute(args ...any) (string, error) {
	strArgs, ok := args[0].([]string)
	if !ok {
		return "", fmt.Errorf("invalid argument type")
	}
	return a.handler(strArgs)
}

type multiHandlerWrapper struct {
	handler MultiHandler
}

func (m multiHandlerWrapper) Execute(args ...any) (string, error) {
	conn, ok1 := args[0].(net.Conn)
	strArgs, ok2 := args[1].([]string)
	if !ok1 || !ok2 {
		return "", fmt.Errorf("invalid argument types for MULTI")
	}
	return m.handler(conn, strArgs)
}
