package server

import (
	"fmt"
	"net"

	"github.com/codecrafters-io/redis-starter-go/utils"
)

func InitiateHandshake(ip string, port int) error {

	addr := fmt.Sprintf("%s:%d", ip, port)

	c, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}

	// send PING to master
	ping := utils.ToArrayBulkString("PING")
	c.Write([]byte(ping))

	// send REPLCONF
	replconf := utils.ToArrayBulkString("REPLFCONF", "6380")
	c.Write([]byte(replconf))

	replconf = utils.ToArrayBulkString("REPLCONF", "capa", "psync2")
	c.Write([]byte(replconf))

	return nil
}
