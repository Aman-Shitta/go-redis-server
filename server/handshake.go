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
	replconf := utils.ToArrayBulkString("REPLCONF", "listening-port", "6380")
	fmt.Println("replconf :: ", replconf)
	c.Write([]byte(replconf))

	replconf = utils.ToArrayBulkString("REPLCONF", "capa", "psync2")
	fmt.Println("replconf :: ", replconf)
	c.Write([]byte(replconf))

	return nil
}
