package replication

import (
	"fmt"
	"net"

	"github.com/codecrafters-io/redis-starter-go/utils"
)

func InitiateHandshake(ip string, port int, replicaPort uint) error {

	addr := fmt.Sprintf("%s:%d", ip, port)

	c, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}
	// defer c.Close()

	// send PING to master
	ping := utils.ToArrayBulkString("PING")
	c.Write([]byte(ping))
	var d = make([]byte, 1024)
	c.Read(d)
	// send REPLCONF

	replconf := utils.ToArrayBulkString("REPLCONF", "listening-port", fmt.Sprintf("%d", replicaPort))
	c.Write([]byte(replconf))
	c.Read(d)

	replconf = utils.ToArrayBulkString("REPLCONF", "capa", "psync2")
	c.Write([]byte(replconf))
	c.Read(d)

	psync := utils.ToArrayBulkString("PSYNC", "?", "-1")
	c.Write([]byte(psync))
	n, _ := c.Read(d)
	fmt.Println(d[:n])

	return nil
}
