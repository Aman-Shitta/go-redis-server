package replication

import (
	"fmt"
	"net"

	"github.com/codecrafters-io/redis-starter-go/utils"
)

func InitiateHandshake(ip string, port int, replicaPort uint) (net.Conn, error) {
	addr := fmt.Sprintf("%s:%d", ip, port)

	c, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	// Send PING
	ping := utils.ToArrayBulkString("PING")
	c.Write([]byte(ping))
	var d = make([]byte, 1024)
	n, _ := c.Read(d)
	fmt.Println("[DEBUG] PING Response: ", string(d[:n]))

	// Send REPLCONF
	replconf := utils.ToArrayBulkString("REPLCONF", "listening-port", fmt.Sprintf("%d", replicaPort))
	c.Write([]byte(replconf))
	n, _ = c.Read(d)
	fmt.Println("[DEBUG] REPLCONF listening-port Response: ", string(d[:n]))

	replconf = utils.ToArrayBulkString("REPLCONF", "capa", "psync2")
	c.Write([]byte(replconf))
	n, _ = c.Read(d)
	fmt.Println("[DEBUG] REPLCONF capa Response: ", string(d[:n]))

	// Send PSYNC
	psync := utils.ToArrayBulkString("PSYNC", "?", "-1")
	c.Write([]byte(psync))
	n, _ = c.Read(d)
	fmt.Println("[DEBUG] PSYNC Response: ", string(d[:n]))

	return c, nil
}
