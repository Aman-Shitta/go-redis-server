package replication

import (
	"fmt"
	"net"

	"github.com/codecrafters-io/redis-starter-go/utils"
)

func InitiateHandshake(ip string, port int, replicaPort uint) (net.Conn, error) {
	addr := fmt.Sprintf("%s:%d", ip, port)

	c, err := net.Dial("tcp", addr)

	fmt.Println("Just so you know I am listeding on :: ", c.LocalAddr().String(), " for : ", c.RemoteAddr().String())
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

	// time.Sleep(time.Second * 1)
	// n, _ = c.Read(d) // Read response from master
	// fmt.Println("[DEBUG] PSYNC Response: ", string(d[:n]), "||||")

	// n, _ = c.Read(d) // Read response from master
	// // Read the RDB file before handling further commands
	// fmt.Println("[DEBUG] Read RDB:", d[:n])

	// // Now the connection is ready to receive further replication commands
	// n, _ = c.Read(d) // This will now read the next command (e.g., REPLCONF GETACK)
	// fmt.Println("[DEBUG] Received after RDB:", string(d[:n]))

	return c, nil
}
