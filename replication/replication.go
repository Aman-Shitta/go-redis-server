package replication

import (
	"fmt"
	"net"

	"github.com/codecrafters-io/redis-starter-go/utils"
)

// map to command and args
type CommandBuffer map[string][]string

var ReplicaCommands = make(chan CommandBuffer, 100)

type Replica struct {
	Path string
	Port uint
}

// func SendCommand(rep Replica, cargs ...string) error {
// 	// cargs -> command & args
// 	// time.Sleep(1 * time.Second)
// 	c, err := net.Dial("tcp", fmt.Sprintf("%s:%d", rep.Path, rep.Port))
// 	if err != nil {
// 		return nil
// 	}

// 	resp := utils.ToArrayBulkString(cargs...)
// 	fmt.Println("Sending to replica :: ", c, " data :: ", resp)
// 	_, err = c.Write([]byte(resp))

// 	return err

// }

func SendCommand(c net.Conn, cargs ...string) error {

	resp := utils.ToArrayBulkString(cargs...)
	fmt.Println("Sending to replica :: ", c, " data :: ", resp)
	_, err := c.Write([]byte(resp))

	return err

}

func AddCommandToBuffer(command string, args []string) {
	// Queue command for replication
	ReplicaCommands <- CommandBuffer{command: args}
}
