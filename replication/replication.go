package replication

import (
	"net"

	"github.com/codecrafters-io/redis-starter-go/utils"
)

// map to command and args
type CommandBuffer map[string][]string

var ReplicaCommands = make(chan CommandBuffer, 100)

func SendCommand(cargs []string, c net.Conn) error {
	// cargs -> command & args

	resp := utils.ToArrayBulkString(cargs...)

	_, err := c.Write([]byte(resp))

	return err

}

func AddCommandToBuffer(command string, args []string) {
	// Queue command for replication
	ReplicaCommands <- CommandBuffer{command: args}
}
