package server

import (
	"fmt"
	"net"

	"github.com/codecrafters-io/redis-starter-go/replication"
	"github.com/codecrafters-io/redis-starter-go/utils"
)

type ServerInterface interface {
	Start(uint) (net.Listener, error)
}

type RedisServer struct {
	Cnf                     *Config
	Role                    string
	MasterReplicationID     string
	MasterReplicationOffset int
	replicas                []net.Conn
}

func NewRedisServer() *RedisServer {
	return &RedisServer{
		Cnf:                     NewConfig(),
		Role:                    "master",
		MasterReplicationID:     utils.GenerateRandomReplID(20),
		MasterReplicationOffset: 0,
	}
}

func (r *RedisServer) UpdateRole(role string) {
	r.Role = role
}

// start the server on supplied port
func (s *RedisServer) Start(port uint) (net.Listener, error) {

	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))
	if err != nil {
		return nil, fmt.Errorf("failed to bind to port %d", port)
	}

	return l, nil
}

func (s *RedisServer) HandleConnection(c net.Conn) {
	defer func(c net.Conn) {
		c.Close()
		fmt.Println("[-]Connection Closed [-]")
	}(c)

	for {
		var data = make([]byte, 1024)
		_, err := c.Read(data)

		if err != nil {
			fmt.Println("Error handling requests : ", err.Error())
			return
		}

		command, args, err := utils.ParseResp(data)

		if err != nil {
			c.Write([]byte(fmt.Sprintf("-ERR %s\r\n", err.Error())))
			continue
		}

		fn, err := s.ProcessCommand(command)

		if err != nil {
			c.Write([]byte(fmt.Sprintf("-ERR %s\r\n", err.Error())))
			continue
		}

		err = fn(c, args)

		if err != nil {
			// c.Write([]byte(fmt.Sprintf("-ERR %s\r\n", err.Error())))
			response := utils.ToSimpleString(err.Error(), "ERR")
			c.Write([]byte(response))

			continue
		}
	}
}

func (s *RedisServer) AddReplica(c net.Conn) {
	s.replicas = append(s.replicas, c)
}

// func (s *RedisServer) SyncReplica() {
// 	for command := range replication.ReplicaCommands {
// 		for _, replica := range s.replicas {
// 			for cmd, args := range command {
// 				go replication.SendCommand(append([]string{cmd}, args...), replica)
// 			}
// 		}
// 	}
// }

func (s *RedisServer) SyncReplica() {
	const workerLimit = 2                   // Limit concurrent replication workers
	sem := make(chan struct{}, workerLimit) // Semaphore to control goroutine count

	for command := range replication.ReplicaCommands {
		for _, replica := range s.replicas {
			for cmd, args := range command {
				sem <- struct{}{} // Acquire a slot

				go func(replica net.Conn, cmd string, args []string) {
					defer func() { <-sem }() // Release the slot

					if err := replication.SendCommand(append([]string{cmd}, args...), replica); err != nil {
						// Handle errors (log, retry, remove dead replica, etc.)
						fmt.Println("Error sending command to replica:", err)
					}
				}(replica, cmd, args)
			}
		}
	}
}
