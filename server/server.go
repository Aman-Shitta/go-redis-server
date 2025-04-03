package server

import (
	"fmt"
	"net"
	"sync"

	"github.com/codecrafters-io/redis-starter-go/replication"
	"github.com/codecrafters-io/redis-starter-go/utils"
)

type ServerInterface interface {
	Start(uint) (net.Listener, error)
}

type RedisServer struct {
	sync.Mutex
	Cnf                     *Config
	Role                    string
	MasterReplicationID     string
	MasterReplicationOffset int
	// replicas                []replication.Replica
	replicas []net.Conn
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
	utils.LogEntry("PINK", s.Role, "[+] ------------------Connection started ------------------ [+]")
	defer func(c net.Conn) {
		c.Close()
		// fmt.Printf("%s => [-]Connection Closed [-]\n", s.Role)
		utils.LogEntry("PINK", s.Role, "[+] ------------------Connection closed ------------------ [+]")
	}(c)

	for {
		var data = make([]byte, 1024)
		_, err := c.Read(data)

		if err != nil {
			fmt.Println(s.Role, " => Error handling requests : ", err.Error())
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

// func (s *RedisServer) AddReplica(path string, port uint) {
// 	s.replicas = append(s.replicas, replication.Replica{Path: path, Port: port})
// 	fmt.Println("Replica dded :: ", s.replicas)
// }

func (s *RedisServer) AddReplica(c net.Conn) {

	s.replicas = append(s.replicas, c)
}

func (s *RedisServer) SyncReplica() {

	fmt.Println("replication.ReplicaCommands :: ", replication.ReplicaCommands)

	for command := range replication.ReplicaCommands {
		for _, replica := range s.replicas {
			fmt.Println("replica :: ", replica)
			for cmd, args := range command {
				fmt.Println("cmd, args :: ", cmd, args)
				if err := replication.SendCommand(replica, append([]string{cmd}, args...)...); err != nil {
					// Handle errors (log, retry, remove dead replica, etc.)
					fmt.Println("Error sending command to replica:", err)
				}
			}
		}
	}
}

func (s *RedisServer) PropogateCommands(rep net.Conn) {

	defer rep.Close()

	if s.Role != "slave" {
		fmt.Println("[ERROR] SyncReplica should only run on a slave instance")
		return
	}

	buffer := make([]byte, 4096) // Read buffer

	for {
		n, err := rep.Read(buffer)
		if err != nil {
			fmt.Println("[ERROR] Master connection lost:", err)
			break
		}

		data := buffer[:n]

		// skip if read data
		if data[0] != '*' {
			continue
		}

		// for len(data) > 0 {
		fmt.Println("data :: ", data)

		command_item, err := utils.ParseRESPCommands(data)
		if err != nil {
			fmt.Println("Cannot procees the data to get commands :: ", data)
			continue
		}

		fmt.Println("command_item :: ", command_item)

		if len(command_item) == 0 {
			continue
		}

		for _, command := range command_item {
			// Parse and execute the command
			c, args, err := utils.ParseResp([]byte(command))
			if err != nil {
				fmt.Println("[ERROR] Failed to parse command:", err)
				continue
			}

			fn, err := s.ProcessCommand(c)
			if err != nil {
				fmt.Println("[ERROR] Command processing failed:", err)
				continue
			}

			_ = fn(rep, args) // Execute the command function
		}
	}
}
