package server

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

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
	replicas      []net.Conn
	Offset        int
	PendingWrites int
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

func (s *RedisServer) AddReplica(c net.Conn) {

	s.replicas = append(s.replicas, c)
}

func (s *RedisServer) waitForReplicas(numReplicas int, timeout int) int {
	acknowledged := 0
	start := time.Now()

	for {
		s.Lock()
		for _, replica := range s.replicas {

			if s.PendingWrites == 0 {
				return acknowledged
			}

			// Check if the replica has acknowledged (placeholder logic)
			if replication.HasAcknowledged(replica) {
				acknowledged++
			}
		}
		s.Unlock()

		if acknowledged >= numReplicas || time.Since(start).Milliseconds() >= int64(timeout) {
			break
		}

		// Sleep briefly to avoid busy-waiting
		time.Sleep(10 * time.Millisecond)
	}

	return acknowledged
}

func (s *RedisServer) SyncReplica() {

	fmt.Println("replication.ReplicaCommands :: ", replication.ReplicaCommands)

	for command := range replication.ReplicaCommands {
		for _, replica := range s.replicas {
			fmt.Println("replica :: ", replica.RemoteAddr().String())
			for cmd, args := range command {
				fmt.Println("cmd, args :: ", cmd, args)
				if err := replication.SendCommand(replica, append([]string{cmd}, args...)...); err != nil {
					// Handle errors (log, retry, remove dead replica, etc.)
					fmt.Println("Error sending command to replica:", err)
				} else {
					// handle the command success run
					// Mark the replica as having acknowledged the command
					fmt.Println("acknowledgedMarked : ", replica)
					replication.MarkAcknowledged(replica)
				}
			}
		}
	}
}

func (s *RedisServer) ProcessPropogatedCommands(rep net.Conn) {

	defer func() {
		rep.Close()
		fmt.Println("-----------------------------------Closed----------------------------------------")
	}()

	if s.Role != "slave" {
		fmt.Println("[ERROR] SyncReplica should only run on a slave instance")
		return
	}
	x := 0
	reader := bufio.NewReader(rep)
	readingRDB := false
	// k := true
	i := 0
	for {
		fmt.Println("==================================================", i)
		// time.Sleep(1 * time.Second)
		message, err := utils.ReadRESPMessage(reader, readingRDB)
		// pass readingRDB to determine +2 or not

		if err != nil {
			if err == io.EOF && x > 10 {
				fmt.Println("[ERROR] Connection closed or read failed:", err)
				return
			}
			// } else {
			// continue
			// }
		}

		// check if this is the FULLRESYNC
		if strings.HasPrefix(string(message), "+FULLRESYNC") {
			readingRDB = true
		} else if readingRDB {
			// After reading RDB, next command resets
			readingRDB = false
		}

		fmt.Println(i, "Message: ", string(message), len(string(message)))

		i++
		// if message[0] == '*' {
		c, args, err := utils.ParseResp(message)
		fmt.Println("Command : ", c)
		fmt.Println("args : ", args)

		fmt.Println("==================================================", i)

		if err != nil {
			fmt.Println("[ERROR] Failed to parse command", err)
			continue
		}

		fmt.Println("==============================OFFSET = ", s.Offset)

		// SKIP Responding to ping
		if strings.ToLower(c) == "ping" {
			s.Offset += len(message)
			continue
		}

		fn, err := s.ProcessCommand(c)
		if err != nil {
			fmt.Println("[ERROR] Command processing failed:", err)
			continue
		}

		_ = fn(rep, args)
		s.Offset += len(message)
	}
}
