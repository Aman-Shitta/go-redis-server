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

type QueuedCommand struct {
	Name string
	Args []string
}

func (s *RedisServer) HandleConnection(c net.Conn) {
	utils.LogEntry("PINK", s.Role, "[+] Connection started [+]")
	defer func() {
		c.Close()
		utils.LogEntry("PINK", s.Role, "[+] Connection closed [+]")
	}()

	transactions := make(map[net.Conn][]QueuedCommand)

	for {
		var data = make([]byte, 1024)
		n, err := c.Read(data)
		if err != nil {
			fmt.Println(s.Role, " => Error handling request:", err)
			return
		}

		cmd, args, err := utils.ParseResp(data[:n])
		if err != nil {
			c.Write([]byte(fmt.Sprintf("-ERR %s\r\n", err.Error())))
			continue
		}

		command := strings.ToLower(cmd)

		switch command {
		case "multi":
			transactions[c] = []QueuedCommand{}
			c.Write([]byte(utils.ToSimpleString("OK", "OK")))

		case "exec":
			queued, exists := transactions[c]
			if !exists {
				c.Write([]byte("-ERR EXEC without MULTI\r\n"))
				continue
			}
			delete(transactions, c)

			if len(queued) == 0 {
				c.Write([]byte("*0\r\n"))
				continue
			}
			for _, qc := range queued {
				handler, err := s.ProcessCommand(qc.Name)
				if err != nil {
					c.Write([]byte(fmt.Sprintf("-ERR %s\r\n", err.Error())))
					continue
				}

				var resp string
				if qc.Name == "multi" || qc.Name == "psync" {
					resp, err = handler.Execute(c, qc.Args)
				} else {
					resp, err = handler.Execute(qc.Args)
				}

				if err != nil {
					c.Write([]byte(utils.ToSimpleString(err.Error(), "ERR")))
				} else if resp != "" {
					c.Write([]byte(resp))
				}
			}
			c.Write([]byte(utils.ToSimpleString("OK", "OK")))

		default:
			if queued, inTx := transactions[c]; inTx {
				// Inside a transaction, just queue
				transactions[c] = append(queued, QueuedCommand{
					Name: command,
					Args: args,
				})
				c.Write([]byte(utils.ToSimpleString("QUEUED", "")))
			} else {
				// Normal command
				handler, err := s.ProcessCommand(command)
				if err != nil {
					c.Write([]byte(fmt.Sprintf("-ERR %s\r\n", err.Error())))
					continue
				}

				var resp string
				if command == "multi" || command == "psync" {
					resp, err = handler.Execute(c, args)
				} else {
					resp, err = handler.Execute(args)
				}

				if err != nil {
					c.Write([]byte(utils.ToSimpleString(err.Error(), "ERR")))
				} else if resp != "" {
					c.Write([]byte(resp))
				}
			}
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

		handler, err := s.ProcessCommand(c)
		if err != nil {
			fmt.Println("[ERROR] Command processing failed:", err)
			continue
		}

		// _ = fn(rep, args)
		if strings.ToLower(c) == "multi" {
			handler.Execute(rep, args)
		} else {
			handler.Execute(args)
		}

		s.Offset += len(message)
	}
}
