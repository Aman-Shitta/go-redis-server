package server

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/replication"
	"github.com/codecrafters-io/redis-starter-go/utils"
)

func (r *RedisServer) ProcessCommand(c string) (func(net.Conn, []string) error, error) {

	fmt.Println(r.Role, " => strings.ToLower(c) :: ", strings.ToLower(c))
	switch strings.ToLower(c) {
	case "ping":
		return r.ping, nil
	case "echo":
		return r.echo, nil
	case "set":
		return r.set, nil
	case "get":
		return r.get, nil
	case "config":
		return r.config, nil
	case "keys":
		return r.keys, nil
	case "info":
		return r.info, nil
	// handshake commands
	case "replconf":
		return r.replconf, nil
	case "psync":
		return r.psync, nil
	case "wait":
		return r.wait, nil
	case "type":
		return r.vtype, nil
	case "xadd":
		return r.xadd, nil
	default:
		utils.LogEntry("crossed", "Default case triggered :: ", c)
		return nil, fmt.Errorf("not yet implemented")
	}
}

var xadd_previous_parsed_id = ""

func (r *RedisServer) xadd(c net.Conn, args []string) error {

	if len(args) < 4 {
		return fmt.Errorf("ERR arguments missing")
	}
	fmt.Println("[DEBUG] xadd args : ", args)
	streamKey := args[0]
	streamID := args[1]
	resp := ""

	fmt.Println("streamID :: ", streamID)
	fmt.Println("==================================================")
	fmt.Println(SessionStore.Data)
	fmt.Println("==================================================")
	// handle auto-increment id generation
	if strings.HasSuffix(streamID, "-*") {
		baseID := strings.TrimSuffix(streamID, "-*")
		SessionStore.Lock()

		item, exists := SessionStore.Data[streamKey]
		if !exists {
			// No previous entries: start at 1
			streamID = baseID + "-1"
		} else {
			prevData, ok := item.Data.(map[string]string)
			if !ok {
				SessionStore.Unlock()
				return fmt.Errorf("ERR invalid stream data")
			}

			prevID := prevData["id"] // e.g. "1-2"
			prevParts := strings.Split(prevID, "-")
			if len(prevParts) != 2 {
				SessionStore.Unlock()
				return fmt.Errorf("ERR invalid stream id format")
			}

			prevBase := prevParts[0]
			prevSeq, err := strconv.Atoi(prevParts[1])
			if err != nil {
				SessionStore.Unlock()
				return fmt.Errorf("ERR invalid sequence number")
			}

			if baseID != prevBase {
				// New base ID (like moving from 1-* to 2-*)
				streamID = baseID + "-0"
			} else {
				// Same base, increment sequence
				prevSeq++
				streamID = baseID + "-" + strconv.Itoa(prevSeq)
			}
		}
		SessionStore.Unlock()
	}

	// addedd check for validating
	// entry Ids
	if xadd_previous_parsed_id != "" {
		// Parse previous ID
		prevParts := strings.Split(xadd_previous_parsed_id, "-")
		if len(prevParts) != 2 {
			return fmt.Errorf("ERR invalid previous ID format")
		}
		prevMs, _ := strconv.Atoi(prevParts[0])
		prevSeq, _ := strconv.Atoi(prevParts[1])

		// Parse current incoming ID
		currParts := strings.Split(streamID, "-")
		if len(currParts) != 2 {
			return fmt.Errorf("ERR invalid current ID format")
		}
		currMs, _ := strconv.Atoi(currParts[0])
		currSeq, _ := strconv.Atoi(currParts[1])

		// Check against 0-0
		if currMs == 0 && currSeq == 0 {
			return fmt.Errorf("ERR The ID specified in XADD must be greater than 0-0")
		}

		// Check if smaller or equal to previous
		if currMs < prevMs || (currMs == prevMs && currSeq <= prevSeq) {
			return fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
		}
	}

	data := map[string]string{
		"id": streamID,
	}

	i := 2
	for i < len(args) {
		key := args[i]
		i++
		value := args[i]
		i++
		data[key] = value
	}

	SessionStore.Lock()
	SessionStore.Data[streamKey] = Item{Data: data, Type: "stream"}
	SessionStore.Unlock()

	resp = utils.ToBulkString(streamID)

	xadd_previous_parsed_id = streamID
	c.Write([]byte(resp))

	return nil
}

// func (r *RedisServer) wait(c net.Conn, args []string) error {
// 	connectedReplicas := len(r.replicas)
// 	resp := utils.ToInteger(connectedReplicas)

// 	c.Write([]byte(resp))
// 	return nil
// }

func (s *RedisServer) wait(conn net.Conn, args []string) error {
	if len(args) != 2 {
		return fmt.Errorf("ERR wrong number of arguments for 'WAIT' command")
	}
	connected_replicas := s.replicas

	fmt.Println("[DEBUG] wait args :: ", args)
	// Parse arguments
	xReplicas, err := strconv.Atoi(args[0])
	if err != nil || xReplicas < 0 {
		return fmt.Errorf("ERR invalid number of replicas")
	}

	timeout, err := strconv.Atoi(args[1])
	if err != nil || timeout < 0 {
		return fmt.Errorf("ERR invalid timeout")
	}

	fmt.Println("[DEBUG] s.PendingWrites :: ", s.PendingWrites)
	acknowledged := 0

	if s.PendingWrites != 0 {
		// Wait for replicas to acknowledge
		acknowledged = s.waitForReplicas(xReplicas, timeout)
	}

	// Clear acknowledgments after WAIT completes
	s.Lock()
	for _, replica := range s.replicas {
		replication.ClearAcknowledgment(replica)
	}
	s.Unlock()

	fmt.Println("[DEBUG] acknlowledged was : ", acknowledged)
	fmt.Println("[DEBUG] xReplicas was : ", xReplicas)
	fmt.Println("[DEBUG] connected_replicas was : ", len(connected_replicas))
	if s.PendingWrites != 0 && acknowledged == 0 {
		acknowledged = len(connected_replicas)
	} else if acknowledged > xReplicas {
		acknowledged = xReplicas
	}

	// Send the number of replicas that acknowledged
	conn.Write([]byte(utils.ToInteger(acknowledged)))
	fmt.Println("Written acknowledge")
	return nil
}

func (r *RedisServer) replconf(c net.Conn, args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("ERR wrong number of arguments for REPLCONF")
	}
	fmt.Println("replconf args :: ", args, r.Role)
	// default response
	resp := utils.ToSimpleString("OK", "OK")

	switch strings.ToLower(args[0]) {
	case "listening-port":
		port, err := strconv.Atoi(args[1])
		if err != nil || port < 0 || port > 65535 {
			return fmt.Errorf("ERR invalid port number")
		}

	case "capa":

		if strings.ToLower(args[1]) != "eof" && strings.ToLower(args[1]) != "psync2" {
			return fmt.Errorf("ERR invalid value for capa")
		}

	case "getack":
		if r.Role == "slave" {
			resp = utils.ToArrayBulkString("REPLCONF", "ACK", strconv.Itoa(r.Offset))
		}
	case "ack":
		fmt.Println("[DEBUG]  GET ACK KE LIYE : ", resp)

	default:
		// return nil
		return fmt.Errorf("ERR unknown REPLCONF parameter: %s", args[0])
	}

	fmt.Println("Sending : ", resp)
	_, err := c.Write([]byte(resp))
	return err
}

func (r *RedisServer) psync(c net.Conn, args []string) error {
	if len(args) != 2 {
		return fmt.Errorf("ERR wrong number of arguments for PSYNC")
	}

	// Validate replication ID ("?" or actual ID)
	if args[0] != "?" && args[0] != r.MasterReplicationID {
		return fmt.Errorf("ERR invalid MasterReplicationID")
	}

	// Send FULLRESYNC response
	resp := utils.ToSimpleString(fmt.Sprintf("FULLRESYNC %s 0", r.MasterReplicationID), "OK")
	_, err := c.Write([]byte(resp))

	if err != nil {
		return err
	}

	// send rdb file contents
	content, err := os.ReadFile("empty.rdb")
	if err != nil {
		return err
	}

	resp = fmt.Sprintf("$%d\r\n%s", len(content), content)
	c.Write([]byte(resp))

	r.Lock()
	r.replicas = append(r.replicas, c)
	r.Unlock()

	return err
}

func (r *RedisServer) info(c net.Conn, args []string) error {

	if len(args) != 0 && strings.ToLower(args[0]) != "replication" {
		return fmt.Errorf("wrong argument : %s", args[0])
	}
	role := fmt.Sprintf("role:%s", r.Role)
	master_replid := fmt.Sprintf("master_replid:%s", r.MasterReplicationID)
	master_repl_offset := fmt.Sprintf("master_repl_offset:%d", r.MasterReplicationOffset)

	resp := utils.ToBulkString(fmt.Sprintf("%s\n%s\n%s\n", role, master_replid, master_repl_offset))
	// resp := utils.ToBulkString(role, master_replid, master_repl_offset)
	c.Write([]byte(resp))
	return nil
}

func (r *RedisServer) config(c net.Conn, args []string) error {
	if len(args) != 2 || strings.ToLower(args[0]) != "get" {
		return fmt.Errorf("ERR not yet supported")
	}

	var resp string

	switch strings.ToLower(args[1]) {
	case "dir":
		dir := r.Cnf.Dir
		resp = utils.ToArrayBulkString([]string{"dir", dir}...)
	case "dbfilename":
		dbFileName := r.Cnf.Dbfilename
		resp = utils.ToArrayBulkString([]string{"dbFileName", dbFileName}...)
	}

	_, err := c.Write([]byte(resp))

	return err
}

func (r *RedisServer) ping(c net.Conn, args []string) error {
	c.Write([]byte("+PONG\r\n"))
	return nil
}

func (r *RedisServer) echo(c net.Conn, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("ECHO requires an argument")
	}

	response := strings.Join(args, " ")
	resp := utils.ToSimpleString(response, "OK")
	c.Write([]byte(resp))

	return nil
}

func (r *RedisServer) set(c net.Conn, args []string) error {

	if len(args) < 2 {
		return fmt.Errorf("syntax error")
	}

	key := args[0]
	value := args[1]

	SessionStore.Lock()
	defer SessionStore.Unlock()

	vtype, _ := utils.CheckValueType(value)
	SessionStore.Data[key] = Item{Data: value, Type: vtype}

	// Handle PX (expiry in milliseconds)
	if len(args) > 2 {
		if len(args) != 4 || strings.ToLower(args[2]) != "px" {
			return fmt.Errorf("syntax error")
		}

		expiry, err := strconv.Atoi(args[3])
		fmt.Println("expiry :: ", expiry)
		if err != nil || expiry < 0 {
			return fmt.Errorf("invalid PX value")
		} else {

			// Launch expiration goroutine
			go func(key string, exp int) {

				time.Sleep(time.Duration(exp) * time.Millisecond)
				SessionStore.Lock()
				delete(SessionStore.Data, key)

				// Only delete if the key is still the same value
				if v, exists := SessionStore.Data[key]; exists && v.Data == value {
					delete(SessionStore.Data, key)
				}
				SessionStore.Unlock()
			}(key, expiry)
		}
	}

	if r.Role == "master" {
		c.Write([]byte(utils.ToSimpleString("OK", "OK")))

		fmt.Println("Command added to buffer :: ", "SET", args)
		// Add command to replication buffer
		replication.AddCommandToBuffer("SET", args)
		r.PendingWrites++

	}

	return nil
}

func (r *RedisServer) get(c net.Conn, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("GET requires an argument")
	}
	fmt.Println(r.Role, " => store:  ", SessionStore.Data)
	response, ok := SessionStore.Data[args[0]]
	expiry, exists := ExpKeys[args[0]]
	if ok && (!exists || time.Now().Compare(expiry) < 0) {
		resp := utils.ToBulkString(response.Data.(string))
		c.Write([]byte(resp))
	} else {
		c.Write([]byte("$-1\r\n"))
	}

	return nil
}

func (r *RedisServer) vtype(c net.Conn, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("TYPE requires an argument")
	}
	t := "none"
	fmt.Println(r.Role, " => store:  ", SessionStore.Data)
	response, ok := SessionStore.Data[args[0]]
	expiry, exists := ExpKeys[args[0]]
	if ok && (!exists || time.Now().Compare(expiry) < 0) {

		t = response.Type

	}
	c.Write([]byte(utils.ToSimpleString(t, "OK")))

	return nil
}

func (r *RedisServer) keys(c net.Conn, args []string) error {

	if len(args) == 0 {
		return fmt.Errorf("ERR not yet supported")
	}

	allKeys := make([]string, 0, len(SessionStore.Data))
	for k := range SessionStore.Data {
		allKeys = append(allKeys, k)
	}
	var resp string
	if args[0] == "*" {
		resp = utils.ToArrayBulkString(allKeys...)
	} else {
		filteredKeys, err := utils.MatchPatternKeys(allKeys, args[0])
		if err != nil {
			return err
		}
		resp = utils.ToArrayBulkString(filteredKeys...)
	}

	_, err := c.Write([]byte(resp))
	return err
}
