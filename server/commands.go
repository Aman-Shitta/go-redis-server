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
	case "xrange":
		return r.xrange, nil
	case "xread":
		return r.xread, nil
	case "incr":
		return r.incr, nil
	default:
		utils.LogEntry("crossed", "Default case triggered :: ", c)
		return nil, fmt.Errorf("not yet implemented")
	}
}

func (r *RedisServer) incr(c net.Conn, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("GET requires an argument")
	}
	fmt.Println(r.Role, " => store:  ", SessionStore.Data)
	response, ok := SessionStore.Data[args[0]]

	fmt.Println("response :: ", response)
	fmt.Println("response ok :: ", ok)
	value := 1
	if ok {

		if response.Type != "integer" {
			return fmt.Errorf("ERR value is not an integer or out of range")
		}
		value, _ = strconv.Atoi(response.Data.(string))
		value++
	}

	SessionStore.Data[args[0]] = Item{Data: fmt.Sprintf("%d", value), Type: "integer"}

	resp := utils.ToInteger(value)
	_, err := c.Write([]byte(resp))

	return err

}

// helper fuction for XREAD
func deduceStreamParams(args ...string) (map[string]string, error) {

	var res = make(map[string]string)

	totalArgs := len(args)
	n := totalArgs / 2
	if totalArgs%2 != 0 {
		return nil, fmt.Errorf("wrong param count")
	}

	for i := range n {
		res[args[i]] = args[n+i]
	}

	return res, nil

}

func findMaxID(data []map[string]string) string {

	maxIDBaseInt := 0
	maxIDSeqInt := 0

	for _, item := range data {

		itemParts := strings.Split(item["id"], "-")
		itemBase := itemParts[0]

		itemBaseInt, _ := strconv.Atoi(itemBase)

		itemSeq := itemParts[len(itemParts)-1]
		itemSeqInt, _ := strconv.Atoi(itemSeq)

		if maxIDBaseInt <= itemBaseInt {
			if maxIDSeqInt <= itemSeqInt {
				maxIDBaseInt = itemBaseInt
				maxIDSeqInt = itemSeqInt
			}
		}
	}

	return fmt.Sprintf("%d-%d", maxIDBaseInt, maxIDSeqInt)
}

var ZeroBlockChan = make(chan map[string]bool)

func (r *RedisServer) xread(c net.Conn, args []string) error {

	fmt.Println("args : ", args)
	if len(args) < 3 {
		return fmt.Errorf("ERR arguments missing")
	}

	var streamParams map[string]string
	var err error

	if strings.ToLower(args[0]) == "streams" {

		streamParams, err = deduceStreamParams(args[1:]...)

		if err != nil {
			return err
		}
	}

	blockUnitilInt := -1

	if strings.ToLower(args[0]) == "block" {

		blockUnitil := args[1]
		blockUnitilInt, err = strconv.Atoi(blockUnitil)
		if err != nil {
			return fmt.Errorf("ERR block time wrong")
		}

		if strings.ToLower(args[2]) != "streams" {
			return fmt.Errorf("ERR wrong type ecpected streams option")
		}
		streamParams, err = deduceStreamParams(args[3:]...)

		if err != nil {
			return err
		}

	}

	for streamKey, itemKey := range streamParams {

		SessionStore.Lock()
		storedData := SessionStore.Data[streamKey]
		SessionStore.Unlock()
		if storedData.Type != "stream" {
			return fmt.Errorf("ERR item is not stream type")
		}

		storedItems := storedData.Data.([]map[string]string)

		if itemKey == "$" {
			ik := findMaxID(storedItems)
			streamParams[streamKey] = ik
			fmt.Println("[DEBUG] ----------------------- NEW KEy is :: ", itemKey)
		}

	}

	fmt.Println("[DEBUG] Paused until :: ", blockUnitilInt)
	// handle indefinite block

	if blockUnitilInt > 0 {
		time.Sleep(time.Millisecond * time.Duration(blockUnitilInt))
	} else if blockUnitilInt == 0 {
		// block connection for idefinite time until required key is found
		func() {
		outer:
			for {
				time.Sleep(time.Millisecond * time.Duration(1000))

				for streamKey, itemKey := range streamParams {
					SessionStore.Lock()
					storedData := SessionStore.Data[streamKey]
					SessionStore.Unlock()

					storedItems := storedData.Data.([]map[string]string)

					fmt.Println("storedItems :: ", storedItems)
					for _, item := range storedItems {

						itemParts := strings.Split(item["id"], "-")

						itemBase := itemParts[0]
						itemSeq := itemParts[len(itemParts)-1]

						itemBaseInt, _ := strconv.Atoi(itemBase)
						itemSeqInt, _ := strconv.Atoi(itemSeq)

						itemKeyParts := strings.Split(itemKey, "-")

						itemKeyBase := itemKeyParts[0]
						itemKeyBaseInt, _ := strconv.Atoi(itemKeyBase)

						itemKeySeq := itemKeyParts[len(itemKeyParts)-1]
						itemKeySeqInt, _ := strconv.Atoi(itemKeySeq)

						if itemBaseInt == itemKeyBaseInt && itemKeySeqInt < itemSeqInt {
							break outer
						}
					}
				}
			}
		}()
	}

	streamKeysCount := 0
	resp := ""

	fmt.Println("streamParams :: ", streamParams)
	for streamKey, itemKey := range streamParams {
		// streamKey := args[1]
		streamKeysCount++

		// itemKey := args[2]
		SessionStore.Lock()
		storedData := SessionStore.Data[streamKey]
		SessionStore.Unlock()
		if storedData.Type != "stream" {
			return fmt.Errorf("ERR item is not stream type")
		}

		storedItems := storedData.Data.([]map[string]string)

		// if itemKey == "$" {
		// 	itemKey = findMaxID(storedItems)
		// 	fmt.Println("[DEBUG] ----------------------- NEW KEy is :: ", itemKey)
		// }

		ix := 0

		respx := ""

		// var levelArrs []string
		for _, item := range storedItems {

			var key string
			var values []string

			itemParts := strings.Split(item["id"], "-")

			itemBase := itemParts[0]
			itemSeq := itemParts[len(itemParts)-1]

			itemBaseInt, _ := strconv.Atoi(itemBase)
			itemSeqInt, _ := strconv.Atoi(itemSeq)

			itemKeyParts := strings.Split(itemKey, "-")

			itemKeyBase := itemKeyParts[0]
			itemKeyBaseInt, _ := strconv.Atoi(itemKeyBase)

			itemKeySeq := itemKeyParts[len(itemKeyParts)-1]
			itemKeySeqInt, _ := strconv.Atoi(itemKeySeq)

			if itemBaseInt == itemKeyBaseInt && itemKeySeqInt < itemSeqInt {

				for k, v := range item {
					if k == "id" {
						key = v
					} else {
						values = append(values, k, v)
					}
				}
				ix++
			}

			if len(values) != 0 {
				keyArr := utils.ToBulkString(key)
				valArr := utils.ToArrayBulkString(values...)

				respx += fmt.Sprintf("*%d\r\n%s%s", 2, keyArr, valArr)
			}
		}

		if respx != "" {
			respx = fmt.Sprintf("*%d\r\n%s", ix, respx)

			resp += fmt.Sprintf("*%d\r\n%s%s", 2, utils.ToBulkString(streamKey), respx)

		}

	}

	if resp != "" {
		resp = fmt.Sprintf("*%d\r\n%s", streamKeysCount, resp)
		// resp = fmt.Sprintf("*%d\r\n%s", len(storedItems), resp)

		// fmt.Println("resp :: ", resp)
		fmt.Println("resp :: ", strings.ReplaceAll(resp, "\r\n", "\\r\\n"))
	} else {
		// sendf null reponse back
		resp = "$-1\r\n"
	}

	c.Write([]byte(resp))

	return nil
}

func (r *RedisServer) xrange(c net.Conn, args []string) error {

	fmt.Println("args : ", args)
	if len(args) < 3 {
		return fmt.Errorf("ERR arguments missing")
	}

	key := args[0]

	startS, endS := args[1], args[2]
	storedData := SessionStore.Data[key]

	if storedData.Type != "stream" {
		return fmt.Errorf("ERR item is not stream type")
	}

	if startS == "-" {
		startS = fmt.Sprintf("%s-0", endS)
	}
	if endS == "+" {
		endS = fmt.Sprintf("%s-9999", startS)
	}
	if startS[0] != endS[0] {
		return fmt.Errorf("ERR range not correct")
	}

	fmt.Println("startS :: ", startS)
	fmt.Println("endS :: ", endS)

	startParts := strings.Split(startS, "-")

	start, err := strconv.Atoi(startParts[len(startParts)-1])
	if err != nil {
		return fmt.Errorf("ERR startS sequence not correct")
	}
	endParts := strings.Split(endS, "-")
	end, err := strconv.Atoi(endParts[len(endParts)-1])

	if err != nil {
		return fmt.Errorf("ERR endS sequence not correct")
	}

	// start, err := strconv.Atoi(startS)
	// if err != nil {
	// 	return fmt.Errorf("ERR %s", err.Error())
	// }

	// end, err := strconv.Atoi(endS)
	// if err != nil {
	// 	return fmt.Errorf("ERR %s", err.Error())
	// }

	if end < start {
		return fmt.Errorf("ERR incorrect range supplied")
	}

	resp := ""

	storedItems := storedData.Data.([]map[string]string)

	ix := 0
	// var levelArrs []string
	for _, item := range storedItems {
		var key string
		var values []string
		itemKeyParts := strings.Split(item["id"], "-")
		// base :=  itemKeyParts[0]
		seq := itemKeyParts[len(itemKeyParts)-1]
		seqInt, _ := strconv.Atoi(seq)
		if !(start <= seqInt && seqInt <= end) {
			continue
		}
		ix++
		for k, v := range item {
			if k == "id" {
				key = v
			} else {
				values = append(values, k, v)
			}
		}
		keyArr := utils.ToBulkString(key)
		valArr := utils.ToArrayBulkString(values...)

		resp += fmt.Sprintf("*%d\r\n%s%s", 2, keyArr, valArr)
	}

	resp = fmt.Sprintf("*%d\r\n%s", ix, resp)

	// resp = fmt.Sprintf("*%d\r\n%s", len(storedItems), resp)

	// fmt.Println("resp :: ", resp)
	fmt.Println("resp :: ", strings.ReplaceAll(resp, "\r\n", "\\r\\n"))

	c.Write([]byte(resp))

	return nil
}

var xadd_previous_parsed_id = ""

func (r *RedisServer) xadd(c net.Conn, args []string) error {

	if len(args) < 4 {
		return fmt.Errorf("ERR arguments missing")
	}

	streamKey := args[0]
	streamID := args[1]
	resp := ""

	SessionStore.Lock()
	defer SessionStore.Unlock()
	item, exists := SessionStore.Data[streamKey]

	if exists && item.Type != "stream" {
		return fmt.Errorf("ERR WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	maxSeqId := 0
	isDefault := true
	// maxBase := 0

	IdParts := strings.Split(streamID, "-")
	base := IdParts[0]
	baseInt, err := strconv.Atoi(base)
	if err != nil {
		baseInt = 0
		// return fmt.Errorf("ERR invalid base number")
	}

	intermediateData, ok := item.Data.([]map[string]string)
	if ok {
		for _, i := range intermediateData {

			prevID := i["id"]

			prevParts := strings.Split(prevID, "-")
			if len(prevParts) != 2 {
				return fmt.Errorf("ERR invalid stream id format")
			}

			prevBase := prevParts[0]
			prevBaseInt, err := strconv.Atoi(prevBase)
			if err != nil {
				return fmt.Errorf("ERR invalid base number")
			}

			if prevBaseInt != baseInt {
				// skip if the base ID isn't same
				continue
			}

			prevSeq := prevParts[1]
			prevSeqInt, err := strconv.Atoi(prevSeq)

			if err != nil {
				return fmt.Errorf("ERR invalid sequence number")
			}

			if maxSeqId <= prevSeqInt {
				maxSeqId = prevSeqInt
				xadd_previous_parsed_id = prevID
				isDefault = false
			}

			// maxBase = max(maxBase, prevBaseInt)

		}
	}

	// handle auto-increment id generation
	if strings.HasSuffix(streamID, "-*") {
		if !exists {
			// No previous entries: start at 1
			if baseInt == 0 {
				maxSeqId = 1
			}

		} else {
			if !isDefault {
				maxSeqId++
			}
		}
		streamID = base + "-" + strconv.Itoa(maxSeqId)
	} else if streamID == "*" {
		streamID = strconv.Itoa(int(time.Now().UnixMilli())) + "-0"
	}

	// addedd check for validating
	// entry Ids
	if xadd_previous_parsed_id != "" {
		for _, i := range intermediateData {
			if streamID == i["id"] {
				// if xadd_previous_parsed_id == streamID {
				return fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
			}
		}

		// Parse previous ID
		prevParts := strings.Split(xadd_previous_parsed_id, "-")
		if len(prevParts) != 2 {
			return fmt.Errorf("ERR invalid previous ID format")
		}
		prevMs := prevParts[0]
		prevSeq := prevParts[1]

		// Parse current incoming ID
		currParts := strings.Split(streamID, "-")
		if len(currParts) != 2 {
			return fmt.Errorf("ERR invalid current ID format")
		}
		currMs := currParts[0]
		currSeq := currParts[1]

		// Check against 0-0
		if currMs == "0" && currSeq == "0" {
			return fmt.Errorf("ERR The ID specified in XADD must be greater than 0-0")
		}

		prevMsInt, _ := strconv.Atoi(prevMs)
		currMsInt, _ := strconv.Atoi(currMs)

		prevSeqInt, _ := strconv.Atoi(prevSeq)
		currSeqInt, _ := strconv.Atoi(currSeq)
		// Check if smaller or equal to previous
		if currMsInt < prevMsInt || (currMsInt == prevMsInt && currSeqInt <= prevSeqInt) {
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

	if exists {
		newData := item.Data
		newData = append(newData.([]map[string]string), data)
		SessionStore.Data[streamKey] = Item{Data: newData, Type: "stream"}
	} else {
		SessionStore.Data[streamKey] = Item{Data: []map[string]string{data}, Type: "stream"}
	}
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
