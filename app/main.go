package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	rs "github.com/codecrafters-io/redis-starter-go/server"
	"github.com/codecrafters-io/redis-starter-go/utils"
)

func MasterSlaveHandshake(ip string, port int) error {

	addr := fmt.Sprintf("%s:%d", ip, port)
	c, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}

	// send PING to master
	ping := utils.ToArrayBulkString("PING")

	c.Write([]byte(ping))
	return nil
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment this block to pass the first stage

	var redisServer = rs.NewRedisServer()

	var PORT uint = 6379

	// Define command-line flags
	dir := flag.String("dir", "redis", "Directory to store RDB file")
	dbFileName := flag.String("dbfilename", "dump.rdb", "RDB file name")

	port := flag.Uint("port", PORT, "Port number")

	replicaof := flag.String("replicaof", "", "replica for")
	// Parse the command-line flags
	flag.Parse()

	if (*replicaof) != "" {
		redisServer.UpdateRole("slave")
		repl_args := strings.Split(*replicaof, " ")

		if len(repl_args) != 2 {
			panic("master not activated")
		}
		master_IP := repl_args[0]
		master_PORT, err := strconv.Atoi(repl_args[1])

		if err != nil {
			panic("port number incorrect :: " + err.Error())
		}

		fmt.Println("(master_IP, master_PORT) :: ", master_IP, master_PORT)

		err = MasterSlaveHandshake(master_IP, master_PORT)
		if err != nil {
			panic("handhaske error : " + err.Error())
		}
	}

	// for the spawned server update configs accordingly
	redisServer.Cnf.UpdateConfig(*dir, *dbFileName)

	// Load the persistent data from file.
	redisServer.Cnf.AutoLoad()
	// os.Exit(-1)

	// start the cleanup of expired keys
	go func() {
		for {
			time.Sleep(10 * time.Second)
			rs.CleanExpKeys()
		}
	}()

	// start the redis server
	l, err := redisServer.Start(*port)

	if err != nil {
		utils.LogEntry("RED", "Failed to start redis server : ", err.Error())
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			utils.LogEntry("RED", "Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		utils.LogEntry("green", "Conntected : ", l.Addr().String())

		go redisServer.HandleConnection(conn)

	}
}
