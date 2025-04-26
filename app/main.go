package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/replication"
	rs "github.com/codecrafters-io/redis-starter-go/server"
	"github.com/codecrafters-io/redis-starter-go/utils"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	var PORT uint = 6379

	// Define command-line flags
	dir := flag.String("dir", "", "Directory to store RDB file")
	dbFileName := flag.String("dbfilename", "", "RDB file name")

	port := flag.Uint("port", PORT, "Port number")

	replicaof := flag.String("replicaof", "", "replica for")
	// Parse the command-line flags
	flag.Parse()

	var redisServer = rs.NewRedisServer()
	// start the redis server
	l, err := redisServer.Start(*port)

	if (*replicaof) != "" {
		utils.LogEntry("RED", fmt.Sprintf("[+] REPLICA STARTED @ %d | master : %s [+]", *port, *replicaof))

		redisServer.UpdateRole("slave")

		repl_args := strings.Split(*replicaof, " ")

		if len(repl_args) != 2 {
			panic("master not provided for this slave instance")
		}

		master_IP := repl_args[0]
		master_PORT, err := strconv.Atoi(repl_args[1])

		if err != nil {
			panic("port number incorrect :: " + err.Error())
		}

		fmt.Println("(master_IP, master_PORT) :: ", master_IP, master_PORT, *port)

		rep, err := replication.InitiateHandshake(master_IP, master_PORT, *port)

		if err != nil {
			panic("handshake error : " + err.Error())
		}

		go redisServer.ProcessPropogatedCommands(rep)
	}

	if redisServer.Role == "master" {
		// for the spawned server update configs accordingly
		redisServer.Cnf.UpdateConfig(*dir, *dbFileName)

		// Load the persistent data from file.
		redisServer.Cnf.AutoLoad()
		// os.Exit(-1)

		go redisServer.SyncReplica()

		// start the cleanup of expired keys
		go func() {
			for {
				time.Sleep(10 * time.Second)
				rs.CleanExpKeys()
			}
		}()

		if err != nil {
			utils.LogEntry("RED", "Failed to start redis server : ", err.Error())
			os.Exit(1)
		}

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
