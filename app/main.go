package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	rs "github.com/codecrafters-io/redis-starter-go/server"
	"github.com/codecrafters-io/redis-starter-go/utils"
)

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

	// Parse the command-line flags
	flag.Parse()

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
