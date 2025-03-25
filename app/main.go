package main

import (
	"fmt"
	"net"
	"os"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment this block to pass the first stage

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	// var conns []net.Conn

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		fmt.Println("[+] New Connection [+]")
		fmt.Println("Conntected : ", l.Addr().String())
		// conns = append(conns, conn)

		go hanldeConnRequests(conn)
		// var input []byte

		// if _, err := conn.Read(input); err != nil {
		// 	fmt.Println("Error reading from connection: ", err.Error())
		// 	os.Exit(1)
		// }
		// conn.Write([]byte("+PONG\r\n"))

		// conn.Close()
	}
}

func hanldeConnRequests(c net.Conn) {
	defer c.Close()
	for {
		var data = make([]byte, 1024)
		_, err := c.Read(data)
		fmt.Println("connection data : ", string(data))
		if err != nil {
			fmt.Println("Error handling requests : ", err.Error())
			return
		}

		command, args, err := parseResp(data)
		fmt.Println("command, args :: ", command, args)
		if err != nil {
			c.Write([]byte(fmt.Sprintf("-ERR %s\r\n", err.Error())))
			continue
		}

		fn, err := processCommand(command)

		if err != nil {
			c.Write([]byte(fmt.Sprintf("-ERR %s\r\n", err.Error())))
			continue
		}

		err = fn(c, args)

		if err != nil {
			c.Write([]byte(fmt.Sprintf("-ERR %s\r\n", err.Error())))
			continue
		}
		// c.Write([]byte("+PONG\r\n"))
		// fmt.Println("[+] READ something [+]")
	}
}
