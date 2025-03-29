package server

import (
	"fmt"
	"net"

	"github.com/codecrafters-io/redis-starter-go/utils"
)

type ServerInterface interface {
	Start(uint) (net.Listener, error)
}

type RedisServer struct {
	Cnf *Config
}

func NewRedisServer() *RedisServer {
	return &RedisServer{
		Cnf: NewConfig(),
	}
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
