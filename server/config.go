package server

import "sync"

type Config struct {
	Dir        string
	Dbfilename string
}

func NewConfig() *Config {
	return &Config{
		Dir:        "/tmp/redis",
		Dbfilename: "db.rdb",
	}
}

func (c *Config) UpdateConfig(dir string, dbfilename string) error {

	c.Dir = dir
	c.Dbfilename = dbfilename

	return nil
}

type Store struct {
	sync.Mutex
	Data map[string]string
}

// hold all data for the current session
var SessionStore = &Store{
	Data: make(map[string]string),
}

// Loads the data from givenn RDB file
// adding persistence
func (c *Config) AutoLoad() error {

	return nil
}
