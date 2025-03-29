package server

import (
	"encoding/binary"
	"fmt"

	"github.com/codecrafters-io/redis-starter-go/utils"
)

// ReadString reads a fixed-length string from the byte slice
func ReadString(data *[]byte, length int) string {
	str := string((*data)[:length])
	*data = (*data)[length:] // Move forward
	return str
}

// ReadByte reads a single byte from the slice
func ReadByte(data *[]byte) byte {
	b := (*data)[0]
	*data = (*data)[1:]
	return b
}

// ParseRDB processes the RDB file
func ParseRDB(data []byte) {

	// Read magic string and version
	magic := ReadString(&data, 5)
	version := ReadString(&data, 4)
	utils.LogEntry("CYAN", "RDB file:  ", magic, ":", version)

	// neeed to change into actual data rather than
	// RDB_DB := make(map[int]map[string]string)

	for b := ReadByte(&data); b != EOF; b = ReadByte(&data) {
		if b == SELECTDB {
			x := data[1:]
			var err error
			dbmap, err := processDBKV(x)
			if err != nil {
				panic("something is wrong at : ParseRDB :: " + err.Error())
			}
			SessionStore.Data = dbmap
			break // added becuase need to read only 1 db for first exercise
		}

	}

	utils.LogEntry("faint", "[+] RDB file read complete. [+]")
}

// given a list of bytes
// process the database bytes to key value
func processDBKV(bytes []byte) (map[string]string, error) {

	// TODO: need to start reading from RESIZEDB
	fmt.Println("Need to process : ", bytes)

	var mapData = make(map[string]string)

	b := ReadByte(&bytes)

	if b == RESIZEDB {
		/*hashtablesize*/ _ = binary.LittleEndian.Uint16([]byte{ReadByte(&bytes), 00})
		/*exphashtablesize*/ _ = binary.LittleEndian.Uint16([]byte{ReadByte(&bytes), 00})
		processByteToMap(&bytes, &mapData)

	} else {
		return map[string]string{}, fmt.Errorf("wrong db format")
	}

	return mapData, nil
}

func processByteToMap(bytes *[]byte, mapData *map[string]string) {

	for {
		b := ReadByte(bytes)

		if b == 0x00 {
			k, v := readKeyValue(bytes)
			(*mapData)[k] = v
		}
		if b == 0xff || b == 0xfe {
			utils.LogEntry("pink", "[+] rdb ended [+]")
			return
		}
	}
}

// ReadKeyValue reads a key-value pair from RDB
func readKeyValue(data *[]byte) (string, string) {
	fmt.Println("rkv data :: ", data)
	var key, value string
	t := ReadByte(data)
	fmt.Println("t 1: ", t, data)
	for range int(t) {
		key += string(ReadByte(data))
	}
	fmt.Println("key :: ", key)
	t = ReadByte(data)
	fmt.Println("t 2: ", t, data)
	for range int(t) {
		value += string(ReadByte(data))
	}
	fmt.Println("value :: ", value)

	return key, value
}
