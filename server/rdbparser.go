package server

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

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

	for b := ReadByte(&data); b != EOF && len(data) > 0; b = ReadByte(&data) {

		// Check directly for start of db
		// skipping aux and meta data
		if b == SELECTDB {
			// read DB number
			dnNumber := ReadByte(&data)
			fmt.Println("DB number :", dnNumber)
			var x []byte
			copy(data, x)
			var err error
			dbmap, err := processDBKV(data)
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
	fmt.Println("Need to process DB : ", bytes)

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

func bytesToTimestamp(b []byte) time.Time {
	var timestamp int64
	buf := bytes.NewReader(b)

	// Assuming big-endian byte order
	err := binary.Read(buf, binary.BigEndian, &timestamp)
	if err != nil {
		// Handle error appropriately
		panic(err)
	}

	return time.Unix(timestamp, 0)
}

func processByteToMap(bytes *[]byte, mapData *map[string]string) {

	for {

		b := ReadByte(bytes)

		fmt.Println("read byte : ", b)
		if b == EOF || b == SELECTDB {
			utils.LogEntry("pink", "[+] rdb ended [+]")
			return
		}

		var exp time.Time
		if b == EXPIRETIMEMS {
			// read next 8 bytes timestamp
			var tsb []byte
			for range 8 {
				tsb = append(tsb, ReadByte(bytes))
			}
			exp = bytesToTimestamp(tsb)
		}

		if b == 0x00 {
			k, v := readKeyValue(bytes)
			fmt.Println(k, v, exp)
			fmt.Printf("%s:%v expired @ %s\n", k, v, exp.Format("2006-01-02 15:04:05"))
			(*mapData)[k] = v
			if !exp.IsZero() {
				ExpKeys[k] = exp
			}
		}
	}
}

// ReadKeyValue reads a key-value pair from RDB
func readKeyValue(data *[]byte) (string, string) {

	for _, b := range *data {
		fmt.Printf("%02X ", b) // Ensures two-digit hex values
	}

	var key, value string

	key_size := ReadByte(data)
	fmt.Println("key_size :: ", key_size)
	for range int(key_size) {
		key += string(ReadByte(data))
	}
	fmt.Println("key : ", key)

	value_size := ReadByte(data)
	fmt.Println("value_size :: ", value_size)
	for range int(value_size) {
		value += string(ReadByte(data))
	}

	return key, value
}
