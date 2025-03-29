package server

import (
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
			// dnNumber := ReadByte(&data)
			_ = ReadByte(&data)
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
	// ConvertExpiryTime converts 8 bytes of little-endian data into a Go time.Time object
	if len(b) != 8 {
		return time.Time{}
	}

	// Read little-endian 8-byte integer
	expiryMs := binary.LittleEndian.Uint64(b)

	// Convert to time.Time (assuming absolute timestamp in milliseconds)
	timestamp := time.UnixMilli(int64(expiryMs))

	return timestamp
}

func processByteToMap(bytes *[]byte, mapData *map[string]string) {
	var exp time.Time

	for {

		b := ReadByte(bytes)
		if b == EOF || b == SELECTDB {
			utils.LogEntry("pink", "[+] rdb ended [+]")
			return
		}

		if b == EXPIRETIMEMS {
			// read next 8 bytes timestamp
			var tsb []byte
			for range 8 {
				tsb = append(tsb, ReadByte(bytes))
			}
			exp = bytesToTimestamp(tsb)
			continue
		}

		if b == 0x00 {
			k, v := readKeyValue(bytes)

			(*mapData)[k] = v
			if !exp.IsZero() {
				ExpKeys[k] = exp
			}
			// reset to zero time
			exp = time.Time{}
		}
	}
}

// ReadKeyValue reads a key-value pair from RDB
func readKeyValue(data *[]byte) (string, string) {

	// for _, b := range *data {
	// 	fmt.Printf("%02X ", b) // Ensures two-digit hex values
	// }

	var key, value string

	key_size := ReadByte(data)
	for range int(key_size) {
		key += string(ReadByte(data))
	}

	value_size := ReadByte(data)
	for range int(value_size) {
		value += string(ReadByte(data))
	}

	return key, value
}
