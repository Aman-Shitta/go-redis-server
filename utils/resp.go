// Read and parse the redis-cli input

package utils

import (
	"fmt"
	"strconv"
	"strings"
)

// Type of RESP
type Type byte

// Various RESP kinds
const (
	Integer = ':'
	String  = '+'
	Bulk    = '$'
	Array   = '*'
	Error   = '-'
)

func parseRESPArray(input string) ([]string, error) {
	lines := strings.Split(input, "\r\n")
	if len(lines) < 1 || len(lines[0]) == 0 || lines[0][0] != Array {
		return nil, fmt.Errorf("invalid RESP format")
	}

	// Extract number of elements in the RESP array
	numElements, err := strconv.Atoi(lines[0][1:])
	if err != nil || numElements < 1 {
		return nil, fmt.Errorf("invalid RESP array count")
	}

	result := []string{}
	index := 1

	// Process each bulk string in the RESP array
	for range numElements {
		if index >= len(lines) {
			return nil, fmt.Errorf("unexpected end of input")
		}

		if lines[index][0] != Bulk {
			return nil, fmt.Errorf("expected bulk string prefix '$'")
		}

		// Read the length of the next string
		length, err := strconv.Atoi(lines[index][1:])
		if err != nil || length < 0 {
			return nil, fmt.Errorf("invalid bulk string length")
		}

		index++ // Move to actual data

		if index >= len(lines) || len(lines[index]) != length {
			return nil, fmt.Errorf("bulk string length mismatch")
		}

		result = append(result, lines[index])
		index++ // Move to next length specifier
	}

	return result, nil
}

func ParseResp(d []byte) (string, []string, error) {
	input := string(d)

	if len(input) == 0 {
		return "", nil, fmt.Errorf("empty input")
	}

	// Ensure it's a RESP array
	if input[0] != Array {
		return "", nil, fmt.Errorf("invalid RESP: must start with '*' for array")
	}

	// Parse RESP array
	arr, err := parseRESPArray(input)
	if err != nil {
		return "", nil, err
	}

	// Ensure command is present
	if len(arr) < 1 {
		return "", nil, fmt.Errorf("no command found in RESP")
	}

	// Extract command and arguments
	command := arr[0]
	args := arr[1:] // Properly extract args

	return command, args, nil
}

var Types = map[string]string{
	"OK":  "+",
	"ERR": "-",
}

// generate redis RESP supported simple string where
// second argument denotes OK > `+`, ERR `-`.
//
// e.g utils.ToSimpleString("Hi there", "OK") -> +OK\r\n
func ToSimpleString(msg string, t string) string {

	if respType, ok := Types[strings.ToUpper(t)]; ok {
		return fmt.Sprintf("%s%s\r\n", respType, msg)
	}

	return ""
}

func ToBulkString(data ...string) string {
	var resp strings.Builder

	// resp.WriteString(fmt.Sprintf("*%d\r\n", len(data)))
	for _, item := range data {
		resp.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(item), item))
	}

	return resp.String()
}

func ToArrayBulkString(data ...string) string {
	var resp strings.Builder

	resp.WriteString(fmt.Sprintf("*%d\r\n", len(data)))
	for _, item := range data {
		resp.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(item), item))
	}

	return resp.String()
}

func ToInteger(i int) string {
	return fmt.Sprintf("%c%d\r\n", Integer, i)
}
