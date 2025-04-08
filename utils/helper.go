package utils

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand"
	"regexp"
	"strconv"
	"strings"
)

func MatchPatternKeys(keys []string, pattr string) ([]string, error) {
	res := make([]string, 0)

	// check made if a valid regex Unicode point is present
	if strings.ContainsAny(pattr, "*?[]^") {
		re, err := regexp.Compile(pattr)
		if err != nil {
			return res, err
		}
		for _, key := range keys {
			if re.MatchString(key) {
				res = append(res, key)
			}
		}
	} else {
		// if valid regex unicode point doesn't match use direct match
		for _, key := range keys {
			if key == pattr {
				res = append(res, key)
			}
		}
	}

	return res, nil
}

func GenerateRandomReplID(len int) string {
	bytes := make([]byte, len)
	if _, err := rand.Read(bytes); err != nil {
		return ""
	}

	return hex.EncodeToString(bytes)
}

func ReadRESPMessage(r *bufio.Reader, readingRDB bool) ([]byte, error) {
	startByte, err := r.ReadByte()
	if err != nil {
		fmt.Println("[DEBUG] ReadByte error:", err)
		return nil, err
	}

	fmt.Printf("[DEBUG] Start byte: %q (%d)\n", startByte, startByte)

	switch startByte {
	// case '\r', '\n':
	// 	fmt.Println("SKIP CR;LF")

	case '+', '-', ':': // simple string, error, or integer
		fmt.Println("Read => +, -, :")
		line, err := r.ReadBytes('\n')
		if err != nil {
			fmt.Println("[DEBUG] Error reading line after simple type:", err)
			return nil, err
		}
		return append([]byte{startByte}, line...), nil

		// return nil, fmt.Errorf("skipping +, -, : case")

	case '$': // bulk string
		fmt.Println("[DEBUG] Read => $ (bulk string)")

		// Read the length line
		// startTime := time.Now()
		lengthLine, err := r.ReadString('\n')
		// readLenTime := time.Since(startTime)
		if err != nil {
			fmt.Println("[DEBUG] Error reading length of bulk string:", err)
			return nil, err
		}
		fmt.Printf("[DEBUG] Bulk string length line (raw): %q\n", lengthLine)

		lengthStr := strings.TrimSpace(lengthLine)
		length, err := strconv.Atoi(lengthStr)
		if err != nil {
			fmt.Println("[DEBUG] Error converting bulk length:", err)
			return nil, err
		}
		fmt.Printf("[DEBUG] Parsed bulk string length: %d bytes\n", length)

		// Start assembling the message
		message := []byte{'$'}
		message = append(message, []byte(lengthLine)...)

		if length == -1 {
			// fmt.Println("[DEBUG] Null bulk string detected")
			return message, nil
		}

		if !readingRDB {
			length += 2
		}

		// Read the bulk string content + CRLF
		// fmt.Printf("[DEBUG] Attempting to read %d content bytes + 2 (CRLF) = %d total\n", length, length+2)
		// startTime = time.Now()
		content := make([]byte, length)
		n, err := io.ReadFull(r, content)
		// readContentTime := time.Since(startTime)

		if err != nil {
			fmt.Printf("[DEBUG] Error reading bulk string content: %v (read %d bytes)\n", err, n)
			return nil, err
		}

		// fmt.Printf("[DEBUG] Read %d bytes of content successfully in %v\n", n, readContentTime)
		fmt.Printf("[DEBUG] Content preview (first 50 bytes): %q\n", content[:min(50, len(content))])
		message = append(message, content...)

		return message, nil

	case '*': // array
		fmt.Println("Read => *")
		lengthLine, err := r.ReadString('\n')
		if err != nil {
			fmt.Println("[DEBUG] Error reading array length:", err)
			return nil, err
		}
		fmt.Println("[DEBUG] Array length line:", lengthLine)

		message := []byte{'*'}
		message = append(message, []byte(lengthLine)...)

		count, err := strconv.Atoi(strings.TrimSpace(lengthLine))
		if err != nil {
			fmt.Println("[DEBUG] Error converting array count:", err)
			return nil, err
		}

		for i := 0; i < count; i++ {
			part, err := ReadRESPMessage(r, false)
			if err != nil {
				fmt.Printf("[DEBUG] Error reading array element %d: %v\n", i, err)
				return nil, err
			}
			message = append(message, part...)
		}
		return message, nil
	default:
		// fmt.Printf("[DEBUG] Unknown RESP type: | %c | %d |\n", startByte, startByte)
		return nil, fmt.Errorf("unknown RESP type:| %c | ", startByte)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func CheckValueType(d string) (string, error) {

	var err error
	_, err = strconv.Atoi(d)

	if err == nil {
		return "integer", nil
	} else {
		return "string", nil
	}
}
