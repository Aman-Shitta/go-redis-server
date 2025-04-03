package utils

import (
	"encoding/hex"
	"fmt"
	"math/rand"
	"regexp"
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

func ParseRESPCommands(data []byte) ([]string, error) {
	var commands []string
	input := string(data)

	for len(input) > 0 {
		// Ensure the command starts with '*'
		if !strings.HasPrefix(input, "*") {
			return nil, fmt.Errorf("invalid RESP format: expected '*', got '%c'", input[0])
		}

		// Find the end of the current command
		endIdx := strings.Index(input, "\r\n")
		if endIdx == -1 {
			return nil, fmt.Errorf("incomplete RESP command")
		}

		// Extract the command
		cmdEnd := endIdx + 2
		remaining := input[cmdEnd:]

		// Find the next command (if any)
		nextCmdIdx := strings.Index(remaining, "*")
		if nextCmdIdx == -1 {
			// No more commands, append remaining data and break
			commands = append(commands, input)
			break
		}

		// Extract the current command and move to the next
		commands = append(commands, input[:cmdEnd+nextCmdIdx])
		input = remaining[nextCmdIdx:]
	}
	fmt.Println("Commands :: ", len(commands))

	return commands, nil
}
