package utils

import (
	"fmt"
	"strings"
)

// ANSI Color Constants
const (
	BLACK        = "\033[0;30m"
	RED          = "\033[0;31m"
	GREEN        = "\033[0;32m"
	BROWN        = "\033[0;33m"
	BLUE         = "\033[0;34m"
	PURPLE       = "\033[0;35m"
	CYAN         = "\033[0;36m"
	LIGHT_GRAY   = "\033[0;37m"
	DARK_GRAY    = "\033[1;30m"
	LIGHT_RED    = "\033[1;31m"
	LIGHT_GREEN  = "\033[1;32m"
	YELLOW       = "\033[1;33m"
	LIGHT_BLUE   = "\033[1;34m"
	LIGHT_PURPLE = "\033[1;35m"
	LIGHT_CYAN   = "\033[1;36m"
	LIGHT_WHITE  = "\033[1;37m"
	BOLD         = "\033[1m"
	FAINT        = "\033[2m"
	ITALIC       = "\033[3m"
	UNDERLINE    = "\033[4m"
	BLINK        = "\033[5m"
	NEGATIVE     = "\033[7m"
	CROSSED      = "\033[9m"
	END          = "\033[0m"
)

// Color Mapping from User Input
var colorMap = map[string]string{
	"black":        BLACK,
	"red":          RED,
	"green":        GREEN,
	"brown":        BROWN,
	"blue":         BLUE,
	"purple":       PURPLE,
	"cyan":         CYAN,
	"light_gray":   LIGHT_GRAY,
	"dark_gray":    DARK_GRAY,
	"light_red":    LIGHT_RED,
	"light_green":  LIGHT_GREEN,
	"yellow":       YELLOW,
	"light_blue":   LIGHT_BLUE,
	"light_purple": LIGHT_PURPLE,
	"light_cyan":   LIGHT_CYAN,
	"light_white":  LIGHT_WHITE,
	"bold":         BOLD,
	"faint":        FAINT,
	"italic":       ITALIC,
	"underline":    UNDERLINE,
	"blink":        BLINK,
	"negative":     NEGATIVE,
	"crossed":      CROSSED,
}

// // LogEntry logs a message with the specified color
// func LogEntry(userColor string, msg ...string) {
// 	color, exists := colorMap[strings.ToLower(userColor)]
// 	if !exists {
// 		color = END
// 	}

// 	var p strings.Builder
// 	for _, m := range msg {
// 		p.WriteString(m)
// 	}

// 	fmt.Println(color + p.String() + END)
// }

// LogEntry logs a message with the specified color
func LogEntry(userColor string, msg ...interface{}) {
	color, exists := colorMap[strings.ToLower(userColor)]
	if !exists {
		color = END
	}

	var p strings.Builder
	for _, m := range msg {
		p.WriteString(fmt.Sprint(m)) // Convert each argument to a string
		p.WriteString(" ")           // Add space between arguments like Println
	}

	fmt.Println(color + p.String() + END)
}
