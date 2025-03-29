package server

import (
	"fmt"
	"time"
)

var ExpKeys = make(map[string]time.Time)

func CleanExpKeys() {
	fmt.Println("ExpKeys :: ", ExpKeys, time.Now())
	for k, v := range ExpKeys {
		if time.Now().Compare(v) >= 0 {
			delete(SessionStore.Data, k)
		}
	}
}
