package server

import (
	"fmt"
	"time"
)

var ExpKeys = make(map[string]time.Time)

func CleanExpKeys() {
	fmt.Println("ExpKeys :: ", ExpKeys, time.Now())
	for k, v := range ExpKeys {
		fmt.Println("time.Now().Compare(v) :: ", k, v, time.Now().Compare(v), time.Now(), " <-> ", v)
		if time.Now().Compare(v) >= 0 {
			delete(SessionStore.Data, k)
		}
	}
}
