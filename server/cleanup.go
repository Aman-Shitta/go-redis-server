package server

import (
	"time"
)

var ExpKeys = make(map[string]time.Time)

func CleanExpKeys() {
	for k, v := range ExpKeys {

		if time.Now().Compare(v) >= 0 {
			delete(SessionStore.Data, k)
			delete(ExpKeys, k)
		}
	}
}
