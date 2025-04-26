package replication

import (
	"net"
	"sync"
)

var replicaAcknowledgments = make(map[net.Conn]bool)
var mutex sync.Mutex

// Mark a replica as having acknowledged a write
func MarkAcknowledged(replica net.Conn) {
	mutex.Lock()
	defer mutex.Unlock()
	replicaAcknowledgments[replica] = true
}

// Check if a replica has acknowledged a write
func HasAcknowledged(replica net.Conn) bool {

	SendCommand(replica, "REPLCONF", "GETACK", "*")
	mutex.Lock()
	defer mutex.Unlock()
	return replicaAcknowledgments[replica]
}

// Clear acknowledgment for a replica
func ClearAcknowledgment(replica net.Conn) {
	mutex.Lock()
	defer mutex.Unlock()
	delete(replicaAcknowledgments, replica)
}
