package raft

// import (
// 	"fmt"
// 	"log"
// 	"os"
// 	"strings"
// 	"time"
// )

// func main() {
// 	if len(os.Args) != 2 {
// 		log.Fatal("Usage: go run . <node-id>")
// 	}

// 	var nodeID string
// 	fmt.Sscanf(os.Args[1], "%s", &nodeID)

// 	cluster := map[string]string{
// 		"1": "localhost:5001",
// 		"2": "localhost:5002",
// 		"3": "localhost:5003",
// 	}

// 	addrParts := strings.Split(cluster[nodeID], ":")

// 	server := NewServer(nodeID, addrParts[0], addrParts[1], cluster)

// 	applyChan := make(chan ApplyMessage)

// 	raft := NewRaft(applyChan)
// 	raft.server = server
// 	server.raft = raft

// 	go server.serve()
// 	time.Sleep(time.Second * 2)

// 	server.connectToAllPeers()

// 	go raft.startElectionLoop()
// 	go raft.applyLoop()
// 	go monitorApplyChan(applyChan)

// 	select {}
// }

// func monitorApplyChan(applyChan chan ApplyMessage) {
// 	for msg := range applyChan {
// 		fmt.Printf("Index: %d | Command: %s\n", msg.Index, string(msg.Command))
// 	}
// }
