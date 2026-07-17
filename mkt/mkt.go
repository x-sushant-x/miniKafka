package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/x-sushant-x/miniKafka/client"
)

var (
	port *string
	host *string
	cl   *client.TCPClient
)

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func main() {
	action := flag.String("action", "", "An action to perform.")

	// Generic Flags
	port = flag.String("port", "5555", "Port on which miniKafka is running.")
	host = flag.String("host", "127.0.0.1", "Host on which miniKafka is running.")
	topic := flag.String("topic", "", "Topic on which action will be performed.")

	// Flags related to produce.
	pCount := flag.Int("pCount", 0, "Number of messages to produce.")
	pMsgLen := flag.Int("pMsgLen", 0, "Length of message (in bytes) to produce")

	flag.Parse()

	setupClient()

	switch *action {
	case "produce":
		produce(*topic, *pCount, *pMsgLen)
	default:
		panic("invalid action: " + *action)
	}
}

func produce(topic string, pCount, pMsgLen int) {
	now := time.Now()

	if topic == "" || pCount == 0 || pMsgLen == 0 {
		fmt.Println("Invalid input for print command. Try this.")
		fmt.Println("./mkt --action=produce --topic=my-topic --pCount=10 --pMsgLen=32")
		os.Exit(-1)
	}

	msg := generateFixedLengthMessage(pMsgLen)
	msgBytes := []byte(msg)

	for range pCount {
		err := cl.Produce(topic, msgBytes, msg)
		if err != nil {
			fmt.Println("produce error:", err.Error())
		}
	}

	timeTaken := time.Since(now)
	throughput := int(pCount / int(timeTaken.Seconds()))
	fmt.Printf("Throughput: %d records/secs\n", throughput)
}

func setupClient() {
	newClient, err := client.NewTCPClient(*host, *port)
	if err != nil {
		panic("unable to connect to server: " + err.Error())
	}

	cl = newClient
}

func generateFixedLengthMessage(length int) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}
