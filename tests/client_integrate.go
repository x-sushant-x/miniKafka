package main

import (
	"fmt"
	"time"

	"github.com/x-sushant-x/miniKafka/client"
)

func main() {
	c, err := client.NewTCPClient("127.0.0.1", "5555")
	if err != nil {
		panic(err)
	}

	go func() {
		time.Sleep(time.Second * 5)
		err = c.Produce("notifications", []byte("User created: #81414"))
		if err != nil {
			panic(err)
		}
	}()

	data, err := c.Consume("notifications", 0)
	if err != nil {
		panic(err)
	}

	fmt.Print(data)
}
