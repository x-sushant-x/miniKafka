package main

import (
	"fmt"

	"github.com/x-sushant-x/miniKafka/client"
)

func main() {
	c, err := client.NewTCPClient("127.0.0.1", "5555")
	if err != nil {
		panic(err)
	}

	// err = c.Produce("notifications", []byte("User created: #81414"))
	// if err != nil {
	// 	panic(err)
	// }

	data, err := c.Consume("notifications", 4)
	if err != nil {
		panic(err)
	}

	fmt.Print(data)
}
