package main

import (
	"bytes"
	"fmt"

	"github.com/x-sushant-x/miniKafka/client"
)

func main() {
	c, err := client.NewTCPClient("127.0.0.1", "5555")
	if err != nil {
		panic(err)
	}

	// go func() {
	// time.Sleep(time.Second * 5)
	for i := range 10 {
		var buf bytes.Buffer
		buf.WriteString("User created: #")
		fmt.Fprintf(&buf, "%d", i)

		err = c.Produce("notifications", buf.Bytes(), buf.String())
		if err != nil {
			panic(err)
		}

		fmt.Printf("Produced: %d\n", i)
	}
	// }()

	// data, err := c.Consume("notifications", 0, 0)
	// if err != nil {
	// 	panic(err)
	// }

	// fmt.Print(data)
}
