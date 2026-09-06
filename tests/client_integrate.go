package main

import (
	"bytes"
	"fmt"

	"github.com/x-sushant-x/miniKafka/client"
)

func main() {
	c, err := client.NewTCPClient("127.0.0.1", "5557")
	if err != nil {
		panic(err)
	}

	for i := range 1 {
		var buf bytes.Buffer
		buf.WriteString("#55")
		fmt.Fprintf(&buf, "%d", i)

		err = c.Produce("orders", buf.Bytes(), buf.String())
		if err != nil {
			panic(err)
		}

	}

	// data, err := c.Consume("orders", 1, 0)
	// if err != nil {
	// 	panic(err)
	// }

	// fmt.Print(data)
}
