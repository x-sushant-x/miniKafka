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

	// for range 2 {
	// 	var buf bytes.Buffer
	// 	buf.WriteString("Asmuth")

	// 	err = c.Produce("orders", buf.Bytes(), buf.String())
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// }

	for offset := range 4 {
		data, err := c.Consume("orders", uint64(offset), 0)
		if err != nil {
			panic(err)
		}

		fmt.Println(data)
	}
}
