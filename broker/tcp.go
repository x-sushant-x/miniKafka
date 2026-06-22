package broker

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

const (
	tcpMsgLenWidth = 4 // Bytes
)

type TCPServer struct {
	Port string
}

func (t TCPServer) StartServer(outputChan chan<- string) error {
	ln, err := net.Listen("tcp", ":"+t.Port)
	if err != nil {
		return err
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("unable to accept connection:", err.Error())
			continue
		}

		lenBuf := make([]byte, tcpMsgLenWidth)

		_, err = io.ReadFull(conn, lenBuf)
		if err != nil {
			fmt.Println("unable to msg len:", err.Error())
			conn.Close()
			continue
		}

		length := binary.BigEndian.Uint32(lenBuf)

		data := make([]byte, length)

		_, err = io.ReadFull(conn, data)
		if err != nil {
			fmt.Println("unable to data:", err.Error())
			conn.Close()
			continue
		}

		outputChan <- string(data)
	}
}
