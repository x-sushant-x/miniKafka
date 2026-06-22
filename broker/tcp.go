package broker

import (
	"encoding/binary"
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
			continue
		}

		go t.handleConnection(conn, outputChan)
	}
}

func (t TCPServer) handleConnection(conn net.Conn, outputChan chan<- string) {
	defer conn.Close()

	lenBuf := make([]byte, tcpMsgLenWidth)

	_, err := io.ReadFull(conn, lenBuf)
	if err != nil {
		return
	}

	length := binary.BigEndian.Uint32(lenBuf)

	data := make([]byte, length)

	_, err = io.ReadFull(conn, data)
	if err != nil {
		return
	}

	outputChan <- string(data)
}
