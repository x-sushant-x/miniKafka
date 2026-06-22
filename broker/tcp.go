package broker

import (
	"encoding/binary"
	"io"
	"net"
)

const (
	tcpMsgLenWidth = 4 // Bytes
)

type RequestHandler func([]byte) ([]byte, error)

type TCPServer struct {
	Port string
}

func (t TCPServer) StartServer(handler RequestHandler) error {
	ln, err := net.Listen("tcp", ":"+t.Port)
	if err != nil {
		return err
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}

		go t.handleConnection(conn, handler)
	}
}

func (t TCPServer) handleConnection(conn net.Conn, handler RequestHandler) {
	defer conn.Close()

	lenBuf := make([]byte, tcpMsgLenWidth)

	_, err := io.ReadFull(conn, lenBuf)
	if err != nil {
		return
	}

	msgLen := binary.BigEndian.Uint32(lenBuf)

	data := make([]byte, msgLen)

	_, err = io.ReadFull(conn, data)
	if err != nil {
		return
	}

	resp, err := handler(data)
	if err != nil {
		return
	}

	respLen := make([]byte, 4)
	binary.BigEndian.PutUint32(respLen, uint32(len(resp)))

	conn.Write(respLen)
	conn.Write(resp)
}
