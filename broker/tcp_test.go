package broker

import (
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestHandleConnection(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	outputChan := make(chan string, 1)

	server := TCPServer{}

	go server.handleConnection(serverConn, outputChan)

	msg := "hello world"

	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(msg)))

	_, err := clientConn.Write(lenBuf)
	require.NoError(t, err)

	_, err = clientConn.Write([]byte(msg))
	require.NoError(t, err)

	select {
	case received := <-outputChan:
		require.Equal(t, msg, received)

	case <-time.After(time.Second):
		t.Fatal("timeout waiting for message")
	}
}

func TestHandleConnection_PartialLength(t *testing.T) {
	serverConn, clientConn := net.Pipe()

	outputChan := make(chan string, 1)

	server := TCPServer{}

	go server.handleConnection(serverConn, outputChan)

	clientConn.Write([]byte{0x00, 0x00})
	clientConn.Close()

	select {
	case <-outputChan:
		t.Fatal("message should not be published")

	case <-time.After(100 * time.Millisecond):
	}
}

func TestHandleConnection_PartialPayload(t *testing.T) {
	serverConn, clientConn := net.Pipe()

	outputChan := make(chan string, 1)

	server := TCPServer{}

	go server.handleConnection(serverConn, outputChan)

	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, 10)

	clientConn.Write(lenBuf)
	clientConn.Write([]byte("abc"))
	clientConn.Close()

	select {
	case <-outputChan:
		t.Fatal("message should not be published")

	case <-time.After(100 * time.Millisecond):
	}
}

func TestHandleConnection_EmptyMessage(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	outputChan := make(chan string, 1)

	server := TCPServer{}

	go server.handleConnection(serverConn, outputChan)

	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, 0)

	clientConn.Write(lenBuf)

	select {
	case msg := <-outputChan:
		require.Equal(t, "", msg)

	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
}
