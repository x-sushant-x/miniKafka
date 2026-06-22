package broker

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestHandleConnection(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	server := TCPServer{}

	expectedReq := []byte(`{"type":"ping"}`)
	expectedResp := []byte(`{"success":true}`)

	handler := func(data []byte) ([]byte, error) {
		require.Equal(t, expectedReq, data)
		return expectedResp, nil
	}

	go server.handleConnection(serverConn, handler)

	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(expectedReq)))

	_, err := clientConn.Write(lenBuf)
	require.NoError(t, err)

	_, err = clientConn.Write(expectedReq)
	require.NoError(t, err)

	respLenBuf := make([]byte, 4)
	_, err = io.ReadFull(clientConn, respLenBuf)
	require.NoError(t, err)

	respLen := binary.BigEndian.Uint32(respLenBuf)

	resp := make([]byte, respLen)
	_, err = io.ReadFull(clientConn, resp)
	require.NoError(t, err)

	require.Equal(t, expectedResp, resp)
}

func TestHandleConnection_PartialLength(t *testing.T) {
	serverConn, clientConn := net.Pipe()

	server := TCPServer{}

	handlerCalled := false

	go server.handleConnection(serverConn, func(data []byte) ([]byte, error) {
		handlerCalled = true
		return nil, nil
	})

	_, err := clientConn.Write([]byte{0x00, 0x00})
	require.NoError(t, err)

	clientConn.Close()

	time.Sleep(100 * time.Millisecond)

	require.False(t, handlerCalled)
}

func TestHandleConnection_PartialPayload(t *testing.T) {
	serverConn, clientConn := net.Pipe()

	server := TCPServer{}

	handlerCalled := false

	go server.handleConnection(serverConn, func(data []byte) ([]byte, error) {
		handlerCalled = true
		return nil, nil
	})

	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, 10)

	clientConn.Write(lenBuf)
	clientConn.Write([]byte("abc"))
	clientConn.Close()

	time.Sleep(100 * time.Millisecond)

	require.False(t, handlerCalled)
}

func TestHandleConnection_HandlerError(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	server := TCPServer{}

	go server.handleConnection(serverConn, func(data []byte) ([]byte, error) {
		return nil, errors.New("boom")
	})

	req := []byte("hello")

	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(req)))

	clientConn.Write(lenBuf)
	clientConn.Write(req)

	respLenBuf := make([]byte, 4)

	clientConn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))

	_, err := io.ReadFull(clientConn, respLenBuf)

	require.Error(t, err)
}

func TestHandleConnection_EmptyPayload(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	server := TCPServer{}

	go server.handleConnection(serverConn, func(data []byte) ([]byte, error) {
		require.Empty(t, data)
		return []byte("ok"), nil
	})

	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, 0)

	clientConn.Write(lenBuf)

	respLenBuf := make([]byte, 4)
	_, err := io.ReadFull(clientConn, respLenBuf)
	require.NoError(t, err)

	respLen := binary.BigEndian.Uint32(respLenBuf)

	resp := make([]byte, respLen)
	_, err = io.ReadFull(clientConn, resp)
	require.NoError(t, err)

	require.Equal(t, "ok", string(resp))
}
