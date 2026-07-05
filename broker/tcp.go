package broker

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"

	"github.com/x-sushant-x/miniKafka/utils"
)

const (
	tcpMsgLenWidth = 4 // Bytes
)

type RequestHandler func([]byte) ([]byte, error)

type TCPServer struct {
	Port string
	ln   net.Listener
	wg   sync.WaitGroup
}

func NewTCPServer(port string) (*TCPServer, error) {
	ln, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return nil, err
	}

	server := TCPServer{
		Port: port,
		ln:   ln,
	}

	return &server, nil
}

func (t *TCPServer) StartServer(handler RequestHandler) error {
	for {
		conn, err := t.ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			continue
		}

		t.wg.Go(func() {
			go t.handleConnection(conn, handler)
		})
	}
}

func (t *TCPServer) handleConnection(conn net.Conn, handler RequestHandler) {
	defer conn.Close()

	for {
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

		if _, err := utils.WriteFull(conn, respLen); err != nil {
			return
		}

		if _, err := utils.WriteFull(conn, resp); err != nil {
			return
		}
	}
}

func (t *TCPServer) close() {
	t.ln.Close()
}
