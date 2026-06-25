package client

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"

	"github.com/x-sushant-x/miniKafka/models"
)

type Client interface {
	Produce(topic string, data []byte) error
	Consume(topic string, offset uint64) (string, error)
}

type TCPClient struct {
	host         string
	port         string
	producerConn net.Conn
	consumerConn net.Conn
}

func NewTCPClient(host, port string) (*TCPClient, error) {
	address := host + ":" + port
	producerConn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, err
	}

	consumerConn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, err
	}

	tcpClient := TCPClient{
		host:         host,
		port:         port,
		producerConn: producerConn,
		consumerConn: consumerConn,
	}

	return &tcpClient, nil
}

func (c *TCPClient) Produce(topic string, data []byte) error {
	req := models.Request{
		Type:  "produce",
		Topic: topic,
		Data:  string(data),
	}

	resp, err := c.send(req)
	if err != nil {
		return err
	}

	if resp.Success == false {
		return errors.New(resp.Data)
	}

	return nil
}

func (c *TCPClient) Consume(topic string, offset uint64) (string, error) {
	req := models.Request{
		Type:   "consume",
		Topic:  topic,
		Offset: offset,
	}

	resp, err := c.send(req)
	if err != nil {
		return "", errors.New(resp.Data)
	}

	if resp.Success == false {
		return "", err
	}

	return resp.Data, nil
}

func (c *TCPClient) send(req models.Request) (*models.Response, error) {
	var conn net.Conn

	if req.Type == "produce" {
		conn = c.producerConn
	} else {
		conn = c.consumerConn
	}

	data, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))

	_, err = conn.Write(lenBuf)
	if err != nil {
		return nil, err
	}

	_, err = conn.Write(data)
	if err != nil {
		return nil, err
	}

	respLenBuf := make([]byte, 4)
	_, err = io.ReadFull(conn, respLenBuf)
	if err != nil {
		return nil, err
	}

	respLen := binary.BigEndian.Uint32(respLenBuf)

	respData := make([]byte, respLen)
	_, err = io.ReadFull(conn, respData)
	if err != nil {
		return nil, err
	}

	var resp models.Response

	if err := json.Unmarshal(respData, &resp); err != nil {
		return nil, err
	}

	return &resp, nil
}
