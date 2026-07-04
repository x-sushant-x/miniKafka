package client

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"

	"github.com/x-sushant-x/miniKafka/models"
)

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

func (c *TCPClient) Produce(topic string, data []byte, key string) error {
	req := models.Request{
		Type:  "produce",
		Topic: topic,
		Data:  string(data),
		Key:   key,
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

func (c *TCPClient) Consume(topic string, offset uint64, partition int) (string, error) {
	req := models.Request{
		Type:      "consume",
		Topic:     topic,
		Offset:    offset,
		Partition: partition,
	}

	resp, err := c.send(req)
	if err != nil {
		return "", err
	}

	if resp.Success == false {
		return "", errors.New(resp.Data)
	}

	return resp.Data, nil
}

func (c *TCPClient) CreateTopic(topic string, totalPartitions int) (*models.Response, error) {
	req := models.Request{
		Type:            "create_topic",
		Topic:           topic,
		TotalPartitions: totalPartitions,
	}

	resp, err := c.send(req)

	if err != nil {
		return nil, err
	}

	if resp.Success == false {
		return nil, errors.New(resp.Error)
	}

	return resp, nil
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
