/*
 * TODO - Replace HTTP with TCP.
 * TODO - Optimize this client.
 * TODO - Add batching to client and prevent broker from massive traffic from client.
 * TODO - Remove the limitation of single consumer -> single topic and introduce concurrency support via paritions.
 * TODO - Handle Proper errors.
 * Needs a lot of other improvements.
 */
package client

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"
)

type ProduceRequest struct {
	Data string `json:"data"`
}

type Client struct {
	Host                string
	Port                string
	AutoCommitFrequency int          // Seconds
	HTTPClient          *http.Client // For connecting to broker
	ActiveConsumers     map[string]bool
	TopicOffset         map[string]int64
}

func NewClient(host, port string, autoCommitFrequency int) Client {
	return Client{
		Host:                host,
		Port:                port,
		AutoCommitFrequency: autoCommitFrequency,
		HTTPClient:          &http.Client{},
		ActiveConsumers:     make(map[string]bool),
		TopicOffset:         make(map[string]int64),
	}
}

func (c *Client) Produce(topic, message string) error {
	endpoint := fmt.Sprintf("%s:%s/topics/%s/messages", c.Host, c.Port, topic)

	reqBody := ProduceRequest{
		Data: message,
	}

	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", endpoint, bytes.NewBuffer(jsonBody))
	if err != nil {
		return err
	}

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return errors.New("non 200 status code")
	}

	return nil
}

func (c *Client) Commit(topic string) error {
	offset, ok := c.TopicOffset[topic]
	if !ok {
		return fmt.Errorf("topic %s not being consumed", topic)
	}

	offsetFileName := fmt.Sprintf(".offset_%s", topic)

	return os.WriteFile(
		offsetFileName,
		[]byte(strconv.FormatInt(offset, 10)),
		0644,
	)
}

func (c *Client) loadOffset(topic string) (int64, error) {
	offsetFileName := fmt.Sprintf(".offset_%s", topic)

	bytes, err := os.ReadFile(offsetFileName)
	if os.IsNotExist(err) {
		return 0, nil
	}

	if err != nil {
		return 0, err
	}

	if len(bytes) == 0 {
		return 0, nil
	}

	return strconv.ParseInt(string(bytes), 10, 64)
}

func (c *Client) Consume(topic string, receiverChan chan<- string) error {
	if c.ActiveConsumers[topic] {
		return errors.New(
			"another consumer is already consuming this topic",
		)
	}

	offset, err := c.loadOffset(topic)
	if err != nil {
		return err
	}

	c.TopicOffset[topic] = offset
	c.ActiveConsumers[topic] = true

	go c.consumeLoop(topic, receiverChan)

	return nil
}

func (c *Client) consumeLoop(topic string, receiverChan chan<- string) {
	defer func() {
		c.ActiveConsumers[topic] = false
		close(receiverChan)
	}()

	commitTicker := time.NewTicker(
		time.Duration(c.AutoCommitFrequency) * time.Second,
	)
	defer commitTicker.Stop()

	for {
		select {
		case <-commitTicker.C:
			_ = c.Commit(topic)

		default:
			message, err := c.fetch(
				topic,
				c.TopicOffset[topic],
			)

			if err != nil {
				time.Sleep(time.Second)
				continue
			}

			receiverChan <- message

			c.TopicOffset[topic] = c.TopicOffset[topic] + 1

			if len(message) == 0 {
				time.Sleep(time.Second)
			}
		}
	}
}

func (c *Client) fetch(topic string, offset int64) (string, error) {
	endpoint := fmt.Sprintf(
		"%s:%s/topics/%s/messages?offset=%d",
		c.Host,
		c.Port,
		topic,
		offset,
	)

	req, err := http.NewRequest(
		http.MethodGet,
		endpoint,
		nil,
	)
	if err != nil {
		return "", err
	}

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf(
			"broker returned status %d",
			resp.StatusCode,
		)
	}

	var message string

	if err := json.NewDecoder(resp.Body).Decode(&message); err != nil {
		return "", err
	}

	return message, nil
}
