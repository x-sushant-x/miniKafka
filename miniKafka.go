package main

import (
	"os"

	"github.com/joho/godotenv"
	"github.com/x-sushant-x/miniKafka/broker"
)

func init() {
	err := godotenv.Load(".env")
	if err != nil {
		panic("unable to load .env")
	}
}

func main() {
	brokerPort := os.Getenv("BROKER_PORT")
	if brokerPort == "" {
		panic("BROKER_PORT not specified in .env")
	}

	b, err := broker.New(brokerPort)
	if err != nil {
		panic("unable to initialize broker")
	}

	go startBroker(b)

	select {}
}

func startBroker(b *broker.Broker) {
	err := b.Start()
	if err != nil {
		panic("unable to start broker")
	}
}
