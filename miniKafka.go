package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

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
	log.Println("Starting miniKafka broker")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	brokerPort := os.Getenv("BROKER_PORT")
	if brokerPort == "" {
		panic("BROKER_PORT not specified in .env")
	}

	b, err := broker.New(ctx, brokerPort)
	if err != nil {
		panic("unable to initialize broker " + err.Error())
	}

	go startBroker(b)

	shutdownChan := make(chan os.Signal, 1)
	signal.Notify(shutdownChan, syscall.SIGINT, syscall.SIGTERM)

	sig := <-shutdownChan
	log.Println("Received graceful shutdown signal:", sig.String())
	cancel()
	b.Shutdown()
	log.Println("Graceful shutdown completed")
}

func startBroker(b *broker.Broker) {
	err := b.Start()
	if err != nil {
		panic("unable to start broker")
	}
}
