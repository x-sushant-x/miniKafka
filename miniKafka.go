package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/x-sushant-x/miniKafka/broker"
	"github.com/x-sushant-x/miniKafka/config"
)

func main() {
	log.Println("Starting miniKafka broker")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := config.LoadConfig(); err != nil {
		panic("unable to load config:" + err.Error())
	}

	b, err := broker.New(ctx, config.Config.BrokerPort)
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
