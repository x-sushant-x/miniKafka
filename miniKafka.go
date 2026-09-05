package main

import (
	"context"

	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/x-sushant-x/miniKafka/broker"
	"github.com/x-sushant-x/miniKafka/config"
)

func init() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
}

func main() {
	log.Info().Msg("Starting miniKafka broker")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := config.LoadConfig(); err != nil {
		panic("unable to load config:" + err.Error())
	}

	if err := config.LoadClusterConfig(); err != nil {
		panic("unable to load cluster config:" + err.Error())
	}

	b, err := broker.New(ctx, config.Config.Broker.Port)
	if err != nil {
		panic("unable to initialize broker " + err.Error())
	}

	go startBroker(b)

	shutdownChan := make(chan os.Signal, 1)
	signal.Notify(shutdownChan, syscall.SIGINT, syscall.SIGTERM)

	sig := <-shutdownChan
	log.Info().Str("signal", sig.String()).Msg("Received graceful shutdown:")
	cancel()
	b.Shutdown()
	log.Info().Msg("Graceful shutdown completed")
}

func startBroker(b *broker.Broker) {
	err := b.Start()
	if err != nil {
		panic("unable to start broker")
	}
}
