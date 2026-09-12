package main

import (
	"context"
	"flag"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"

	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/x-sushant-x/miniKafka/broker"
	"github.com/x-sushant-x/miniKafka/config"
	"github.com/x-sushant-x/miniKafka/raft"
	"github.com/x-sushant-x/miniKafka/utils"
)

func init() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
}

func main() {
	brokerId := flag.String("broker_id", "", "Broker ID")
	truncateFlag := flag.Bool("truncate", false, "Delete all topics data")
	truncateOnly := flag.Bool("truncate_only", false, "Truncate data and do not start miniKafka")
	flag.Parse()

	if brokerId == nil || *brokerId == "" {
		panic("broker_id must be provided while starting miniKafka")
	}

	configFile := fmt.Sprintf("config-%s.json", *brokerId)

	log.Info().Msg("Starting miniKafka broker")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := config.LoadConfig(configFile); err != nil {
		panic("unable to load config:" + err.Error())
	}

	if *truncateOnly {
		truncateData(config.Config)
		return
	} else if *truncateFlag {
		truncateData(config.Config)
	}

	if err := config.LoadClusterConfig(); err != nil {
		panic("unable to load cluster config:" + err.Error())
	}

	raftConfig, found := utils.GetCurrentNodeClusterData(config.Config.Broker.ID)
	if !found {
		panic("raft configuration not found for current node")
	}

	raftConfigMap := utils.BuildRaftConfigMap()
	raftServer := raft.NewServer(raftConfig.ID, raftConfig.Host, raftConfig.RaftPort, raftConfigMap)

	go raftServer.Serve()
	time.Sleep(time.Millisecond * 500)
	raftServer.ConnectToAllPeers()

	b, err := broker.New(ctx, config.Config.Broker.Port, raftServer)
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

func truncateData(c config.Configuration) {
	log.Info().Msg("Truncating Data")
	filepath.Walk(c.TopicsStorageDir, func(path string, file fs.FileInfo, err error) error {
		if strings.HasSuffix(file.Name(), "index") ||
			strings.HasSuffix(file.Name(), "meta") ||
			strings.HasSuffix(file.Name(), "store") {
			err := os.Remove(path)
			if err != nil {
				log.Fatal().Err(err).Msg("unable to truncate data")
			}
		}
		return err
	})
}
