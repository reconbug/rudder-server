package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/event-schema-2/worker"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

func main() {
	conf := config.Default
	stat := stats.Default

	nodeIdx := conf.GetInt("PULSAR_CONSUMER_NODE_IDX", 0)
	totalNodes := conf.GetInt("PULSAR_CONSUMER_TOTAL_NODES", 1)
	workerName := fmt.Sprintf("worker-%d-%d", nodeIdx, totalNodes)
	log := logger.Default.NewLogger().Child("schema-worker").With("worker", workerName)

	log.Info("Starting event schema worker")
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	stat.Start(ctx)
	defer stat.Stop()
	err := worker.Run(ctx, conf, stat, log)
	if err != nil {
		log.Errorf("Stopped event schema worker: %v", err)
		os.Exit(1)
	}
}
