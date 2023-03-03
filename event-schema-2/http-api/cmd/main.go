package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/rudderlabs/rudder-server/config"
	httpapi "github.com/rudderlabs/rudder-server/event-schema-2/http-api"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

func main() {
	conf := config.Default
	stat := stats.Default
	log := logger.Default.NewLogger().Child("schema-http-api")

	log.Info("Starting event schemas http api server")
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	stat.Start(ctx)
	defer stat.Stop()
	err := httpapi.Run(ctx, conf, stat, log)
	if err != nil {
		log.Errorf("Stopped event schemas http api server: %v", err)
		os.Exit(1)
	}
}
