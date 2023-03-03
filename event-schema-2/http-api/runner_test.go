package httpapi

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/phayes/freeport"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/event-schema-2/testcommons"
	"github.com/rudderlabs/rudder-server/services/stats/memstats"
	"github.com/rudderlabs/rudder-server/utils/httputil"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestRunner(t *testing.T) {
	postgres := testcommons.PostgresqlResource(t)
	refTime := time.Now().Round(24 * time.Hour)
	workspaceID1 := "ws1"
	writeKey1 := "test-write-key-1"

	_, _ = seedSchemas(t, postgres.DB, workspaceID1, writeKey1, refTime, 1, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	port, err := freeport.GetFreePort()
	require.NoError(t, err)
	conf := config.New()
	conf.Set("HTTP_PORT", port)
	conf.Set("DB_DSN", postgres.DBDsn)
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return Run(ctx, conf, memstats.New(), logger.NOP)
	})

	require.Eventually(t, func() bool {
		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/%s/schemas", port, workspaceID1))
		defer httputil.CloseResponse(resp)
		return err == nil && resp.StatusCode == http.StatusOK
	}, 5*time.Second, 100*time.Millisecond, "http server should start")

	t.Run("try to run again (same port)", func(t *testing.T) {
		err := Run(ctx, conf, memstats.New(), logger.NOP)
		require.Error(t, err)
		require.Contains(t, err.Error(), "address already in use")
	})

	t.Run("invalid postgres DSN", func(t *testing.T) {
		conf.Set("DB_DSN", fmt.Sprintf("postgres://%s:%s@localhost:%s/%s?sslmode=disable", postgres.User, postgres.Password, postgres.Port, postgres.Database+"1"))
		err := Run(ctx, conf, memstats.New(), logger.NOP)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to setup remote postgres db")
	})

	cancel()
	require.NoError(t, g.Wait())
}
