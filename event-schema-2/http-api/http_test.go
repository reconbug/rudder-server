package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/phayes/freeport"
	"github.com/rudderlabs/rudder-server/config"
	eventschema2 "github.com/rudderlabs/rudder-server/event-schema-2"
	"github.com/rudderlabs/rudder-server/event-schema-2/testcommons"
	"github.com/rudderlabs/rudder-server/services/stats/memstats"
	"github.com/rudderlabs/rudder-server/utils/httputil"
	"github.com/samber/lo"
	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"
)

func TestHttpAPI(t *testing.T) {
	postgres := testcommons.PostgresqlResource(t)
	refTime := time.Now().Round(24 * time.Hour)
	workspaceID1 := "ws1"
	writeKey1 := "test-write-key-1"
	writeKey2 := "test-write-key-2"
	var ws1Schemas []*eventschema2.EventSchemaInfo
	var ws1Versions []*eventschema2.EventSchemaVersionInfo

	s, v := seedSchemas(t, postgres.DB, workspaceID1, writeKey1, refTime, 2, 1)
	ws1Schemas = append(ws1Schemas, s...)
	ws1Versions = append(ws1Versions, v...)
	s, v = seedSchemas(t, postgres.DB, workspaceID1, writeKey2, refTime, 2, 1)
	ws1Schemas = append(ws1Schemas, s...)
	ws1Versions = append(ws1Versions, v...)

	slices.SortFunc(ws1Schemas, func(i, j *eventschema2.EventSchemaInfo) bool { return i.UID < j.UID })

	repo := &dbRepo{
		pageSize:      10,
		retentionDays: 2,
		db:            postgres.DB,
	}
	mockRepo := &mockRepo{DBRepo: repo}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	port, err := freeport.GetFreePort()
	require.NoError(t, err)
	conf := config.New()
	conf.Set("HTTP_PORT", port)
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		err := StartServer(ctx, conf, memstats.New(), mockRepo)
		if err != nil {
			t.Error(err)
		}
		return err
	})

	require.Eventually(t, func() bool {
		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/%s/schemas", port, workspaceID1))
		defer httputil.CloseResponse(resp)
		return err == nil && resp.StatusCode == http.StatusOK
	}, 5*time.Second, 100*time.Millisecond, "http server should start")

	t.Run("list schemas", func(t *testing.T) {
		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/%s/schemas", port, workspaceID1))
		require.NoError(t, err)
		defer httputil.CloseResponse(resp)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		var page PageInfo[*Schema]
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&page))
		require.Len(t, page.Results, 4)
		require.Equal(t, lo.Map(ws1Schemas, func(s *eventschema2.EventSchemaInfo, _ int) *Schema { return EventSchemaInfoToSchema(s) }), page.Results)

		t.Run("provide page param", func(t *testing.T) {
			resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/%s/schemas?page=2", port, workspaceID1))
			require.NoError(t, err)
			defer httputil.CloseResponse(resp)
			require.Equal(t, http.StatusOK, resp.StatusCode)
			var page PageInfo[*Schema]
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&page))
			require.Len(t, page.Results, 0)
			require.False(t, page.HasNext)
		})

		t.Run("provide invalid page param", func(t *testing.T) {
			resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/%s/schemas?page=0", port, workspaceID1))
			require.NoError(t, err)
			defer httputil.CloseResponse(resp)
			require.Equal(t, http.StatusBadRequest, resp.StatusCode)
		})

		t.Run("repo error", func(t *testing.T) {
			mockRepo.err = fmt.Errorf("some error")
			defer func() { mockRepo.err = nil }()
			resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/%s/schemas", port, workspaceID1))
			require.NoError(t, err)
			defer httputil.CloseResponse(resp)
			require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
		})
	})

	t.Run("get schema", func(t *testing.T) {
		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/%s/schemas/%s", port, workspaceID1, ws1Schemas[0].UID))
		require.NoError(t, err)
		defer httputil.CloseResponse(resp)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		var schema *SchemaInfo
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&schema))

		versions := lo.Filter(ws1Versions, func(v *eventschema2.EventSchemaVersionInfo, _ int) bool {
			return v.SchemaUID == ws1Schemas[0].UID
		})
		slices.SortFunc(versions, func(i, j *eventschema2.EventSchemaVersionInfo) bool { // descending order
			return i.UID > j.UID
		})

		require.Equal(t, EventSchemaInfoToSchemaInfo(ws1Schemas[0], versions), schema)

		t.Run("provide invalid schema uid", func(t *testing.T) {
			resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/%s/schemas/%s", port, workspaceID1, ksuid.New().String()))
			require.NoError(t, err)
			defer httputil.CloseResponse(resp)
			require.Equal(t, http.StatusNotFound, resp.StatusCode)
		})
		t.Run("repo error", func(t *testing.T) {
			mockRepo.err = fmt.Errorf("some error")
			defer func() { mockRepo.err = nil }()
			resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/%s/schemas/%s", port, workspaceID1, ws1Schemas[0].UID))
			require.NoError(t, err)
			defer httputil.CloseResponse(resp)
			require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
		})
	})

	t.Run("list versions", func(t *testing.T) {
		schemaUID := ws1Schemas[0].UID
		schemaVersions := lo.Filter(ws1Versions, func(v *eventschema2.EventSchemaVersionInfo, _ int) bool {
			return v.SchemaUID == schemaUID
		})

		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/%s/schemas/%s/versions", port, workspaceID1, schemaUID))
		require.NoError(t, err)
		defer httputil.CloseResponse(resp)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		var page PageInfo[*Version]
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&page))
		require.Len(t, page.Results, len(schemaVersions))
		require.Equal(t, lo.Map(schemaVersions, func(v *eventschema2.EventSchemaVersionInfo, _ int) *Version {
			return EventSchemaVersionInfoToVersion(v)
		}), page.Results)

		t.Run("provide page param", func(t *testing.T) {
			resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/%s/schemas/%s/versions?page=2", port, workspaceID1, schemaUID))
			require.NoError(t, err)
			defer httputil.CloseResponse(resp)
			require.Equal(t, http.StatusOK, resp.StatusCode)
			var page PageInfo[*Version]
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&page))
			require.Len(t, page.Results, 0)
			require.False(t, page.HasNext)
		})

		t.Run("provide invalid page param", func(t *testing.T) {
			resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/%s/schemas/%s/versions?page=0", port, workspaceID1, schemaUID))
			require.NoError(t, err)
			defer httputil.CloseResponse(resp)
			require.Equal(t, http.StatusBadRequest, resp.StatusCode)
		})

		t.Run("repo error", func(t *testing.T) {
			mockRepo.err = fmt.Errorf("some error")
			defer func() { mockRepo.err = nil }()
			resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/%s/schemas/%s/versions", port, workspaceID1, schemaUID))
			require.NoError(t, err)
			defer httputil.CloseResponse(resp)
			require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
		})
	})

	t.Run("get version", func(t *testing.T) {
		schemaUID := ws1Schemas[0].UID
		v := lo.Filter(ws1Versions, func(v *eventschema2.EventSchemaVersionInfo, _ int) bool {
			return v.SchemaUID == schemaUID
		})[0]

		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/%s/schemas/%s/versions/%s", port, workspaceID1, schemaUID, v.UID))
		require.NoError(t, err)
		defer httputil.CloseResponse(resp)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		var version *VersionInfo
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&version))
		require.Equal(t, EventSchemaVersionInfoToVersionInfo(v), version)
		t.Run("provide invalid version uid", func(t *testing.T) {
			resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/%s/schemas/%s/versions/%s", port, workspaceID1, schemaUID, ksuid.New().String()))
			require.NoError(t, err)
			defer httputil.CloseResponse(resp)
			require.Equal(t, http.StatusNotFound, resp.StatusCode)
		})
		t.Run("repo error", func(t *testing.T) {
			mockRepo.err = fmt.Errorf("some error")
			defer func() { mockRepo.err = nil }()
			resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/%s/schemas/%s/versions/%s", port, workspaceID1, schemaUID, v.UID))
			require.NoError(t, err)
			defer httputil.CloseResponse(resp)
			require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
		})
	})

	cancel()
	require.NoError(t, g.Wait())
}

type mockRepo struct {
	DBRepo
	err error
}

func (m *mockRepo) ListSchemas(ctx context.Context, workspaceID string, filter SchemaFilter, page int) (*PageInfo[*Schema], error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.DBRepo.ListSchemas(ctx, workspaceID, filter, page)
}

func (m *mockRepo) GetSchema(ctx context.Context, workspaceID, schemaUid string) (*SchemaInfo, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.DBRepo.GetSchema(ctx, workspaceID, schemaUid)
}

func (m *mockRepo) ListVersions(ctx context.Context, workspaceID, schemaUid string, page int) (*PageInfo[*Version], error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.DBRepo.ListVersions(ctx, workspaceID, schemaUid, page)
}

func (m *mockRepo) GetVersion(ctx context.Context, workspaceID, schemaUid, versionUid string) (*VersionInfo, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.DBRepo.GetVersion(ctx, workspaceID, schemaUid, versionUid)
}
