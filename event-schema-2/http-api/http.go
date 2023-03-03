package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/middleware"
	"github.com/rudderlabs/rudder-server/services/stats"
	"golang.org/x/sync/errgroup"
)

// StartServer starts the http server and returns an error in case of failure.
func StartServer(ctx context.Context, conf *config.Config, stat stats.Stats, repo DBRepo) error {
	wg, ctx := errgroup.WithContext(ctx)

	handler := &httpHandler{
		timeout: conf.GetDuration("HTTP_SERVER_TIMEOUT", 10, time.Second),
		repo:    repo,
	}
	srvMux := mux.NewRouter()
	srvMux.Use(middleware.StatMiddleware(ctx, srvMux, stats.Default, "event_schemas"))
	srvMux.HandleFunc("/{WorkspaceID}/schemas", withJsonContentType(responseHandler(handler.listSchemas))).Methods("GET")
	srvMux.HandleFunc("/{WorkspaceID}/schemas/{SchemaUID}", withJsonContentType(responseHandler(handler.getSchema))).Methods("GET")
	srvMux.HandleFunc("/{WorkspaceID}/schemas/{SchemaUID}/versions", withJsonContentType(responseHandler(handler.listVersions))).Methods("GET")
	srvMux.HandleFunc("/{WorkspaceID}/schemas/{SchemaUID}/versions/{VersionUID}", withJsonContentType(handler.getVersion)).Methods("GET")

	httpServer := &http.Server{
		Addr:    ":" + conf.GetString("HTTP_PORT", "8080"),
		Handler: srvMux,
	}
	wg.Go(func() error {
		err := httpServer.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	})
	wg.Go(func() error {
		<-ctx.Done()
		return httpServer.Shutdown(context.Background())
	})
	return wg.Wait()
}

type httpHandler struct {
	timeout time.Duration
	repo    DBRepo
}

func (h *httpHandler) listSchemas(w http.ResponseWriter, r *http.Request) (*PageInfo[*Schema], *HttpError) {
	workspaceID := workspaceIDParam(r)
	page, httpErr := pageParam(r)
	if httpErr != nil {
		return nil, httpErr
	}
	filter := SchemaFilter{
		WriteKey: h.writeKeyParam(r),
	}
	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()
	schemas, err := h.repo.ListSchemas(ctx, workspaceID, filter, page)
	if err != nil {
		return nil, &HttpError{
			StatusCode: http.StatusInternalServerError,
			Message:    err.Error(),
		}
	}
	return schemas, nil
}

func (h *httpHandler) getSchema(w http.ResponseWriter, r *http.Request) (*SchemaInfo, *HttpError) {
	workspaceID := workspaceIDParam(r)
	schemaID := schemaUIDParam(r)
	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()
	schema, err := h.repo.GetSchema(ctx, workspaceID, schemaID)
	if err != nil {
		return nil, &HttpError{
			StatusCode: http.StatusInternalServerError,
			Message:    err.Error(),
		}
	}
	if schema == nil {
		return nil, &HttpError{
			StatusCode: http.StatusNotFound,
			Message:    "Schema not found",
		}
	}
	return schema, nil
}

func (h *httpHandler) listVersions(w http.ResponseWriter, r *http.Request) (*PageInfo[*Version], *HttpError) {
	workspaceID := workspaceIDParam(r)
	schemaID := schemaUIDParam(r)
	page, httpErr := pageParam(r)
	if httpErr != nil {
		return nil, httpErr
	}
	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()
	versions, err := h.repo.ListVersions(ctx, workspaceID, schemaID, page)
	if err != nil {
		return nil, &HttpError{
			StatusCode: http.StatusInternalServerError,
			Message:    err.Error(),
		}
	}
	return versions, nil
}

func (h *httpHandler) getVersion(w http.ResponseWriter, r *http.Request) {
	workspaceID := workspaceIDParam(r)
	schemaID := schemaUIDParam(r)
	versionID := versionUIDParam(r)
	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()
	version, err := h.repo.GetVersion(ctx, workspaceID, schemaID, versionID)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(err.Error()))
		return
	}
	if version == nil {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("Schema version not found"))
		return
	}
	payload, err := json.Marshal(version)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(err.Error()))
	}
	_, _ = w.Write(payload)
}

func workspaceIDParam(r *http.Request) string {
	return mux.Vars(r)["WorkspaceID"]
}

func schemaUIDParam(r *http.Request) string {
	return mux.Vars(r)["SchemaUID"]
}

func versionUIDParam(r *http.Request) string {
	return mux.Vars(r)["VersionUID"]
}

func pageParam(r *http.Request) (int, *HttpError) {
	page := r.URL.Query().Get("page")
	if page == "" {
		return 1, nil
	}
	p, err := strconv.Atoi(page)
	if err != nil || p < 1 {
		return 0, &HttpError{
			StatusCode: http.StatusBadRequest,
			Message:    "Invalid page number parameter",
		}
	}
	return p, nil
}

func (h *httpHandler) writeKeyParam(r *http.Request) string {
	return r.URL.Query().Get("writeKey")
}

func withJsonContentType(delegate http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Content-Type", "application/json; charset=utf-8")
		delegate(w, r)
	}
}

func responseHandler[T any](delegate func(w http.ResponseWriter, r *http.Request) (T, *HttpError)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		response, httpErr := delegate(w, r)
		if httpErr != nil {
			w.WriteHeader(httpErr.StatusCode)
			_, _ = w.Write([]byte(httpErr.Message))
			return
		}
		payload, err := json.Marshal(response)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(err.Error()))
			return
		}
		_, _ = w.Write(payload)
	}
}
