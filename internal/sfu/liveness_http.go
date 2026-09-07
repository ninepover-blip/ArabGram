package sfu

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"go.uber.org/zap"
)

var (
	ErrGroupCallControlAddrMissing  = errors.New("groupcall control: addr is empty")
	ErrGroupCallControlURLMissing   = errors.New("groupcall control: url is empty")
	ErrGroupCallControlTokenMissing = errors.New("groupcall control: bearer token is required")
)

const defaultGroupCallHTTPClientTimeout = 5 * time.Second

type GroupCallTouchFunc func(ctx context.Context, callID, userID int64) error

type GroupCallControlHTTPConfig struct {
	Addr   string
	Token  string
	Touch  GroupCallTouchFunc
	Logger *zap.Logger
}

type groupCallTouchRequest struct {
	CallID int64 `json:"call_id"`
	UserID int64 `json:"user_id"`
}

func NewGroupCallControlHTTPHandler(touch GroupCallTouchFunc, token string) http.Handler {
	mux := http.NewServeMux()
	h := &groupCallControlHandler{touch: touch, token: strings.TrimSpace(token)}
	mux.HandleFunc("/healthz", h.health)
	mux.HandleFunc("/v1/groupcalls/touch", h.touchHandler)
	return h.auth(mux)
}

func StartGroupCallControlHTTP(ctx context.Context, cfg GroupCallControlHTTPConfig) (*http.Server, error) {
	if strings.TrimSpace(cfg.Addr) == "" {
		return nil, ErrGroupCallControlAddrMissing
	}
	if cfg.Touch == nil {
		return nil, errors.New("groupcall control: touch callback is required")
	}
	if strings.TrimSpace(cfg.Token) == "" {
		return nil, ErrGroupCallControlTokenMissing
	}
	if cfg.Logger == nil {
		cfg.Logger = zap.NewNop()
	}
	ln, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		return nil, fmt.Errorf("listen groupcall control %s: %w", cfg.Addr, err)
	}
	srv := &http.Server{Addr: cfg.Addr, Handler: NewGroupCallControlHTTPHandler(cfg.Touch, cfg.Token)}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	go func() {
		cfg.Logger.Info("groupcall control listening", zap.String("addr", ln.Addr().String()))
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			cfg.Logger.Warn("groupcall control exited", zap.Error(err))
		}
	}()
	return srv, nil
}

type groupCallControlHandler struct {
	touch GroupCallTouchFunc
	token string
}

func (h *groupCallControlHandler) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if h == nil || h.token == "" {
			if r.URL != nil && r.URL.Path == "/healthz" {
				next.ServeHTTP(w, r)
				return
			}
			http.Error(w, "bearer token is not configured", http.StatusUnauthorized)
			return
		}
		if r.Header.Get("Authorization") != "Bearer "+h.token {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (h *groupCallControlHandler) health(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *groupCallControlHandler) touchHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req groupCallTouchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	if req.CallID <= 0 || req.UserID <= 0 {
		http.Error(w, "bad touch target", http.StatusBadRequest)
		return
	}
	if err := h.touch(r.Context(), req.CallID, req.UserID); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

type HTTPGroupCallTouchReporter struct {
	client *http.Client
	base   string
	token  string
}

func NewHTTPGroupCallTouchReporter(client *http.Client, baseURL, token string) (*HTTPGroupCallTouchReporter, error) {
	baseURL = strings.TrimSpace(baseURL)
	if baseURL == "" {
		return nil, ErrGroupCallControlURLMissing
	}
	if strings.TrimSpace(token) == "" {
		return nil, ErrGroupCallControlTokenMissing
	}
	if _, err := groupCallControlURL(baseURL, "/v1/groupcalls/touch"); err != nil {
		return nil, err
	}
	if client == nil {
		client = &http.Client{Timeout: defaultGroupCallHTTPClientTimeout}
	}
	return &HTTPGroupCallTouchReporter{client: client, base: baseURL, token: strings.TrimSpace(token)}, nil
}

func (r *HTTPGroupCallTouchReporter) Touch(ctx context.Context, callID, userID int64) error {
	if r == nil {
		return ErrGroupCallControlURLMissing
	}
	u, err := groupCallControlURL(r.base, "/v1/groupcalls/touch")
	if err != nil {
		return err
	}
	raw, err := json.Marshal(groupCallTouchRequest{CallID: callID, UserID: userID})
	if err != nil {
		return fmt.Errorf("groupcall control marshal touch: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if r.token != "" {
		req.Header.Set("Authorization", "Bearer "+r.token)
	}
	res, err := r.client.Do(req)
	if err != nil {
		return fmt.Errorf("groupcall control touch: %w", err)
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode/100 != 2 {
		return fmt.Errorf("groupcall control touch: status %d", res.StatusCode)
	}
	return nil
}

func groupCallControlURL(base, path string) (*url.URL, error) {
	base = strings.TrimSpace(base)
	if base == "" {
		return nil, ErrGroupCallControlURLMissing
	}
	if !strings.Contains(base, "://") {
		base = "http://" + base
	}
	u, err := url.Parse(base)
	if err != nil || u.Host == "" || u.User != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, fmt.Errorf("groupcall control: invalid url %q", base)
	}
	u.Path = strings.TrimRight(u.Path, "/") + path
	u.RawQuery = ""
	u.Fragment = ""
	return u, nil
}
