package main

import (
	kvhttp "distributed_cloud_service/internal/http"
	"distributed_cloud_service/internal/auth"
	"distributed_cloud_service/internal/cluster"
	"distributed_cloud_service/internal/metrics"
	"distributed_cloud_service/internal/raft"
	"distributed_cloud_service/internal/store"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	hraft "github.com/hashicorp/raft"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

func computeRaftAddr(httpAddr string, explicit string) (string, error) {
	if explicit != "" {
		return explicit, nil
	}
	host, port, err := net.SplitHostPort(httpAddr)
	if err != nil {
		return "", err
	}
	var p int
	if _, err := fmt.Sscanf(port, "%d", &p); err != nil {
		return "", err
	}
	return net.JoinHostPort(host, fmt.Sprintf("%d", p+10)), nil
}

func instrumentHandler(method, endpoint string, handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		
		// Wrap response writer to capture status
		rw := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}
		
		handler(rw, r)
		
		duration := time.Since(start).Seconds()
		status := strconv.Itoa(rw.statusCode)
		
		metrics.HTTPRequestsTotal.WithLabelValues(method, endpoint, status).Inc()
		metrics.HTTPRequestDuration.WithLabelValues(method, endpoint).Observe(duration)
	}
}

type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

func main() {
	// Parse command line arguments
	configPath := flag.String("config", "configs/node1.yaml", "Path to configuration file")
	flag.Parse()

	// Initialize structured logger
	logger, err := zap.NewProduction()
	if err != nil {
		panic(fmt.Sprintf("failed to initialize logger: %v", err))
	}
	defer logger.Sync()

	// Load configuration
	cfg, err := cluster.LoadConfig(*configPath)
	if err != nil {
		logger.Fatal("Failed to load config", zap.Error(err))
	}

	// Override auth token from env if set
	authToken := cfg.AuthToken
	if envToken := os.Getenv("AUTH_TOKEN"); envToken != "" {
		authToken = envToken
		logger.Info("Using AUTH_TOKEN from environment")
	}

	// Create the store
	kvStore := store.NewStore()

	// Create data directory for Raft
	dataDir := filepath.Join("data", cfg.NodeID)
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		logger.Fatal("Failed to create data directory", zap.Error(err))
	}

	// Initialize Raft
	raftNode, err := raft.NewNode(kvStore, cfg, dataDir)
	if err != nil {
		logger.Fatal("Failed to initialize Raft", zap.Error(err))
	}

	// Optional seed join: if not bootstrap and join_url provided, attempt join
	raftAddr, err := computeRaftAddr(cfg.ListenAddr, cfg.RaftAddr)
	if err != nil {
		logger.Warn("Failed to compute raft addr", zap.Error(err))
	}
	if !cfg.Bootstrap && cfg.JoinURL != "" && raftAddr != "" {
		go func() {
			time.Sleep(2 * time.Second) // Give leader time to start
			payload := map[string]string{
				"node_id":   cfg.NodeID,
				"raft_addr": raftAddr,
			}
			b, _ := json.Marshal(payload)
			joinEndpoint := cfg.JoinURL + "/raft/join"
			resp, err := http.Post(joinEndpoint, "application/json", bytes.NewReader(b))
			if err != nil {
				logger.Error("Join attempt failed", zap.Error(err))
				return
			}
			resp.Body.Close()
			if resp.StatusCode >= 300 {
				logger.Warn("Join attempt returned non-success status", zap.Int("status", resp.StatusCode))
			} else {
				logger.Info("Successfully joined cluster", zap.String("node_id", cfg.NodeID))
			}
		}()
	}

	// Create HTTP server
	server := kvhttp.NewServer(kvStore, raftNode)

	// Setup routes
	mux := http.NewServeMux()

	// Prometheus metrics endpoint
	mux.Handle("/metrics", promhttp.Handler())

	// Key-value store endpoints with auth middleware for writes
	kvHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			metrics.KVPutOperations.Inc()
			server.HandlePut(w, r)
		case http.MethodGet:
			metrics.KVGetOperations.Inc()
			server.HandleGet(w, r)
		case http.MethodDelete:
			metrics.KVDeleteOperations.Inc()
			server.HandleDelete(w, r)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	// Apply auth middleware to write operations (PUT/DELETE)
	writeHandler := auth.AuthMiddleware(authToken)(kvHandler)
	mux.Handle("/kv/", instrumentHandler("KV", "/kv/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut || r.Method == http.MethodDelete {
			writeHandler.ServeHTTP(w, r)
		} else {
			kvHandler.ServeHTTP(w, r)
		}
	}))

	// Health check endpoint
	mux.HandleFunc("/health", instrumentHandler("GET", "/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	}))

	// Raft join endpoint (leader only)
	mux.HandleFunc("/raft/join", instrumentHandler("POST", "/raft/join", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if !raftNode.IsLeader() {
			http.Error(w, "Not the leader", http.StatusBadRequest)
			return
		}
		var body struct {
			NodeID   string `json:"node_id"`
			RaftAddr string `json:"raft_addr"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "invalid body", http.StatusBadRequest)
			return
		}
		if body.NodeID == "" || body.RaftAddr == "" {
			http.Error(w, "node_id and raft_addr required", http.StatusBadRequest)
			return
		}
		if err := raftNode.Join(body.NodeID, body.RaftAddr); err != nil {
			logger.Error("Join failed", zap.Error(err), zap.String("node_id", body.NodeID))
			http.Error(w, "join failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
		logger.Info("Node joined cluster", zap.String("node_id", body.NodeID))
		w.WriteHeader(http.StatusNoContent)
	}))

	// Raft remove endpoint (leader only)
	mux.HandleFunc("/raft/remove", instrumentHandler("POST", "/raft/remove", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if !raftNode.IsLeader() {
			http.Error(w, "Not the leader", http.StatusBadRequest)
			return
		}
		var body struct {
			NodeID string `json:"node_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "invalid body", http.StatusBadRequest)
			return
		}
		if body.NodeID == "" {
			http.Error(w, "node_id required", http.StatusBadRequest)
			return
		}
		if err := raftNode.Remove(body.NodeID); err != nil {
			logger.Error("Remove failed", zap.Error(err), zap.String("node_id", body.NodeID))
			http.Error(w, "remove failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
		logger.Info("Node removed from cluster", zap.String("node_id", body.NodeID))
		w.WriteHeader(http.StatusNoContent)
	}))

	// Raft config endpoint (live membership)
	mux.HandleFunc("/raft/config", instrumentHandler("GET", "/raft/config", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		cfgFuture := raftNode.GetRaft().GetConfiguration()
		if err := cfgFuture.Error(); err != nil {
			http.Error(w, "failed to get config: "+err.Error(), http.StatusInternalServerError)
			return
		}
		type server struct {
			ID      string `json:"id"`
			Address string `json:"address"`
			Voter   bool   `json:"voter"`
		}
		resp := struct{ Servers []server `json:"servers"` }{}
		for _, s := range cfgFuture.Configuration().Servers {
			resp.Servers = append(resp.Servers, server{ID: string(s.ID), Address: string(s.Address), Voter: s.Suffrage == hraft.Voter})
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))

	// Cluster status endpoint
	mux.HandleFunc("/cluster/status", instrumentHandler("GET", "/cluster/status", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		response := map[string]interface{}{
			"node_id": cfg.NodeID,
			"address": cfg.ListenAddr,
			"peers":   cfg.Peers,
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))

	// Cluster endpoints - live members from Raft
	mux.HandleFunc("/cluster/members", instrumentHandler("GET", "/cluster/members", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		cfgFuture := raftNode.GetRaft().GetConfiguration()
		if err := cfgFuture.Error(); err != nil {
			http.Error(w, "failed to get members: "+err.Error(), http.StatusInternalServerError)
			return
		}
		out := map[string]interface{}{"members": []map[string]interface{}{}}
		for _, s := range cfgFuture.Configuration().Servers {
			out["members"] = append(out["members"].([]map[string]interface{}), map[string]interface{}{
				"id":      string(s.ID),
				"address": string(s.Address),
				"voter":   s.Suffrage == hraft.Voter,
			})
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(out)
	}))

	// Raft status endpoint
	mux.HandleFunc("/raft/status", instrumentHandler("GET", "/raft/status", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		
		// Update metrics
		if raftNode.IsLeader() {
			metrics.RaftIsLeader.Set(1)
		} else {
			metrics.RaftIsLeader.Set(0)
		}
		
		stats := raftNode.GetRaft().Stats()
		parseIndex := func(key string) uint64 {
			v, _ := strconv.ParseUint(stats[key], 10, 64)
			return v
		}
		appliedIdx := parseIndex("applied_index")
		commitIdx := parseIndex("commit_index")
		metrics.RaftAppliedIndex.Set(float64(appliedIdx))
		metrics.RaftCommitIndex.Set(float64(commitIdx))
		keys := kvStore.Len()
		metrics.KVStoreSize.Set(float64(keys))

		status := map[string]interface{}{
			"node_id":        cfg.NodeID,
			"is_leader":      raftNode.IsLeader(),
			"leader":         raftNode.Leader(),
			"state":          raftNode.GetRaft().State().String(),
			"term":           parseIndex("term"),
			"last_log_index": parseIndex("last_log_index"),
			"commit_index":   commitIdx,
			"applied_index":  appliedIdx,
			"keys":           keys,
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(status)
	}))

	// Dashboard - serve HTML file
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		http.Redirect(w, r, "/dashboard", http.StatusFound)
	})
	mux.HandleFunc("/dashboard", func(w http.ResponseWriter, r *http.Request) {
		dashboardPath := filepath.Join("web", "dashboard.html")
		http.ServeFile(w, r, dashboardPath)
	})

	// Background task to update store size metric
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			// Store doesn't expose size, so we'll skip this for now
			// In a real implementation, you'd add a Size() method to the store
		}
	}()

	// Create HTTP server with graceful shutdown support
	addr := cfg.ListenAddr
	httpServer := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	// Setup graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Start server in goroutine
	go func() {
		logger.Info("Starting node",
			zap.String("node_id", cfg.NodeID),
			zap.String("address", addr),
			zap.Bool("auth_enabled", authToken != ""),
		)
		logger.Info("Endpoints available",
			zap.String("kv", "/kv/{key}"),
			zap.String("health", "/health"),
			zap.String("metrics", "/metrics"),
			zap.String("raft_join", "/raft/join"),
			zap.String("raft_remove", "/raft/remove"),
			zap.String("raft_config", "/raft/config"),
			zap.String("raft_status", "/raft/status"),
			zap.String("cluster_status", "/cluster/status"),
			zap.String("cluster_members", "/cluster/members"),
			zap.String("dashboard", "/dashboard"),
		)

		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal("Server failed to start", zap.Error(err))
		}
	}()

	// Wait for shutdown signal
	<-sigChan
	logger.Info("Shutdown signal received, gracefully shutting down...")

	// Shutdown HTTP server
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("HTTP server shutdown error", zap.Error(err))
	}

	// Shutdown Raft node
	if err := raftNode.Shutdown(); err != nil {
		logger.Error("Raft shutdown error", zap.Error(err))
	}

	logger.Info("Node shutdown complete")
}
