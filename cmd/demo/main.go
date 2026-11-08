// Command demo runs a local 3-node cluster under a supervisor and serves a
// control room UI for crashing nodes, generating load and watching failover.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"go.uber.org/zap"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "Control room HTTP address")
	fresh := flag.Bool("fresh", true, "Wipe demo cluster data before starting")
	restartDelay := flag.Duration("restart-delay", 3*time.Second, "Delay before a killed node is restarted (0 disables auto-restart)")
	bench := flag.Int("bench", 0, "Run N leader-kill trials without the UI, print a Markdown report and exit")
	flag.Parse()
	if *bench > 0 && *restartDelay <= 0 {
		fmt.Fprintln(os.Stderr, "-bench needs auto-restart; set -restart-delay above 0")
		os.Exit(2)
	}

	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(fmt.Sprintf("failed to initialize logger: %v", err))
	}
	defer logger.Sync()

	// Demo nodes run inside data/demo so they never touch data from manual runs
	workDir, err := filepath.Abs(filepath.Join("data", "demo"))
	if err != nil {
		logger.Fatal("Failed to resolve demo directory", zap.Error(err))
	}
	if *fresh {
		if err := os.RemoveAll(workDir); err != nil {
			logger.Fatal("Failed to wipe demo data", zap.Error(err))
		}
	}
	if err := os.MkdirAll(workDir, 0755); err != nil {
		logger.Fatal("Failed to create demo directory", zap.Error(err))
	}

	bin, err := buildNode(workDir)
	if err != nil {
		logger.Fatal("Failed to build node binary", zap.Error(err))
	}

	configs := []string{"configs/node1.yaml", "configs/node2.yaml", "configs/node3.yaml"}
	sup, err := newSupervisor(bin, workDir, configs, *restartDelay, logger)
	if err != nil {
		logger.Fatal("Failed to create supervisor", zap.Error(err))
	}
	if err := sup.checkPortsFree(); err != nil {
		logger.Fatal("Cannot start demo cluster", zap.Error(err))
	}

	stopPolling := make(chan struct{})
	go sup.pollLoop(stopPolling)

	logger.Info("Starting 3-node cluster", zap.String("logs", sup.logDir))
	if err := sup.startAll(); err != nil {
		sup.shutdown()
		logger.Fatal("Cluster failed to start", zap.Error(err))
	}

	load := newLoadGen(sup)
	if *bench > 0 {
		report, err := runBench(sup, load, *bench)
		close(stopPolling)
		sup.shutdown()
		if err != nil {
			logger.Fatal("Benchmark failed", zap.Error(err))
		}
		fmt.Println(report)
		return
	}

	httpServer := &http.Server{Addr: *addr, Handler: routes(sup, load)}
	go func() {
		logger.Info("Control room ready", zap.String("url", "http://"+*addr))
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			sup.shutdown()
			logger.Fatal("Control room failed to start", zap.Error(err))
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan
	logger.Info("Shutting down demo cluster")
	close(stopPolling)
	httpServer.Close()
	sup.shutdown()
}

// buildNode compiles cmd/node so the demo always runs the current code
func buildNode(workDir string) (string, error) {
	bin := filepath.Join(workDir, "bin", "node")
	if runtime.GOOS == "windows" {
		bin += ".exe"
	}
	cmd := exec.Command("go", "build", "-o", bin, "./cmd/node")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return "", err
	}
	return bin, nil
}

func routes(sup *supervisor, load *loadGen) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, filepath.Join("web", "demo.html"))
	})

	mux.HandleFunc("GET /api/state", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, struct {
			Cluster clusterView `json:"cluster"`
			Load    loadView    `json:"load"`
			Now     time.Time   `json:"now"`
		}{sup.view(), load.view(), time.Now()})
	})

	mux.HandleFunc("POST /api/nodes/{id}/kill", func(w http.ResponseWriter, r *http.Request) {
		respond(w, sup.kill(r.PathValue("id")))
	})
	mux.HandleFunc("POST /api/nodes/{id}/start", func(w http.ResponseWriter, r *http.Request) {
		respond(w, sup.start(r.PathValue("id")))
	})
	mux.HandleFunc("POST /api/kill-leader", func(w http.ResponseWriter, r *http.Request) {
		respond(w, sup.killLeader())
	})

	mux.HandleFunc("POST /api/settings", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			AutoRestart bool `json:"auto_restart"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "invalid body", http.StatusBadRequest)
			return
		}
		sup.setAutoRestart(body.AutoRestart)
		w.WriteHeader(http.StatusNoContent)
	})

	mux.HandleFunc("POST /api/load/start", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Rate int `json:"rate"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "invalid body", http.StatusBadRequest)
			return
		}
		respond(w, load.start(body.Rate))
	})
	mux.HandleFunc("POST /api/load/stop", func(w http.ResponseWriter, r *http.Request) {
		respond(w, load.stop())
	})

	return mux
}

func respond(w http.ResponseWriter, err error) {
	if err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	json.NewEncoder(w).Encode(v)
}
