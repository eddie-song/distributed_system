package main

import (
	"distributed_cloud_service/internal/cluster"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"go.uber.org/zap"
)

// raftStatus mirrors the node's /raft/status response
type raftStatus struct {
	NodeID       string `json:"node_id"`
	IsLeader     bool   `json:"is_leader"`
	Leader       string `json:"leader"`
	State        string `json:"state"`
	Term         uint64 `json:"term"`
	LastLogIndex uint64 `json:"last_log_index"`
	CommitIndex  uint64 `json:"commit_index"`
	AppliedIndex uint64 `json:"applied_index"`
	Keys         int    `json:"keys"`
}

// node is one supervised node process
type node struct {
	id      string
	cfgPath string
	httpURL string

	cmd       *exec.Cmd
	gen       int // bumped on every start so callbacks from an old process are ignored
	running   bool
	killed    bool // killed by the demo rather than exiting on its own
	startedAt time.Time
	restartAt time.Time
	status    *raftStatus

	// Catch-up tracking after a restart
	catchingUp  bool
	catchTarget uint64
}

type event struct {
	Time time.Time `json:"time"`
	Kind string    `json:"kind"`
	Msg  string    `json:"msg"`
}

// failover records one leader crash and how long the cluster took to recover
type failover struct {
	From       string    `json:"from"`
	To         string    `json:"to"`
	KilledAt   time.Time `json:"killed_at"`
	ElectionMs int64     `json:"election_ms"`
	WriteGapMs *int64    `json:"write_gap_ms"`

	lastWriteBefore time.Time
	elected         bool
}

type supervisor struct {
	bin     string
	workDir string
	logDir  string
	logger  *zap.Logger
	client  *http.Client

	mu           sync.Mutex
	nodes        []*node
	autoRestart  bool
	restartDelay time.Duration
	shuttingDown bool
	leader       string
	events       []event
	pending      *failover
	failovers    []*failover
	catchUpsMs   []int64 // restart-to-caught-up durations
	lastWrite    time.Time
}

func newSupervisor(bin, workDir string, configPaths []string, restartDelay time.Duration, logger *zap.Logger) (*supervisor, error) {
	s := &supervisor{
		bin:          bin,
		workDir:      workDir,
		logDir:       filepath.Join(workDir, "logs"),
		logger:       logger,
		client:       &http.Client{Timeout: 300 * time.Millisecond},
		autoRestart:  restartDelay > 0,
		restartDelay: restartDelay,
	}
	if s.restartDelay <= 0 {
		s.restartDelay = 3 * time.Second
	}
	if err := os.MkdirAll(s.logDir, 0755); err != nil {
		return nil, err
	}
	for _, p := range configPaths {
		cfg, err := cluster.LoadConfig(p)
		if err != nil {
			return nil, err
		}
		abs, err := filepath.Abs(p)
		if err != nil {
			return nil, err
		}
		s.nodes = append(s.nodes, &node{
			id:      cfg.NodeID,
			cfgPath: abs,
			httpURL: "http://" + cfg.ListenAddr,
		})
	}
	return s, nil
}

// checkPortsFree fails fast if nodes from a manual run are still listening
func (s *supervisor) checkPortsFree() error {
	for _, n := range s.nodes {
		addr := n.httpURL[len("http://"):]
		if c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond); err == nil {
			c.Close()
			return fmt.Errorf("%s is already in use; stop any nodes you started manually first", addr)
		}
	}
	return nil
}

// startAll starts the bootstrap node, waits for it to lead, then starts the rest
func (s *supervisor) startAll() error {
	s.mu.Lock()
	first := s.nodes[0]
	err := s.startLocked(first)
	s.mu.Unlock()
	if err != nil {
		return err
	}

	deadline := time.Now().Add(15 * time.Second)
	for s.currentLeader() != first.id {
		if time.Now().After(deadline) {
			return fmt.Errorf("%s did not become leader; see %s", first.id, s.logDir)
		}
		time.Sleep(100 * time.Millisecond)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for _, n := range s.nodes[1:] {
		if err := s.startLocked(n); err != nil {
			return err
		}
	}
	return nil
}

func (s *supervisor) startLocked(n *node) error {
	logFile, err := os.OpenFile(filepath.Join(s.logDir, n.id+".log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	cmd := exec.Command(s.bin, "-config", n.cfgPath)
	cmd.Dir = s.workDir
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		logFile.Close()
		return err
	}

	n.gen++
	gen := n.gen
	n.cmd = cmd
	n.running = true
	n.killed = false
	n.startedAt = time.Now()
	n.restartAt = time.Time{}
	n.status = nil
	if gen > 1 {
		n.catchingUp = true
		n.catchTarget = s.leaderCommitLocked()
	}

	go func() {
		err := cmd.Wait()
		logFile.Close()
		s.onExit(n, gen, err)
	}()
	return nil
}

func (s *supervisor) onExit(n *node, gen int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if gen != n.gen || s.shuttingDown {
		return
	}
	n.running = false
	n.status = nil
	n.catchingUp = false
	if !n.killed {
		s.addEventLocked("crash", fmt.Sprintf("%s exited unexpectedly: %v", n.id, err))
	}
	if s.autoRestart {
		s.scheduleRestartLocked(n)
	}
}

func (s *supervisor) scheduleRestartLocked(n *node) {
	gen := n.gen
	n.restartAt = time.Now().Add(s.restartDelay)
	time.AfterFunc(s.restartDelay, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if n.gen != gen || n.running || !s.autoRestart || s.shuttingDown {
			return
		}
		if err := s.startLocked(n); err != nil {
			s.addEventLocked("error", fmt.Sprintf("Failed to restart %s: %v", n.id, err))
			return
		}
		s.addEventLocked("restart", fmt.Sprintf("Restarted %s; replaying log to catch up", n.id))
	})
}

// kill hard-kills a node process, like a machine losing power
func (s *supervisor) kill(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.findLocked(id)
	if n == nil {
		return fmt.Errorf("unknown node %q", id)
	}
	if !n.running {
		return fmt.Errorf("%s is not running", id)
	}

	wasLeader := id == s.leader
	if wasLeader && s.pending == nil {
		f := &failover{From: id, KilledAt: time.Now()}
		// Only measure a write gap when writes were flowing right before the crash
		if time.Since(s.lastWrite) < time.Second {
			f.lastWriteBefore = s.lastWrite
		}
		s.pending = f
	}

	n.killed = true
	if err := n.cmd.Process.Kill(); err != nil {
		return err
	}
	if wasLeader {
		s.addEventLocked("crash", fmt.Sprintf("Killed leader %s", id))
	} else {
		s.addEventLocked("crash", fmt.Sprintf("Killed follower %s", id))
	}
	return nil
}

func (s *supervisor) killLeader() error {
	leader := s.currentLeader()
	if leader == "" {
		return fmt.Errorf("there is no leader right now")
	}
	return s.kill(leader)
}

func (s *supervisor) start(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.findLocked(id)
	if n == nil {
		return fmt.Errorf("unknown node %q", id)
	}
	if n.running {
		return fmt.Errorf("%s is already running", id)
	}
	if err := s.startLocked(n); err != nil {
		return err
	}
	s.addEventLocked("restart", fmt.Sprintf("Started %s; replaying log to catch up", id))
	return nil
}

func (s *supervisor) setAutoRestart(on bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.autoRestart = on
	for _, n := range s.nodes {
		if on && !n.running && n.restartAt.IsZero() {
			s.scheduleRestartLocked(n)
		}
		if !on {
			n.restartAt = time.Time{}
		}
	}
}

func (s *supervisor) shutdown() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.shuttingDown = true
	for _, n := range s.nodes {
		if n.running {
			n.cmd.Process.Kill()
		}
	}
}

// pollLoop refreshes every node's Raft status until stop is closed
func (s *supervisor) pollLoop(stop <-chan struct{}) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			s.pollOnce()
		}
	}
}

func (s *supervisor) pollOnce() {
	type target struct {
		n   *node
		gen int
		url string
	}
	s.mu.Lock()
	var targets []target
	for _, n := range s.nodes {
		if n.running {
			targets = append(targets, target{n, n.gen, n.httpURL})
		}
	}
	s.mu.Unlock()

	results := make([]*raftStatus, len(targets))
	var wg sync.WaitGroup
	for i, t := range targets {
		wg.Add(1)
		go func(i int, url string) {
			defer wg.Done()
			results[i] = s.fetchStatus(url)
		}(i, t.url)
	}
	wg.Wait()

	s.mu.Lock()
	defer s.mu.Unlock()
	for i, t := range targets {
		if t.n.gen == t.gen && t.n.running {
			t.n.status = results[i]
		}
	}
	s.updateLeaderLocked()
	s.updateCatchUpLocked()
}

func (s *supervisor) fetchStatus(baseURL string) *raftStatus {
	resp, err := s.client.Get(baseURL + "/raft/status")
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil
	}
	var st raftStatus
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		return nil
	}
	return &st
}

func (s *supervisor) updateLeaderLocked() {
	// A deposed leader can briefly still claim leadership, so prefer the highest term
	leader := ""
	var term uint64
	for _, n := range s.nodes {
		if n.running && n.status != nil && n.status.IsLeader && n.status.Term >= term {
			leader, term = n.id, n.status.Term
		}
	}
	if leader == s.leader {
		s.maybeFinishFailoverLocked()
		return
	}

	prev := s.leader
	s.leader = leader
	switch {
	case leader == "" && prev != "":
		s.addEventLocked("warn", "No leader: writes are unavailable until an election completes")
	case s.pending != nil && !s.pending.elected && leader != s.pending.From:
		s.pending.elected = true
		s.pending.To = leader
		s.pending.ElectionMs = time.Since(s.pending.KilledAt).Milliseconds()
		s.addEventLocked("leader", fmt.Sprintf("%s elected leader for term %d, %s after %s crashed",
			leader, term, fmtMs(s.pending.ElectionMs), s.pending.From))
	case leader != "":
		s.addEventLocked("leader", fmt.Sprintf("%s is leader (term %d)", leader, term))
	}
	s.maybeFinishFailoverLocked()
}

func (s *supervisor) updateCatchUpLocked() {
	for _, n := range s.nodes {
		if !n.catchingUp || n.status == nil {
			continue
		}
		if n.status.AppliedIndex >= n.catchTarget {
			n.catchingUp = false
			ms := time.Since(n.startedAt).Milliseconds()
			s.catchUpsMs = append(s.catchUpsMs, ms)
			s.addEventLocked("recover", fmt.Sprintf("%s caught up to index %d (%d keys) in %s",
				n.id, n.status.AppliedIndex, n.status.Keys, fmtMs(ms)))
		}
	}
}

// noteWrite is called by the load generator for every acknowledged write
func (s *supervisor) noteWrite(at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastWrite = at
	if f := s.pending; f != nil && !f.lastWriteBefore.IsZero() && f.WriteGapMs == nil && at.After(f.KilledAt) {
		gap := at.Sub(f.lastWriteBefore).Milliseconds()
		f.WriteGapMs = &gap
		s.addEventLocked("recover", fmt.Sprintf("Writes resumed; clients saw a %s gap", fmtMs(gap)))
	}
	s.maybeFinishFailoverLocked()
}

// loadStopped stops waiting for a write gap that can no longer be measured
func (s *supervisor) loadStopped() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pending != nil {
		s.pending.lastWriteBefore = time.Time{}
	}
	s.maybeFinishFailoverLocked()
}

func (s *supervisor) maybeFinishFailoverLocked() {
	f := s.pending
	if f == nil || !f.elected {
		return
	}
	if !f.lastWriteBefore.IsZero() && f.WriteGapMs == nil {
		return
	}
	s.failovers = append(s.failovers, f)
	s.pending = nil
}

func (s *supervisor) currentLeader() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.leader
}

// leaderURL returns the HTTP base URL of the current leader, or "" if none
func (s *supervisor) leaderURL() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if n := s.findLocked(s.leader); n != nil {
		return n.httpURL
	}
	return ""
}

type nodeRef struct {
	ID  string
	URL string
}

// reachableNodes returns nodes that answered the last status poll
func (s *supervisor) reachableNodes() (up []nodeRef, down []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, n := range s.nodes {
		if n.running && n.status != nil {
			up = append(up, nodeRef{n.id, n.httpURL})
		} else {
			down = append(down, n.id)
		}
	}
	return up, down
}

func (s *supervisor) leaderCommitLocked() uint64 {
	if n := s.findLocked(s.leader); n != nil && n.status != nil {
		return n.status.CommitIndex
	}
	return 0
}

func (s *supervisor) findLocked(id string) *node {
	for _, n := range s.nodes {
		if n.id == id {
			return n
		}
	}
	return nil
}

func (s *supervisor) addEvent(kind, msg string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.addEventLocked(kind, msg)
}

func (s *supervisor) addEventLocked(kind, msg string) {
	s.logger.Info(msg, zap.String("kind", kind))
	s.events = append(s.events, event{Time: time.Now(), Kind: kind, Msg: msg})
	if len(s.events) > 200 {
		s.events = s.events[len(s.events)-200:]
	}
}

type nodeView struct {
	ID          string      `json:"id"`
	HTTP        string      `json:"http"`
	Running     bool        `json:"running"`
	Reachable   bool        `json:"reachable"`
	Status      *raftStatus `json:"status"`
	UptimeMs    int64       `json:"uptime_ms"`
	RestartInMs int64       `json:"restart_in_ms"`
	CatchingUp  bool        `json:"catching_up"`
}

type clusterView struct {
	Nodes        []nodeView  `json:"nodes"`
	Leader       string      `json:"leader"`
	AutoRestart  bool        `json:"auto_restart"`
	RestartDelay int64       `json:"restart_delay_ms"`
	Events       []event     `json:"events"`
	Failovers    []*failover `json:"failovers"`
	Pending      *failover   `json:"pending"`
}

func (s *supervisor) view() clusterView {
	s.mu.Lock()
	defer s.mu.Unlock()
	v := clusterView{
		Leader:       s.leader,
		AutoRestart:  s.autoRestart,
		RestartDelay: s.restartDelay.Milliseconds(),
		Events:       append([]event(nil), s.events...),
		Failovers:    append([]*failover(nil), s.failovers...),
	}
	if s.pending != nil {
		// Copy so the caller can encode it after the lock is released
		p := *s.pending
		v.Pending = &p
	}
	for _, n := range s.nodes {
		nv := nodeView{
			ID:         n.id,
			HTTP:       n.httpURL,
			Running:    n.running,
			Reachable:  n.status != nil,
			Status:     n.status,
			CatchingUp: n.catchingUp,
		}
		if n.running {
			nv.UptimeMs = time.Since(n.startedAt).Milliseconds()
		} else if !n.restartAt.IsZero() {
			nv.RestartInMs = max(0, time.Until(n.restartAt).Milliseconds())
		}
		v.Nodes = append(v.Nodes, nv)
	}
	return v
}

func fmtMs(ms int64) string {
	if ms < 1000 {
		return fmt.Sprintf("%dms", ms)
	}
	return fmt.Sprintf("%.2fs", float64(ms)/1000)
}
