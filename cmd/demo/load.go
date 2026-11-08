package main

import (
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	loadWorkers     = 8
	latencyWindow   = 20000 // most recent acknowledged writes used for percentiles
	seriesSeconds   = 60
	verifyWorkers   = 16
	verifyRetries   = 6
	verifyRetryWait = 500 * time.Millisecond
)

// bucket aggregates one wall-clock second of load
type bucket struct {
	ok   int
	fail int
	lat  []float64
}

type nodeVerify struct {
	ID      string `json:"id"`
	Missing int    `json:"missing"`
}

type verifyResult struct {
	Acked      int          `json:"acked"`
	Lost       int          `json:"lost"` // acknowledged but missing on every reachable node
	Nodes      []nodeVerify `json:"nodes"`
	Skipped    []string     `json:"skipped"`
	DurationMs int64        `json:"duration_ms"`
}

// loadGen writes sequential keys to the current leader and checks that none
// of the acknowledged writes are lost
type loadGen struct {
	sup    *supervisor
	client *http.Client

	mu        sync.Mutex
	running   bool
	stopCh    chan struct{}
	wg        sync.WaitGroup
	rate      int // writes per second; 0 means as fast as possible
	seq       int
	acked     []string
	attempted int
	failed    int
	latencies []float64
	latPos    int
	buckets   map[int64]*bucket
	startedAt time.Time
	verifying bool
	verify    *verifyResult
}

func newLoadGen(sup *supervisor) *loadGen {
	return &loadGen{
		sup:     sup,
		client:  &http.Client{Timeout: 3 * time.Second},
		rate:    100,
		buckets: make(map[int64]*bucket),
	}
}

func (l *loadGen) start(rate int) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if rate < 0 {
		return fmt.Errorf("rate must be >= 0")
	}
	l.rate = rate
	if l.running {
		// Already running: just apply the new rate
		return nil
	}
	if l.verifying {
		return fmt.Errorf("wait for verification to finish")
	}

	l.running = true
	l.stopCh = make(chan struct{})
	l.seq = 0
	l.acked = nil
	l.attempted = 0
	l.failed = 0
	l.latencies = make([]float64, 0, latencyWindow)
	l.latPos = 0
	l.buckets = make(map[int64]*bucket)
	l.startedAt = time.Now()
	l.verify = nil

	tokens := make(chan struct{}, loadWorkers)
	l.wg.Add(1)
	go l.pace(l.stopCh, tokens)
	for i := 0; i < loadWorkers; i++ {
		l.wg.Add(1)
		go l.worker(l.stopCh, tokens)
	}
	l.sup.addEvent("load", fmt.Sprintf("Load started at %s", rateLabel(rate)))
	return nil
}

func (l *loadGen) stop() error {
	l.mu.Lock()
	if !l.running {
		l.mu.Unlock()
		return fmt.Errorf("load is not running")
	}
	l.running = false
	l.verifying = true
	close(l.stopCh)
	l.mu.Unlock()

	go func() {
		l.wg.Wait()
		l.sup.loadStopped()
		l.runVerify()
	}()
	return nil
}

// pace emits write tokens at the configured rate in 10ms steps, which stays
// accurate even with coarse OS timers
func (l *loadGen) pace(stop <-chan struct{}, tokens chan<- struct{}) {
	defer l.wg.Done()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	var credit float64
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
		}
		l.mu.Lock()
		rate := l.rate
		l.mu.Unlock()
		if rate == 0 {
			credit = loadWorkers
		} else {
			credit += float64(rate) / 100
		}
		for credit >= 1 {
			select {
			case tokens <- struct{}{}:
				credit--
			default:
				// Workers are saturated; drop the excess instead of bursting later
				credit = 0
			}
		}
	}
}

func (l *loadGen) worker(stop <-chan struct{}, tokens <-chan struct{}) {
	defer l.wg.Done()
	for {
		select {
		case <-stop:
			return
		case <-tokens:
		}
		l.writeOne()
	}
}

func (l *loadGen) writeOne() {
	l.mu.Lock()
	l.seq++
	key := fmt.Sprintf("demo-%07d", l.seq)
	l.attempted++
	l.mu.Unlock()

	ok := false
	start := time.Now()
	if leader := l.sup.leaderURL(); leader != "" {
		req, err := http.NewRequest(http.MethodPut, leader+"/kv/"+key, strings.NewReader(key))
		if err == nil {
			if resp, err := l.client.Do(req); err == nil {
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				ok = resp.StatusCode == http.StatusNoContent
			}
		}
	}
	done := time.Now()

	l.mu.Lock()
	b := l.bucketLocked(done.Unix())
	if ok {
		ms := float64(done.Sub(start).Microseconds()) / 1000
		l.acked = append(l.acked, key)
		b.ok++
		b.lat = append(b.lat, ms)
		if len(l.latencies) < latencyWindow {
			l.latencies = append(l.latencies, ms)
		} else {
			l.latencies[l.latPos] = ms
			l.latPos = (l.latPos + 1) % latencyWindow
		}
	} else {
		l.failed++
		b.fail++
	}
	l.mu.Unlock()

	if ok {
		l.sup.noteWrite(done)
	} else {
		// Back off briefly so a leaderless cluster isn't hammered
		time.Sleep(20 * time.Millisecond)
	}
}

func (l *loadGen) bucketLocked(sec int64) *bucket {
	b := l.buckets[sec]
	if b == nil {
		b = &bucket{}
		l.buckets[sec] = b
		for s := range l.buckets {
			if s < sec-2*seriesSeconds {
				delete(l.buckets, s)
			}
		}
	}
	return b
}

// runVerify reads every acknowledged key back from every reachable node,
// retrying so followers that are a few entries behind can catch up
func (l *loadGen) runVerify() {
	started := time.Now()
	l.mu.Lock()
	keys := append([]string(nil), l.acked...)
	l.mu.Unlock()

	up, down := l.sup.reachableNodes()
	result := &verifyResult{Acked: len(keys), Skipped: down}
	missingByNode := make([]map[string]bool, len(up))
	for i, n := range up {
		missing := map[string]bool{}
		pending := keys
		for attempt := 0; attempt < verifyRetries && len(pending) > 0; attempt++ {
			if attempt > 0 {
				time.Sleep(verifyRetryWait)
			}
			pending = l.missingOn(n.URL, pending)
		}
		for _, k := range pending {
			missing[k] = true
		}
		missingByNode[i] = missing
		result.Nodes = append(result.Nodes, nodeVerify{ID: n.ID, Missing: len(missing)})
	}
	if len(up) > 0 {
		for k := range missingByNode[0] {
			lostEverywhere := true
			for _, m := range missingByNode[1:] {
				if !m[k] {
					lostEverywhere = false
					break
				}
			}
			if lostEverywhere {
				result.Lost++
			}
		}
	}
	result.DurationMs = time.Since(started).Milliseconds()

	l.mu.Lock()
	l.verify = result
	l.verifying = false
	l.mu.Unlock()

	inconsistent := 0
	for _, n := range result.Nodes {
		inconsistent += n.Missing
	}
	msg := fmt.Sprintf("Verified %d acknowledged writes on %d nodes: %d lost", result.Acked, len(up), result.Lost)
	if inconsistent > result.Lost {
		msg += fmt.Sprintf(", %d missing on some nodes", inconsistent-result.Lost)
	}
	if len(down) > 0 {
		msg += fmt.Sprintf(" (skipped %s: down)", strings.Join(down, ", "))
	}
	kind := "verify"
	if result.Lost > 0 || inconsistent > 0 {
		kind = "error"
	}
	l.sup.addEvent(kind, msg)
}

// missingOn returns the keys that the node does not have with the expected value
func (l *loadGen) missingOn(baseURL string, keys []string) []string {
	var mu sync.Mutex
	var missing []string
	work := make(chan string)
	var wg sync.WaitGroup
	for i := 0; i < verifyWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for k := range work {
				if !l.hasKey(baseURL, k) {
					mu.Lock()
					missing = append(missing, k)
					mu.Unlock()
				}
			}
		}()
	}
	for _, k := range keys {
		work <- k
	}
	close(work)
	wg.Wait()
	return missing
}

func (l *loadGen) hasKey(baseURL, key string) bool {
	resp, err := l.client.Get(baseURL + "/kv/" + key)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	return err == nil && resp.StatusCode == http.StatusOK && string(body) == key
}

type seriesPoint struct {
	T    int64   `json:"t"` // unix seconds
	OK   int     `json:"ok"`
	Fail int     `json:"fail"`
	P50  float64 `json:"p50"`
	P99  float64 `json:"p99"`
}

type loadView struct {
	Running   bool          `json:"running"`
	Verifying bool          `json:"verifying"`
	Rate      int           `json:"rate"`
	Attempted int           `json:"attempted"`
	Acked     int           `json:"acked"`
	Failed    int           `json:"failed"`
	P50       float64       `json:"p50_ms"`
	P99       float64       `json:"p99_ms"`
	ElapsedMs int64         `json:"elapsed_ms"`
	Series    []seriesPoint `json:"series"`
	Verify    *verifyResult `json:"verify"`
}

func (l *loadGen) view() loadView {
	l.mu.Lock()
	defer l.mu.Unlock()
	v := loadView{
		Running:   l.running,
		Verifying: l.verifying,
		Rate:      l.rate,
		Attempted: l.attempted,
		Acked:     len(l.acked),
		Failed:    l.failed,
		Verify:    l.verify,
	}
	if !l.startedAt.IsZero() {
		v.ElapsedMs = time.Since(l.startedAt).Milliseconds()
	}
	v.P50, v.P99 = percentiles(l.latencies)

	// The current second is still filling up, so the series ends at the last full one
	now := time.Now().Unix() - 1
	for sec := now - seriesSeconds + 1; sec <= now; sec++ {
		p := seriesPoint{T: sec}
		if b := l.buckets[sec]; b != nil {
			p.OK, p.Fail = b.ok, b.fail
			p.P50, p.P99 = percentiles(b.lat)
		}
		v.Series = append(v.Series, p)
	}
	return v
}

func percentiles(samples []float64) (p50, p99 float64) {
	if len(samples) == 0 {
		return 0, 0
	}
	sorted := append([]float64(nil), samples...)
	sort.Float64s(sorted)
	at := func(q float64) float64 {
		return sorted[int(q*float64(len(sorted)-1))]
	}
	return at(0.50), at(0.99)
}

func rateLabel(rate int) string {
	if rate == 0 {
		return "max rate"
	}
	return fmt.Sprintf("%d writes/s", rate)
}
