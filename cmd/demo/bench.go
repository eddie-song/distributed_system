package main

import (
	"fmt"
	"runtime"
	"sort"
	"strings"
	"time"
)

const (
	benchThroughputRun = 10 * time.Second
	benchFailoverRate  = 100
	benchSettle        = 2 * time.Second
)

// runBench drives the same scenario as the control room without a browser and
// prints a Markdown report: max throughput, then repeated leader kills under load
func runBench(sup *supervisor, load *loadGen, trials int) (string, error) {
	var b strings.Builder
	fmt.Fprintf(&b, "## Benchmark results\n\n")
	fmt.Fprintf(&b, "%s · %s/%s · %d logical CPUs · 3 nodes on one machine · %s\n\n",
		time.Now().Format("2006-01-02"), runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), runtime.Version())

	// Phase 1: saturate the leader to find max throughput
	if err := waitReady(sup, 30*time.Second); err != nil {
		return "", err
	}
	if err := load.start(0); err != nil {
		return "", err
	}
	time.Sleep(benchThroughputRun)
	tp := load.view()
	if err := load.stop(); err != nil {
		return "", err
	}
	tpVerify, err := waitVerify(load)
	if err != nil {
		return "", err
	}
	secs := float64(tp.ElapsedMs) / 1000
	fmt.Fprintf(&b, "### Throughput (%d concurrent clients, %s, no failures injected)\n\n", loadWorkers, benchThroughputRun)
	fmt.Fprintf(&b, "| Metric | Value |\n|---|---|\n")
	fmt.Fprintf(&b, "| Acknowledged writes | %d |\n", tp.Acked)
	fmt.Fprintf(&b, "| Throughput | %.0f writes/s |\n", float64(tp.Acked)/secs)
	fmt.Fprintf(&b, "| Write latency p50 | %.1f ms |\n", tp.P50)
	fmt.Fprintf(&b, "| Write latency p99 | %.1f ms |\n", tp.P99)
	fmt.Fprintf(&b, "| Lost acknowledged writes | %d of %d |\n\n", tpVerify.Lost, tpVerify.Acked)

	// Phase 2: kill the leader repeatedly under steady load
	if err := load.start(benchFailoverRate); err != nil {
		return "", err
	}
	for i := 0; i < trials; i++ {
		if err := waitReady(sup, 30*time.Second); err != nil {
			return "", err
		}
		time.Sleep(benchSettle)
		before := len(sup.view().Failovers)
		if err := sup.killLeader(); err != nil {
			return "", err
		}
		deadline := time.Now().Add(15 * time.Second)
		for len(sup.view().Failovers) == before {
			if time.Now().After(deadline) {
				return "", fmt.Errorf("trial %d: no failover recorded within 15s", i+1)
			}
			time.Sleep(50 * time.Millisecond)
		}
		fmt.Printf("trial %d/%d done\n", i+1, trials)
	}
	// Let the last killed node come back so the verification covers all three
	if err := waitReady(sup, 30*time.Second); err != nil {
		return "", err
	}
	fo := load.view()
	if err := load.stop(); err != nil {
		return "", err
	}
	foVerify, err := waitVerify(load)
	if err != nil {
		return "", err
	}

	v := sup.view()
	var elections, gaps []int64
	for _, f := range v.Failovers {
		elections = append(elections, f.ElectionMs)
		if f.WriteGapMs != nil {
			gaps = append(gaps, *f.WriteGapMs)
		}
	}
	sup.mu.Lock()
	catchUps := append([]int64(nil), sup.catchUpsMs...)
	sup.mu.Unlock()

	fmt.Fprintf(&b, "### Leader failover (%d hard kills of the leader at %d writes/s)\n\n", trials, benchFailoverRate)
	fmt.Fprintf(&b, "| Metric | p50 | p90 | max |\n|---|---|---|---|\n")
	fmt.Fprintf(&b, "| Time to elect a new leader | %s |\n", statsRow(elections))
	fmt.Fprintf(&b, "| Write gap seen by clients | %s |\n", statsRow(gaps))
	fmt.Fprintf(&b, "| Restarted node catch-up | %s |\n\n", statsRow(catchUps))
	fmt.Fprintf(&b, "| Metric | Value |\n|---|---|\n")
	fmt.Fprintf(&b, "| Writes attempted during failover run | %d |\n", fo.Attempted)
	fmt.Fprintf(&b, "| Acknowledged | %d |\n", fo.Acked)
	fmt.Fprintf(&b, "| Rejected while no leader | %d |\n", fo.Failed)
	fmt.Fprintf(&b, "| Lost acknowledged writes | %d of %d (verified on %d nodes) |\n",
		foVerify.Lost, foVerify.Acked, len(foVerify.Nodes))
	return b.String(), nil
}

// waitReady blocks until every node is up, caught up and a leader exists
func waitReady(sup *supervisor, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		v := sup.view()
		ready := v.Leader != "" && v.Pending == nil
		for _, n := range v.Nodes {
			if !n.Reachable || n.CatchingUp {
				ready = false
			}
		}
		if ready {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("cluster not ready after %s", timeout)
}

func waitVerify(load *loadGen) (*verifyResult, error) {
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		if v := load.view(); !v.Verifying && v.Verify != nil {
			return v.Verify, nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return nil, fmt.Errorf("verification did not finish")
}

func statsRow(ms []int64) string {
	if len(ms) == 0 {
		return "– | – | –"
	}
	sorted := append([]int64(nil), ms...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	at := func(q float64) int64 { return sorted[int(q*float64(len(sorted)-1))] }
	return fmt.Sprintf("%s | %s | %s", fmtMs(at(0.5)), fmtMs(at(0.9)), fmtMs(sorted[len(sorted)-1]))
}
