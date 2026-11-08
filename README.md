# Mini Cloud - Distributed KV (Go + Raft)

A fault-tolerant, replicated key–value store in Go. Three nodes agree on every write with the Raft consensus algorithm, so the cluster keeps serving requests and loses no acknowledged write when any node crashes, leader included.

## Results

Measured with the included benchmark (`go run ./cmd/demo -bench 10`) on one machine; [full methodology and caveats](docs/demo-and-metrics.md#benchmark-results).

| Metric | Result |
|---|---|
| Acknowledged writes lost across 10 hard leader kills under load | **0 of 4,737** (verified on all 3 nodes) |
| Leader failover time (kill → new leader elected) | **1.03 s** p50 · 1.27 s p90 · 1.33 s max |
| Write unavailability seen by clients during failover | **1.04 s** p50 · 1.28 s p90 |
| Sustained replicated write throughput (8 clients, fsync on every write) | **796 writes/s** |
| Write latency including majority commit | **4.6 ms** p50 · 11.5 ms p99 |
| Crashed node restart → caught up with the leader | **0.7 s** p50 · 1.0 s max |
| Unit test coverage | store 100% · HTTP handlers 61.5% · Raft FSM 17.0%, plus multi-node integration tests |

## Documentation

| Document | What's in it |
|---|---|
| [Architecture](docs/architecture.md) | System and node diagrams, code layout, replication model, persistence, membership, observability, design trade-offs |
| [Request flows](docs/request-flows.md) | Sequence diagrams: writes, reads, follower rejection, cluster formation, leader failover, restart catch-up, shutdown |
| [Demo and metrics](docs/demo-and-metrics.md) | How the control room and benchmark work, what each metric measures, full results, caveats |
| [API reference](docs/API.md) | Every HTTP endpoint with request/response examples |

## Features

A small, opinionated distributed key–value service with:
- **Raft consensus** for leader election and replication
- **Dynamic membership** (join/remove nodes at runtime)
- **Bearer token authentication** for write operations
- **Prometheus metrics** and structured logging
- **Graceful shutdown** and durability via snapshots
- **CLI tool** and web dashboard
- **Live control room** for crashing nodes under load and measuring failover

## Live demo: Raft control room

One command starts a local 3-node cluster under a supervisor and opens a control room UI:
```powershell
go run ./cmd/demo
```
Then open http://127.0.0.1:8080. Add `-bench 10` to run the failover benchmark headless instead.

From the control room you can:
- **Generate write load** (50/100/300/max writes per second) against whichever node is leader
- **Kill the leader** (a hard process kill) and watch a new leader get elected
- **Kill or start any node**; with auto-restart on, killed nodes come back after 3s and replay the log to catch up
- **Stop & verify**: every acknowledged write is read back from every live node to prove none were lost

It measures and displays:
- Leader election time after a crash, and the write gap clients actually saw
- Write throughput and p50/p99 latency over the last 60s
- Per-node term, applied index, key count and replication lag
- Lost acknowledged writes (should always be 0)

Flags: `-addr` (default `127.0.0.1:8080`), `-restart-delay` (default `3s`, `0` disables auto-restart), `-fresh` (default `true`, wipes demo data on start). Demo nodes use the normal `configs/node*.yaml` ports (9001–9003), so stop any manually started nodes first. Their data and logs live in `data/demo/`, separate from manual runs.

## 1) Prerequisites

- Go 1.22+ (or 1.23+ for Prometheus metrics)
- Windows PowerShell (recommended for commands below)
- Optional: Docker Desktop (for Docker Compose demo)
- Optional: `make` (for Makefile commands, or use `go` directly)

## 2) Project setup

Open PowerShell at the project root (the cloned `distributed_system` folder).

Fetch dependencies, run tests, and build:
```powershell
go mod tidy
go test ./...
go build ./cmd/node
go build ./cmd/cloudctl
```

Or use Makefile:
```powershell
make test          # Run all tests
make test-coverage # Run tests with coverage report
make build         # Build binaries
make clean         # Remove build artifacts
make help          # Show all commands
```

### Testing

The project includes comprehensive tests:
- **Unit tests**: Store, FSM, HTTP handlers
- **Integration tests**: Multi-node scenarios

Run tests:
```powershell
go test ./...              # All tests
go test ./internal/store   # Store tests only
go test ./test -v          # Integration tests
make test-coverage         # With coverage report
```

Test coverage:
- Store: 100%
- HTTP handlers: 61.5%
- Raft FSM: 17.0%

## 3) Run locally (single node or multi-node)

### Single node (leader-only)
Open a new PowerShell window at the project root and run:
```powershell
go run ./cmd/node -config configs/node1.yaml
```
- Dashboard: open http://127.0.0.1:9001/dashboard
- Health: `curl.exe http://127.0.0.1:9001/health`
- **Note**: You may see connection errors in the logs (like `dial tcp 127.0.0.1:9012: connectex: No connection could be made`). These are harmless - Raft is trying to reach peers that aren't running. The single node will still work and become the leader.

### Multi-node (3 processes)
Open three PowerShell windows (one per node):
```powershell
# Window 1 (bootstrap leader)
go run ./cmd/node -config configs\node1.yaml

# Window 2 (auto-joins leader)
go run ./cmd/node -config configs\node2.yaml

# Window 3 (auto-joins leader)
go run ./cmd/node -config configs\node3.yaml
```
Notes:
- `configs/node1.yaml` has `bootstrap: true`. Use it only for first-time cluster creation with an empty `data/`.
- `configs/node2.yaml` and `node3.yaml` have `bootstrap: false` and a `join_url` pointing to node1; they auto-join.
- **Single node works fine**: A single bootstrapped node will become the leader and work normally. Connection errors in logs are harmless.
- **Multi-node quorum**: For a 3-node cluster, you need at least 2 nodes running to form a quorum and handle leader failover.

## 4) Run with Docker Compose (3-node demo)

Docker uses separate configs (`docker-node*.yaml`) with service names for Raft addresses.

From the project root:
```powershell
docker compose build
docker compose up -d
```
Then:
- Dashboard: open http://localhost:9001/dashboard
- Logs: `docker compose logs -f node1`
- Tear down: `docker compose down -v`

## 5) Testing features

Use `curl.exe` in PowerShell (to avoid PowerShell’s Invoke-WebRequest differences). You can run these against any node.

### 5.1 Key–Value API (leader writes, any-node reads)
- PUT store value
```powershell
curl.exe -X PUT http://127.0.0.1:9001/kv/foo -d "bar"
```
- GET retrieve value
```powershell
curl.exe http://127.0.0.1:9001/kv/foo
```
- DELETE remove key
```powershell
curl.exe -X DELETE http://127.0.0.1:9001/kv/foo
```

### 5.2 Leader-only writes
Try writing to a follower (e.g., node2 at port 9002):
```powershell
curl.exe -i -X PUT http://127.0.0.1:9002/kv/leader-test -d "ok"
```
- Expected: `HTTP/1.1 400 Bad Request` with body `Not the leader` and an `X-Leader` header holding the leader's Raft address (e.g. `127.0.0.1:9011`; its HTTP port is 10 lower).
- Send the write to the leader instead:
```powershell
curl.exe -X PUT http://127.0.0.1:9001/kv/leader-test -d "ok"
```
- Verify on any node:
```powershell
curl.exe http://127.0.0.1:9003/kv/leader-test
```

### 5.3 Dynamic membership (join voters at runtime)
Auto-join is configured in `configs/node2.yaml`/`node3.yaml` via `join_url`.
To join manually (leader only), POST to `/raft/join` on the leader:
```powershell
curl.exe -X POST http://127.0.0.1:9001/raft/join -H "Content-Type: application/json" -d '{"node_id":"nodeX","raft_addr":"127.0.0.1:90XX"}'
```
Inspect live membership (any node):
```powershell
curl.exe http://127.0.0.1:9001/raft/config
curl.exe http://127.0.0.1:9001/cluster/members
```

### 5.4 Durability via snapshots (state survives restarts)
```powershell
# Write some durable data
curl.exe -X PUT http://127.0.0.1:9001/kv/durable -d "value"

# Stop a node (Ctrl+C where it runs), then start it again with the same config
# Verify value still present (committed state restored)
curl.exe http://127.0.0.1:9001/kv/durable
```

### 5.5 Raft status and health
- Raft status (per node):
```powershell
curl.exe http://127.0.0.1:9001/raft/status
```
- Health:
```powershell
curl.exe http://127.0.0.1:9001/health
```

### 5.6 Authentication (Bearer Token)
If `auth_token` is set in config or `AUTH_TOKEN` env var is provided, write operations (PUT/DELETE) require a Bearer token:
```powershell
# Set token (example)
$env:AUTH_TOKEN="my-secret-token"

# Write with token
curl.exe -X PUT http://127.0.0.1:9001/kv/protected -H "Authorization: Bearer my-secret-token" -d "value"

# Without token (fails with 401)
curl.exe -X PUT http://127.0.0.1:9001/kv/protected -d "value"
```

### 5.7 Prometheus Metrics
Metrics endpoint (no auth required):
```powershell
curl.exe http://127.0.0.1:9001/metrics
```
Metrics include:
- HTTP request counts and latency
- KV operations (PUT/GET/DELETE counts)
- Raft state (leader status, applied/commit indices)

### 5.8 Node Removal (Leader Only)
Remove a node from the cluster:
```powershell
curl.exe -X POST http://127.0.0.1:9001/raft/remove -H "Content-Type: application/json" -d '{"node_id":"node2"}'
```

### 5.9 Graceful Shutdown
Nodes handle SIGINT/SIGTERM gracefully:
- HTTP server stops accepting new connections
- Raft node shuts down cleanly
- In-flight requests complete (10s timeout)
- Press Ctrl+C in the node terminal to trigger shutdown

### 5.10 Leader Failover Testing
Test leader reassignment by killing the leader and observing a new leader election.

**Step 1: Start multiple nodes**
```powershell
# Terminal 1 - Node 1 (bootstrap)
go run ./cmd/node -config configs/node1.yaml

# Terminal 2 - Node 2
go run ./cmd/node -config configs/node2.yaml

# Terminal 3 - Node 3
go run ./cmd/node -config configs/node3.yaml
```

**Step 2: Identify the current leader**
```powershell
# Check which node is leader
curl.exe http://127.0.0.1:9001/raft/status
curl.exe http://127.0.0.1:9002/raft/status
curl.exe http://127.0.0.1:9003/raft/status
# Look for "is_leader": true
```

**Step 3: Kill the leader**
```powershell
# IMPORTANT: You need to kill the process that owns BOTH the HTTP port AND Raft port
# Find the process ID for the leader (e.g., if node1 is leader on ports 9001 and 9011)
$pid = (Get-NetTCPConnection -LocalPort 9001 -ErrorAction SilentlyContinue | Select-Object -First 1 -ExpandProperty OwningProcess)
if ($pid) {
    Write-Host "Killing process $pid (node1)" -ForegroundColor Yellow
    Stop-Process -Id $pid -Force
} else {
    Write-Host "No process found on port 9001" -ForegroundColor Red
}

# Alternative: Find by Raft port (9011 for node1)
$pid = (Get-NetTCPConnection -LocalPort 9011 -ErrorAction SilentlyContinue | Select-Object -First 1 -ExpandProperty OwningProcess)
Stop-Process -Id $pid -Force
```

**Step 4: Observe new leader election**
```powershell
# Wait 2-5 seconds for election (Raft election timeout is ~500ms, but allow time for detection)
Write-Host "Waiting for leader election..." -ForegroundColor Cyan
Start-Sleep -Seconds 3

# Check remaining nodes
curl.exe http://127.0.0.1:9002/raft/status
curl.exe http://127.0.0.1:9003/raft/status
# One should show "is_leader": true

# If no leader appears, wait longer (up to 10 seconds) and check again
```

**Step 5: Verify cluster still works**
```powershell
# Write to the new leader
curl.exe -X PUT http://127.0.0.1:9002/kv/test -d "value" -H "Content-Type: application/octet-stream"

# Read from any remaining node
curl.exe http://127.0.0.1:9003/kv/test
```

**Monitor all nodes during failover:**
```powershell
# Run this in a loop to watch leader election
while ($true) {
    Clear-Host
    Write-Host "=== Cluster Status $(Get-Date -Format 'HH:mm:ss') ===" -ForegroundColor Cyan
    $hasLeader = $false
    @(9001, 9002, 9003) | ForEach-Object {
        $port = $_
        try {
            $status = curl.exe -s http://127.0.0.1:$port/raft/status | ConvertFrom-Json
            $health = curl.exe -s http://127.0.0.1:$port/health
            if ($status.is_leader) {
                $hasLeader = $true
                $color = "Green"
            } else {
                $color = "Yellow"
            }
            Write-Host "Port $port : $($status.state) | Leader: $($status.is_leader) | Health: $health" -ForegroundColor $color
        } catch {
            Write-Host "Port $port : DOWN" -ForegroundColor Red
        }
    }
    if (-not $hasLeader) {
        Write-Host "`nWARNING: No leader detected! Waiting for election..." -ForegroundColor Red
    }
    Start-Sleep -Seconds 1
}
# Then kill the leader process (Ctrl+C in another terminal) and watch the election happen
```

**Quick script to kill leader and verify:**
```powershell
# Find and kill the leader
$leaderPort = $null
@(9001, 9002, 9003) | ForEach-Object {
    $port = $_
    try {
        $status = curl.exe -s http://127.0.0.1:$port/raft/status | ConvertFrom-Json
        if ($status.is_leader) {
            $leaderPort = $port
            Write-Host "Found leader on port $port" -ForegroundColor Green
            $pid = (Get-NetTCPConnection -LocalPort $port -ErrorAction SilentlyContinue | Select-Object -First 1 -ExpandProperty OwningProcess)
            if ($pid) {
                Write-Host "Killing process $pid..." -ForegroundColor Yellow
                Stop-Process -Id $pid -Force
            }
        }
    } catch {
        # Node might be down, skip
    }
}

# Wait for election
Write-Host "`nWaiting for new leader election (2 seconds)..." -ForegroundColor Cyan
Start-Sleep -Seconds 2

# Check for new leader
Write-Host "`nChecking for new leader:" -ForegroundColor Cyan
@(9001, 9002, 9003) | ForEach-Object {
    $port = $_
    try {
        $status = curl.exe -s http://127.0.0.1:$port/raft/status | ConvertFrom-Json
        if ($status.is_leader) {
            Write-Host "  Port $port is now the leader!" -ForegroundColor Green
        } else {
            Write-Host "  Port $port : $($status.state)" -ForegroundColor Yellow
        }
    } catch {
        Write-Host "  Port $port : DOWN" -ForegroundColor Red
    }
}
```

**Troubleshooting leader election:**
- **Single node mode**: Works fine! A single bootstrapped node will become the leader. Connection errors in logs are harmless - Raft is trying to reach peers that aren't running.
- **Multi-node quorum**: For a 3-node cluster, you need at least 2 nodes running to form a quorum and handle leader failover.
  - If only 1 node is running in a multi-node setup, you'll see connection errors like: `dial tcp 127.0.0.1:9012: connectex: No connection could be made because the target machine actively refused it`
  - This is normal - the single node can't reach the others, but it will still work as a single-node cluster
- **Connection refused errors**: These are harmless if you're running a single node. For multi-node, start all nodes to form a cluster.
- **Election takes too long**: Raft uses randomized timeouts (500ms-1s) to prevent split votes. With the updated code, elections should complete in 1-2 seconds.
- **Wrong process killed**: Make sure you killed the process that owns both HTTP and Raft ports
- **Process restarted**: Check if the process auto-restarted (some IDEs/terminals do this)
- **Still no leader after 5 seconds**: Restart all nodes to ensure they're using the updated timeout settings

### 5.11 CLI (optional)
Build once:
```powershell
go build ./cmd/cloudctl
```
Use:
```powershell
# Against node1
.\cloudctl.exe put foo bar
.\cloudctl.exe get foo
.\cloudctl.exe delete foo
.\cloudctl.exe members
.\cloudctl.exe status
# Different node
.\cloudctl.exe -server http://127.0.0.1:9002 put k v
```

## 5A) Complete Operations Guide

This section provides a comprehensive reference for all operations you can perform on the servers.

### Basic Key-Value Operations

**PUT - Store a value:**
```powershell
curl.exe -X PUT http://127.0.0.1:9001/kv/mykey -d "Hello World" -H "Content-Type: application/octet-stream"
# Expected: 204 No Content (or 400 "Not the leader" if this node is a follower)
```

**GET - Retrieve a value:**
```powershell
curl.exe http://127.0.0.1:9001/kv/mykey
# Expected: "Hello World"
```

**DELETE - Remove a key:**
```powershell
curl.exe -X DELETE http://127.0.0.1:9001/kv/mykey
# Expected: 204 No Content
curl.exe http://127.0.0.1:9001/kv/mykey
# Expected: 404 Key not found
```

### Leader Detection

**Check which node is leader:**
```powershell
curl.exe http://127.0.0.1:9001/raft/status
curl.exe http://127.0.0.1:9002/raft/status
curl.exe http://127.0.0.1:9003/raft/status
# Look for "is_leader": true
```

**Test follower rejection (write to non-leader):**
```powershell
# If node2 is not leader, try writing to it:
curl.exe -v -X PUT http://127.0.0.1:9002/kv/test -d "value"
# Expected: 400 "Not the leader" with an X-Leader header holding the leader's Raft address
```

### Cluster Membership

**View all cluster members (from Raft):**
```powershell
curl.exe http://127.0.0.1:9001/cluster/members
# Expected: JSON with all nodes in the cluster
```

**View node status:**
```powershell
curl.exe http://127.0.0.1:9001/cluster/status
# Expected: node_id, address, peers
```

### Data Replication (Multi-Node)

**Write to leader, read from followers:**
```powershell
# Write to node1 (if leader)
curl.exe -X PUT http://127.0.0.1:9001/kv/replicated -d "This should appear on all nodes"

# Wait 1-2 seconds for replication, then read from other nodes:
curl.exe http://127.0.0.1:9002/kv/replicated
curl.exe http://127.0.0.1:9003/kv/replicated
# Expected: "This should appear on all nodes" from all nodes
```

### Dynamic Node Management

**Join a new node (if you have a 4th node config):**
```powershell
curl.exe -X POST http://127.0.0.1:9001/raft/join `
  -H "Content-Type: application/json" `
  -d '{"node_id":"node4","raft_addr":"127.0.0.1:9014"}'
# Expected: 204 No Content
```

**Remove a node:**
```powershell
curl.exe -X POST http://127.0.0.1:9001/raft/remove `
  -H "Content-Type: application/json" `
  -d '{"node_id":"node3"}'
# Expected: 204 No Content
# Then check members - node3 should be gone
curl.exe http://127.0.0.1:9001/cluster/members
```

### Health & Monitoring

**Health check:**
```powershell
curl.exe http://127.0.0.1:9001/health
# Expected: "OK"
```

**Prometheus metrics:**
```powershell
curl.exe http://127.0.0.1:9001/metrics
# Expected: Prometheus-formatted metrics (lots of text)
```

**View specific metrics:**
```powershell
curl.exe http://127.0.0.1:9001/metrics | Select-String "kv_|raft_|http_request"
```

### Raft Configuration

**View Raft config:**
```powershell
curl.exe http://127.0.0.1:9001/raft/config
# Expected: JSON with server list
```

**View Raft status:**
```powershell
curl.exe http://127.0.0.1:9001/raft/status
# Expected: is_leader, leader address, state
```

### Web Dashboard

**Open in browser:**
```powershell
Start-Process "http://127.0.0.1:9001/dashboard"
# Or manually navigate to: http://127.0.0.1:9001/dashboard
```

### Complete Test Sequence

**Full workflow test:**
```powershell
# 1. Check health
Write-Host "1. Health Check:" -ForegroundColor Cyan
curl.exe http://127.0.0.1:9001/health

# 2. Find leader
Write-Host "`n2. Finding Leader:" -ForegroundColor Cyan
$leader = (curl.exe -s http://127.0.0.1:9001/raft/status | ConvertFrom-Json)
Write-Host "Leader: $($leader.leader), Is Leader: $($leader.is_leader)"

# 3. Store data
Write-Host "`n3. Storing Data:" -ForegroundColor Cyan
curl.exe -X PUT http://127.0.0.1:9001/kv/testkey -d "testvalue" -H "Content-Type: application/octet-stream"

# 4. Read from all nodes
Write-Host "`n4. Reading from all nodes:" -ForegroundColor Cyan
@(9001, 9002, 9003) | ForEach-Object {
    $val = curl.exe -s http://127.0.0.1:$_/kv/testkey
    Write-Host "Node $_: $val"
}

# 5. Check cluster members
Write-Host "`n5. Cluster Members:" -ForegroundColor Cyan
curl.exe http://127.0.0.1:9001/cluster/members | ConvertFrom-Json | Format-List

# 6. View metrics
Write-Host "`n6. Key Metrics:" -ForegroundColor Cyan
curl.exe -s http://127.0.0.1:9001/metrics | Select-String "kv_put_operations_total|kv_get_operations_total|raft_is_leader"
```

### Authentication (if enabled)

**Test with auth token:**
```powershell
# Set token in config or env
$env:AUTH_TOKEN = "my-secret-token"

# Write without token (should fail)
curl.exe -X PUT http://127.0.0.1:9001/kv/secret -d "data"
# Expected: 401 Unauthorized

# Write with token (should succeed)
curl.exe -X PUT http://127.0.0.1:9001/kv/secret -d "data" `
  -H "Authorization: Bearer my-secret-token"
# Expected: 200 OK

# Read doesn't require auth
curl.exe http://127.0.0.1:9001/kv/secret
# Expected: "data"
```

### Data Persistence Test

**Test snapshot/restore:**
```powershell
# 1. Store data
curl.exe -X PUT http://127.0.0.1:9001/kv/persistent -d "survives restart"

# 2. Stop node1 (Ctrl+C or kill process)

# 3. Restart node1
go run ./cmd/node -config configs/node1.yaml

# 4. Wait a few seconds, then read
curl.exe http://127.0.0.1:9001/kv/persistent
# Expected: "survives restart" (if snapshot was taken)
```

### Quick Reference Commands

**One-liners for common checks:**
```powershell
# Who's the leader?
curl.exe -s http://127.0.0.1:9001/raft/status | ConvertFrom-Json | Select-Object is_leader, leader, state

# How many members?
(curl.exe -s http://127.0.0.1:9001/cluster/members | ConvertFrom-Json).members.Count

# Request counts?
curl.exe -s http://127.0.0.1:9001/metrics | Select-String "kv_.*_total"
```

## 6) Configuration reference (YAML)
Each node supports:
```yaml
node_id: "node1"
listen_addr: "127.0.0.1:9001"  # HTTP address (use 127.0.0.1 for local, 0.0.0.0 for Docker)
raft_addr: "127.0.0.1:9011"   # Raft address (optional; defaults to http+10). Must be specific IP, not 0.0.0.0
bootstrap: true                # Only one node should bootstrap a fresh cluster
join_url: ""                   # Followers set this to leader HTTP base, e.g. http://127.0.0.1:9001
auth_token: ""                 # Optional bearer token for write operations (env AUTH_TOKEN overrides)
```
Notes:
- If reusing a `data/` directory, set `bootstrap: false` (existing state wins).
- If `raft_addr` is omitted, it's derived as `http_port + 10`.
- `auth_token` can be set in YAML or via `AUTH_TOKEN` environment variable (env takes precedence).
- **Important**: `raft_addr` must be a specific IP address (e.g., `127.0.0.1` or your network IP), not `0.0.0.0`. Use `0.0.0.0` only for `listen_addr` in Docker.

## 7) Shutting Down All Nodes

**Complete shutdown command:**
```powershell
# Kill all processes using the node ports
Get-NetTCPConnection -LocalPort 9001,9002,9003,9011,9012,9013 -ErrorAction SilentlyContinue | 
    Select-Object -ExpandProperty OwningProcess -Unique | 
    ForEach-Object { Stop-Process -Id $_ -Force -ErrorAction SilentlyContinue }

# Also kill any node processes from go-build
Get-Process | Where-Object {$_.ProcessName -eq "node" -and $_.Path -like "*go-build*"} | 
    Stop-Process -Force -ErrorAction SilentlyContinue

# Verify all ports are free
Get-NetTCPConnection -LocalPort 9001,9002,9003,9011,9012,9013 -ErrorAction SilentlyContinue
# Should return nothing if all processes are stopped
```

**Alternative: Kill all Go processes (be careful - kills ALL Go processes):**
```powershell
Get-Process go | Stop-Process -Force
Get-Process | Where-Object {$_.ProcessName -eq "node" -and $_.Path -like "*go-build*"} | Stop-Process -Force
```

**If dashboard still loads after shutdown:**
- Check browser cache - try hard refresh (Ctrl+F5) or open in incognito/private mode
- Verify ports are actually free: `Get-NetTCPConnection -LocalPort 9001,9002,9003`
- Check if Docker containers are running: `docker ps` (if using Docker Compose)

