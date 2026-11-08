# Request flows

Sequence diagrams for the main paths through the system. Each one matches the current code, with links to where the behavior lives.

See also: [architecture](architecture.md) · [API reference](API.md) · [demo and measured metrics](demo-and-metrics.md)

1. [Write to the leader](#1-write-to-the-leader)
2. [Write sent to a follower](#2-write-sent-to-a-follower)
3. [Read](#3-read)
4. [Cluster formation and joining](#4-cluster-formation-and-joining)
5. [Leader crash and failover](#5-leader-crash-and-failover)
6. [Restarted node catching up](#6-restarted-node-catching-up)
7. [Graceful shutdown](#7-graceful-shutdown)

---

## 1. Write to the leader

`PUT /kv/{key}` and `DELETE /kv/{key}` follow the same path. A client gets `204` only after the command is **committed on a majority and applied on the leader**.

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant H as Leader HTTP<br/>(auth + handler)
    participant R as Leader Raft
    participant L as Leader log<br/>(BoltDB)
    participant F2 as Follower node2
    participant F3 as Follower node3
    participant FSM as Leader FSM → Store

    C->>H: PUT /kv/user:42  body "alice"
    H->>H: Check Bearer token (if configured)
    H->>H: IsLeader()? yes
    H->>R: Apply({"op":"put","key":"user:42","value":"alice"}, 10s)
    R->>L: Append entry at index N (fsync)
    par Replicate to followers
        R->>F2: AppendEntries(term, N)
        F2->>F2: Append + fsync
        F2-->>R: success
    and
        R->>F3: AppendEntries(term, N)
        F3->>F3: Append + fsync
        F3-->>R: success
    end
    Note over R: Majority (leader + 1 follower) has entry N<br/>→ commit index advances to N
    R->>FSM: Apply(entry N)
    FSM->>FSM: store.Put("user:42", "alice")
    R-->>H: future resolved
    H-->>C: 204 No Content
    Note over F2,F3: Followers learn the new commit index on the<br/>next AppendEntries/heartbeat and apply N themselves
```

- The leader doesn't wait for *both* followers: one acknowledgement plus its own copy is a majority, so a slow or dead follower doesn't block writes.
- Code: [`HandlePut`](../internal/http/handlers.go) → [`Node.Apply`](../internal/raft/raft.go) → [`FSM.Apply`](../internal/raft/fsm.go).

## 2. Write sent to a follower

Followers don't forward writes. They reject them and say who the leader is.

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant F as Follower (node2)
    participant LD as Leader (node1)

    C->>F: PUT /kv/k  body "v"
    F->>F: IsLeader()? no
    F-->>C: 400 "Not the leader"<br/>X-Leader: 127.0.0.1:9011 (Raft address)
    C->>F: GET /raft/status
    F-->>C: {"leader":"127.0.0.1:9011", ...}
    Note over C: Map Raft port → HTTP port (−10)<br/>or poll /raft/status on each node
    C->>LD: PUT /kv/k  body "v"
    LD-->>C: 204 No Content
```

The demo's load generator handles this by polling every node's `/raft/status` and always sending to whichever node reports `is_leader: true`.

## 3. Read

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant N as Any node (leader or follower)
    participant S as Local store

    C->>N: GET /kv/user:42
    N->>N: VerifyRead(): state is Leader or Follower?
    alt node is Candidate / Shutdown
        N-->>C: 500 Failed to verify read
    else
        N->>S: Get("user:42")
        alt found
            S-->>N: "alice"
            N-->>C: 200 "alice"
        else missing
            N-->>C: 404 Key not found
        end
    end
```

**Consistency:** the read is served from the node's local copy without contacting other nodes. A follower may not yet have applied the newest committed entries, so a read right after a write can return the older value. In the control room at 100 writes/s, followers typically show 0–1 entries behind the leader. See [design trade-offs](architecture.md#design-decisions-and-trade-offs) for how to make reads strictly linearizable.

## 4. Cluster formation and joining

```mermaid
sequenceDiagram
    autonumber
    participant N1 as node1<br/>(bootstrap: true)
    participant N2 as node2<br/>(join_url: node1)
    participant N3 as node3<br/>(join_url: node1)

    Note over N1: First start, empty data dir
    N1->>N1: BootstrapCluster({node1})
    N1->>N1: Election timeout → wins its own vote → Leader (term 1+)

    Note over N2: Starts with no Raft state
    N2->>N2: Wait 2s for the leader to come up
    N2->>N1: POST /raft/join {"node_id":"node2","raft_addr":"127.0.0.1:9012"}
    N1->>N1: AddVoter(node2) → config-change log entry
    N1->>N2: AppendEntries (full log or snapshot)
    N2-->>N1: ack → config {node1,node2} committed
    N1-->>N2: 204 No Content

    N3->>N1: POST /raft/join {"node_id":"node3", ...}
    N1->>N1: AddVoter(node3)
    N1->>N2: AppendEntries (config change)
    N1->>N3: AppendEntries (full log or snapshot)
    N1-->>N3: 204 No Content
    Note over N1,N3: Cluster = {node1,node2,node3}, tolerates 1 failure
```

- Joining is idempotent. When `node2` restarts with existing state, it re-sends the join request, and `AddVoter` for an existing member changes nothing.
- `POST /raft/remove {"node_id":"node3"}` on the leader runs `RemoveServer` through the same config-change path.

## 5. Leader crash and failover

This is the scenario the control room demo runs and measures. Timings below are from the [benchmark](demo-and-metrics.md#benchmark-results).

```mermaid
sequenceDiagram
    autonumber
    participant C as Client (load generator)
    participant N1 as node1 (Leader, term 3)
    participant N2 as node2
    participant N3 as node3

    loop steady state
        C->>N1: PUT /kv/demo-…
        N1->>N2: AppendEntries
        N1->>N3: AppendEntries
        N1-->>C: 204
    end

    Note over N1: 💥 process killed (no graceful shutdown)
    C-xN1: PUT fails (connection refused)
    Note over C: writes unavailable — counted as failed, not acked

    Note over N2,N3: No heartbeat for ~500ms (randomized timeout)
    N2->>N2: Become Candidate, term 4, vote for self
    N2->>N3: RequestVote(term 4, lastLogIndex, lastLogTerm)
    Note over N3: Grants vote only if node2's log is<br/>at least as up-to-date as its own
    N3-->>N2: vote granted
    N2->>N2: 2 of 3 votes → Leader (term 4)
    N2->>N3: AppendEntries heartbeat (asserts leadership)

    C->>N2: GET /raft/status → is_leader: true
    C->>N2: PUT /kv/demo-…
    N2->>N3: AppendEntries
    N2-->>C: 204 — writes resumed
```

**Why no acknowledged write is lost:**
- Every acknowledged write was on a majority (2 of 3).
- Any new leader needs votes from a majority, and the two majorities must share a node.
- Raft's voting rule (step 7) only lets a candidate whose log is at least as up-to-date as the voter's win.
- So the new leader always has every committed entry. The demo checks this after every run by reading back every acknowledged key from every node.

## 6. Restarted node catching up

```mermaid
sequenceDiagram
    autonumber
    participant S as Supervisor (demo)
    participant N1 as node1 (restarting)
    participant D as node1 disk
    participant LD as Current leader (node2)

    S->>N1: start process (3s after crash)
    N1->>D: HasExistingState? yes
    N1->>D: Load latest snapshot (if any) → store
    N1->>D: Replay log entries after snapshot
    Note over N1: Store now matches node1's last<br/>committed state before the crash
    N1->>N1: Start as Follower (term from stable.db)
    LD->>N1: AppendEntries(prevLogIndex = X)
    alt node1's log matches at X
        N1-->>LD: success → leader streams newer entries
    else mismatch or too far behind
        N1-->>LD: reject → leader backs up nextIndex<br/>or sends InstallSnapshot
    end
    N1->>N1: Apply entries until applied index = leader commit
    S->>N1: GET /raft/status (polled every 100ms)
    Note over S: "node1 caught up to index N in X ms"
```

## 7. Graceful shutdown

```mermaid
sequenceDiagram
    autonumber
    participant OS as OS (Ctrl+C / SIGTERM)
    participant M as main()
    participant HS as http.Server
    participant R as Raft

    OS->>M: SIGINT / SIGTERM
    M->>HS: Shutdown(ctx, 10s)
    Note over HS: Stop accepting connections,<br/>let in-flight requests finish
    HS-->>M: done (or timeout)
    M->>R: Shutdown()
    Note over R: Stop replication / elections,<br/>close transport and BoltDB files
    R-->>M: done
    M->>M: log "Node shutdown complete"
```

A graceful shutdown of the leader still causes an election, because hashicorp/raft doesn't hand off leadership on `Shutdown`. The demo's **Kill** button skips all of this and terminates the process outright, which is the harsher test.
