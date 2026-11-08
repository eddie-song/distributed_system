# API Documentation

## Base URL

All endpoints are relative to the node's HTTP address (default: `http://127.0.0.1:9001`).

## Authentication

Write operations (PUT, DELETE) require Bearer token authentication if `auth_token` is configured:

```
Authorization: Bearer <token>
```

Token can be set via:
- YAML config: `auth_token: "your-token"`
- Environment variable: `AUTH_TOKEN=your-token`

## Key-Value Store Endpoints

### PUT /kv/{key}

Store a value for a key.

**Method**: `PUT`

**Path Parameters**:
- `key` (string, required): The key to store

**Request Body**: Raw bytes (value to store)

**Headers**:
- `Authorization: Bearer <token>` (required if auth enabled)

**Response**:
- `204 No Content`: Success (committed on a majority and applied on the leader)
- `400 Bad Request`: Invalid request, or not the leader (body `Not the leader`; `X-Leader` header holds the leader's Raft address)
- `401 Unauthorized`: Missing or invalid token
- `500 Internal Server Error`: Raft apply failed

**Example**:
```bash
curl -X PUT http://127.0.0.1:9001/kv/foo \
  -H "Authorization: Bearer my-token" \
  -d "bar"
```

### GET /kv/{key}

Retrieve a value by key.

**Method**: `GET`

**Path Parameters**:
- `key` (string, required): The key to retrieve

**Response**:
- `200 OK`: Success (body contains value as raw bytes)
- `400 Bad Request`: Invalid request
- `404 Not Found`: Key does not exist
- `500 Internal Server Error`: Read verification failed

**Headers**:
- `Content-Type: application/octet-stream`

**Example**:
```bash
curl http://127.0.0.1:9001/kv/foo
```

**Note**: Reads are served from the receiving node's local copy. A follower may briefly return a value older than the latest committed write; see [design trade-offs](architecture.md#design-decisions-and-trade-offs).

### DELETE /kv/{key}

Delete a key-value pair.

**Method**: `DELETE`

**Path Parameters**:
- `key` (string, required): The key to delete

**Headers**:
- `Authorization: Bearer <token>` (required if auth enabled)

**Response**:
- `204 No Content`: Success
- `400 Bad Request`: Invalid request, or not the leader (body `Not the leader`; `X-Leader` header holds the leader's Raft address)
- `401 Unauthorized`: Missing or invalid token
- `404 Not Found`: Key does not exist
- `500 Internal Server Error`: Raft apply failed

**Example**:
```bash
curl -X DELETE http://127.0.0.1:9001/kv/foo \
  -H "Authorization: Bearer my-token"
```

## Cluster Management Endpoints

### GET /cluster/members

Get live cluster membership from Raft.

**Method**: `GET`

**Response**:
- `200 OK`: Success

**Response Body** (JSON):
```json
{
  "members": [
    {
      "id": "node1",
      "address": "127.0.0.1:9011",
      "voter": true
    },
    {
      "id": "node2",
      "address": "127.0.0.1:9012",
      "voter": true
    }
  ]
}
```

**Example**:
```bash
curl http://127.0.0.1:9001/cluster/members
```

### GET /cluster/status

Get current node's status (legacy endpoint, uses static config).

**Method**: `GET`

**Response**:
- `200 OK`: Success

**Response Body** (JSON):
```json
{
  "node_id": "node1",
  "address": "127.0.0.1:9001",
  "peers": ["127.0.0.1:9002", "127.0.0.1:9003"]
}
```

## Raft Endpoints

### GET /raft/status

Get Raft consensus status.

**Method**: `GET`

**Response**:
- `200 OK`: Success

**Response Body** (JSON):
```json
{
  "node_id": "node1",
  "is_leader": true,
  "leader": "127.0.0.1:9011",
  "state": "Leader",
  "term": 3,
  "last_log_index": 1999,
  "commit_index": 1999,
  "applied_index": 1999,
  "keys": 1994
}
```

`leader` is the leader's Raft address. `keys` is the number of keys in this node's store. Comparing `applied_index` across nodes shows replication lag.

**States**: `Leader`, `Follower`, `Candidate`, `Shutdown`

**Example**:
```bash
curl http://127.0.0.1:9001/raft/status
```

### GET /raft/config

Get current Raft server configuration.

**Method**: `GET`

**Response**:
- `200 OK`: Success

**Response Body** (JSON):
```json
{
  "servers": [
    {
      "id": "node1",
      "address": "127.0.0.1:9011",
      "voter": true
    }
  ]
}
```

**Example**:
```bash
curl http://127.0.0.1:9001/raft/config
```

### POST /raft/join

Add a node to the Raft cluster (leader only).

**Method**: `POST`

**Request Body** (JSON):
```json
{
  "node_id": "node2",
  "raft_addr": "127.0.0.1:9012"
}
```

**Response**:
- `204 No Content`: Success
- `400 Bad Request`: Not the leader or invalid request
- `500 Internal Server Error`: Join failed

**Example**:
```bash
curl -X POST http://127.0.0.1:9001/raft/join \
  -H "Content-Type: application/json" \
  -d '{"node_id":"node2","raft_addr":"127.0.0.1:9012"}'
```

### POST /raft/remove

Remove a node from the Raft cluster (leader only).

**Method**: `POST`

**Request Body** (JSON):
```json
{
  "node_id": "node2"
}
```

**Response**:
- `204 No Content`: Success
- `400 Bad Request`: Not the leader or invalid request
- `500 Internal Server Error`: Remove failed

**Example**:
```bash
curl -X POST http://127.0.0.1:9001/raft/remove \
  -H "Content-Type: application/json" \
  -d '{"node_id":"node2"}'
```

## System Endpoints

### GET /health

Health check endpoint.

**Method**: `GET`

**Response**:
- `200 OK`: Node is healthy

**Response Body**: `OK`

**Example**:
```bash
curl http://127.0.0.1:9001/health
```

### GET /metrics

Prometheus metrics endpoint.

**Method**: `GET`

**Response**:
- `200 OK`: Metrics in Prometheus format

**Example**:
```bash
curl http://127.0.0.1:9001/metrics
```

**Metrics**:
- `http_requests_total`: Request counts
- `http_request_duration_seconds`: Request latency
- `kv_put_operations_total`: PUT operations
- `kv_get_operations_total`: GET operations
- `kv_delete_operations_total`: DELETE operations
- `kv_store_size`: Number of keys in the store
- `raft_is_leader`: Leader status (1 or 0)
- `raft_applied_index`: Last applied log index
- `raft_commit_index`: Last committed log index

### GET /dashboard

Web dashboard interface.

**Method**: `GET`

**Response**: HTML dashboard page

**Example**: Open `http://127.0.0.1:9001/dashboard` in browser

## Error Responses

All error responses follow this format:

**Status Code**: HTTP status code (4xx or 5xx)

**Response Body**: Error message (plain text)

**Example**:
```
HTTP/1.1 400 Bad Request
Content-Type: text/plain

Not the leader
```

## Rate Limiting

Currently not implemented. Planned for future release.

## CORS

CORS headers are not set. The demo control room (`go run ./cmd/demo`) talks to all nodes from its own server, so it doesn't need them.

## Timeouts

- Raft operations: 10 seconds
- Read verification: 5 seconds
- Graceful shutdown: 10 seconds

