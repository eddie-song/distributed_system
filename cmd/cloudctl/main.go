package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
)

const defaultServer = "http://127.0.0.1:9001"

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	server := flag.String("server", defaultServer, "Server address")
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		printUsage()
		os.Exit(1)
	}

	command := args[0]

	switch command {
	case "put":
		if len(args) < 3 {
			fmt.Fprintf(os.Stderr, "Error: put requires key and value\n")
			fmt.Fprintf(os.Stderr, "Usage: cloudctl put <key> <value>\n")
			os.Exit(1)
		}
		handlePut(*server, args[1], args[2])
	case "get":
		if len(args) < 2 {
			fmt.Fprintf(os.Stderr, "Error: get requires a key\n")
			fmt.Fprintf(os.Stderr, "Usage: cloudctl get <key>\n")
			os.Exit(1)
		}
		handleGet(*server, args[1])
	case "delete":
		if len(args) < 2 {
			fmt.Fprintf(os.Stderr, "Error: delete requires a key\n")
			fmt.Fprintf(os.Stderr, "Usage: cloudctl delete <key>\n")
			os.Exit(1)
		}
		handleDelete(*server, args[1])
	case "members":
		handleMembers(*server)
	case "status":
		handleStatus(*server)
	default:
		fmt.Fprintf(os.Stderr, "Error: unknown command '%s'\n", command)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprintf(os.Stderr, "Usage: cloudctl <command> [options]\n")
	fmt.Fprintf(os.Stderr, "\nCommands:\n")
	fmt.Fprintf(os.Stderr, "  put <key> <value>    Store a key-value pair\n")
	fmt.Fprintf(os.Stderr, "  get <key>            Retrieve a value by key\n")
	fmt.Fprintf(os.Stderr, "  delete <key>          Delete a key-value pair\n")
	fmt.Fprintf(os.Stderr, "  members              List cluster members\n")
	fmt.Fprintf(os.Stderr, "  status               Show cluster status\n")
	fmt.Fprintf(os.Stderr, "\nOptions:\n")
	fmt.Fprintf(os.Stderr, "  -server <addr>        Server address (default: %s)\n", defaultServer)
}

func handlePut(server, key, value string) {
	url := fmt.Sprintf("%s/kv/%s", server, key)
	req, err := http.NewRequest("PUT", url, bytes.NewBufferString(value))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		fmt.Printf("OK\n")
	} else if resp.StatusCode == http.StatusBadRequest {
		body, _ := io.ReadAll(resp.Body)
		leader := resp.Header.Get("X-Leader")
		if leader != "" {
			fmt.Fprintf(os.Stderr, "Error: Not the leader. Leader is at %s\n", leader)
		} else {
			fmt.Fprintf(os.Stderr, "Error: %s\n", string(body))
		}
		os.Exit(1)
	} else {
		body, _ := io.ReadAll(resp.Body)
		fmt.Fprintf(os.Stderr, "Error: %s\n", string(body))
		os.Exit(1)
	}
}

func handleGet(server, key string) {
	url := fmt.Sprintf("%s/kv/%s", server, key)
	resp, err := http.Get(url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		fmt.Printf("%s\n", string(body))
	} else if resp.StatusCode == http.StatusNotFound {
		fmt.Fprintf(os.Stderr, "Error: Key not found\n")
		os.Exit(1)
	} else {
		body, _ := io.ReadAll(resp.Body)
		fmt.Fprintf(os.Stderr, "Error: %s\n", string(body))
		os.Exit(1)
	}
}

func handleDelete(server, key string) {
	url := fmt.Sprintf("%s/kv/%s", server, key)
	req, _ := http.NewRequest("DELETE", url, nil)
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		fmt.Printf("OK\n")
	} else if resp.StatusCode == http.StatusBadRequest {
		leader := resp.Header.Get("X-Leader")
		if leader != "" {
			fmt.Fprintf(os.Stderr, "Error: Not the leader. Leader is at %s\n", leader)
		} else {
			body, _ := io.ReadAll(resp.Body)
			fmt.Fprintf(os.Stderr, "Error: %s\n", string(body))
		}
		os.Exit(1)
	} else if resp.StatusCode == http.StatusNotFound {
		fmt.Fprintf(os.Stderr, "Error: Key not found\n")
		os.Exit(1)
	} else {
		body, _ := io.ReadAll(resp.Body)
		fmt.Fprintf(os.Stderr, "Error: %s\n", string(body))
		os.Exit(1)
	}
}

func handleMembers(server string) {
	url := fmt.Sprintf("%s/cluster/members", server)
	resp, err := http.Get(url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		fmt.Fprintf(os.Stderr, "Error: %s\n", string(body))
		os.Exit(1)
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	members, ok := result["members"].([]interface{})
	if !ok {
		fmt.Fprintf(os.Stderr, "Error: invalid response format\n")
		os.Exit(1)
	}

	fmt.Printf("Cluster Members:\n")
	for _, m := range members {
		member := m.(map[string]interface{})
		id := member["id"].(string)
		addr := member["address"].(string)
		if id == "" {
			id = "unknown"
		}
		fmt.Printf("  %s: %s\n", id, addr)
	}
}

func handleStatus(server string) {
	// Get cluster status
	statusURL := fmt.Sprintf("%s/cluster/status", server)
	statusResp, err := http.Get(statusURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer statusResp.Body.Close()

	var status map[string]interface{}
	if err := json.NewDecoder(statusResp.Body).Decode(&status); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	// Get Raft status
	raftURL := fmt.Sprintf("%s/raft/status", server)
	raftResp, err := http.Get(raftURL)
	if err == nil {
		defer raftResp.Body.Close()
		var raftStatus map[string]interface{}
		if err := json.NewDecoder(raftResp.Body).Decode(&raftStatus); err == nil {
			status["raft"] = raftStatus
		}
	}

	fmt.Printf("Cluster Status:\n")
	fmt.Printf("  Node ID: %s\n", status["node_id"])
	fmt.Printf("  Address: %s\n", status["address"])
	fmt.Printf("  Peers: %v\n", status["peers"])
	
	if raft, ok := status["raft"].(map[string]interface{}); ok {
		fmt.Printf("  Raft Leader: %v\n", raft["is_leader"])
		fmt.Printf("  Leader Address: %s\n", raft["leader"])
		fmt.Printf("  Raft State: %s\n", raft["state"])
	}
}

