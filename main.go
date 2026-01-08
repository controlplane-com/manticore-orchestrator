package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/controlplane-com/manticore-orchestrator/actions"
	"github.com/controlplane-com/manticore-orchestrator/client"
)

func main() {
	// Configuration from environment
	action := getEnv("ACTION", "health")
	replicaCount, _ := strconv.Atoi(getEnv("REPLICA_COUNT", "2"))
	agentPort := getEnv("AGENT_PORT", "8080")
	workloadName := getEnv("WORKLOAD_NAME", "manticore")
	gvc := getEnv("GVC", "")
	location := getEnv("LOCATION", "")
	tableName := getEnv("TABLE_NAME", "")
	tablesConfig := getEnv("TABLES_CONFIG", "{}")
	stateFile := getEnv("STATE_FILE", "/tmp/orchestrator_state.json")
	authToken := getEnv("AUTH_TOKEN", "")

	if authToken == "" {
		log.Fatal("AUTH_TOKEN environment variable is required")
	}

	if tableName == "" {
		log.Fatal("TABLE_NAME environment variable is required")
	}

	// Parse TABLES_CONFIG to get CSV path for this table
	csvPath, err := getCSVPathForTable(tablesConfig, tableName)
	if err != nil {
		log.Fatalf("Failed to get CSV path for table %s: %v", tableName, err)
	}

	log.Printf("Manticore Orchestrator starting")
	log.Printf("Action: %s, Table: %s, CSV: %s", action, tableName, csvPath)
	log.Printf("Replicas: %d, Workload: %s", replicaCount, workloadName)

	// Build replica endpoints
	var endpoints []string
	for i := 0; i < replicaCount; i++ {
		// Control Plane internal DNS format: replica-{i}.{workloadName}.{location}.{gvc}.cpln.local
		var endpoint string
		if gvc != "" && location != "" {
			endpoint = fmt.Sprintf("http://replica-%d.%s.%s.%s.cpln.local:%s",
				i, workloadName, location, gvc, agentPort)
		} else {
			// Fallback for local testing
			endpoint = fmt.Sprintf("http://%s-%d:%s", workloadName, i, agentPort)
		}
		endpoints = append(endpoints, endpoint)
	}

	log.Printf("Agent endpoints: %v", endpoints)

	// Create clients for each replica
	var clients []*client.AgentClient
	for _, endpoint := range endpoints {
		clients = append(clients, client.NewAgentClient(endpoint, authToken))
	}

	// Create action context
	ctx := &actions.Context{
		Clients:   clients,
		Dataset:   tableName,
		CSVPath:   csvPath,
		StateFile: stateFile,
	}

	// Execute action
	var actionErr error
	switch action {
	case "health":
		actionErr = actions.Health(ctx)
	case "init":
		actionErr = actions.Init(ctx)
	case "import":
		actionErr = actions.Import(ctx)
	default:
		log.Fatalf("Unknown action: %s", action)
	}

	if actionErr != nil {
		log.Fatalf("Action %s failed: %v", action, actionErr)
	}

	log.Printf("Action %s completed successfully", action)
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// getCSVPathForTable parses TABLES_CONFIG JSON and returns the CSV path for a table
// TABLES_CONFIG format: {"addresses":"path/to/addresses.csv","products":"path/to/products.csv"}
func getCSVPathForTable(tablesConfigJSON, tableName string) (string, error) {
	var config map[string]string
	if err := json.Unmarshal([]byte(tablesConfigJSON), &config); err != nil {
		return "", fmt.Errorf("failed to parse TABLES_CONFIG: %w", err)
	}

	csvPath, ok := config[tableName]
	if !ok {
		return "", fmt.Errorf("table %s not found in TABLES_CONFIG", tableName)
	}

	return csvPath, nil
}
