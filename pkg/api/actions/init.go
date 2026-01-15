package actions

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"net"
	"net/url"
	"sync"
	"time"

	"github.com/controlplane-com/manticore-orchestrator/pkg/api/client"
	"github.com/controlplane-com/manticore-orchestrator/pkg/shared/cluster"
	"github.com/controlplane-com/manticore-orchestrator/pkg/shared/types"
)

// ClientBuilder builds clients for a given replica count
// This allows Init to rebuild clients on each retry with fresh replica count
type ClientBuilder func(replicaCount int) []*client.AgentClient

// ReplicaCountFetcher fetches the current replica count from Control Plane
type ReplicaCountFetcher func() (int, error)

// Retry configuration for init process
const (
	initMaxRetries     = 10
	initBaseDelay      = 2 * time.Second
	initMaxDelay       = 60 * time.Second
	initJitterFraction = 0.3 // 30% jitter
)

// TableConfig represents a table to initialize
type TableConfig struct {
	Name    string
	CsvPath string
}

// InitResult contains instructions for the agent to execute locally
type InitResult struct {
	Action            string            `json:"action"`                      // "bootstrap", "join", "rejoin", "continue", or "wait"
	RetryAfterSeconds int               `json:"retryAfterSeconds,omitempty"` // For "wait" action
	TableSlots        map[string]string `json:"tableSlots,omitempty"`        // table name -> "a" or "b"
	SourceAddr        string            `json:"sourceAddr,omitempty"`        // For "join" action
	ClusterUUID       string            `json:"clusterUUID,omitempty"`
}

// CallerInfo contains info about the calling replica passed in the request
type CallerInfo struct {
	UUID            string
	Seqno           int64
	SafeToBootstrap int
	Exists          bool
	ClusterStatus   string // "primary", "non-primary", or ""
	NodeState       string // "synced", "donor", etc.
}

// initMutex serializes cluster operations to prevent race conditions
var initMutex sync.Mutex

// initContext holds state that is passed between init steps
type initContext struct {
	clientBuilder          ClientBuilder
	replicaCountFetcher    ReplicaCountFetcher
	originalCallingReplica int // The calling replica's index (stable across retries)

	clients                 []*client.AgentClient // Filtered clients (updated each attempt)
	availableReplicaIndices []int                 // Indices of available replicas (excluding caller)
	tables                  []TableConfig
	callingReplica          int // Filtered calling replica index (updated each attempt)
	callerInfo              *CallerInfo

	// Computed during init
	replicas      []ReplicaInfo
	clusterUUID   string
	tableSlots    map[string]string // table name -> "a" or "b"
	createdTables []string
}

// filterAvailableClients filters clients by DNS resolution and health check.
// Replicas are excluded if:
// - DNS lookup fails (replica hostname doesn't resolve)
// - Health check returns 503 (replica pod doesn't exist, CPLN infra returns 503)
// Returns: filtered clients (excluding calling replica), available indices, boolean indicating if only caller exists
func filterAvailableClients(clients []*client.AgentClient, callingReplica int) ([]*client.AgentClient, []int, bool) {
	var filtered []*client.AgentClient
	var availableIndices []int

	for i, c := range clients {
		if i == callingReplica {
			continue // Always skip calling replica - it's handled separately
		}
		hostname := extractHostname(c.BaseURL())

		// Check 1: DNS resolution
		_, err := net.LookupHost(hostname)
		if err != nil {
			slog.Debug("DNS lookup failed, excluding replica", "replica", i, "hostname", hostname, "error", err)
			continue
		}

		// Check 2: Health endpoint responds (not 503 from infra)
		// Use a quick probe - if the pod doesn't exist, CPLN returns 503 immediately
		if !isReplicaReachable(c) {
			slog.Debug("health check returned 503, excluding replica", "replica", i, "host", c.BaseURL())
			continue
		}

		filtered = append(filtered, c)
		availableIndices = append(availableIndices, i)
	}

	onlyCallerExists := len(filtered) == 0
	slog.Info("filtered available clients", "total", len(clients), "otherAvailable", len(filtered), "availableIndices", availableIndices, "onlyCallerExists", onlyCallerExists)
	return filtered, availableIndices, onlyCallerExists
}

// isReplicaReachable does a quick health check to see if the replica pod exists.
// Returns false if the health endpoint returns 503 (CPLN infra response when pod doesn't exist).
func isReplicaReachable(c *client.AgentClient) bool {
	_, err := c.HealthProbe()
	return err == nil
}

// extractHostname extracts the hostname from a URL
func extractHostname(urlStr string) string {
	u, err := url.Parse(urlStr)
	if err != nil {
		return ""
	}
	return u.Hostname()
}

// Init analyzes the cluster state and returns instructions for the agent to execute locally.
// This is the main entry point called by each agent at startup.
//
// Returns one of three actions:
// - "bootstrap": Agent should create a new cluster and tables locally
// - "join": Agent should join existing cluster at sourceAddr and create tables locally
// - "wait": Agent should wait and retry (other replicas may be starting up)
//
// The callerInfo parameter contains grastate info from the calling agent.
// aliveSinceStart is how long the agent has been alive without initialization.
// bootstrapTimeout is the threshold after which a lone replica should bootstrap.
func Init(reqCtx context.Context, clientBuilder ClientBuilder, replicaCountFetcher ReplicaCountFetcher, tables []TableConfig, callingReplica int, callerInfo *CallerInfo, aliveSinceStart int, bootstrapTimeout int) (*InitResult, error) {
	slog.Info("init starting", "callingReplica", callingReplica, "tableCount", len(tables), "hasCallerInfo", callerInfo != nil, "aliveSinceStart", aliveSinceStart)

	// Serialize cluster operations
	initMutex.Lock()
	defer initMutex.Unlock()

	ctx := &initContext{
		clientBuilder:          clientBuilder,
		replicaCountFetcher:    replicaCountFetcher,
		originalCallingReplica: callingReplica,
		tables:                 tables,
		callerInfo:             callerInfo,
	}

	var lastErr error
	for attempt := 1; attempt <= initMaxRetries; attempt++ {
		// Check if the request was canceled
		if err := reqCtx.Err(); err != nil {
			slog.Info("init cancelled", "attempt", attempt, "reason", err)
			return nil, fmt.Errorf("init cancelled: %w", err)
		}

		if attempt > 1 {
			delay := calculateBackoffWithJitter(attempt)
			slog.Warn("init failed, retrying", "attempt", attempt, "maxRetries", initMaxRetries, "delay", delay, "lastError", lastErr)

			// Use a timer so we can check for cancellation during the delay
			timer := time.NewTimer(delay)
			select {
			case <-reqCtx.Done():
				timer.Stop()
				slog.Info("init cancelled during backoff", "attempt", attempt, "reason", reqCtx.Err())
				return nil, fmt.Errorf("init cancelled: %w", reqCtx.Err())
			case <-timer.C:
				// Continue with retry
			}
		}

		// Re-fetch replica count on each attempt (maxScale may change)
		replicaCount, err := ctx.replicaCountFetcher()
		if err != nil {
			lastErr = fmt.Errorf("failed to fetch replica count: %w", err)
			slog.Warn("failed to fetch replica count", "attempt", attempt, "error", err)
			continue
		}

		// Validate calling replica against current count
		if ctx.originalCallingReplica < 0 || ctx.originalCallingReplica >= replicaCount {
			lastErr = fmt.Errorf("calling replica %d out of range (replicaCount=%d)", ctx.originalCallingReplica, replicaCount)
			slog.Warn("calling replica out of range", "attempt", attempt, "callingReplica", ctx.originalCallingReplica, "replicaCount", replicaCount)
			continue
		}

		// Rebuild clients with fresh replica count
		allClients := ctx.clientBuilder(replicaCount)

		// Filter clients by DNS resolution (excluding calling replica)
		otherClients, availableIndices, onlyCallerExists := filterAvailableClients(allClients, ctx.originalCallingReplica)

		// Handle case where only the calling replica exists
		if onlyCallerExists {
			shouldBootstrap := false
			reason := ""

			// Check safe_to_bootstrap flag
			if ctx.callerInfo != nil && ctx.callerInfo.SafeToBootstrap == 1 {
				shouldBootstrap = true
				reason = "safe_to_bootstrap=1"
			}

			// Check timeout
			if aliveSinceStart >= bootstrapTimeout {
				shouldBootstrap = true
				reason = fmt.Sprintf("alive timeout exceeded (%ds >= %ds)", aliveSinceStart, bootstrapTimeout)
			}

			if shouldBootstrap {
				slog.Info("bootstrap decision", "reason", reason)
				// Agent will determine tableSlots locally before bootstrapping
				return &InitResult{Action: "bootstrap"}, nil
			}

			slog.Info("no other replicas available, telling agent to wait", "aliveSinceStart", aliveSinceStart, "bootstrapTimeout", bootstrapTimeout)
			return &InitResult{Action: "wait", RetryAfterSeconds: 5}, nil
		}

		// Other replicas exist - continue with normal init
		ctx.clients = otherClients
		ctx.availableReplicaIndices = availableIndices
		ctx.callingReplica = 0 // Not used in new flow, but keep for compatibility

		result, err := initInternal(ctx)
		if err == nil {
			if attempt > 1 {
				slog.Info("init succeeded after retry", "attempt", attempt)
			}
			return result, nil
		}
		lastErr = err
	}

	return nil, fmt.Errorf("init failed after %d attempts: %w", initMaxRetries, lastErr)
}

// calculateBackoffWithJitter returns exponential backoff delay with jitter
func calculateBackoffWithJitter(attempt int) time.Duration {
	// Exponential backoff: baseDelay * 2^(attempt-1)
	backoff := float64(initBaseDelay) * math.Pow(2, float64(attempt-1))
	if backoff > float64(initMaxDelay) {
		backoff = float64(initMaxDelay)
	}

	// Add jitter: +/- jitterFraction of the backoff
	jitter := backoff * initJitterFraction * (2*rand.Float64() - 1)
	delay := time.Duration(backoff + jitter)

	if delay < initBaseDelay {
		delay = initBaseDelay
	}
	return delay
}

// initInternal analyzes cluster state and returns join instructions
// (bootstrap case is handled in Init before calling this)
func initInternal(ctx *initContext) (*InitResult, error) {
	// Step 1: Collect replica info from other nodes
	ctx.replicas = make([]ReplicaInfo, len(ctx.clients))
	for i, c := range ctx.clients {
		ctx.replicas[i] = getReplicaInfo(i, c)
	}

	// Step 2: Extract table names (needed for slot discovery in multiple paths)
	tableNames := make([]string, len(ctx.tables))
	for i, t := range ctx.tables {
		tableNames[i] = t.Name
	}

	// Step 3: Find winning cluster
	sharedReplicas := toSharedReplicaInfo(ctx.replicas)
	preferredUUID := ""
	if ctx.callerInfo != nil && ctx.callerInfo.Exists && ctx.callerInfo.UUID != "" {
		preferredUUID = ctx.callerInfo.UUID
	}
	clusterDesc := cluster.FindWinningClusterWithPreference(sharedReplicas, "", "", preferredUUID)

	if clusterDesc == nil {
		// No cluster exists among available replicas - elect lowest-indexed replica as bootstrap leader
		// Available replicas = caller + other available replicas
		lowestAvailable := ctx.originalCallingReplica
		for _, idx := range ctx.availableReplicaIndices {
			if idx < lowestAvailable {
				lowestAvailable = idx
			}
		}

		if ctx.originalCallingReplica == lowestAvailable {
			slog.Info("no cluster found, caller is lowest available index - bootstrapping",
				"callingReplica", ctx.originalCallingReplica, "availableIndices", ctx.availableReplicaIndices)
			// Agent will determine tableSlots locally before bootstrapping
			return &InitResult{Action: "bootstrap"}, nil
		}

		slog.Info("no cluster found, waiting for lower-indexed replica to bootstrap",
			"callingReplica", ctx.originalCallingReplica, "lowestAvailable", lowestAvailable)
		return &InitResult{Action: "wait", RetryAfterSeconds: 5}, nil
	}

	ctx.clusterUUID = clusterDesc.UUID
	slog.Info("existing cluster found", "uuid", ctx.clusterUUID, "sourceIdx", clusterDesc.SourceIdx, "nodeCount", clusterDesc.NodeCount)

	// Step 4: Check if caller is already in the winning cluster AND healthy
	if ctx.callerInfo != nil && ctx.callerInfo.Exists && ctx.callerInfo.UUID == ctx.clusterUUID {
		// UUID matches - check if caller's cluster is healthy
		if ctx.callerInfo.ClusterStatus == "primary" && ctx.callerInfo.NodeState == "synced" {
			slog.Info("caller already in winning cluster and healthy, returning continue")
			tableSlots := DiscoverTableSlots(ctx.clients, tableNames)
			return &InitResult{Action: "continue", TableSlots: tableSlots, ClusterUUID: ctx.clusterUUID}, nil
		}
		slog.Info("caller in winning cluster but unhealthy, returning rejoin",
			"clusterStatus", ctx.callerInfo.ClusterStatus, "nodeState", ctx.callerInfo.NodeState)
	}

	// Step 5: Determine source address for joining
	sourceAddr := deriveReplicationAddr(ctx.clients[clusterDesc.SourceIdx].BaseURL())

	// Step 6: Discover active slot per table from cluster nodes
	tableSlots := DiscoverTableSlots(ctx.clients, tableNames)

	// Step 7: Verify all slots are determined - indeterminate slots could lead to data loss
	for _, tableName := range tableNames {
		if tableSlots[tableName] == "" {
			slog.Info("slot indeterminate for table, telling agent to wait",
				"table", tableName, "reason", "distributed table missing or corrupt")
			return &InitResult{Action: "wait", RetryAfterSeconds: 5}, nil
		}
	}

	// Determine if this is a join (fresh) or rejoin (switching clusters)
	action := "join"
	if ctx.callerInfo != nil && ctx.callerInfo.Exists && ctx.callerInfo.UUID != "" {
		action = "rejoin" // Caller has a different cluster UUID
	}

	slog.Info("returning join instructions", "action", action, "sourceAddr", sourceAddr, "tableSlots", tableSlots, "clusterUUID", ctx.clusterUUID)

	return &InitResult{
		Action:      action,
		TableSlots:  tableSlots,
		SourceAddr:  sourceAddr,
		ClusterUUID: ctx.clusterUUID,
	}, nil
}

// stepBuildCallerReplicaInfo builds the calling replica's info from the request
func stepBuildCallerReplicaInfo(ctx *initContext) error {
	callerReplicaInfo := ReplicaInfo{
		Index:     ctx.callingReplica,
		Reachable: true, // Must be reachable since it called us
	}
	if ctx.callerInfo != nil && ctx.callerInfo.Exists {
		callerReplicaInfo.Grastate = &types.GrastateResponse{
			UUID:            ctx.callerInfo.UUID,
			Seqno:           ctx.callerInfo.Seqno,
			SafeToBootstrap: ctx.callerInfo.SafeToBootstrap,
			Exists:          true,
		}
		callerReplicaInfo.HasValidUUID = ctx.callerInfo.UUID != "" && ctx.callerInfo.UUID != "00000000-0000-0000-0000-000000000000"
		slog.Debug("using caller grastate", "uuid", ctx.callerInfo.UUID, "seqno", ctx.callerInfo.Seqno)
	}

	// Initialize replicas slice and set caller info
	ctx.replicas = make([]ReplicaInfo, len(ctx.clients))
	ctx.replicas[ctx.callingReplica] = callerReplicaInfo
	return nil
}

// stepCollectReplicaInfo collects replica info from other nodes
func stepCollectReplicaInfo(ctx *initContext) error {
	slog.Debug("collecting replica info from other nodes")
	for i, c := range ctx.clients {
		if i == ctx.callingReplica {
			continue // Already set in step 1
		}
		ctx.replicas[i] = getReplicaInfo(i, c)
	}
	return nil
}

// stepFindWinningCluster finds the existing cluster to join, preferring caller's UUID
func stepFindWinningCluster(ctx *initContext) error {
	sharedReplicas := toSharedReplicaInfo(ctx.replicas)
	preferredUUID := ""
	if ctx.callerInfo != nil && ctx.callerInfo.Exists && ctx.callerInfo.UUID != "" {
		preferredUUID = ctx.callerInfo.UUID
	}
	clusterDesc := cluster.FindWinningClusterWithPreference(sharedReplicas, "", "", preferredUUID)

	if clusterDesc != nil {
		ctx.clusterUUID = clusterDesc.UUID
		slog.Info("existing cluster found", "uuid", ctx.clusterUUID, "sourceIdx", clusterDesc.SourceIdx, "nodeCount", clusterDesc.NodeCount)
	} else {
		slog.Info("no existing cluster found, will bootstrap new cluster")
	}
	return nil
}

// stepJoinOrCreateCluster joins an existing cluster or bootstraps a new one
func stepJoinOrCreateCluster(ctx *initContext) error {
	sharedReplicas := toSharedReplicaInfo(ctx.replicas)
	preferredUUID := ""
	if ctx.callerInfo != nil && ctx.callerInfo.Exists && ctx.callerInfo.UUID != "" {
		preferredUUID = ctx.callerInfo.UUID
	}
	clusterDesc := cluster.FindWinningClusterWithPreference(sharedReplicas, "", "", preferredUUID)

	if clusterDesc != nil {
		// Existing cluster found - join it
		ctx.clusterUUID = clusterDesc.UUID

		if ctx.callingReplica != clusterDesc.SourceIdx {
			sourceAddr := deriveReplicationAddr(ctx.clients[clusterDesc.SourceIdx].BaseURL())
			slog.Debug("joining calling replica to existing cluster", "replica", ctx.callingReplica, "via", sourceAddr)
			if err := ctx.clients[ctx.callingReplica].ClusterJoin(sourceAddr, 0); err != nil {
				return fmt.Errorf("failed to join replica %d to cluster: %w", ctx.callingReplica, err)
			}
		}
	} else {
		// No cluster exists - bootstrap fresh
		primaryIdx := findFirstReachable(ctx.replicas)
		if primaryIdx < 0 {
			return fmt.Errorf("no reachable replicas found for cluster bootstrap")
		}

		slog.Debug("bootstrapping cluster on replica", "replica", primaryIdx)
		if err := ctx.clients[primaryIdx].ClusterBootstrap(0); err != nil {
			return fmt.Errorf("failed to bootstrap cluster on replica %d: %w", primaryIdx, err)
		}

		// Join all other reachable replicas
		sourceAddr := deriveReplicationAddr(ctx.clients[primaryIdx].BaseURL())
		for i, c := range ctx.clients {
			if i == primaryIdx {
				continue
			}
			if !ctx.replicas[i].Reachable {
				slog.Warn("replica not reachable, skipping", "replica", i)
				continue
			}
			slog.Debug("joining replica to cluster", "replica", i, "via", sourceAddr)
			if err := c.ClusterJoin(sourceAddr, 0); err != nil {
				slog.Warn("failed to join replica to cluster", "replica", i, "error", err)
				// Continue with other replicas
			}
		}
	}
	return nil
}

// stepDiscoverTableSlots discovers which slot (a or b) is active for each table
func stepDiscoverTableSlots(ctx *initContext) error {
	tableNames := make([]string, len(ctx.tables))
	for i, t := range ctx.tables {
		tableNames[i] = t.Name
	}
	ctx.tableSlots = DiscoverTableSlots(ctx.clients, tableNames)
	slog.Info("table slots determined", "slots", ctx.tableSlots)
	return nil
}

// stepCreateTables creates all configured tables
func stepCreateTables(ctx *initContext) error {
	primaryClient := findPrimaryClient(ctx.clients)
	if primaryClient == nil {
		return fmt.Errorf("no replica has cluster status 'primary' after cluster setup")
	}

	ctx.createdTables = nil
	for _, table := range ctx.tables {
		slot := ctx.tableSlots[table.Name]
		if slot == "" {
			slot = "a" // Default
		}
		mainTable := table.Name + "_main_" + slot
		deltaTable := table.Name + "_delta"
		distTable := table.Name

		slog.Debug("creating tables", "main", mainTable, "delta", deltaTable, "distributed", distTable)

		// Create delta table (idempotent)
		if err := primaryClient.CreateTable(deltaTable, 0); err != nil {
			slog.Debug("delta table creation (may already exist)", "table", deltaTable, "error", err)
		}

		// Add delta table to cluster (idempotent)
		if err := primaryClient.ClusterAdd(deltaTable, 0); err != nil {
			slog.Debug("delta table cluster add (may already be added)", "table", deltaTable, "error", err)
		}

		// Create main table (idempotent)
		if err := primaryClient.CreateTable(mainTable, 0); err != nil {
			slog.Debug("main table creation (may already exist)", "table", mainTable, "error", err)
		}

		// Add main table to cluster (idempotent)
		if err := primaryClient.ClusterAdd(mainTable, 0); err != nil {
			slog.Debug("main table cluster add (may already be added)", "table", mainTable, "error", err)
		}

		// Wait for replication
		if err := waitForTableReplication(ctx.clients, deltaTable); err != nil {
			slog.Warn("timeout waiting for delta table replication", "table", deltaTable, "error", err)
		}
		if err := waitForTableReplication(ctx.clients, mainTable); err != nil {
			slog.Warn("timeout waiting for main table replication", "table", mainTable, "error", err)
		}

		// Create distributed table on all replicas (idempotent)
		locals := []string{mainTable, deltaTable}
		for i, c := range ctx.clients {
			if err := c.CreateDistributed(distTable, locals, 0); err != nil {
				slog.Debug("distributed table creation (may already exist)", "table", distTable, "replica", i, "error", err)
			}
		}

		ctx.createdTables = append(ctx.createdTables, table.Name)
	}
	return nil
}

// findFirstReachable returns the index of the first reachable replica
func findFirstReachable(replicas []ReplicaInfo) int {
	for i, r := range replicas {
		if r.Reachable {
			return i
		}
	}
	return -1
}

// findPrimaryClient returns the first client that has cluster status "primary"
func findPrimaryClient(clients []*client.AgentClient) *client.AgentClient {
	for _, c := range clients {
		health, err := c.Health(0)
		if err != nil {
			continue
		}
		if health.ClusterStatus == "primary" {
			return c
		}
	}
	return nil
}

// waitForTableReplication waits for a table to be replicated to all nodes
func waitForTableReplication(clients []*client.AgentClient, table string) error {
	maxAttempts := 30
	pollInterval := 2 * time.Second

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		allReady := true

		for i, c := range clients {
			tables, err := c.ListTables(0)
			if err != nil {
				slog.Debug("failed to list tables", "replica", i, "error", err)
				allReady = false
				continue
			}

			found := false
			for _, t := range tables {
				if t.Name == table {
					found = true
					break
				}
			}

			if !found {
				slog.Debug("table not yet replicated", "replica", i, "table", table)
				allReady = false
			}
		}

		if allReady {
			slog.Debug("table replicated to all replicas", "table", table, "replicas", len(clients))
			return nil
		}

		if attempt < maxAttempts {
			time.Sleep(pollInterval)
		}
	}

	return fmt.Errorf("replication timeout: table %s not replicated to all nodes after %d attempts", table, maxAttempts)
}

// Note: getReplicaInfo, toSharedReplicaInfo, and deriveReplicationAddr are defined in repair.go
