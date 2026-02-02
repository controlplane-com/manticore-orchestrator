package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/controlplane-com/manticore-orchestrator/pkg/api/client"
	"github.com/controlplane-com/manticore-orchestrator/pkg/shared/types"
)

// Config holds all environment-based configuration for backup/restore operations
type Config struct {
	Action            string
	AuthToken         string
	Type              string // "delta" or "main"
	Dataset           string
	Slot              string // "a" or "b" when Type=main
	AgentPort         string
	SharedVolumeMount string
	ManticoreHost     string
	BackupProvider    string
	BackupBucket      string
	BackupPrefix      string
	BackupRegion      string
	RestoreFile       string // required for restore
	BackupSlot        string // for blue-green restore
	OrchestratorURL   string // for blue-green rotation
	TableName         string // derived from Dataset + Type + Slot
}

func getEnv(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

func requireEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		slog.Error("required env var is not set", "var", key)
		os.Exit(1)
	}
	return v
}

func loadConfig() Config {
	cfg := Config{
		Action:            requireEnv("ACTION"),
		AuthToken:         requireEnv("AUTH_TOKEN"),
		Type:              getEnv("TYPE", "delta"),
		Dataset:           requireEnv("DATASET"),
		AgentPort:         getEnv("AGENT_PORT", "8080"),
		SharedVolumeMount: getEnv("SHARED_VOLUME_MOUNT", "/mnt/shared"),
		ManticoreHost:     requireEnv("MANTICORE_HOST"),
		BackupProvider:    requireEnv("BACKUP_PROVIDER"),
		BackupBucket:      requireEnv("BACKUP_BUCKET"),
		BackupPrefix:      requireEnv("BACKUP_PREFIX"),
		BackupRegion:      getEnv("BACKUP_REGION", ""),
		RestoreFile:       getEnv("RESTORE_FILE", ""),
		BackupSlot:        getEnv("BACKUP_SLOT", ""),
		OrchestratorURL:   getEnv("ORCHESTRATOR_API_URL", ""),
	}

	// Derive table name from type
	switch cfg.Type {
	case "delta":
		cfg.TableName = cfg.Dataset + "_delta"
	case "main":
		cfg.Slot = requireEnv("SLOT")
		cfg.TableName = cfg.Dataset + "_main_" + cfg.Slot
	default:
		slog.Error("unsupported TYPE (expected 'delta' or 'main')", "type", cfg.Type)
		os.Exit(1)
	}

	return cfg
}

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	cfg := loadConfig()

	switch cfg.Action {
	case "backup":
		if err := runBackup(cfg); err != nil {
			slog.Error("backup failed", "error", err)
			os.Exit(1)
		}
	case "restore":
		if err := runRestore(cfg); err != nil {
			slog.Error("restore failed", "error", err)
			os.Exit(1)
		}
	default:
		slog.Error("unsupported ACTION", "action", cfg.Action)
		os.Exit(1)
	}
}

func runBackup(cfg Config) error {
	timestamp := time.Now().UTC().Format("2006-01-02T15-04-05Z")
	filename := fmt.Sprintf("%s-%s.tar.gz", cfg.TableName, timestamp)
	backupDir := filepath.Join(cfg.SharedVolumeMount, "backups", cfg.Dataset, timestamp)

	slog.Info("starting backup", "table", cfg.TableName, "timestamp", timestamp, "backupDir", backupDir)

	// Create storage client early to fail fast on config errors
	storageClient, err := NewStorageClient(cfg.BackupProvider, cfg.BackupBucket, cfg.BackupRegion)
	if err != nil {
		return fmt.Errorf("failed to create storage client: %w", err)
	}

	// Create backup directory
	if err := os.MkdirAll(backupDir, 0755); err != nil {
		return fmt.Errorf("failed to create backup directory: %w", err)
	}
	defer func() {
		slog.Info("cleaning up backup directory", "dir", backupDir)
		os.RemoveAll(backupDir)
	}()

	// Step 1: Call agent to run manticore-backup
	agentURL := fmt.Sprintf("http://%s:%s", cfg.ManticoreHost, cfg.AgentPort)
	agentClient := client.NewAgentClient(agentURL, cfg.AuthToken)

	slog.Info("requesting agent to run manticore-backup")
	jobID, err := agentClient.StartBackup(types.BackupRequest{
		Table:     cfg.TableName,
		BackupDir: backupDir,
	}, 0)
	if err != nil {
		return fmt.Errorf("failed to start backup on agent: %w", err)
	}
	slog.Info("backup job started", "jobId", jobID)

	// Step 2: Poll until backup completes (initial delay to let job start)
	time.Sleep(15 * time.Second)
	if err := pollAgentJob(agentClient, "backup", jobID); err != nil {
		return fmt.Errorf("backup job failed: %w", err)
	}

	// Step 3: Compress and stream to cloud storage
	slog.Info("compressing and uploading backup", "filename", filename)
	storageKey := cfg.BackupPrefix + "/" + filename

	pr, pw := io.Pipe()
	errCh := make(chan error, 1)

	go func() {
		err := createTarGz(backupDir, pw)
		pw.CloseWithError(err)
		errCh <- err
	}()

	if err := storageClient.Upload(context.Background(), storageKey, pr); err != nil {
		return fmt.Errorf("failed to upload backup: %w", err)
	}

	if err := <-errCh; err != nil {
		return fmt.Errorf("failed to create tar.gz: %w", err)
	}

	slog.Info("backup completed", "filename", filename)
	return nil
}

func runRestore(cfg Config) error {
	if cfg.RestoreFile == "" {
		return fmt.Errorf("RESTORE_FILE env var is required for restore action")
	}

	// For blue-green main restore: BACKUP_SLOT is the slot the backup was taken from
	// TABLE_NAME is already set to the target (inactive) slot via SLOT env var
	sourceTable := ""
	if cfg.BackupSlot != "" {
		sourceTable = cfg.Dataset + "_main_" + cfg.BackupSlot
		slog.Info("blue-green restore", "sourceTable", sourceTable, "targetTable", cfg.TableName)
	}

	timestamp := time.Now().UTC().Format("2006-01-02T15-04-05Z")
	restoreDir := filepath.Join(cfg.SharedVolumeMount, "backups", cfg.Dataset, "restore-"+timestamp)

	slog.Info("starting restore", "table", cfg.TableName, "restoreFile", cfg.RestoreFile, "restoreDir", restoreDir)

	// Create storage client early to fail fast on config errors
	storageClient, err := NewStorageClient(cfg.BackupProvider, cfg.BackupBucket, cfg.BackupRegion)
	if err != nil {
		return fmt.Errorf("failed to create storage client: %w", err)
	}

	// Step 1: Download and extract backup to shared volume
	if err := os.MkdirAll(restoreDir, 0755); err != nil {
		return fmt.Errorf("failed to create restore directory: %w", err)
	}
	defer func() {
		slog.Info("cleaning up restore directory", "dir", restoreDir)
		os.RemoveAll(restoreDir)
	}()

	storageKey := cfg.BackupPrefix + "/" + cfg.RestoreFile
	slog.Info("downloading and extracting backup", "key", storageKey, "restoreDir", restoreDir)

	rc, err := storageClient.Download(context.Background(), storageKey)
	if err != nil {
		return fmt.Errorf("failed to download backup: %w", err)
	}
	defer rc.Close()

	if err := extractTarGz(rc, restoreDir); err != nil {
		return fmt.Errorf("failed to extract backup: %w", err)
	}

	// Step 2: Call agent to restore (handles cluster ops + IMPORT TABLE)
	agentURL := fmt.Sprintf("http://%s:%s", cfg.ManticoreHost, cfg.AgentPort)
	agentClient := client.NewAgentClient(agentURL, cfg.AuthToken)

	restoreReq := types.RestoreRequest{
		Table:     cfg.TableName,
		BackupDir: restoreDir,
	}
	if sourceTable != "" {
		restoreReq.SourceTable = sourceTable
	}

	slog.Info("requesting agent to restore from backup")
	jobID, err := agentClient.StartRestore(restoreReq, 0)
	if err != nil {
		return fmt.Errorf("failed to start restore on agent: %w", err)
	}
	slog.Info("restore job started", "jobId", jobID)

	// Step 3: Poll until restore completes (initial delay to let job start)
	time.Sleep(15 * time.Second)
	if err := pollAgentJob(agentClient, "restore", jobID); err != nil {
		return fmt.Errorf("restore job failed: %w", err)
	}

	// Step 4: Blue-green rotation (main tables only)
	if sourceTable != "" && cfg.OrchestratorURL != "" {
		if err := rotateMain(cfg); err != nil {
			slog.Error("rotation failed - restore data is in target table but distributed table has NOT been rotated",
				"targetTable", cfg.TableName,
				"error", err,
			)
			slog.Warn("manual rotation required",
				"url", cfg.OrchestratorURL+"/api/rotate-main",
				"tableName", cfg.Dataset,
				"newSlot", cfg.Slot,
				"oldSlot", cfg.BackupSlot,
			)
			return fmt.Errorf("rotation failed: %w", err)
		}
	}

	slog.Info("restore completed", "restoreFile", cfg.RestoreFile, "table", cfg.TableName)
	return nil
}

// pollAgentJob polls an agent backup/restore job until completion
func pollAgentJob(agentClient *client.AgentClient, action, jobID string) error {
	const pollInterval = 5 * time.Second

	for {
		var job *types.BackupJob
		var err error

		switch action {
		case "backup":
			job, err = agentClient.GetBackupStatus(jobID, 1)
		case "restore":
			job, err = agentClient.GetRestoreStatus(jobID, 1)
		default:
			return fmt.Errorf("unsupported action: %s", action)
		}

		if err != nil {
			slog.Warn("failed to poll job status, retrying", "jobId", jobID, "error", err)
			time.Sleep(pollInterval)
			continue
		}

		switch job.Status {
		case types.BackupJobStatusCompleted:
			slog.Info("job completed successfully", "jobId", jobID)
			return nil
		case types.BackupJobStatusFailed:
			return fmt.Errorf("job %s failed: %s", jobID, job.Error)
		case types.BackupJobStatusPending, types.BackupJobStatusRunning:
			slog.Info("job in progress", "jobId", jobID, "status", job.Status)
			time.Sleep(pollInterval)
		default:
			slog.Warn("unknown job status, retrying", "jobId", jobID, "status", job.Status)
			time.Sleep(pollInterval)
		}
	}
}

// rotateMain calls the orchestrator API to rotate the distributed table after blue-green restore
func rotateMain(cfg Config) error {
	slog.Info("calling orchestrator to rotate main table",
		"dataset", cfg.Dataset, "newSlot", cfg.Slot, "oldSlot", cfg.BackupSlot)

	payload := map[string]string{
		"tableName": cfg.Dataset,
		"newSlot":   cfg.Slot,
		"oldSlot":   cfg.BackupSlot,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal rotation request: %w", err)
	}

	url := cfg.OrchestratorURL + "/api/rotate-main"
	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+cfg.AuthToken)
	req.Header.Set("Content-Type", "application/json")

	httpClient := &http.Client{Timeout: 2 * time.Minute}
	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to connect to orchestrator API at %s: %w", url, err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("orchestrator rotation failed with HTTP %d: %s", resp.StatusCode, string(respBody))
	}

	slog.Info("rotation response", "body", string(respBody))
	return nil
}
