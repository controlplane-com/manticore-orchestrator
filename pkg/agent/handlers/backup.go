package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/controlplane-com/manticore-orchestrator/pkg/shared/types"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
)

// backupJobState holds runtime state for a backup/restore job
type backupJobState struct {
	job    *types.BackupJob
	cancel context.CancelFunc
}

// backupJobManager manages backup/restore jobs in memory
type backupJobManager struct {
	mu   sync.RWMutex
	jobs map[string]*backupJobState
}

var backupJobs = &backupJobManager{
	jobs: make(map[string]*backupJobState),
}

func (m *backupJobManager) createJob(table, action, backupDir string) *types.BackupJob {
	now := time.Now().Unix()
	job := &types.BackupJob{
		ID:        uuid.New().String(),
		Table:     table,
		Action:    action,
		BackupDir: backupDir,
		Status:    types.BackupJobStatusPending,
		StartedAt: &now,
	}

	m.mu.Lock()
	m.jobs[job.ID] = &backupJobState{job: job}
	m.mu.Unlock()

	return job
}

func (m *backupJobManager) getJob(id string) *types.BackupJob {
	m.mu.RLock()
	defer m.mu.RUnlock()

	state, ok := m.jobs[id]
	if !ok {
		return nil
	}
	jobCopy := *state.job
	return &jobCopy
}

func (m *backupJobManager) updateStatus(id string, status types.BackupJobStatus, errMsg string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	state, ok := m.jobs[id]
	if !ok {
		return
	}

	state.job.Status = status
	if errMsg != "" {
		state.job.Error = errMsg
	}

	now := time.Now().Unix()
	if status == types.BackupJobStatusCompleted || status == types.BackupJobStatusFailed {
		state.job.EndedAt = &now
	}
}

func (m *backupJobManager) setCancelFunc(id string, cancel context.CancelFunc) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if state, ok := m.jobs[id]; ok {
		state.cancel = cancel
	}
}

// StartBackup starts an async backup job and returns immediately with a job ID
func (h *Handler) StartBackup(w http.ResponseWriter, r *http.Request) {
	var req types.BackupRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		errorResponse(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.Table == "" {
		errorResponse(w, http.StatusBadRequest, "table is required")
		return
	}
	if req.BackupDir == "" {
		errorResponse(w, http.StatusBadRequest, "backupDir is required")
		return
	}

	job := backupJobs.createJob(req.Table, "backup", req.BackupDir)
	slog.Info("starting backup job", "jobId", job.ID, "table", req.Table, "backupDir", req.BackupDir)

	go h.executeBackup(job)

	jsonResponse(w, http.StatusAccepted, types.StartBackupResponse{JobID: job.ID})
}

// GetBackupStatus returns the status of a backup job
func (h *Handler) GetBackupStatus(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	jobID := vars["jobId"]

	if jobID == "" {
		errorResponse(w, http.StatusBadRequest, "jobId is required")
		return
	}

	job := backupJobs.getJob(jobID)
	if job == nil {
		errorResponse(w, http.StatusNotFound, fmt.Sprintf("job not found: %s", jobID))
		return
	}

	jsonResponse(w, http.StatusOK, types.BackupJobResponse{Job: job})
}

// StartRestore starts an async restore job and returns immediately with a job ID
func (h *Handler) StartRestore(w http.ResponseWriter, r *http.Request) {
	var req types.RestoreRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		errorResponse(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.Table == "" {
		errorResponse(w, http.StatusBadRequest, "table is required")
		return
	}
	if req.BackupDir == "" {
		errorResponse(w, http.StatusBadRequest, "backupDir is required")
		return
	}

	job := backupJobs.createJob(req.Table, "restore", req.BackupDir)
	slog.Info("starting restore job", "jobId", job.ID, "table", req.Table, "backupDir", req.BackupDir)

	go h.executeRestore(job)

	jsonResponse(w, http.StatusAccepted, types.StartBackupResponse{JobID: job.ID})
}

// GetRestoreStatus returns the status of a restore job
func (h *Handler) GetRestoreStatus(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	jobID := vars["jobId"]

	if jobID == "" {
		errorResponse(w, http.StatusBadRequest, "jobId is required")
		return
	}

	job := backupJobs.getJob(jobID)
	if job == nil {
		errorResponse(w, http.StatusNotFound, fmt.Sprintf("job not found: %s", jobID))
		return
	}

	jsonResponse(w, http.StatusOK, types.BackupJobResponse{Job: job})
}

// executeBackup runs manticore-backup to create a physical backup
func (h *Handler) executeBackup(job *types.BackupJob) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	backupJobs.setCancelFunc(job.ID, cancel)
	backupJobs.updateStatus(job.ID, types.BackupJobStatusRunning, "")

	slog.Info("executing manticore-backup", "jobId", job.ID, "table", job.Table, "backupDir", job.BackupDir)

	// Create backup directory (manticore-backup expects it to exist)
	if err := os.MkdirAll(job.BackupDir, 0755); err != nil {
		errMsg := fmt.Sprintf("failed to create backup directory %s: %v", job.BackupDir, err)
		slog.Error("backup failed", "jobId", job.ID, "error", errMsg)
		backupJobs.updateStatus(job.ID, types.BackupJobStatusFailed, errMsg)
		return
	}

	// Run manticore-backup
	cmd := exec.CommandContext(ctx, "manticore-backup",
		"--config=/var/lib/manticore/manticore-runtime.conf",
		"--backup-dir="+job.BackupDir,
		"--tables="+job.Table,
	)

	output, err := cmd.CombinedOutput()
	if err != nil {
		errMsg := fmt.Sprintf("manticore-backup failed: %v, output: %s", err, string(output))
		slog.Error("backup failed", "jobId", job.ID, "error", errMsg)
		backupJobs.updateStatus(job.ID, types.BackupJobStatusFailed, errMsg)
		return
	}

	slog.Info("backup completed", "jobId", job.ID, "table", job.Table, "output", string(output))
	backupJobs.updateStatus(job.ID, types.BackupJobStatusCompleted, "")
}

// executeRestore handles cluster operations and imports table from backup data
func (h *Handler) executeRestore(job *types.BackupJob) {
	_, cancel := context.WithCancel(context.Background())
	defer cancel()

	backupJobs.setCancelFunc(job.ID, cancel)
	backupJobs.updateStatus(job.ID, types.BackupJobStatusRunning, "")

	slog.Info("executing restore", "jobId", job.ID, "table", job.Table, "backupDir", job.BackupDir)

	// Step 1: Remove table from cluster
	slog.Info("removing table from cluster for restore", "table", job.Table, "cluster", h.clusterName)
	dropSQL := fmt.Sprintf("ALTER CLUSTER %s DROP %s", h.clusterName, job.Table)
	if err := h.client.Execute(dropSQL); err != nil {
		// Not fatal - table may not be in cluster
		if !strings.Contains(err.Error(), "is not in cluster") {
			slog.Warn("failed to remove table from cluster", "table", job.Table, "error", err)
		}
	}

	// Step 2: Drop the table so IMPORT TABLE can recreate it
	slog.Info("dropping table for restore", "table", job.Table)
	dropTableSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", job.Table)
	if err := h.client.Execute(dropTableSQL); err != nil {
		errMsg := fmt.Sprintf("failed to drop table before restore: %v", err)
		slog.Error("restore failed", "jobId", job.ID, "error", errMsg)
		backupJobs.updateStatus(job.ID, types.BackupJobStatusFailed, errMsg)
		return
	}

	// Step 3: Import table from backup data using IMPORT TABLE
	// manticore-backup stores data files at: backupDir/data/tableName/tableName.*
	// IMPORT TABLE creates a clean local table without stale cluster metadata
	slog.Info("importing table from backup data", "jobId", job.ID, "backupDir", job.BackupDir)

	var importPath string
	err := filepath.WalkDir(job.BackupDir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !d.IsDir() && d.Name() == job.Table+".meta" {
			importPath = strings.TrimSuffix(path, ".meta")
			return filepath.SkipAll
		}
		return nil
	})
	if err != nil {
		errMsg := fmt.Sprintf("failed to search backup directory for table data: %v", err)
		slog.Error("restore failed", "jobId", job.ID, "error", errMsg)
		backupJobs.updateStatus(job.ID, types.BackupJobStatusFailed, errMsg)
		return
	}
	if importPath == "" {
		errMsg := fmt.Sprintf("table data files not found in backup directory: %s (looking for %s.meta)", job.BackupDir, job.Table)
		slog.Error("restore failed", "jobId", job.ID, "error", errMsg)
		backupJobs.updateStatus(job.ID, types.BackupJobStatusFailed, errMsg)
		return
	}

	importSQL := fmt.Sprintf("IMPORT TABLE %s FROM '%s'", job.Table, importPath)
	slog.Info("executing IMPORT TABLE", "jobId", job.ID, "table", job.Table, "importPath", importPath)
	if err := h.client.Execute(importSQL); err != nil {
		errMsg := fmt.Sprintf("IMPORT TABLE failed: %v", err)
		slog.Error("restore failed", "jobId", job.ID, "error", errMsg)
		backupJobs.updateStatus(job.ID, types.BackupJobStatusFailed, errMsg)
		return
	}
	slog.Info("IMPORT TABLE completed", "jobId", job.ID, "table", job.Table)

	// Step 4: Re-add table to cluster
	slog.Info("re-adding table to cluster after restore", "table", job.Table, "cluster", h.clusterName)
	if err := h.ClusterAdd(job.Table); err != nil {
		errMsg := fmt.Sprintf("failed to re-add table to cluster after restore: %v", err)
		slog.Error("restore cluster re-add failed", "jobId", job.ID, "error", errMsg)
		backupJobs.updateStatus(job.ID, types.BackupJobStatusFailed, errMsg)
		return
	}

	slog.Info("restore completed", "jobId", job.ID, "table", job.Table)
	backupJobs.updateStatus(job.ID, types.BackupJobStatusCompleted, "")
}
