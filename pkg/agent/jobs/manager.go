package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/controlplane-com/manticore-orchestrator/shared/types"
	"github.com/google/uuid"
)

const (
	defaultJobsDir  = "/var/lib/manticore/.jobs" // Hidden dir to avoid table name conflicts
	cleanupMaxAge   = 24 * time.Hour
	cleanupInterval = 1 * time.Hour
)

// Manager handles async import jobs with disk persistence
type Manager struct {
	mu          sync.RWMutex
	jobs        map[string]*jobState
	jobsDir     string
	stopCleanup chan struct{}
}

// jobState holds runtime state for a job
type jobState struct {
	job    *types.ImportJob
	cancel context.CancelFunc // Function to cancel the import
}

// NewManager creates a job manager and loads existing jobs from disk
func NewManager(jobsDir string) (*Manager, error) {
	if jobsDir == "" {
		jobsDir = defaultJobsDir
	}

	m := &Manager{
		jobs:        make(map[string]*jobState),
		jobsDir:     jobsDir,
		stopCleanup: make(chan struct{}),
	}

	// Ensure jobs directory exists
	if err := os.MkdirAll(jobsDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create jobs directory: %w", err)
	}

	// Load existing jobs from disk
	if err := m.loadFromDisk(); err != nil {
		return nil, fmt.Errorf("failed to load jobs from disk: %w", err)
	}

	// Start cleanup goroutine
	go m.cleanupLoop()

	return m, nil
}

// CreateJob creates a new pending job and persists it
func (m *Manager) CreateJob(table, csvPath string, workers, batchSize int) (*types.ImportJob, error) {
	now := time.Now().Unix()
	job := &types.ImportJob{
		ID:        uuid.New().String(),
		Table:     table,
		CSVPath:   csvPath,
		Status:    types.ImportJobStatusPending,
		StartedAt: &now,
		Workers:   workers,
		BatchSize: batchSize,
	}

	m.mu.Lock()
	m.jobs[job.ID] = &jobState{job: job}
	m.mu.Unlock()

	if err := m.persistJob(job); err != nil {
		// Remove from memory if persist fails
		m.mu.Lock()
		delete(m.jobs, job.ID)
		m.mu.Unlock()
		return nil, err
	}

	slog.Info("created import job", "jobId", job.ID, "table", table, "csvPath", csvPath,
		"workers", workers, "batchSize", batchSize)
	return job, nil
}

// GetJob returns a job by ID
func (m *Manager) GetJob(id string) *types.ImportJob {
	m.mu.RLock()
	defer m.mu.RUnlock()

	state, ok := m.jobs[id]
	if !ok {
		return nil
	}

	// Return a copy to avoid race conditions
	jobCopy := *state.job
	return &jobCopy
}

// UpdateJobStatus updates job status and persists to disk
func (m *Manager) UpdateJobStatus(id string, status types.ImportJobStatus, errMsg string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	state, ok := m.jobs[id]
	if !ok {
		return fmt.Errorf("job not found: %s", id)
	}

	state.job.Status = status
	if errMsg != "" {
		state.job.Error = errMsg
	}

	now := time.Now().Unix()
	switch status {
	case types.ImportJobStatusCompleted, types.ImportJobStatusFailed, types.ImportJobStatusCancelled:
		state.job.EndedAt = &now
	}

	return m.persistJobLocked(state.job)
}

// SetJobCancelFunc associates a cancel function with a job (for cancellation)
func (m *Manager) SetJobCancelFunc(id string, cancel context.CancelFunc) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if state, ok := m.jobs[id]; ok {
		state.cancel = cancel
	}
}

// UpdateJob updates a job's fields and persists to disk
func (m *Manager) UpdateJob(job *types.ImportJob) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	state, ok := m.jobs[job.ID]
	if !ok {
		return fmt.Errorf("job not found: %s", job.ID)
	}

	// Update the job in state
	state.job = job
	return m.persistJobLocked(job)
}

// FindJobByTableAndPath finds the most recent job for a table and CSV path
func (m *Manager) FindJobByTableAndPath(table, csvPath string) *types.ImportJob {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var mostRecent *types.ImportJob
	var mostRecentTime int64

	for _, state := range m.jobs {
		if state.job.Table == table && state.job.CSVPath == csvPath {
			if state.job.StartedAt != nil && *state.job.StartedAt > mostRecentTime {
				mostRecentTime = *state.job.StartedAt
				jobCopy := *state.job
				mostRecent = &jobCopy
			}
		}
	}

	return mostRecent
}

// CancelJob cancels a running job by calling its cancel function
func (m *Manager) CancelJob(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	state, ok := m.jobs[id]
	if !ok {
		return fmt.Errorf("job not found: %s", id)
	}

	// Can only cancel pending or running jobs
	if state.job.Status != types.ImportJobStatusPending &&
		state.job.Status != types.ImportJobStatusRunning {
		return fmt.Errorf("cannot cancel job with status: %s", state.job.Status)
	}

	// Call cancel function if set (triggers context cancellation)
	if state.cancel != nil {
		state.cancel()
	}

	// Note: Status will be updated to Cancelled by executeImportJob when it detects cancellation
	// We don't update it here to avoid race conditions

	slog.Info("requested cancellation of import job", "jobId", id)
	return nil
}

// persistJob saves job to disk (acquires lock)
func (m *Manager) persistJob(job *types.ImportJob) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.persistJobLocked(job)
}

// persistJobLocked saves job to disk (caller must hold lock)
func (m *Manager) persistJobLocked(job *types.ImportJob) error {
	data, err := json.MarshalIndent(job, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal job: %w", err)
	}

	path := filepath.Join(m.jobsDir, job.ID+".json")
	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("failed to write job file: %w", err)
	}

	return nil
}

// loadFromDisk loads all jobs from disk on startup
func (m *Manager) loadFromDisk() error {
	entries, err := os.ReadDir(m.jobsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}

		path := filepath.Join(m.jobsDir, entry.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			slog.Warn("failed to read job file", "path", path, "error", err)
			continue
		}

		var job types.ImportJob
		if err := json.Unmarshal(data, &job); err != nil {
			slog.Warn("failed to parse job file", "path", path, "error", err)
			continue
		}

		// Mark running/pending jobs as failed (worker pool died during restart)
		// These jobs can be resumed using the lastLineNum checkpoint
		if job.Status == types.ImportJobStatusRunning ||
			job.Status == types.ImportJobStatusPending {
			job.Status = types.ImportJobStatusFailed
			job.Error = "agent restarted during import (can resume from checkpoint)"
			now := time.Now().Unix()
			job.EndedAt = &now
			// Re-persist the updated status
			data, _ := json.MarshalIndent(&job, "", "  ")
			os.WriteFile(path, data, 0644)
			slog.Info("marked interrupted job as failed", "jobId", job.ID,
				"lastLineNum", job.LastLineNum, "error", job.Error)
		}

		m.jobs[job.ID] = &jobState{job: &job}
	}

	slog.Info("loaded jobs from disk", "count", len(m.jobs))
	return nil
}

// cleanupLoop periodically removes old completed jobs
func (m *Manager) cleanupLoop() {
	ticker := time.NewTicker(cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.cleanupOldJobs()
		case <-m.stopCleanup:
			return
		}
	}
}

// cleanupOldJobs removes jobs older than 24 hours
func (m *Manager) cleanupOldJobs() {
	m.mu.Lock()
	defer m.mu.Unlock()

	cutoff := time.Now().Add(-cleanupMaxAge).Unix()
	cleaned := 0

	for id, state := range m.jobs {
		// Only cleanup completed/failed/cancelled jobs
		if state.job.Status == types.ImportJobStatusPending ||
			state.job.Status == types.ImportJobStatusRunning {
			continue
		}

		// Check if job ended more than 24 hours ago
		if state.job.EndedAt != nil && *state.job.EndedAt < cutoff {
			// Remove job file from disk
			path := filepath.Join(m.jobsDir, id+".json")
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				slog.Warn("failed to remove job file", "path", path, "error", err)
			}
			// Remove error log file if it exists
			errorLogPath := filepath.Join(m.jobsDir, id+".error.log")
			os.Remove(errorLogPath) // Ignore errors - file may not exist
			// Remove from memory
			delete(m.jobs, id)
			cleaned++
		}
	}

	if cleaned > 0 {
		slog.Info("cleaned up old jobs", "count", cleaned)
	}
}

// Stop stops the cleanup goroutine
func (m *Manager) Stop() {
	close(m.stopCleanup)
}

// JobsDir returns the jobs directory path
func (m *Manager) JobsDir() string {
	return m.jobsDir
}

// ErrorLogPath returns the path to the error log file for a job
func (m *Manager) ErrorLogPath(jobID string) string {
	return filepath.Join(m.jobsDir, jobID+".error.log")
}

// CancelAllJobs cancels all pending/running jobs and returns the count of jobs cancelled.
// This is used during graceful shutdown to stop import jobs before the agent exits.
func (m *Manager) CancelAllJobs() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	cancelled := 0
	for _, state := range m.jobs {
		if state.job.Status == types.ImportJobStatusRunning ||
			state.job.Status == types.ImportJobStatusPending {
			if state.cancel != nil {
				state.cancel()
				cancelled++
				slog.Info("cancelled import job", "jobId", state.job.ID)
			}
		}
	}
	return cancelled
}
