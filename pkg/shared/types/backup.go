package types

// BackupJobStatus represents the status of an async backup/restore job
type BackupJobStatus string

const (
	BackupJobStatusPending   BackupJobStatus = "pending"
	BackupJobStatusRunning   BackupJobStatus = "running"
	BackupJobStatusCompleted BackupJobStatus = "completed"
	BackupJobStatusFailed    BackupJobStatus = "failed"
)

// BackupJob represents an async backup or restore job
type BackupJob struct {
	ID          string          `json:"id"`
	Table       string          `json:"table"`
	Action      string          `json:"action"` // "backup" or "restore"
	BackupDir   string          `json:"backupDir"`
	SourceTable string          `json:"sourceTable,omitempty"` // For restore: original table name in backup files (when target differs)
	Status      BackupJobStatus `json:"status"`
	Error       string          `json:"error,omitempty"`
	StartedAt   *int64          `json:"startedAt,omitempty"` // Unix timestamp
	EndedAt     *int64          `json:"endedAt,omitempty"`   // Unix timestamp
}

// BackupRequest is the request body for starting a backup
type BackupRequest struct {
	Table     string `json:"table"`
	BackupDir string `json:"backupDir"`
}

// RestoreRequest is the request body for starting a restore
type RestoreRequest struct {
	Table       string `json:"table"`
	BackupDir   string `json:"backupDir"`
	SourceTable string `json:"sourceTable,omitempty"` // Original table name in backup files (when restoring to a different slot)
}

// StartBackupResponse is returned when starting an async backup/restore
type StartBackupResponse struct {
	JobID string `json:"jobId"`
}

// BackupJobResponse is returned when querying backup/restore job status
type BackupJobResponse struct {
	Job *BackupJob `json:"job"`
}
