package s3

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// Client provides S3 operations for index uploads
type Client struct {
	s3Client *s3.Client
	bucket   string
}

// NewClient creates a new S3 client
func NewClient(bucket, region string) (*Client, error) {
	// Load AWS configuration from environment or IAM role
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(region),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	return &Client{
		s3Client: s3.NewFromConfig(cfg),
		bucket:   bucket,
	}, nil
}

// NewClientWithConfig creates a new S3 client with a custom AWS config
func NewClientWithConfig(bucket string, cfg aws.Config) *Client {
	return &Client{
		s3Client: s3.NewFromConfig(cfg),
		bucket:   bucket,
	}
}

// UploadDirectory uploads all files from a local directory to an S3 prefix
func (c *Client) UploadDirectory(ctx context.Context, localPath, s3Prefix string) error {
	// Ensure s3Prefix doesn't have trailing slash for consistency
	s3Prefix = strings.TrimSuffix(s3Prefix, "/")

	// Get list of files to upload
	var files []string
	err := filepath.Walk(localPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to walk directory: %w", err)
	}

	slog.Info("uploading directory to S3", "localPath", localPath, "s3Prefix", s3Prefix, "files", len(files))

	// Upload each file
	for _, filePath := range files {
		// Get relative path from localPath
		relPath, err := filepath.Rel(localPath, filePath)
		if err != nil {
			return fmt.Errorf("failed to get relative path: %w", err)
		}

		// Construct S3 key
		s3Key := fmt.Sprintf("%s/%s", s3Prefix, relPath)

		if err := c.uploadFile(ctx, filePath, s3Key); err != nil {
			return fmt.Errorf("failed to upload %s: %w", filePath, err)
		}

		slog.Debug("uploaded file", "file", relPath, "s3Key", s3Key)
	}

	slog.Info("directory upload completed", "s3Prefix", s3Prefix)
	return nil
}

// uploadFile uploads a single file to S3
func (c *Client) uploadFile(ctx context.Context, localPath, s3Key string) error {
	file, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	_, err = c.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(s3Key),
		Body:   file,
	})
	if err != nil {
		return fmt.Errorf("failed to put object: %w", err)
	}

	return nil
}

// DeletePrefix deletes all objects under the specified S3 prefix
func (c *Client) DeletePrefix(ctx context.Context, prefix string) error {
	// Ensure prefix doesn't have trailing slash for consistency
	prefix = strings.TrimSuffix(prefix, "/")

	// List all objects with the prefix
	paginator := s3.NewListObjectsV2Paginator(c.s3Client, &s3.ListObjectsV2Input{
		Bucket: aws.String(c.bucket),
		Prefix: aws.String(prefix + "/"),
	})

	var objectsToDelete []types.ObjectIdentifier
	for paginator.HasMorePages() {
		output, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("failed to list objects: %w", err)
		}

		for _, obj := range output.Contents {
			objectsToDelete = append(objectsToDelete, types.ObjectIdentifier{
				Key: obj.Key,
			})
		}
	}

	if len(objectsToDelete) == 0 {
		slog.Debug("no objects to delete", "prefix", prefix)
		return nil
	}

	// Delete objects in batches of 1000 (S3 limit)
	for i := 0; i < len(objectsToDelete); i += 1000 {
		end := i + 1000
		if end > len(objectsToDelete) {
			end = len(objectsToDelete)
		}

		batch := objectsToDelete[i:end]
		_, err := c.s3Client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(c.bucket),
			Delete: &types.Delete{
				Objects: batch,
				Quiet:   aws.Bool(true),
			},
		})
		if err != nil {
			return fmt.Errorf("failed to delete objects: %w", err)
		}

		slog.Debug("deleted batch of objects", "count", len(batch))
	}

	slog.Info("prefix deleted", "prefix", prefix, "objects", len(objectsToDelete))
	return nil
}

// DownloadDirectory downloads all files from an S3 prefix to a local directory
func (c *Client) DownloadDirectory(ctx context.Context, s3Prefix, localPath string) error {
	// Ensure s3Prefix doesn't have trailing slash for consistency
	s3Prefix = strings.TrimSuffix(s3Prefix, "/")

	// List all objects with the prefix
	paginator := s3.NewListObjectsV2Paginator(c.s3Client, &s3.ListObjectsV2Input{
		Bucket: aws.String(c.bucket),
		Prefix: aws.String(s3Prefix + "/"),
	})

	var downloadCount int
	for paginator.HasMorePages() {
		output, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("failed to list objects: %w", err)
		}

		for _, obj := range output.Contents {
			// Get relative path from prefix
			relPath := strings.TrimPrefix(*obj.Key, s3Prefix+"/")
			localFilePath := filepath.Join(localPath, relPath)

			// Create parent directory
			if err := os.MkdirAll(filepath.Dir(localFilePath), 0755); err != nil {
				return fmt.Errorf("failed to create directory: %w", err)
			}

			// Download file
			if err := c.downloadFile(ctx, *obj.Key, localFilePath); err != nil {
				return fmt.Errorf("failed to download %s: %w", *obj.Key, err)
			}

			downloadCount++
			slog.Debug("downloaded file", "s3Key", *obj.Key, "localPath", localFilePath)
		}
	}

	slog.Info("directory download completed", "s3Prefix", s3Prefix, "files", downloadCount)
	return nil
}

// downloadFile downloads a single file from S3
func (c *Client) downloadFile(ctx context.Context, s3Key, localPath string) error {
	result, err := c.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(s3Key),
	})
	if err != nil {
		return fmt.Errorf("failed to get object: %w", err)
	}
	defer result.Body.Close()

	file, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	if _, err := io.Copy(file, result.Body); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	return nil
}

// Bucket returns the bucket name
func (c *Client) Bucket() string {
	return c.bucket
}

// BackupFile represents a backup file in cloud storage
type BackupFile struct {
	Filename     string `json:"filename"`
	Key          string `json:"key"`
	Size         int64  `json:"size"`
	LastModified string `json:"lastModified"` // ISO timestamp
}

// ListBackups lists backup files for a specific table from S3
// backupType: "delta" searches for {tableName}_delta-, "main" searches for {tableName}_main_
func (c *Client) ListBackups(ctx context.Context, prefix, tableName, backupType string) ([]BackupFile, error) {
	// Ensure prefix doesn't have trailing slash
	prefix = strings.TrimSuffix(prefix, "/")

	// Build the search prefix based on backup type
	var searchPrefix string
	if backupType == "main" {
		searchPrefix = fmt.Sprintf("%s/%s_main_", prefix, tableName)
	} else {
		searchPrefix = fmt.Sprintf("%s/%s_delta-", prefix, tableName)
	}

	paginator := s3.NewListObjectsV2Paginator(c.s3Client, &s3.ListObjectsV2Input{
		Bucket: aws.String(c.bucket),
		Prefix: aws.String(searchPrefix),
	})

	var backups []BackupFile
	for paginator.HasMorePages() {
		output, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list objects: %w", err)
		}

		for _, obj := range output.Contents {
			// Extract filename from key
			filename := *obj.Key
			if idx := strings.LastIndex(filename, "/"); idx != -1 {
				filename = filename[idx+1:]
			}

			// Only include .tar.gz files (physical backups from manticore-backup)
			if !strings.HasSuffix(filename, ".tar.gz") {
				continue
			}

			backups = append(backups, BackupFile{
				Filename:     filename,
				Key:          *obj.Key,
				Size:         *obj.Size,
				LastModified: obj.LastModified.UTC().Format("2006-01-02T15:04:05Z"),
			})
		}
	}

	// Sort by LastModified descending (newest first)
	for i := 0; i < len(backups)-1; i++ {
		for j := i + 1; j < len(backups); j++ {
			if backups[i].LastModified < backups[j].LastModified {
				backups[i], backups[j] = backups[j], backups[i]
			}
		}
	}

	return backups, nil
}
