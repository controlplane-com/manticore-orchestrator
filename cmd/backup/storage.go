package main

import (
	"context"
	"fmt"
	"io"

	"cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// StorageClient abstracts cloud storage operations for backup/restore
type StorageClient interface {
	Upload(ctx context.Context, key string, r io.Reader) error
	Download(ctx context.Context, key string) (io.ReadCloser, error)
}

// S3Client implements StorageClient for AWS S3
type S3Client struct {
	client   *s3.Client
	uploader *manager.Uploader
	bucket   string
}

// NewS3Client creates a new S3 storage client
func NewS3Client(bucket, region string) (*S3Client, error) {
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion(region),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	client := s3.NewFromConfig(cfg)
	return &S3Client{
		client:   client,
		uploader: manager.NewUploader(client),
		bucket:   bucket,
	}, nil
}

func (c *S3Client) Upload(ctx context.Context, key string, r io.Reader) error {
	_, err := c.uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
		Body:   r,
	})
	if err != nil {
		return fmt.Errorf("s3 upload failed: %w", err)
	}
	return nil
}

func (c *S3Client) Download(ctx context.Context, key string) (io.ReadCloser, error) {
	result, err := c.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("s3 download failed: %w", err)
	}
	return result.Body, nil
}

// GCSClient implements StorageClient for Google Cloud Storage
type GCSClient struct {
	client *storage.Client
	bucket string
}

// NewGCSClient creates a new GCS storage client
func NewGCSClient(bucket string) (*GCSClient, error) {
	client, err := storage.NewClient(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}
	return &GCSClient{
		client: client,
		bucket: bucket,
	}, nil
}

func (c *GCSClient) Upload(ctx context.Context, key string, r io.Reader) error {
	w := c.client.Bucket(c.bucket).Object(key).NewWriter(ctx)
	if _, err := io.Copy(w, r); err != nil {
		w.Close()
		return fmt.Errorf("gcs upload failed: %w", err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("gcs upload close failed: %w", err)
	}
	return nil
}

func (c *GCSClient) Download(ctx context.Context, key string) (io.ReadCloser, error) {
	r, err := c.client.Bucket(c.bucket).Object(key).NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("gcs download failed: %w", err)
	}
	return r, nil
}

// NewStorageClient creates the appropriate storage client based on provider
func NewStorageClient(provider, bucket, region string) (StorageClient, error) {
	switch provider {
	case "aws":
		return NewS3Client(bucket, region)
	case "gcp":
		return NewGCSClient(bucket)
	default:
		return nil, fmt.Errorf("unsupported backup provider: %s", provider)
	}
}
