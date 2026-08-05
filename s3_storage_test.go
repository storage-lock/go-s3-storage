package s3_storage

import (
	"context"
	storage_test_helper "github.com/storage-lock/go-storage-test-helper"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/assert"
	"os"
	"os/exec"
	"testing"
	"time"
)

var minioCmd *exec.Cmd

func TestMain(m *testing.M) {
	envName := "STORAGE_LOCK_S3_ENDPOINT"
	if os.Getenv(envName) != "" {
		os.Exit(m.Run())
	}

	// 尝试用 Docker 启动 MinIO
	port := "9000"
	consolePort := "9001"
	minioCmd = exec.Command("docker", "run", "--rm",
		"-p", port+":9000",
		"-p", consolePort+":9001",
		"-e", "MINIO_ROOT_USER=minioadmin",
		"-e", "MINIO_ROOT_PASSWORD=minioadmin",
		"--name", "s3-storage-test",
		"minio/minio:RELEASE.2024-05-28T17-19-04Z", "server", "/data", "--console-address", ":9001",
	)
	if err := minioCmd.Start(); err != nil {
		os.Exit(0)
	}

	// 等待 MinIO 就绪
	time.Sleep(time.Second * 5)
	os.Setenv("STORAGE_LOCK_S3_ENDPOINT", "localhost:"+port)
	os.Setenv("STORAGE_LOCK_S3_ACCESS_KEY", "minioadmin")
	os.Setenv("STORAGE_LOCK_S3_SECRET_KEY", "minioadmin")
	os.Setenv("STORAGE_LOCK_S3_BUCKET", "storage-lock-test")
	exitCode := m.Run()

	minioCmd.Process.Kill()
	minioCmd.Wait()
	exec.Command("docker", "rm", "-f", "s3-storage-test").Run()
	os.Exit(exitCode)
}

func TestS3Storage(t *testing.T) {
	endpoint := os.Getenv("STORAGE_LOCK_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("STORAGE_LOCK_S3_ENDPOINT not set and MinIO not available")
	}

	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4("minioadmin", "minioadmin", ""),
		Secure: false,
	})
	assert.Nil(t, err)

	// 确保 bucket 存在
	bucket := "storage-lock-test"
	ctx := context.Background()
	exists, err := client.BucketExists(ctx, bucket)
	assert.Nil(t, err)
	if !exists {
		err = client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{})
		assert.Nil(t, err)
	}
	// 清理旧数据（S3 不支持条件删除，必须先无条件清理）
	prefix := storage_test_helper.TestTableName + "/lock/"
	ch := client.ListObjects(ctx, bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true})
	for obj := range ch {
		if obj.Err != nil {
			continue
		}
		client.RemoveObject(ctx, bucket, obj.Key, minio.RemoveObjectOptions{})
	}

	cm := NewS3ConnectionManagerFromClient(client)
	s, err := NewS3Storage(ctx, &S3StorageOptions{
		ConnectionManager: cm,
		BucketName:        bucket,
		KeyPrefix:         storage_test_helper.TestTableName,
		TimeProvider:      &LocalTimeProvider{},
	})
	assert.Nil(t, err)
	storage_test_helper.TestStorage(t, s)
}