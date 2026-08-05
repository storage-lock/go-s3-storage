package s3_storage

import (
	"context"
	"fmt"
	"sync"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/storage-lock/go-storage"
)

// S3ConnectionManager 管理 S3 兼容客户端连接
// 复用于 S3、MinIO、OSS、COS、BOS 等所有 S3 协议介质
type S3ConnectionManager struct {
	// Endpoint S3 兼容服务的端点
	Endpoint string
	// AccessKey 访问密钥
	AccessKey string
	// SecretKey 密钥
	SecretKey string
	// UseSSL 是否使用 HTTPS
	UseSSL bool
	// Region 区域
	Region string

	// presetClient 预设客户端（FromClient 构造时使用）
	presetClient *minio.Client

	initMu      sync.Mutex
	initialized bool
	client      *minio.Client
}

var _ storage.ConnectionManager[*minio.Client] = &S3ConnectionManager{}

// NewS3ConnectionManagerFromEndpoint 从端点参数创建连接管理器
func NewS3ConnectionManagerFromEndpoint(endpoint, accessKey, secretKey string, useSSL bool) *S3ConnectionManager {
	return &S3ConnectionManager{
		Endpoint:  endpoint,
		AccessKey: accessKey,
		SecretKey: secretKey,
		UseSSL:    useSSL,
	}
}

// NewS3ConnectionManagerFromClient 从已有客户端创建连接管理器
func NewS3ConnectionManagerFromClient(client *minio.Client) *S3ConnectionManager {
	return &S3ConnectionManager{
		presetClient: client,
		initialized:  true,
		client:       client,
	}
}

func (x *S3ConnectionManager) Name() string {
	return "s3-connection-manager"
}

func (x *S3ConnectionManager) Take(ctx context.Context) (*minio.Client, error) {
	x.initMu.Lock()
	defer x.initMu.Unlock()

	if x.initialized {
		return x.client, nil
	}

	client, err := minio.New(x.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(x.AccessKey, x.SecretKey, ""),
		Secure: x.UseSSL,
		Region: x.Region,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create S3 client: %w", err)
	}

	x.client = client
	x.initialized = true
	return x.client, nil
}

func (x *S3ConnectionManager) Return(ctx context.Context, connection *minio.Client) error {
	return nil
}

func (x *S3ConnectionManager) Shutdown(ctx context.Context) error {
	x.initMu.Lock()
	defer x.initMu.Unlock()
	x.client = nil
	x.initialized = false
	return nil
}