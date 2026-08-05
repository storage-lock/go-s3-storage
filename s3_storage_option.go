package s3_storage

import (
	"github.com/minio/minio-go/v7"
	"github.com/storage-lock/go-storage"
)

// S3StorageOptions S3 兼容存储的选项
type S3StorageOptions struct {
	// ConnectionManager 连接管理器
	ConnectionManager storage.ConnectionManager[*minio.Client]
	// BucketName 存储锁对象的 Bucket
	BucketName string
	// KeyPrefix 对象 key 前缀，实际 key = <KeyPrefix>/lock/<lockId>
	KeyPrefix string
	// Region S3 区域（用于 MakeBucket，可选）
	Region string
	// TimeProvider 可选时间源（对象存储无服务端时钟，必须注入）
	TimeProvider storage.TimeProvider
}

func NewS3StorageOptions() *S3StorageOptions {
	return &S3StorageOptions{
		KeyPrefix: storage.DefaultStorageTableName,
	}
}

func (x *S3StorageOptions) SetConnectionManager(cm storage.ConnectionManager[*minio.Client]) *S3StorageOptions {
	x.ConnectionManager = cm
	return x
}

func (x *S3StorageOptions) SetBucketName(bucketName string) *S3StorageOptions {
	x.BucketName = bucketName
	return x
}

func (x *S3StorageOptions) SetKeyPrefix(keyPrefix string) *S3StorageOptions {
	x.KeyPrefix = keyPrefix
	return x
}

func (x *S3StorageOptions) SetRegion(region string) *S3StorageOptions {
	x.Region = region
	return x
}

func (x *S3StorageOptions) SetTimeProvider(tp storage.TimeProvider) *S3StorageOptions {
	x.TimeProvider = tp
	return x
}

func (x *S3StorageOptions) Check() error {
	if x.KeyPrefix == "" {
		return ErrKeyPrefixEmpty
	}
	if x.BucketName == "" {
		return ErrBucketNameEmpty
	}
	if x.ConnectionManager == nil {
		return ErrConnectionManagerNil
	}
	return nil
}