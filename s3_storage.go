package s3_storage

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	go_iterator "github.com/golang-infrastructure/go-iterator"
	"github.com/storage-lock/go-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
)

// 存储在对象 value 中的分隔符
const valueSeparator = "\x00"

// isPreconditionError 检查是否为 S3 前置条件失败错误
// MinIO 错误消息: "At least one of the pre-conditions you specified did not hold"
func isPreconditionError(err error) bool {
	errStr := err.Error()
	return strings.Contains(errStr, "PreconditionFailed") ||
		strings.Contains(errStr, "412") ||
		strings.Contains(errStr, "pre-condition") ||
		strings.Contains(errStr, "Condition")
}

// S3Storage 基于 S3 协议的存储引擎实现
//
// CAS 范式：S3 条件 PUT
// - CreateWithVersion: PutObject + If-None-Match:*（对象不存在才写入）
// - UpdateWithVersion: 先 Get 取 ETag → PutObject + If-Match:<ETag>（ETag 匹配才覆盖）
//
// 注意：S3 Delete 不支持条件删除，因此不声明 CapabilityAtomicDelete，
// 释放路径由框架降级为写墓碑（LockCount=0）。这是框架漏洞 B 修复的真实对象存储验证。
//
// 时间源：S3 无服务端时钟，不声明 CapabilityReliableTime，
// 由 options.TimeProvider 注入（生产 NTP，测试 LocalTimeProvider）。
type S3Storage struct {
	options *S3StorageOptions
	client  *minio.Client

	timeProvider storage.TimeProvider
}

var _ storage.Storage = &S3Storage{}

// NewS3Storage 创建 S3 存储引擎
func NewS3Storage(ctx context.Context, options *S3StorageOptions) (*S3Storage, error) {
	if err := options.Check(); err != nil {
		return nil, err
	}
	s := &S3Storage{options: options}
	if err := s.Init(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

const S3StorageName = "s3-storage"

func (x *S3Storage) GetName() string {
	return S3StorageName
}

// Capabilities 声明 CAS 和 ReliableTime（通过注入 TimeProvider 满足）
// - ReliableTime：由外部 options.TimeProvider 满足（框架在 NewStorageLockWithOptions 检测）
// - AtomicDelete：S3 Delete 无条件删除，释放走墓碑降级
func (x *S3Storage) Capabilities() []storage.StorageCapability {
	return []storage.StorageCapability{
		storage.CapabilityCAS,
		storage.CapabilityReliableTime,
		storage.CapabilityWatch,
	}
}

func (x *S3Storage) Init(ctx context.Context) error {
	client, err := x.options.ConnectionManager.Take(ctx)
	if err != nil {
		return err
	}
	x.client = client

	// 确保 Bucket 存在
	exists, err := client.BucketExists(ctx, x.options.BucketName)
	if err != nil {
		return fmt.Errorf("failed to check bucket: %w", err)
	}
	if !exists {
		err = client.MakeBucket(ctx, x.options.BucketName, minio.MakeBucketOptions{Region: x.options.Region})
		if err != nil {
			return fmt.Errorf("failed to create bucket: %w", err)
		}
	}

	if x.options.TimeProvider != nil {
		x.timeProvider = x.options.TimeProvider
	} else {
		x.timeProvider = &LocalTimeProvider{}
	}
	return nil
}

// LocalTimeProvider 本地时间源（导出，供 locks 包注入）
type LocalTimeProvider struct{}

func (p *LocalTimeProvider) GetTime(ctx context.Context) (time.Time, error) {
	return time.Now(), nil
}

// objectKey 返回 S3 对象 key
func (x *S3Storage) objectKey(lockId string) string {
	return fmt.Sprintf("%s/lock/%s", x.options.KeyPrefix, lockId)
}

// encodeValue 编码 version 和 json
func encodeValue(version storage.Version, jsonStr string) []byte {
	return []byte(strconv.FormatUint(uint64(version), 10) + valueSeparator + jsonStr)
}

// decodeValue 解码 version 和 json
func decodeValue(data []byte) (storage.Version, string, error) {
	parts := strings.SplitN(string(data), valueSeparator, 2)
	if len(parts) != 2 {
		return 0, "", fmt.Errorf("invalid s3 value format")
	}
	version, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return 0, "", err
	}
	return storage.Version(version), parts[1], nil
}

// CreateWithVersion 原子创建锁记录
// S3 PutObject + If-None-Match:*：对象不存在才写入，否则返回 ErrEntityTooLarge/已存在错误
func (x *S3Storage) CreateWithVersion(ctx context.Context, lockId string, version storage.Version, lockInformation *storage.LockInformation) error {
	key := x.objectKey(lockId)
	value := encodeValue(version, lockInformation.ToJsonString())

	// 使用 SetMatchETagExcept 设置 If-None-Match: * 条件
	opts := minio.PutObjectOptions{}
	opts.SetMatchETagExcept("*") // 仅当对象不存在时写入
	_, err := x.client.PutObject(ctx, x.options.BucketName, key, strings.NewReader(string(value)), int64(len(value)), opts)
	if err != nil {
		// S3 在 If-None-Match:* 失败时返回 PreconditionFailed 错误
		// MinIO 错误消息: "At least one of the pre-conditions you specified did not hold"
		if isPreconditionError(err) {
			return storage_lock.ErrVersionMiss
		}
		return err
	}
	return nil
}

// UpdateWithVersion 原子更新锁记录
// 流程：Get 取 ETag → 检查业务 version → PutObject + If-Match:<ETag>
func (x *S3Storage) UpdateWithVersion(ctx context.Context, lockId string, exceptedVersion, newVersion storage.Version, lockInformation *storage.LockInformation) error {
	key := x.objectKey(lockId)

	// 取当前对象和 ETag
	obj, err := x.client.GetObject(ctx, x.options.BucketName, key, minio.GetObjectOptions{})
	if err != nil {
		errStr := err.Error()
		if strings.Contains(errStr, "NoSuchKey") || strings.Contains(errStr, "404") || strings.Contains(errStr, "does not exist") {
			// 记录不存在时必须返回 ErrVersionMiss 而不是 ErrLockNotFound：
			// 核心层只把 ErrVersionMiss 当可重试（抢占路径），NotFound 是硬错误。
			// 对齐 memory/etcd/zookeeper 已修口径。
			return storage_lock.ErrVersionMiss
		}
		return err
	}
	defer obj.Close()
	data, err := io.ReadAll(obj)
	if err != nil {
		return err
	}
	stat, err := obj.Stat()
	if err != nil {
		if strings.Contains(err.Error(), "NoSuchKey") || strings.Contains(err.Error(), "404") {
			return storage_lock.ErrVersionMiss
		}
		return err
	}

	// 解析当前业务版本
	currentVersion, _, err := decodeValue(data)
	if err != nil {
		return err
	}
	if currentVersion != exceptedVersion {
		return storage_lock.ErrVersionMiss
	}

	// 用 ETag 作 CAS 条件覆盖
	newValue := encodeValue(newVersion, lockInformation.ToJsonString())
	opts := minio.PutObjectOptions{}
	opts.SetMatchETag(stat.ETag) // 仅当 ETag 匹配时覆盖
	_, err = x.client.PutObject(ctx, x.options.BucketName, key, strings.NewReader(string(newValue)), int64(len(newValue)), opts)
	if err != nil {
		if isPreconditionError(err) {
			return storage_lock.ErrVersionMiss
		}
		return err
	}
	return nil
}

// DeleteWithVersion S3 不支持条件删除，采用"读取-检查-无条件删除"降级方案
// 注意：这不是原子操作，检查和删除之间可能有并发修改
// 但这是 S3 能做到的最佳方案，用于测试场景和清理操作
func (x *S3Storage) DeleteWithVersion(ctx context.Context, lockId string, exceptedVersion storage.Version, lockInformation *storage.LockInformation) error {
	key := x.objectKey(lockId)

	// 取当前对象
	obj, err := x.client.GetObject(ctx, x.options.BucketName, key, minio.GetObjectOptions{})
	if err != nil {
		errStr := err.Error()
		if strings.Contains(errStr, "NoSuchKey") || strings.Contains(errStr, "404") || strings.Contains(errStr, "does not exist") {
			return storage_lock.ErrLockNotFound
		}
		return err
	}
	defer obj.Close()
	data, err := io.ReadAll(obj)
	if err != nil {
		return err
	}

	// 检查版本是否匹配
	currentVersion, _, err := decodeValue(data)
	if err != nil {
		return err
	}
	if currentVersion != exceptedVersion {
		return storage_lock.ErrVersionMiss
	}

	// 版本匹配，执行删除（注意：这不是原子的）
	return x.client.RemoveObject(ctx, x.options.BucketName, key, minio.RemoveObjectOptions{})
}

// Get 获取锁信息
func (x *S3Storage) Get(ctx context.Context, lockId string) (string, error) {
	key := x.objectKey(lockId)
	obj, err := x.client.GetObject(ctx, x.options.BucketName, key, minio.GetObjectOptions{})
	if err != nil {
		errStr := err.Error()
		if strings.Contains(errStr, "NoSuchKey") || strings.Contains(errStr, "404") || strings.Contains(errStr, "does not exist") {
			return "", storage_lock.ErrLockNotFound
		}
		return "", err
	}
	defer obj.Close()
	data, err := io.ReadAll(obj)
	if err != nil {
		errStr := err.Error()
		if strings.Contains(errStr, "NoSuchKey") || strings.Contains(errStr, "404") || strings.Contains(errStr, "does not exist") {
			return "", storage_lock.ErrLockNotFound
		}
		return "", err
	}
	if len(data) == 0 {
		return "", storage_lock.ErrLockNotFound
	}
	_, jsonStr, err := decodeValue(data)
	if err != nil {
		return "", err
	}
	return jsonStr, nil
}

// GetTime 返回注入的时间源
func (x *S3Storage) GetTime(ctx context.Context) (time.Time, error) {
	return x.timeProvider.GetTime(ctx)
}

// Close 仅清空引用，连接由 ConnectionManager 管理
func (x *S3Storage) Close(ctx context.Context) error {
	x.client = nil
	return nil
}

// List 通过 ListObjects 枚举 <prefix>/lock/ 前缀下的所有对象
func (x *S3Storage) List(ctx context.Context) (go_iterator.Iterator[*storage.LockInformation], error) {
	prefix := fmt.Sprintf("%s/lock/", x.options.KeyPrefix)
	slice := make([]*storage.LockInformation, 0)

	objectCh := x.client.ListObjects(ctx, x.options.BucketName, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	})
	for object := range objectCh {
		if object.Err != nil {
			return nil, object.Err
		}
		obj, err := x.client.GetObject(ctx, x.options.BucketName, object.Key, minio.GetObjectOptions{})
		if err != nil {
			continue
		}
		data, err := io.ReadAll(obj)
		obj.Close()
		if err != nil {
			continue
		}
		if len(data) == 0 {
			continue
		}
		_, jsonStr, err := decodeValue(data)
		if err != nil {
			continue
		}
		lockInfo, err := storage.LockInformationFromJsonString(jsonStr)
		if err != nil {
			continue
		}
		slice = append(slice, lockInfo)
	}
	return go_iterator.FromSlice(slice), nil
}

func (x *S3Storage) Watch(ctx context.Context, lockId string) (<-chan *storage.LockInformation, error) {
	return storage.NewPollingLockWatcher(x, 500*time.Millisecond).Watch(ctx, lockId)
}