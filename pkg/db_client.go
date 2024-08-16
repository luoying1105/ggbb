package pkg

import (
	bolt "go.etcd.io/bbolt"
	"strconv"
	"time"
)

type DBClient struct {
	db     *bolt.DB
	dbPath string
}

var _ DBClientInterface = &DBClient{}

// NewDBClient initializes a new DBClient.
func NewDBClient(dbPath string) (*DBClient, error) {
	db, err := bolt.Open(dbPath, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, ErrFailedToCreate
	}

	return &DBClient{db: db, dbPath: dbPath}, nil
}

// CreateBucket creates a bucket if it does not already exist.
func (client *DBClient) CreateBucket(bucketName string) error {
	return client.update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		if err != nil {
			return ErrBucketNotFound
		}
		return nil
	})
}

// Put stores a key-value pair in a specified bucket.
func (client *DBClient) Put(bucketName, key, value string) error {
	return client.update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketName))
		if bucket == nil {
			return ErrBucketNotFound
		}
		return bucket.Put([]byte(key), []byte(value))
	})
}

// Get retrieves a value for a given key from a specified bucket.
func (client *DBClient) Get(bucketName, key string) ([]byte, error) {
	var value []byte
	err := client.view(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketName))
		if bucket == nil {
			return ErrBucketNotFound
		}
		value = bucket.Get([]byte(key))
		if value == nil {
			return ErrKeyNotFound
		}
		return nil
	})
	return value, err
}

// GetAll retrieves all key-value pairs from a specified bucket.
func (client *DBClient) GetAll(bucketName string) ([]*KeyValue, error) {
	var results []*KeyValue
	err := client.view(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketName))
		if bucket == nil {
			return ErrBucketNotFound
		}
		return bucket.ForEach(func(k, v []byte) error {
			results = append(results, &KeyValue{Key: string(k), Value: v})
			return nil
		})
	})
	return results, err
}

// PutWithAutoIncrementKey stores a value with an auto-incremented key in a specified bucket.
func (client *DBClient) PutWithAutoIncrementKey(bucketName string, value []byte) error {
	return client.update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketName))
		if bucket == nil {
			return ErrBucketNotFound
		}

		seq, err := bucket.NextSequence()
		if err != nil {
			return ErrFailedToCreate
		}

		key := strconv.FormatUint(seq, 10)
		return bucket.Put([]byte(key), value)
	})
}

// GetNext retrieves the next key-value pair based on the progress in a specified bucket.
func (client *DBClient) GetNext(bucketName string, progress int64) (*KeyValue, error) {
	var result *KeyValue
	err := client.view(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketName))
		if bucket == nil {
			return ErrBucketNotFound
		}

		cursor := bucket.Cursor()
		var key, value []byte

		for i := int64(0); i <= progress; i++ {
			if i == 0 {
				key, value = cursor.First()
			} else {
				key, value = cursor.Next()
			}
			if key == nil {
				return ErrNoMoreMessages
			}
		}

		result = &KeyValue{Key: string(key), Value: value}
		return nil
	})
	return result, err
}

//func (client *DBClient) CleanupAllConsumed() error {
//	err := client.update(func(tx *bolt.Tx) error {
//		// 遍历数据库中的所有 bucket
//		err := tx.ForEach(func(bucketName []byte, _ *bolt.Bucket) error {
//			// 获取当前 bucket 的消费进度
//			progressValue := tx.Bucket([]byte(consumerProgressBucket)).Get(bucketName)
//			if progressValue == nil {
//				Logger.Info(fmt.Sprintf("Skipping bucket: %s, no progress recorded", bucketName))
//				return nil // 如果没有进度记录，跳过这个 bucket
//			}
//
//			progress, err := strconv.ParseInt(string(progressValue), 10, 64)
//			if err != nil {
//				Logger.Error("Invalid progress value", fmt.Sprintf("bucketName %s %s", string(bucketName)), err)
//				return ErrInvalidProgress
//			}
//			Logger.Debug("Cleaning up consumed messages", fmt.Sprintf("bucketName %s progress %d", string(bucketName), progress))
//
//			// 清理已消费的消息
//			err = client.cleanupBucket(tx, string(bucketName), progress)
//			if err != nil {
//				return err
//			}
//			return nil
//		})
//		return err
//	})
//
//	if err != nil {
//		Logger.Error("Failed to cleanup consumed messages", err)
//		return err
//	}
//
//	err = client.backupAndReopen()
//	if err != nil {
//		Logger.Error("Failed to backup and reopen the database", err)
//	}
//	return err
//}
//
//func (client *DBClient) cleanupBucket(tx *bolt.Tx, bucketName string, progress int64) error {
//	bucket := tx.Bucket([]byte(bucketName))
//	if bucket == nil {
//		Logger.Error(fmt.Sprintf("Bucket not found: %s", bucketName))
//		return ErrBucketNotFound
//	}
//
//	cursor := bucket.Cursor()
//	if cursor == nil {
//		Logger.Info(fmt.Sprintf("Bucket is empty, nothing to clean: %s", bucketName))
//		return nil
//	}
//
//	for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
//		keyInt, err := strconv.ParseUint(string(k), 10, 64)
//		if err != nil {
//			Logger.Error("Invalid progress value", fmt.Sprintf("bucketName %s %s", string(bucketName)), err)
//			return ErrInvalidProgress
//		}
//
//		if keyInt <= uint64(progress) {
//			if err := bucket.Delete(k); err != nil {
//				Logger.Error(fmt.Sprintf("Failed to delete key: %s in bucket: %s", k, bucketName))
//				return ErrFailedToDelete
//			}
//			Logger.Debug(fmt.Sprintf("Deleted key: %s in bucket: %s", k, bucketName))
//		}
//	}
//	return nil
//}

// backupAndReopen backs up the current database and reopens it.

// Close closes the database connection.
func (client *DBClient) Close() error {
	return client.db.Close()
}

func (client *DBClient) view(fn func(*bolt.Tx) error) error {
	return client.db.View(fn)
}

func (client *DBClient) update(fn func(*bolt.Tx) error) error {
	return client.db.Update(fn)
}
