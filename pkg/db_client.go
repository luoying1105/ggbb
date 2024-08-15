package pkg

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	bolt "go.etcd.io/bbolt"
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

func (client *DBClient) CleanupAllConsumed() error {
	err := client.update(func(tx *bolt.Tx) error {
		// 遍历数据库中的所有 bucket
		err := tx.ForEach(func(bucketName []byte, _ *bolt.Bucket) error {
			// 如果是进度记录的 bucket，跳过
			if string(bucketName) == consumerProgressBucket {
				return nil
			}

			// 获取当前 bucket 的消费进度
			progressValue := tx.Bucket([]byte(consumerProgressBucket)).Get(bucketName)
			if progressValue == nil {
				return nil // 如果没有进度记录，跳过这个 bucket
			}

			progress, err := strconv.ParseInt(string(progressValue), 10, 64)
			if err != nil {
				return ErrInvalidProgress
			}

			// 清理已消费的消息
			err = client.cleanupBucket(tx, string(bucketName), progress)
			if err != nil {
				return err
			}
			return nil
		})

		return err
	})

	if err == nil {
		err = client.backupAndReopen()
	}
	return err
}

// cleanupBucket removes all keys that have been consumed from a specified bucket.
func (client *DBClient) cleanupBucket(tx *bolt.Tx, bucketName string, progress int64) error {
	bucket := tx.Bucket([]byte(bucketName))
	if bucket == nil {
		return ErrBucketNotFound
	}

	cursor := bucket.Cursor()
	for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
		keyInt, err := strconv.ParseUint(string(k), 10, 64)
		if err != nil {
			return ErrInvalidProgress
		}

		if keyInt <= uint64(progress) {
			if err := bucket.Delete(k); err != nil {
				return ErrFailedToDelete
			}
		}
	}
	return nil
}

// backupAndReopen backs up the current database and reopens it.
func (client *DBClient) backupAndReopen() error {
	// 确保数据库正确关闭
	err := client.db.Close()
	if err != nil {
		return NewDBError(CodeClosingDatabase, err, "closing database")
	}

	// 获取数据库路径
	dbPath := client.dbPath

	// 确保路径存在
	dir := filepath.Dir(dbPath)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return NewDBError(CodeRenamingDatabaseFile, fmt.Errorf("database directory does not exist: %s", dir), "checking database directory")
	}

	// 生成备份文件路径
	backupPath := dbPath + ".bak"

	// 确保没有同名文件存在
	_, err = os.Stat(backupPath)
	if !os.IsNotExist(err) {
		// 如果存在，则先删除旧的备份文件
		err = os.Remove(backupPath)
		if err != nil {
			return NewDBError(CodeDeletingBackupFile, err, "deleting existing backup file")
		}
	}

	// 尝试重命名数据库文件为备份文件
	err = os.Rename(dbPath, backupPath)
	if err != nil {
		return NewDBError(CodeRenamingDatabaseFile, err, "renaming database file")
	}

	// 重新打开数据库文件
	db, err := bolt.Open(dbPath, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		_ = os.Rename(backupPath, dbPath) // 尝试恢复备份
		return NewDBError(CodeReopeningDatabase, err, "reopening database")
	}
	client.db = db

	// 重建数据库
	err = client.rebuildDatabase(backupPath)
	if err != nil {
		return err
	}

	return nil
}

func (client *DBClient) rebuildDatabase(backupPath string) error {
	err := client.db.Update(func(tx *bolt.Tx) error {
		backupDB, err := bolt.Open(backupPath, 0600, nil)
		if err != nil {
			return ErrOpeningBackupDatabase
		}
		defer backupDB.Close()

		err = backupDB.View(func(backupTx *bolt.Tx) error {
			return backupTx.ForEach(func(name []byte, bucket *bolt.Bucket) error {
				newBucket, err := tx.CreateBucketIfNotExists(name)
				if err != nil {
					return ErrBucketNotFound
				}
				return bucket.ForEach(func(k, v []byte) error {
					return newBucket.Put(k, v)
				})
			})
		})
		return err
	})

	if err == nil {
		_ = os.Remove(backupPath)
	}

	return err
}

// Close closes the database connection.
func (client *DBClient) Close() error {
	return client.db.Close()
}

// 辅助方法

func (client *DBClient) view(fn func(*bolt.Tx) error) error {
	return client.db.View(fn)
}

func (client *DBClient) update(fn func(*bolt.Tx) error) error {
	return client.db.Update(fn)
}
