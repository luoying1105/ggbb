package pkg

import (
	"fmt"
	bolt "go.etcd.io/bbolt"
	"os"
	"strconv"
	"time"
)

type DBClient struct {
	db *bolt.DB
}

var _ DBClientInterface = &DBClient{}

func NewDBClient(dbPath string) (*DBClient, error) {
	db, err := bolt.Open(dbPath, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, NewDBError(err, "opening database")
	}
	return &DBClient{db: db}, nil
}

func (client *DBClient) CreateBucket(bucketName string) error {
	return client.update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		if err != nil {
			return NewDBError(err, fmt.Sprintf("creating bucket %s", bucketName))
		}
		return nil
	})
}

func (client *DBClient) Put(bucketName, key, value string) error {
	return client.updateBucket(bucketName, func(bucket *bolt.Bucket) error {
		return bucket.Put([]byte(key), []byte(value))
	})
}

func (client *DBClient) Get(bucketName, key string) ([]byte, error) {
	var value []byte
	err := client.viewBucket(bucketName, func(bucket *bolt.Bucket) error {
		value = bucket.Get([]byte(key))
		if value == nil {
			return NewDBError(ErrKeyNotFound, fmt.Sprintf("key %s in bucket %s", key, bucketName))
		}
		return nil
	})
	return value, err
}

func (client *DBClient) GetAll(bucketName string) ([]*KeyValue, error) {
	var results []*KeyValue
	err := client.viewBucket(bucketName, func(bucket *bolt.Bucket) error {
		return bucket.ForEach(func(k, v []byte) error {
			results = append(results, &KeyValue{Key: string(k), Value: v})
			return nil
		})
	})
	return results, err
}

func (client *DBClient) PutWithAutoIncrementKey(bucketName string, value []byte) error {
	return client.updateBucket(bucketName, func(bucket *bolt.Bucket) error {
		seq, err := bucket.NextSequence()
		if err != nil {
			return NewDBError(err, fmt.Sprintf("getting next sequence for bucket %s", bucketName))
		}
		key := strconv.FormatUint(seq, 10)
		return bucket.Put([]byte(key), value)
	})
}

func (client *DBClient) GetNext(bucketName string, progress int64) (*KeyValue, error) {
	var result *KeyValue
	err := client.viewBucket(bucketName, func(bucket *bolt.Bucket) error {
		cursor := bucket.Cursor()
		var key, value []byte

		for i := int64(0); i <= progress; i++ {
			if i == 0 {
				key, value = cursor.First()
			} else {
				key, value = cursor.Next()
			}
			if key == nil {
				return NewDBError(ErrKeyNotFound, fmt.Sprintf("no more keys found in bucket %s after progress %d", bucketName, i-1))
			}
		}

		result = &KeyValue{Key: string(key), Value: value}
		return nil
	})
	return result, err
}

func (client *DBClient) CleanupAllConsumed() error {
	err := client.update(func(tx *bolt.Tx) error {
		progressBucket := tx.Bucket([]byte(consumerProgressBucket))
		if progressBucket == nil {
			return NewDBError(ErrBucketNotFound, consumerProgressBucket)
		}

		cursor := progressBucket.Cursor()
		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			progress, err := strconv.ParseInt(string(v), 10, 64)
			if err != nil {
				return NewDBError(err, fmt.Sprintf("parsing progress for bucket %s", k))
			}

			err = client.cleanupBucket(tx, string(k), progress)
			if err != nil {
				return err
			}
		}
		return nil
	})

	if err == nil {
		err = client.backupAndReopen()
	}
	return err
}

func (client *DBClient) cleanupBucket(tx *bolt.Tx, bucketName string, progress int64) error {
	bucket := tx.Bucket([]byte(bucketName))
	if bucket == nil {
		return NewDBError(ErrBucketNotFound, bucketName)
	}

	cursor := bucket.Cursor()
	for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
		keyInt, err := strconv.ParseUint(string(k), 10, 64)
		if err != nil {
			return NewDBError(err, fmt.Sprintf("parsing key in bucket %s", bucketName))
		}

		if keyInt <= uint64(progress) {
			if err := bucket.Delete(k); err != nil {
				return NewDBError(err, fmt.Sprintf("deleting key in bucket %s", bucketName))
			}
		}
	}
	return nil
}

func (client *DBClient) backupAndReopen() error {
	client.db.Close()

	backupPath := client.db.Path() + ".bak"
	err := os.Rename(client.db.Path(), backupPath)
	if err != nil {
		return NewDBError(err, "renaming database file")
	}

	db, err := bolt.Open(client.db.Path(), 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		_ = os.Rename(backupPath, client.db.Path()) // 尝试恢复备份
		return NewDBError(err, "reopening database")
	}
	client.db = db

	return client.rebuildDatabase(backupPath)
}

func (client *DBClient) rebuildDatabase(backupPath string) error {
	err := client.db.Update(func(tx *bolt.Tx) error {
		backupDB, err := bolt.Open(backupPath, 0600, nil)
		if err != nil {
			return NewDBError(err, "opening backup database")
		}
		defer backupDB.Close()

		err = backupDB.View(func(backupTx *bolt.Tx) error {
			return backupTx.ForEach(func(name []byte, bucket *bolt.Bucket) error {
				newBucket, err := tx.CreateBucketIfNotExists(name)
				if err != nil {
					return NewDBError(err, fmt.Sprintf("creating bucket %s", name))
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

func (client *DBClient) viewBucket(bucketName string, fn func(*bolt.Bucket) error) error {
	return client.view(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketName))
		if bucket == nil {
			return NewDBError(ErrBucketNotFound, bucketName)
		}
		return fn(bucket)
	})
}

func (client *DBClient) updateBucket(bucketName string, fn func(*bolt.Bucket) error) error {
	return client.update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketName))
		if bucket == nil {
			return NewDBError(ErrBucketNotFound, bucketName)
		}
		return fn(bucket)
	})
}
