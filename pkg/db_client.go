package pkg

import (
	"fmt"
	bolt "go.etcd.io/bbolt"
	"strconv"
	"time"
)

type DBClient struct {
	db *bolt.DB
}
type DBClientInterface interface {
	CreateBucket(bucketName string) error
	Put(bucketName, key, value string) error
	Get(bucketName, key string) ([]byte, error)
	PutWithAutoIncrementKey(bucketName string, value []byte) error
	GetNext(bucketName string, progress int64) (*KeyValue, error)
	GetAll(bucketName string) ([]*KeyValue, error)
	Close() error
}

func NewDBClient(dbPath string) (*DBClient, error) {
	db, err := bolt.Open(dbPath, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, err
	}
	return &DBClient{db: db}, nil
}

func (client *DBClient) CreateBucket(bucketName string) error {
	return client.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		return err
	})
}

func (client *DBClient) Put(bucketName, key, value string) error {
	return client.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketName))
		if bucket == nil {
			return &DBError{
				Err:     ErrBucketNotFound,
				Context: bucketName,
			}
		}
		return bucket.Put([]byte(key), []byte(value))
	})
}

func (client *DBClient) Get(bucketName, key string) ([]byte, error) {
	var value []byte
	err := client.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketName))
		if bucket == nil {
			return &DBError{
				Err:     ErrBucketNotFound,
				Context: bucketName,
			}
		}
		value = bucket.Get([]byte(key))
		return nil
	})
	return value, err
}

func (client *DBClient) GetAll(bucketName string) ([]*KeyValue, error) {
	var results []*KeyValue

	err := client.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketName))
		if bucket == nil {
			return &DBError{
				Err:     ErrBucketNotFound,
				Context: bucketName,
			}
		}

		cursor := bucket.Cursor()

		for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
			results = append(results, &KeyValue{
				Key:   string(key),
				Value: value,
			})
		}

		return nil
	})

	if err != nil {
		return nil, err
	}
	return results, nil
}

// PutWithAutoIncrementKey stores a value with an auto-incremented key in the specified bucket.
func (client *DBClient) PutWithAutoIncrementKey(bucketName string, value []byte) error {
	return client.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketName))
		if bucket == nil {
			return &DBError{
				Err:     ErrBucketNotFound,
				Context: bucketName,
			}
		}
		// 获取自增的消息ID
		seq, err := bucket.NextSequence()
		if err != nil {
			return err
		}
		messageID := strconv.FormatUint(seq, 10)
		return bucket.Put([]byte(messageID), value)
	})
}

func (client *DBClient) GetNext(bucketName string, progress int64) (*KeyValue, error) {
	var result *KeyValue

	err := client.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketName))
		if bucket == nil {
			return &DBError{
				Err:     ErrBucketNotFound,
				Context: bucketName,
			}
		}

		cursor := bucket.Cursor()
		var key, value []byte

		if progress == 0 {
			// Get the first key-value pair directly
			key, value = cursor.First()
		} else {
			// Move the cursor to the correct position by skipping 'progress' elements
			for i := int64(0); i <= progress; i++ {
				if i == 0 {
					// For the first iteration, call cursor.First() to start from the beginning
					key, value = cursor.First()
				} else {
					// For subsequent iterations, call cursor.Next()
					key, value = cursor.Next()
				}
				// If key is nil, we have reached the end
				if key == nil {
					return fmt.Errorf("no more keys found in bucket %s after progress %d", bucketName, i-1)
				}
			}
		}

		// Only set result if key and value are valid
		if key != nil && value != nil {
			result = &KeyValue{
				Key:   string(key),
				Value: value,
			}
		} else {
			return fmt.Errorf("failed to retrieve valid key-value pair from bucket %s", bucketName)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}
	return result, nil
}

func (client *DBClient) Close() error {
	return client.db.Close()
}
