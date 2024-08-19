package bunnymq

import (
	"errors"
	"fmt"
	bolt "go.etcd.io/bbolt"
	"strconv"
	"time"
)

type dbClient struct {
	db     *bolt.DB
	dbPath string
}

// ErrTxTimeout is an error for transaction timeout
var ErrTxTimeout = errors.New("transaction timed out")

// newDBClient creates a new database client with an increased timeout for write transactions
func newDBClient(dbPath string) (*dbClient, error) {
	db, err := bolt.Open(dbPath, 0600, &bolt.Options{Timeout: 5 * time.Second}) // Increased timeout to 5 seconds
	if err != nil {
		return nil, ErrFailedToCreate
	}
	return &dbClient{db: db, dbPath: dbPath}, nil
}

// Put stores a key-value pair in a specified bucket with a retry mechanism
func (client *dbClient) Put(bucketName, key string, value []byte) error {
	return client.update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		if err != nil {
			return err
		}

		return bucket.Put([]byte(key), value)
	})
}

// GetAll retrieves all key-value pairs from a specified bucket
func (client *dbClient) GetAll(bucketName string) ([]*KeyValue, error) {
	var results []*KeyValue
	err := client.view(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		if err != nil {
			return err
		}
		return bucket.ForEach(func(k, v []byte) error {
			results = append(results, &KeyValue{Key: string(k), Value: v})
			return nil
		})
	})
	return results, err
}

func (client *dbClient) get(bucketName, key string) ([]byte, error) {
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

// PutWithAutoIncrementKey stores a value with an auto-incremented key in a specified bucket with retry mechanism
func (client *dbClient) PutWithAutoIncrementKey(bucketName string, value []byte) error {
	var lastErr error
	for i := 0; i < 3; i++ { // Retry mechanism for up to 3 attempts
		lastErr = client.update(func(tx *bolt.Tx) error {
			bucket, err := tx.CreateBucketIfNotExists([]byte(bucketName))
			if err != nil {
				return err
			}
			seq, err := bucket.NextSequence()
			if err != nil {
				return ErrFailedToCreate
			}
			key := strconv.FormatUint(seq, 10)
			return bucket.Put([]byte(key), value)
		})
		if lastErr == nil || !errors.Is(lastErr, ErrTxTimeout) {
			break
		}
		time.Sleep(100 * time.Millisecond) // Small delay before retrying
	}
	return lastErr
}

// GetNext retrieves the next key-value pair based on the progress in a specified bucket
func (client *dbClient) GetNext(bucketName, progress string) (*KeyValue, error) {
	var result *KeyValue
	err := client.view(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketName))
		if bucket == nil {
			return ErrBucketNotFound
		}
		fmt.Println("get bucket", bucketName, progress)
		// 使用 progress 作为键直接获取对应的值
		value := bucket.Get([]byte(progress))
		if value == nil {
			return ErrKeyNotFound
		}

		result = &KeyValue{Key: progress, Value: value}
		return nil
	})
	return result, err
}

// Close closes the database connection
func (client *dbClient) Close() error {
	return client.db.Close()
}

func (client *dbClient) view(fn func(*bolt.Tx) error) error {
	return client.db.View(fn)
}

func (client *dbClient) update(fn func(*bolt.Tx) error) error {
	tx, err := client.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func (client *dbClient) Delete(bucketName, key string) error {
	return client.update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketName))
		if bucket == nil {
			return ErrBucketNotFound
		}
		return bucket.Delete([]byte(key))
	})
}

func (client *dbClient) ensureBucketExists(bucketName string) error {
	return client.update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		if err != nil {
			return fmt.Errorf("failed to create or access bucket: %v", err)
		}
		return nil
	})
}
