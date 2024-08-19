package bunnymq

import (
	bolt "go.etcd.io/bbolt"
	"strconv"
	"sync"
	"time"
)

var globalDBClient *DBClient
var dbClientOnce sync.Once

func initDBClient() error {
	var err error
	dbClientOnce.Do(func() {
		globalDBClient, err = NewDBClient(dbName)

	})
	return err
}

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
func (client *DBClient) Put(bucketName, key string, value []byte) error {
	return client.update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketName))
		if bucket == nil {
			return ErrBucketNotFound
		}
		return bucket.Put([]byte(key), value)
	})
}

func (client *DBClient) GetFirst(bucketName string) (*KeyValue, error) {
	var result *KeyValue
	err := client.view(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketName))
		if bucket == nil {
			return ErrBucketNotFound
		}
		cursor := bucket.Cursor()
		key, value := cursor.First()
		if key == nil {
			return ErrKeyNotFound
		}
		result = &KeyValue{Key: string(key), Value: value}
		return nil
	})
	return result, err
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

func (client *DBClient) Delete(bucketName, key string) error {
	return client.update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketName))
		if bucket == nil {
			return ErrBucketNotFound
		}
		return bucket.Delete([]byte(key))
	})
}
