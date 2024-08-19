package bunnymq

import (
	"fmt"
	bolt "go.etcd.io/bbolt"
	"os"
	"strconv"
	"time"
)

func (client *DBClient) isBucketConsumedByAll(tx *bolt.Tx, bucketName string) (bool, error) {
	progressBucket := tx.Bucket([]byte(consumerProgressBucket))
	bucket := tx.Bucket([]byte(bucketName))
	if bucket == nil {
		return false, ErrBucketNotFound
	}

	maxKey, _ := bucket.Cursor().Last()
	maxID, err := strconv.ParseInt(string(maxKey), 10, 64)
	if err != nil {
		return false, err
	}

	consumedByAll := true

	// 遍历所有消费者进度
	cursor := progressBucket.Cursor()
	for consumerID, progress := cursor.First(); consumerID != nil; consumerID, progress = cursor.Next() {
		progressInt, err := strconv.ParseInt(string(progress), 10, 64)
		if err != nil {
			return false, err
		}

		if progressInt < maxID {
			consumedByAll = false
			break
		}
	}

	return consumedByAll, nil
}

func (client *DBClient) CleanupAllConsumed() error {
	err := client.update(func(tx *bolt.Tx) error {
		return tx.ForEach(func(bucketName []byte, _ *bolt.Bucket) error {
			if string(bucketName) == consumerProgressBucket {
				return nil
			}
			consumedByAll, err := client.isBucketConsumedByAll(tx, string(bucketName))
			if err != nil {
				return err
			}

			if consumedByAll {
				//如果全部已经消费完毕就删掉整个bucket 创建个新的
				err = tx.DeleteBucket(bucketName)
				if err != nil {
					Logger.Error(fmt.Sprintf("Failed to delete fully consumed bucket: %s", bucketName))
					return err
				}
				_, err = tx.CreateBucket(bucketName) // 创建一个新的空 bucket
				if err != nil {
					Logger.Error(fmt.Sprintf("Failed to create new empty bucket: %s", bucketName))
					return err
				}
				Logger.Info(fmt.Sprintf("Bucket %s fully consumed, deleted, and recreated as empty.", bucketName))
			} else {
				//如果还未完全消费，则只删除当前已消费完成的消息，保留未消费的消息
				err = client.cleanupBucket(tx, string(bucketName))
				if err != nil {
					return err
				}
			}

			return nil
		})
	})

	if err == nil {
		err = client.backupAndReopen()
	}
	return err
}

func (client *DBClient) cleanupBucket(tx *bolt.Tx, bucketName string) error {
	bucket := tx.Bucket([]byte(bucketName))
	if bucket == nil {
		return ErrBucketNotFound
	}

	cursor := bucket.Cursor()
	Logger.Debug(fmt.Sprintf("Cleaning up bucket %s", bucketName))

	// 仅删除当前消费完成最小的一条消息
	_, v := cursor.First()
	if v != nil {
		if err := bucket.Delete([]byte(v)); err != nil {
			return ErrFailedToDelete
		}
		Logger.Debug(fmt.Sprintf("Deleted message with key: %s", string(v)))
	}

	return nil
}

func (client *DBClient) rebuildDatabase(backupPath string) error {
	defer client.recoverFromPanic()

	var backupDB *bolt.DB
	var err error

	err = client.db.Update(func(tx *bolt.Tx) error {
		backupDB, err = client.processBackup(tx, backupPath)
		return err
	})

	if backupDB != nil {
		defer func() {
			if cerr := backupDB.Close(); cerr != nil {
				Logger.Error(fmt.Sprintf("关闭备份数据库失败: %s", cerr))
			}
		}()
	}
	Logger.Debug("Database 完成更新")
	return err
}

func (client *DBClient) recoverFromPanic() {
	if r := recover(); r != nil {
		Logger.Error(fmt.Sprintf("重建数据库时发生崩溃: %v", r))
	}
}

func (client *DBClient) processBackup(tx *bolt.Tx, backupPath string) (*bolt.DB, error) {
	backupDB, err := bolt.Open(backupPath, 0600, nil)
	if err != nil {
		Logger.Error(fmt.Sprintf("打开备份数据库失败: %s", err))
		return nil, ErrOpeningBackupDatabase
	}

	err = backupDB.View(func(backupTx *bolt.Tx) error {
		return backupTx.ForEach(func(name []byte, bucket *bolt.Bucket) error {
			return client.restoreBucket(tx, name, bucket)
		})
	})
	if err != nil {
		Logger.Error(fmt.Sprintf("处理备份时出错: %s", err))
		return backupDB, err
	}

	Logger.Debug("备份处理完成")
	return backupDB, nil
}

func (client *DBClient) restoreBucket(tx *bolt.Tx, name []byte, bucket *bolt.Bucket) error {
	if bucket == nil {
		Logger.Warn(fmt.Sprintf("Bucket %s is empty, skipping.", name))
		return nil
	}

	newBucket, err := tx.CreateBucketIfNotExists(name)
	if err != nil {
		Logger.Error(fmt.Sprintf("创建Bucket失败: %s", err))
		return ErrBucketNotFound
	}

	cursor := bucket.Cursor()
	Logger.Debug(fmt.Sprintf("写入新Bucket: %s", name))
	for k, v := cursor.First(); k != nil && v != nil; k, v = cursor.Next() {
		if k == nil || v == nil {
			Logger.Warn("Nil key or value encountered, skipping")
			continue
		}
		err = newBucket.Put(k, v)
		if err != nil {
			Logger.Error(fmt.Sprintf("Failed to write to new bucket: %s, error: %v", name, err))
			return err
		}
	}
	return nil
}

func (client *DBClient) deleteBackup(backupPath string) error {
	Logger.Debug("删除备份")
	err := os.Remove(backupPath)
	if err != nil {
		Logger.Error(fmt.Sprintf("删除备份数据库失败: %s", err))
	}
	return err
}

func (client *DBClient) backupAndReopen() error {
	client.db.Close()

	backupPath := client.dbPath + ".bak"
	if err := os.Rename(client.dbPath, backupPath); err != nil {
		Logger.Error(fmt.Sprintf("Failed to rename database file: %v", err))
		return ErrRenamingDatabaseFile
	}

	db, err := bolt.Open(client.dbPath, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		_ = os.Rename(backupPath, client.dbPath)
		Logger.Error(fmt.Sprintf("Failed to reopen database: %v", err))
		return ErrReopeningDatabase
	}

	client.db = db
	err = client.rebuildDatabase(backupPath)
	defer client.deleteBackup(backupPath)
	if err != nil {
		return err
	}
	return nil
}
