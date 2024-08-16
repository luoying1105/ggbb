package pkg

import (
	"fmt"
	bolt "go.etcd.io/bbolt"
	"os"
	"strconv"
	"time"
)

func (client *DBClient) CleanupAllConsumed() error {
	err := client.update(func(tx *bolt.Tx) error {
		// 遍历数据库中的所有 bucket
		err := tx.ForEach(func(bucketName []byte, _ *bolt.Bucket) error {
			// 获取当前 bucket 的消费进度
			progressValue := tx.Bucket([]byte(consumerProgressBucket)).Get(bucketName)
			if progressValue == nil {
				return nil // 如果没有进度记录，跳过这个 bucket
			}

			progress, err := strconv.ParseInt(string(progressValue), 10, 64)
			if err != nil {
				Logger.Error("Invalid progress value", fmt.Sprintf("bucketName %s %s", string(bucketName)), err)
				return ErrInvalidProgress
			}
			Logger.Debug("Cleaning up consumed messages", fmt.Sprintf("bucketName %s progress %d", string(bucketName), progress))
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

func (client *DBClient) cleanupBucket(tx *bolt.Tx, bucketName string, progress int64) error {
	bucket := tx.Bucket([]byte(bucketName))
	if bucket == nil {
		return ErrBucketNotFound
	}

	cursor := bucket.Cursor()
	for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
		keyInt, err := strconv.ParseUint(string(k), 10, 64)
		if err != nil {
			Logger.Error("Invalid progress value", fmt.Sprintf("bucketName %s %s", string(bucketName)), err)
			return ErrInvalidProgress
		}
		fmt.Printf("Progress value: %v", v)

		if keyInt <= uint64(progress) {
			if err := bucket.Delete(k); err != nil {
				return ErrFailedToDelete
			}
		}
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

	// Ensure the backupDB is closed outside of the transaction
	if backupDB != nil {
		defer func() {
			if cerr := backupDB.Close(); cerr != nil {
				Logger.Error(fmt.Sprintf("关闭备份数据库失败: %s", cerr))
			} else {
				Logger.Info("备份数据库成功关闭")
			}
		}()
	}

	Logger.Info("Database 完成更新")

	if err == nil {
		err = client.deleteBackup(backupPath)
	}

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

	Logger.Info("备份处理完成")
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
	Logger.Info(fmt.Sprintf("写入新Bucket: %s", name))
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
	Logger.Info("删除备份")
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
	if err != nil {
		return err
	}

	return nil
}
