package pkg

import "fmt"

type MessageStore struct {
	dbClient *DBClient
}
type MessageStoreInterface interface {
	StoreMessage(bucketName, message string) error
	RetrieveMessage(bucketName string, progress int64) (*KeyValue, error)
}

func NewMessageStore(dbClient *DBClient) *MessageStore {
	// 创建 bucket 如果不存在
	err := dbClient.CreateBucket("consumer_progress")
	if err != nil {
		panic(fmt.Sprintf("Failed to create consumer_progress bucket: %v", err))
	}
	return &MessageStore{dbClient: dbClient}
}

func (ms *MessageStore) StoreMessage(bucketName, message string) error {
	err := ms.dbClient.CreateBucket(bucketName)
	if err != nil {
		return err
	}
	return ms.StoreByte(bucketName, []byte(message))
}
func (ms *MessageStore) StoreByte(bucketName string, message []byte) error {
	return ms.dbClient.PutWithAutoIncrementKey(bucketName, message)
}

func (ms *MessageStore) RetrieveMessage(bucketName string, progress int64) (*KeyValue, error) {

	return ms.dbClient.GetNext(bucketName, progress)
}
