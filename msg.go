package bunnymq

type Msg[T any] interface {
	Ack() error
	NAck() error
	Data() T
}

type MsgImpl[T any] struct {
	data            T
	queueName       string
	acked           bool
	consumerID      string
	progressManager *ConsumerProgressManager
	processed       bool
}

func (m *MsgImpl[T]) Ack() error {
	consumerID := m.consumerID
	queueName := m.queueName

	// 获取当前进度
	progress, err := m.progressManager.GetProgress(consumerID, queueName)
	if err != nil {
		return err
	}

	// 更新进度
	newProgress := progress + 1
	err = m.progressManager.UpdateProgress(consumerID, queueName, newProgress)
	if err != nil {
		return err
	}

	return nil

}

func (m *MsgImpl[T]) NAck() error {
	// Message is not acknowledged, do nothing
	return nil
}

func (m *MsgImpl[T]) Data() T {
	return m.data
}
