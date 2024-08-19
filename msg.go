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
	if !m.processed {
		// 更新该消费者的进度，而不删除消息
		currentProgress, err := m.progressManager.GetProgress(m.consumerID, m.queueName)
		if err != nil {
			return err
		}
		err = m.progressManager.UpdateProgress(m.consumerID, m.queueName, currentProgress+1)
		if err != nil {
			return err
		}
		m.processed = true
	}
	return nil
}

func (m *MsgImpl[T]) NAck() error {
	// Message is not acknowledged, do nothing
	m.processed = false
	return nil
}

func (m *MsgImpl[T]) Data() T {
	return m.data
}
