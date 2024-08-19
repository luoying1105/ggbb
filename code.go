package bunnymq

import (
	"encoding/json"
)

type Coder[T any] interface {
	Encode(T) ([]byte, error)
	Decode(encodedData []byte) (T, error)
}

type JsonCoder[T any] struct{}

// Encode 将任意类型 T 编码为 JSON 字节数组
func (c *JsonCoder[T]) Encode(data T) ([]byte, error) {
	return json.Marshal(data)
}

// Decode 将 JSON 字节数组解码为任意类型 T
func (c *JsonCoder[T]) Decode(encodedData []byte) (T, error) {
	var data T
	err := json.Unmarshal(encodedData, &data)
	return data, err
}
