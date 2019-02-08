package goworker

import "encoding/json"

type QueueItem struct {
	QueueName string
	Item      interface{}
}

func NewQueueItemFromBytes(data []byte) (item *QueueItem, err error) {
	err = json.Unmarshal(data, item)
	return
}

func (q QueueItem) ToBytes() ([]byte, error) {
	return json.Marshal(q)
}
