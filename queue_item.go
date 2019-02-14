package goworker

import "encoding/json"

// QueueItem wrap any object before pushing to the Queue. This might contain some important internal metadata
type QueueItem struct {
	QueueName string
	Item      interface{}
}

// NewQueueItemFromBytes will convert slice of byte to QeuueItem
func NewQueueItemFromBytes(data []byte) (item *QueueItem, err error) {
	err = json.Unmarshal(data, item)
	return
}

// ToBytes will convert QueueItem to slice of byte
func (q QueueItem) ToBytes() ([]byte, error) {
	return json.Marshal(q)
}
