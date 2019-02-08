package goworker

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// TODO consider implement registering queue names and queue other than Push method (Add Queue would be good)

type (
	Queue interface {
		Push(entry []byte, timeout time.Duration) error
		Pop(timeout time.Duration) ([]byte, error)
		Channel() chan []byte
		Acknowledge(message []byte)
		Shutdown() error
	}

	QueueManager struct {
		queues         map[string]Queue
		fetchChannel   chan QueueItem
		sync           sync.Mutex
		createNewQueue QueueFactory
		shutdown       chan os.Signal
	}

	QueueFactory func(name string) (Queue, error)

	QueueItem struct {
		QueueName string
		Item      interface{}
	}
)

func NewQueueManager(factory QueueFactory) *QueueManager {

	qm := &QueueManager{
		queues:         make(map[string]Queue),
		fetchChannel:   make(chan QueueItem),
		sync:           sync.Mutex{},
		createNewQueue: factory,
		shutdown:       make(chan os.Signal),
	}

	signal.Notify(qm.shutdown, syscall.SIGTERM)

	return qm
}

func (q *QueueManager) Push(queueItem QueueItem) error {

	name := queueItem.QueueName

	q.sync.Lock()
	_, ok := q.queues[name]
	if !ok {
		newQueue, err := q.createNewQueue(name)
		if err != nil {
			return err
		}
		q.queues[name] = newQueue

		q.listenQueueChannelAsync(name, newQueue)
	}
	q.sync.Unlock()

	bytes, err := queueItem.ToBytes()
	if err != nil {
		return err
	}
	return q.queues[name].Push(bytes, 0)
}

func (q *QueueManager) Fetch() chan QueueItem {
	return q.fetchChannel
}

func (q *QueueManager) listenQueueChannelAsync(name string, queue Queue) error {
	go func(name string, queue Queue) {
		for {
			select {
			case item := <-queue.Channel():

				queue.Acknowledge(item)

				queueItem, err := q.toQueueItem(item)
				if err != nil {

				}

				if queueItem != nil {
					q.fetchChannel <- *queueItem
				}

			case <-q.shutdown:
				break
			}
		}
	}(name, q.queues[name])

	return nil
}

func (q *QueueManager) toQueueItem(item interface{}) (*QueueItem, error) {
	var bytes []byte
	switch v := item.(type) {
	case []byte:
		bytes = v
	case string:
		bytes = []byte(v)
	}
	if bytes == nil {
		return nil, fmt.Errorf("could not convert %T to QueueItem", item)
	}

	var queueItem QueueItem
	err := json.Unmarshal(bytes, &queueItem)
	if err != nil {
		return nil, err
	}

	return &queueItem, nil
}

func NewQueueItemFromBytes(data []byte) (item *QueueItem, err error) {
	err = json.Unmarshal(data, item)
	return
}

func (q QueueItem) ToBytes() ([]byte, error) {
	return json.Marshal(q)
}
