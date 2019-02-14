package goworker

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

// QueueManager manage multiple queues and fetch the item within single channel
type QueueManager struct {
	queues         map[string]Queue
	fetchChannel   chan QueueItem
	sync           sync.Mutex
	createNewQueue QueueFactory
	shutdown       chan os.Signal
}

// NewQueueManager create new instance of QueueManager
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

// AddQueue register queue to be fetched and listened
func (q *QueueManager) AddQueue(name string) error {
	existing := q.getQueue(name)
	if existing != nil {
		return nil
	}

	newQueue, err := q.createNewQueue(name)
	if err != nil {
		return err
	}

	q.setQueue(name, newQueue)

	return q.listenQueueChannelAsync(newQueue)
}

// Push will push the item to designated queue
func (q *QueueManager) Push(queueItem QueueItem) error {

	name := queueItem.QueueName

	existing := q.getQueue(name)

	if existing == nil {
		return errors.New("trying to push to unregistered queue")
	}

	bytes, err := queueItem.ToBytes()
	if err != nil {
		return err
	}
	return existing.Push(bytes, 0)
}

// Fetch will return channel to get the queue item from all queue
func (q *QueueManager) Fetch() chan QueueItem {
	return q.fetchChannel
}

func (q *QueueManager) listenQueueChannelAsync(queue Queue) error {
	go func(queue Queue) {
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
	}(queue)

	return nil
}

func (q *QueueManager) setQueue(name string, queue Queue) {
	q.sync.Lock()
	defer q.sync.Unlock()

	q.queues[name] = queue
}

func (q *QueueManager) getQueue(name string) Queue {
	q.sync.Lock()
	defer q.sync.Unlock()

	queue, ok := q.queues[name]
	if !ok {
		return nil
	}

	return queue
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
