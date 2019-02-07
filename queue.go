package goworker

import (
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type (
	Queue interface {
		Push(entry interface{}, timeout time.Duration) error
		Pop(timeout time.Duration) (interface{}, error)
		Channel() chan interface{}
		Acknowledge(message interface{})
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
	q.sync.Lock()
	defer q.sync.Unlock()

	name := queueItem.QueueName
	item := queueItem.Item

	_, ok := q.queues[name]
	if !ok {
		newQueue, err := q.createNewQueue(name)
		if err != nil {
			return err
		}
		q.queues[name] = newQueue

		go func(name string, queue Queue) {
			for {
				select {
				case item := <-queue.Channel():
					q.fetchChannel <- QueueItem{
						QueueName: name,
						Item:      item,
					}

					queue.Acknowledge(item)
				case <-q.shutdown:
					break
				}
			}
		}(name, q.queues[name])
	}

	return q.queues[name].Push(item, 0)
}

func (q *QueueManager) Fetch() chan QueueItem {
	return q.fetchChannel
}
