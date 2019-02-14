package goworker

import (
	"errors"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type memQueue struct {
	storage chan []byte
	close   chan os.Signal
}

// NewMemoryQueue return In-Memory Queue implementation
func NewMemoryQueue(length int) Queue {
	q := &memQueue{
		storage: make(chan []byte, length),
		close:   make(chan os.Signal),
	}

	signal.Notify(q.close, syscall.SIGTERM)

	return q
}

// NewMemoryQueueFactory will return In-Memory QueueFactory
func NewMemoryQueueFactory(length int) QueueFactory {
	return func(name string) (queue Queue, e error) {
		return NewMemoryQueue(length), nil
	}
}

func (m *memQueue) Push(entry []byte, timeout time.Duration) error {
	timeoutReached := time.After(timeout)
	if timeout == 0 {
		timeoutReached = make(chan time.Time)
	}

	select {
	case m.storage <- entry:
	case <-m.close:
		close(m.storage)
	case <-timeoutReached:
		return errors.New("timeout while pushing to queueManager")
	}

	return nil
}

func (m *memQueue) Pop(timeout time.Duration) ([]byte, error) {
	timeoutReached := time.After(timeout)
	if timeout == 0 {
		timeoutReached = make(chan time.Time)
	}
	select {
	case e := <-m.storage:
		return e, nil
	case <-m.close:
		close(m.storage)
	case <-timeoutReached:
		return nil, errors.New("timeout while poping queueManager")
	}

	return nil, nil
}

func (m *memQueue) Channel() chan []byte {
	return m.storage
}

func (m *memQueue) Acknowledge(message []byte) {
	return
}

func (m *memQueue) Shutdown() error {
	close(m.storage)
	return nil
}
