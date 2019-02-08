package goworker

import (
	"time"
)

type (
	Queue interface {
		Push(entry []byte, timeout time.Duration) error
		Pop(timeout time.Duration) ([]byte, error)
		Channel() chan []byte
		Acknowledge(message []byte)
		Shutdown() error
	}

	QueueFactory func(name string) (Queue, error)
)
