package goworker

import (
	"time"
)

type (

	// Queue define methods that will be used to interact with Queue
	Queue interface {
		// Push will push the item to the memory queue
		Push(entry []byte, timeout time.Duration) error
		// Pop will fetch one item from queue
		Pop(timeout time.Duration) ([]byte, error)
		// Channel will return channel to get the item
		Channel() chan []byte
		// Acknowledge will notify the queue that the item has been received. Mostly the queue impl will delete the item from the queue.
		Acknowledge(message []byte)
		// Shutdown will stop the queue item background listener
		Shutdown() error
	}

	// QueueFactory define a function to create a new Queue instance. QueueFactory is used mostly by Worker or QueueManager
	QueueFactory func(name string) (Queue, error)
)
