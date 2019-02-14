package goworker

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type redisQueue struct {
	pool        *redis.Pool
	queueName   string
	dataChannel chan []byte
	close       chan os.Signal
}

// NewRedisQueue will return In-Redis Queue implementation
func NewRedisQueue(name string, pool *redis.Pool) Queue {
	q := &redisQueue{
		pool:        pool,
		queueName:   name,
		close:       make(chan os.Signal),
		dataChannel: make(chan []byte),
	}

	signal.Notify(q.close, syscall.SIGTERM)

	q.startListener()

	return q
}

// NewRedisQueueFactory will return the In-Redis QueueFactory implementation
func NewRedisQueueFactory(pool *redis.Pool) QueueFactory {
	return func(name string) (queue Queue, e error) {
		return NewRedisQueue(name, pool), nil
	}
}

func (m *redisQueue) Push(entry []byte, timeout time.Duration) error {
	conn := m.pool.Get()
	defer conn.Close()

	_, err := conn.Do("lpush", m.queueName, entry)
	if err != nil {
		return err
	}

	return nil
}

func (m *redisQueue) Pop(timeout time.Duration) ([]byte, error) {
	conn := m.pool.Get()
	defer conn.Close()

	message, err := redis.Bytes(conn.Do("rpop", m.queueName, 0))
	if err != nil {
		return nil, err
	}

	return message, nil
}

func (m *redisQueue) Acknowledge(message []byte) {
	conn := m.pool.Get()
	defer conn.Close()

	conn.Do("lrem", m.inProgressQueueName(), -1, message)
}

func (m *redisQueue) Channel() chan []byte {
	return m.dataChannel
}

func (m *redisQueue) Shutdown() error {
	if len(m.close) == 0 {
		m.close <- os.Interrupt
	}

	close(m.dataChannel)

	return nil
}

func (m *redisQueue) inProgressQueueName() string {
	return m.queueName + ":processing"
}

func (m *redisQueue) enqueueUnfinishedItems() error {
	conn := m.pool.Get()
	defer conn.Close()

	messages, err := redis.ByteSlices(conn.Do("lrange", m.inProgressQueueName(), 0, -1))
	if err != nil {
		return err
	}

	for _, v := range messages {
		m.dataChannel <- v
	}

	return nil
}

func (m *redisQueue) startListener() {

	go func() {
		m.enqueueUnfinishedItems()

		for {
			select {
			case <-m.close:
				break
			default:
			}

			m.readItem()
		}
	}()
}

func (m *redisQueue) readItem() error {
	conn := m.pool.Get()
	defer conn.Close()

	message, err := redis.Bytes(conn.Do("brpoplpush", m.queueName, m.inProgressQueueName(), 1))
	if err != nil {
		if err.Error() != "redigo: nil returned" {
			fmt.Printf("error while listening redis queue %s: %s\n", m.queueName, err.Error())
			<-time.After(1 * time.Second)
		}

		return err
	}

	m.dataChannel <- message

	return nil

}
