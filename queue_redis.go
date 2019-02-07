package goworker

import (
	"encoding/json"
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
	dataChannel chan interface{}
	close       chan os.Signal
}

func NewRedisQueue(name string, pool *redis.Pool) Queue {
	q := &redisQueue{
		pool:        pool,
		queueName:   name,
		close:       make(chan os.Signal),
		dataChannel: make(chan interface{}),
	}

	signal.Notify(q.close, syscall.SIGTERM)

	q.startListener()

	return q
}

func NewRedisQueueFactory(pool *redis.Pool) QueueFactory {
	return func(name string) (queue Queue, e error) {
		return NewRedisQueue(name, pool), nil
	}
}

func (m *redisQueue) Push(entry interface{}, timeout time.Duration) error {
	conn := m.pool.Get()
	defer conn.Close()

	bytes, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	_, err = conn.Do("lpush", m.queueName, bytes)
	if err != nil {
		return err
	}

	return nil
}

func (m *redisQueue) Pop(timeout time.Duration) (interface{}, error) {
	conn := m.pool.Get()
	defer conn.Close()

	message, err := redis.String(conn.Do("rpop", m.queueName, 0))
	if err != nil {
		return nil, err
	}

	return message, nil
}

func (m *redisQueue) Acknowledge(message interface{}) {
	conn := m.pool.Get()
	defer conn.Close()

	conn.Do("lrem", m.queueName+":processing", -1, message)
}

func (m *redisQueue) Channel() chan interface{} {
	return m.dataChannel
}

func (m *redisQueue) Shutdown() error {
	if len(m.close) == 0 {
		m.close <- os.Interrupt
	}

	close(m.dataChannel)

	return nil
}

func (m *redisQueue) startListener() {
	go func() {
		for {
			conn := m.pool.Get()
			defer conn.Close()

			select {
			case <-m.close:
				break
			default:
			}

			message, err := redis.String(conn.Do("brpoplpush", m.queueName, m.queueName+":processing", 1))
			if err != nil {
				if err.Error() != "redigo: nil returned" {
					fmt.Printf("error while listening redis queue %s: %s\n", m.queueName, err.Error())
				}
			} else {
				m.dataChannel <- message
			}

		}
	}()
}
