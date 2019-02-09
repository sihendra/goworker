package goworker

import (
	"encoding/json"
	"github.com/garyburd/redigo/redis"
	"github.com/stretchr/testify/require"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

func TestRedisQueue_Channel(t *testing.T) {
	// Init
	redisPool := &redis.Pool{
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", "localhost:6379")
			if err != nil {
				return nil, err
			}

			return c, err
		},
	}
	queueName := "queue.test-" + strconv.Itoa(rand.Intn(100))
	queueProcessingName := queueName + ":procesing"
	defer func() {
		conn := redisPool.Get()
		defer conn.Close()

		conn.Do("del", queueName)
		conn.Do("del", queueProcessingName)
	}()
	queue := NewRedisQueue(queueName, redisPool)
	require.True(t, queue.Channel() != nil, "Expected channel not nil, got nil")

	// Populate Queue
	payload1, err := json.Marshal([]string{"1"})
	require.NoError(t, err)
	payload2, err := json.Marshal([]string{"1"})
	require.NoError(t, err)
	payload3, err := json.Marshal([]string{"1"})
	require.NoError(t, err)
	queue.Push(payload1, 0)
	queue.Push(payload2, 0)
	queue.Push(payload3, 0)

	// Check data in channel
	go func() {
		select {
		case <-time.After(1 * time.Second):
			queue.Shutdown()
		}
	}()

	i := 0
	for v := range queue.Channel() {
		var tmp []string
		err = json.Unmarshal(v, &tmp)
		require.NoError(t, err)
		require.Len(t, tmp, 1)

		queue.Acknowledge(v)
		i++
	}
	require.Equal(t, 3, i)

	// Test Acknowledge
	ret, err := redis.Int(redisPool.Get().Do("llen", queueName))
	require.NoError(t, err)
	require.Equal(t, 0, ret)

	ret, err = redis.Int(redisPool.Get().Do("llen", queueProcessingName))
	require.NoError(t, err)
	require.Equal(t, 0, ret)

}

func TestRedisQueue_Channel_WhenHasUnprocessedItems(t *testing.T) {
	// Init
	redisPool := &redis.Pool{
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", "localhost:6379")
			if err != nil {
				return nil, err
			}

			return c, err
		},
	}
	queueName := "queue.test-" + strconv.Itoa(rand.Intn(100))
	queueProcessingName := queueName + ":procesing"
	defer func() {
		conn := redisPool.Get()
		defer conn.Close()

		conn.Do("del", queueName)
		conn.Do("del", queueProcessingName)
	}()
	queue := NewRedisQueue(queueName, redisPool)
	require.True(t, queue.Channel() != nil, "Expected channel not nil, got nil")

	// Populate Queue
	payload1, err := json.Marshal([]string{"1"})
	require.NoError(t, err)
	payload2, err := json.Marshal([]string{"1"})
	require.NoError(t, err)
	payload3, err := json.Marshal([]string{"1"})
	require.NoError(t, err)
	queue.Push(payload1, 0)
	queue.Push(payload2, 0)
	queue.Push(payload3, 0)

	// Get items without ACK
	<-queue.Channel()
	<-queue.Channel()
	<-queue.Channel()

	// Shutdown
	queue.Shutdown()

	// Start
	queue = NewRedisQueue(queueName, redisPool)

	// Assert unprocessed queue items are enqueued back to main queue

	go func() {
		select {
		case <-time.After(1 * time.Second):
			queue.Shutdown()
		}
	}()

	i := 0
	for v := range queue.Channel() {
		var tmp []string
		err = json.Unmarshal(v, &tmp)
		require.NoError(t, err)
		require.Len(t, tmp, 1)

		queue.Acknowledge(v)
		i++
	}
	require.Equal(t, 3, i)

}
