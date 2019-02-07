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

func TestMemQueue_Push(t *testing.T) {
	queue := NewMemoryQueue(3)

	items := []string{"Item1", "Item2", "Item3", "Item4"}

	err := queue.Push(items[0], time.Second)
	require.NoError(t, err)

	item, err := queue.Pop(time.Second)
	require.NoError(t, err)

	require.True(t, item == items[0], "Expected: %s, got: %s", items[0], item)

}

func TestMemQueue_Pop(t *testing.T) {
	queue := NewMemoryQueue(3)

	items := []string{"Item1"}

	err := queue.Push(items[0], time.Second)
	require.NoError(t, err)

	item, err := queue.Pop(time.Second)
	require.NoError(t, err)

	require.True(t, item == items[0], "Expected: %s, got: %s", items[0], item)

	item2, err := queue.Pop(time.Second)

	require.True(t, item2 == nil, "Expected nil, got: %+v", item2)
}

func TestMemQueue_Channel(t *testing.T) {
	queue := NewMemoryQueue(3)

	require.True(t, queue.Channel() != nil, "Expected channel not nil, got nil")
}

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
	queue.Push([]string{"1"}, 0)
	queue.Push([]string{"2"}, 0)
	queue.Push([]string{"3"}, 0)

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
		bytes, ok := v.(string)
		require.True(t, ok)
		json.Unmarshal([]byte(bytes), &tmp)
		require.Len(t, tmp, 1)

		queue.Acknowledge(bytes)
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
