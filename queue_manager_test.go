package goworker

import (
	"github.com/stretchr/testify/require"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

func TestQueueManager_Fetch(t *testing.T) {
	queueFactory := NewMemoryQueueFactory(10)
	queueManager := NewQueueManager(queueFactory)

	// insert different queue with data
	q1 := "queue.test-mgr-" + strconv.Itoa(rand.Intn(100))
	q2 := "queue.test-mgr-" + strconv.Itoa(rand.Intn(100))
	q3 := "queue.test-mgr-" + strconv.Itoa(rand.Intn(100))
	err := queueManager.Push(QueueItem{
		QueueName: q1,
		Item:      "Item1",
	})
	require.NoError(t, err)
	err = queueManager.Push(QueueItem{
		QueueName: q2,
		Item:      "Item2",
	})
	require.NoError(t, err)
	err = queueManager.Push(QueueItem{
		QueueName: q3,
		Item:      "Item3",
	})
	require.NoError(t, err)

	go func() {
		<-time.After(1 * time.Second)
		close(queueManager.Fetch())
	}()

	// read centralized queue
	var items []string
	for v := range queueManager.Fetch() {
		str, ok := v.Item.(string)
		require.True(t, ok)
		items = append(items, str)
	}

	require.Len(t, items, 3)

	require.Contains(t, items, "Item1")
	require.Contains(t, items, "Item2")
	require.Contains(t, items, "Item3")
}

func TestQueueManager_Fetch(t *testing.T) {
	queueFactory := NewMemoryQueueFactory(10)
	queueManager := NewQueueManager(queueFactory)

	// insert different queue with data
	q1 := "queue.test-mgr-" + strconv.Itoa(rand.Intn(100))
	q2 := "queue.test-mgr-" + strconv.Itoa(rand.Intn(100))
	q3 := "queue.test-mgr-" + strconv.Itoa(rand.Intn(100))
	err := queueManager.Push(QueueItem{
		QueueName: q1,
		Item:      "Item1",
	})
	require.NoError(t, err)
	err = queueManager.Push(QueueItem{
		QueueName: q2,
		Item:      "Item2",
	})
	require.NoError(t, err)
	err = queueManager.Push(QueueItem{
		QueueName: q3,
		Item:      "Item3",
	})
	require.NoError(t, err)

	go func() {
		<-time.After(1 * time.Second)
		close(queueManager.Fetch())
	}()

	// read centralized queue
	var items []string
	for v := range queueManager.Fetch() {
		str, ok := v.Item.(string)
		require.True(t, ok)
		items = append(items, str)
	}

	require.Len(t, items, 3)

	require.Contains(t, items, "Item1")
	require.Contains(t, items, "Item2")
	require.Contains(t, items, "Item3")
}
