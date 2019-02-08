package goworker

import (
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestMemQueue_Push(t *testing.T) {
	queue := NewMemoryQueue(3)

	items := []string{"Item1", "Item2", "Item3", "Item4"}

	err := queue.Push([]byte(items[0]), time.Second)
	require.NoError(t, err)

	item, err := queue.Pop(time.Second)
	require.NoError(t, err)

	require.True(t, string(item) == items[0], "Expected: %s, got: %s", items[0], item)

}

func TestMemQueue_Pop(t *testing.T) {
	queue := NewMemoryQueue(3)

	items := []string{"Item1"}

	err := queue.Push([]byte(items[0]), time.Second)
	require.NoError(t, err)

	item, err := queue.Pop(time.Second)
	require.NoError(t, err)

	require.True(t, string(item) == items[0], "Expected: %s, got: %s", items[0], item)

	item2, err := queue.Pop(time.Second)

	require.True(t, item2 == nil, "Expected nil, got: %+v", item2)
}

func TestMemQueue_Channel(t *testing.T) {
	queue := NewMemoryQueue(3)

	require.True(t, queue.Channel() != nil, "Expected channel not nil, got nil")
}
