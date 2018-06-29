package worker

import (
	"testing"
	"time"
	"runtime"
	"fmt"
	"path/filepath"
)

func TestMemQueue_Push(t *testing.T) {
	queue := NewMemoryQueue(3)

	items := []string{"Item1", "Item2", "Item3", "Item4"}

	err := queue.Push(items[0], time.Second)
	ok(t, err)

	item, err := queue.Pop(time.Second)
	ok(t, err)

	assert(t, item == items[0], "Expected: %s, got: %s", items[0], item)

}

func TestMemQueue_Pop(t *testing.T) {
	queue := NewMemoryQueue(3)

	items := []string{"Item1"}

	err := queue.Push(items[0], time.Second)
	ok(t, err)

	item, err := queue.Pop(time.Second)
	ok(t, err)

	assert(t, item == items[0], "Expected: %s, got: %s", items[0], item)

	item2, err := queue.Pop(time.Second)

	assert(t, item2 == nil, "Expected nil, got: %+v", item2)
}

func TestMemQueue_Channel(t *testing.T) {
	queue := NewMemoryQueue(3)

	assert(t, queue.Channel() != nil, "Expected channel not nil, got nil")
}

// assert fails the test if the condition is false.
func assert(tb testing.TB, condition bool, msg string, v ...interface{}) {
	if !condition {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d: "+msg+"\033[39m\n\n", append([]interface{}{filepath.Base(file), line}, v...)...)
		tb.FailNow()
	}
}

// ok fails the test if an err is not nil.
func ok(tb testing.TB, err error) {
	if err != nil {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d: unexpected error: %s\033[39m\n\n", filepath.Base(file), line, err.Error())
		tb.FailNow()
	}
}
