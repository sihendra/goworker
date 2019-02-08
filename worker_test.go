package goworker

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/require"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

type dummyJob struct {
}

func (dummyJob) Handle(item QueueItem) error {
	fmt.Printf("[DummyJob] Handle %s from queue:%s\n", item.Item, item.QueueName)
	return nil
}

type dummyJob2 struct {
}

func (dummyJob2) Handle(item QueueItem) error {
	fmt.Printf("[DummyJob2] Handle %s from queue:%s\n", item.Item, item.QueueName)
	return nil
}

func TestWorker_Register(t *testing.T) {

	output := captureOutput(func() {
		// insert existing data to queue
		q1 := "test.queue1-" + strconv.Itoa(rand.Intn(10))
		q2 := "test.queue2-" + strconv.Itoa(rand.Intn(10))
		queueManager := NewQueueManager(NewMemoryQueueFactory(10))
		err := queueManager.Push(QueueItem{
			QueueName: q1,
			Item:      []string{"Item 1"},
		})
		require.NoError(t, err)
		err = queueManager.Push(QueueItem{
			QueueName: q2,
			Item:      map[string]string{"two": "Item 2"},
		})
		require.NoError(t, err)

		// register worker
		w := NewWorker(queueManager, 1)
		defer w.Stop()

		job := &dummyJob{}
		job2 := &dummyJob2{}

		err = w.Register(job, q1)
		require.NoError(t, err)

		err = w.Register(job2, q2)
		require.NoError(t, err)

		err = w.Start()
		require.NoError(t, err)

		time.Sleep(time.Second * 1)
	})

	// assert queue processed
	toks := strings.Split(output, "\n")
	filtered := make([]string, 0)
	for _, v := range toks {
		if strings.TrimSpace(v) != "" {
			filtered = append(filtered, v)
		}
	}
	if len(filtered) != 2 {
		t.Fatalf("Expect to print 2 lines, got: %d", len(filtered))
	}

}

func TestWorker_Dispatch(t *testing.T) {

	output := captureOutput(func() {
		queueManager := NewQueueManager(NewMemoryQueueFactory(10))
		w := NewWorker(queueManager, 1)
		defer w.Stop()

		err := w.Start()
		if err != nil {
			t.Fatalf("Failed starting worker: %s", err.Error())
		}

		job := &dummyJob{}
		job2 := &dummyJob2{}

		w.Dispatch(job, "something", "q1")
		w.Dispatch(job2, "something", "q2")

		time.Sleep(time.Second * 1)
	})

	toks := strings.Split(output, "\n")
	filtered := make([]string, 0)
	for _, v := range toks {
		if strings.TrimSpace(v) != "" {
			filtered = append(filtered, v)
		}
	}
	if len(filtered) != 2 {
		t.Fatalf("Expect to print 2 lines, got: %d", len(filtered))
	}

}

func captureOutput(f func()) string {
	var buf bytes.Buffer
	prev := os.Stdout
	log.SetOutput(&buf)
	f()
	log.SetOutput(prev)
	return buf.String()
}
