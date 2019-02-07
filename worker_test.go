package goworker

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"
	"time"
)

type dummyJob struct {
}

func (dummyJob) Handle(item interface{}) error {
	fmt.Printf("Dummy: Hello World!\n")
	return nil
}

type dummyJob2 struct {
}

func (dummyJob2) Handle(item interface{}) error {
	fmt.Printf("Dummy2: Hello World!\n")
	return nil
}

func TestWorker_Start(t *testing.T) {

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
