package goworker

import (
	"testing"
	"fmt"
	"time"
	"bytes"
	"log"
	"os"
	"strings"
)

type dummyJob struct {

}

func (dummyJob) Handle() error {
	fmt.Printf("Hello World!\n")
	return nil
}

func TestWorker_Start(t *testing.T) {

	output := captureOutput(func(){
		queue := NewMemoryQueue(10)
		w := NewWorker(queue, 4)
		defer w.Stop()

		err := w.Start()
		if err != nil {
			t.Fatalf("Failed starting worker: %s", err.Error())
		}

		var job Job
		job = &dummyJob{}

		w.Dispatch(job)
		w.Dispatch(job)

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