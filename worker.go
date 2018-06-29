package worker

import (
	"time"
	"os"
	"os/signal"
	"syscall"
	"log"
	"fmt"
)

type Worker interface {
	Dispatch(job Job) error
	Start() error
	Stop() error
}

type Job interface {
	Handle() error
}


// Worker
func NewWorker(queue Queue, workerCount int) Worker {
	w := &worker{
		queue:    queue,
		timeout:  2 * time.Second,
		count:    workerCount,
		shutdown: make(chan os.Signal),
	}

	signal.Notify(w.shutdown, syscall.SIGTERM)

	return w
}

type worker struct {
	queue    Queue
	timeout  time.Duration
	count    int
	shutdown chan os.Signal
}

func (w *worker) Start() error {
	for i := 1; i <= w.count; i++ {
		go func(name string) {
			for {
				select {
				case item := <-w.queue.Channel():
					err := w.process(name, item)
					if err != nil {
						w.logThread(name, err.Error())
					}
				case <-w.shutdown:
					break
				}
			}

		}(fmt.Sprintf("Thread #%d", i))
	}
	return nil
}

func (w *worker) Stop() error {
	w.shutdown <- os.Interrupt
	return nil
}

func (w *worker) Dispatch(job Job) error {
	return w.queue.Push(job, w.timeout)
}

func (w *worker) process(threadName string, item interface{}) error {
	job, ok := item.(Job)
	if !ok {
		return fmt.Errorf("expect a Job type got: %T", item)
	}

	w.logThread(threadName, "Processing %T", item)

	return job.Handle()

	return nil
}

func (w *worker) logThread(name string, message string, params ...interface{}) {
	log.Printf("[%s] %s - | %s\n", name, time.Now().Format(time.RFC3339), fmt.Sprintf(message, params...))
}

