package goworker

import (
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type Worker interface {
	Register(job Job, queueName string) error
	Dispatch(job Job, item interface{}, queueName string) error
	Start() error
	Stop() error
}

type Job interface {
	Handle(item QueueItem) error
}

type worker struct {
	queueManager *QueueManager
	jobRegistry  map[string]Job
	timeout      time.Duration
	count        int
	started      bool
	shutdown     chan os.Signal
	sync         sync.Mutex
}

// Worker
func NewWorker(queue *QueueManager, workerCount int) Worker {
	w := &worker{
		queueManager: queue,
		jobRegistry:  make(map[string]Job),
		timeout:      2 * time.Second,
		count:        workerCount,
		shutdown:     make(chan os.Signal),
		sync:         sync.Mutex{},
	}

	signal.Notify(w.shutdown, syscall.SIGTERM)

	return w
}

func (w *worker) Start() error {
	w.sync.Lock()
	if w.started {
		return errors.New("worker has already been started")
	} else {
		w.started = true
	}
	w.sync.Unlock()

	for i := 1; i <= w.count; i++ {
		threadName := fmt.Sprintf("Thread #%d", i)
		go func(name string) {
			for {
				select {
				case item := <-w.queueManager.Fetch():
					err := w.process(name, item)
					if err != nil {
						w.logThread(name, err.Error())
					}
				case <-w.shutdown:
					break
				}
			}

		}(threadName)
	}
	return nil
}

func (w *worker) Stop() error {
	w.sync.Lock()
	if !w.started {
		return errors.New("worker has not been started")
	} else {
		w.started = false
	}
	defer w.sync.Unlock()

	w.shutdown <- os.Interrupt

	return nil
}

func (w *worker) Dispatch(job Job, item interface{}, queueName string) error {
	w.sync.Lock()
	defer w.sync.Unlock()

	_, ok := w.jobRegistry[queueName]
	if !ok {
		w.jobRegistry[queueName] = job
	}

	return w.queueManager.Push(QueueItem{
		QueueName: queueName,
		Item:      item,
	})
}

func (w *worker) Register(job Job, queueName string) error {
	w.sync.Lock()
	defer w.sync.Unlock()

	_, ok := w.jobRegistry[queueName]
	if !ok {
		w.jobRegistry[queueName] = job
	}

	err := w.queueManager.AddQueue(queueName)
	if err != nil {
		return err
	}

	return nil
}

func (w *worker) process(threadName string, queueItem QueueItem) (err error) {
	defer func(error) {
		if r := recover(); r != nil {
			switch x := r.(type) {
			case string:
				err = errors.New(x)
			case error:
				err = x
			default:
				err = errors.New("unknown error while processing")
			}
		}
	}(err)

	job, ok := w.jobRegistry[queueItem.QueueName]
	if !ok {
		return fmt.Errorf("no handler for queue %s", queueItem.QueueName)
	}

	w.logThread(threadName, "Processing %s", queueItem.QueueName)

	return job.Handle(queueItem)
}

func (w *worker) logThread(name string, message string, params ...interface{}) {
	log.Printf("[%s] %s - | %s\n", name, time.Now().Format(time.RFC3339), fmt.Sprintf(message, params...))
}
