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
	Enqueue(item interface{}, queueName string) error
	Dispatch(job Job, item interface{}, queueName string) error
	Start() error
	Stop() error
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
	if w.getStarted() {
		return errors.New("worker has already been started")
	} else {
		w.setStarted(true)
	}

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
	if !w.getStarted() {
		return errors.New("worker has not been started")
	} else {
		w.setStarted(false)
	}

	w.shutdown <- os.Interrupt

	return nil
}

func (w *worker) Dispatch(job Job, item interface{}, queueName string) error {
	existing := w.getJob(queueName)
	if existing == nil {
		w.setJob(queueName, job)
	}

	err := w.queueManager.AddQueue(queueName)
	if err != nil {
		return err
	}

	return w.queueManager.Push(QueueItem{
		QueueName: queueName,
		Item:      item,
	})
}

func (w *worker) Register(job Job, queueName string) error {
	existing := w.getJob(queueName)
	if existing == nil {
		w.setJob(queueName, job)
	}

	err := w.queueManager.AddQueue(queueName)
	if err != nil {
		return err
	}

	return nil
}

func (w *worker) Enqueue(item interface{}, queueName string) error {
	return w.queueManager.Push(QueueItem{
		QueueName: queueName,
		Item:      item,
	})
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

	job := w.getJob(queueItem.QueueName)
	if job == nil {
		return fmt.Errorf("no handler for queue %s", queueItem.QueueName)
	}

	w.logThread(threadName, "Processing %s", queueItem.QueueName)

	return job.Handle(queueItem)
}

func (w *worker) getJob(name string) Job {
	w.sync.Lock()
	defer w.sync.Unlock()

	job, ok := w.jobRegistry[name]
	if !ok {
		return nil
	}

	return job
}

func (w *worker) setJob(name string, job Job) {
	w.sync.Lock()
	defer w.sync.Unlock()

	w.jobRegistry[name] = job
}

func (w *worker) setStarted(started bool) {
	w.sync.Lock()
	defer w.sync.Unlock()

	w.started = started
}

func (w *worker) getStarted() bool {
	w.sync.Lock()
	defer w.sync.Unlock()

	return w.started
}

func (w *worker) logThread(name string, message string, params ...interface{}) {
	log.Printf("[%s] %s - | %s\n", name, time.Now().Format(time.RFC3339), fmt.Sprintf(message, params...))
}
