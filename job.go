package goworker

// Job define function that will be called when there is queue item to be processed
type Job interface {
	Handle(item QueueItem) error
}
