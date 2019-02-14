package goworker

type Job interface {
	Handle(item QueueItem) error
}
