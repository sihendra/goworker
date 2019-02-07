# GO Worker

Go worker pool with flexible queue implementations. You can implement your own Queue implementation other than the default MemoryQueue.

# Install

    go get github.com/sihendra/goworker

# Usage


    queueManager := NewQueueManager(NewMemoryQueueFactory(10))
    w := NewWorker(queueManager, 1)
    defer w.Stop()

    err := w.Start()
    if err != nil {
        t.Fatalf("Failed starting worker: %s", err.Error())
    }

    job := &dummyJob{}
    job2 := &dummyJob2{}

    w.Dispatch(job, "payload1", "q1")
    w.Dispatch(job2, "payload2", "q2")

