[![Build Status](https://travis-ci.org/sihendra/goworker.svg)](https://travis-ci.org/sihendra/goworker)

# GO Worker

Go worker pool with flexible queue implementations. You can implement your own Queue implementation other than the default MemoryQueue and RedisQueue.

# Install

    go get github.com/sihendra/goworker

# Usage


    // Instantiate the worker
    queueManager := NewQueueManager(NewMemoryQueueFactory(10))
    w := NewWorker(queueManager, 5)
    defer w.Stop()

    // Register the job handler
    job := &dummyJob{}
    job2 := &dummyJob2{}        
    w.Register(job, "queue1")
    w.Register(job2, "queue2")
    
    // Enqueue the item to be processed
    w.Enqueue(map[string]interface{}{"id":"123"}, "queue1")
    w.Enqueue("some payload", "queue2")

    // Start working
    w.Start()
    

    
