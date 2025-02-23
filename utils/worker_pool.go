package utils

import (
	"log"
	"sync"
)

// WorkerPool represents a generic worker pool for processing data
type WorkerPool struct {
	NumWorkers  int
	JobsChan    chan interface{}
	ResultsChan chan interface{}
	ProcessFunc func(interface{}) interface{}
}

// NewWorkerPool creates a new worker pool with the specified number of workers
func NewWorkerPool(numWorkers int, jobBufferSize int, processFunc func(interface{}) interface{}) *WorkerPool {
	return &WorkerPool{
		NumWorkers:  numWorkers,
		JobsChan:    make(chan interface{}, jobBufferSize),
		ResultsChan: make(chan interface{}, jobBufferSize),
		ProcessFunc: processFunc,
	}
}

// Start launches the worker pool and begins processing jobs
func (wp *WorkerPool) Start() {
	var wg sync.WaitGroup

	// Launch workers
	for i := 0; i < wp.NumWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			wp.worker(workerID)
		}(i)
	}

	// Wait for all workers to complete in a separate goroutine
	go func() {
		wg.Wait()
		close(wp.ResultsChan)
	}()
}

// worker processes jobs from the jobs channel
func (wp *WorkerPool) worker(id int) {
	for job := range wp.JobsChan {
		log.Printf("Worker %d processing job", id)
		result := wp.ProcessFunc(job)
		if result != nil {
			wp.ResultsChan <- result
		}
	}
}

// Submit adds a new job to the pool
func (wp *WorkerPool) Submit(job interface{}) {
	wp.JobsChan <- job
}

// Close signals that no more jobs will be submitted
func (wp *WorkerPool) Close() {
	close(wp.JobsChan)
}
