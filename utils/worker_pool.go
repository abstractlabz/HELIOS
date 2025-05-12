package utils

import (
	"log"
	"sync"
)

// WorkerPool represents a generic worker pool for processing data.
type WorkerPool struct {
	NumWorkers  int
	JobsChan    chan interface{}
	ResultsChan chan interface{}
	ProcessFunc func(interface{}) interface{}
}

// NewWorkerPool creates a new worker pool with the specified number of workers.
func NewWorkerPool(numWorkers int, jobBufferSize int, processFunc func(interface{}) interface{}) *WorkerPool {
	return &WorkerPool{
		NumWorkers:  numWorkers,
		JobsChan:    make(chan interface{}, jobBufferSize),
		ResultsChan: make(chan interface{}, jobBufferSize),
		ProcessFunc: processFunc,
	}
}

// Start launches the worker pool and begins processing jobs.
func (wp *WorkerPool) Start() {
	var waitGroup sync.WaitGroup

	// Launch workers.
	for workerID := 0; workerID < wp.NumWorkers; workerID++ {
		waitGroup.Add(1)
		go func(id int) {
			defer waitGroup.Done()
			wp.worker(id)
		}(workerID)
	}

	// Close ResultsChan after all workers have finished.
	go func() {
		waitGroup.Wait()
		close(wp.ResultsChan)
	}()
}

// worker processes jobs from the jobs channel.
func (wp *WorkerPool) worker(id int) {
	for job := range wp.JobsChan {
		log.Printf("Worker %d processing job", id)

		var result interface{}

		// Protect the pool from any panic inside ProcessFunc.
		func() {
			defer func() {
				if recovered := recover(); recovered != nil {
					log.Printf("Worker %d recovered from panic: %v", id, recovered)
				}
			}()
			result = wp.ProcessFunc(job)
		}()

		if result != nil {
			wp.ResultsChan <- result
		}
	}
}

// Submit adds a new job to the pool.
func (wp *WorkerPool) Submit(job interface{}) {
	wp.JobsChan <- job
}

// Close signals that no more jobs will be submitted.
func (wp *WorkerPool) Close() {
	close(wp.JobsChan)
}