package workerpool

import (
	"sync"

	"github.com/redis/go-redis/v9"
)

type WorkerPool struct {
	Workers []*Worker
	WG      sync.WaitGroup
}

func NewWorkerPool(workerCount int, client *redis.Client) *WorkerPool {
	pool := &WorkerPool{}

	for i := 1; i <= workerCount; i++ {
		worker := NewWorker(i, client)
		pool.Workers = append(pool.Workers, worker)
	}

	return pool
}

func (wp *WorkerPool) Start() {
	for _, worker := range wp.Workers {
		wp.WG.Add(1)
		go worker.Start(&wp.WG)
	}

	wp.WG.Wait() // Keep running until all workers are up.
}
