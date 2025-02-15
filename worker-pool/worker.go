package workerpool

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type Worker struct {
	ID          int
	RedisClient *redis.Client
}

func NewWorker(id int, client *redis.Client) *Worker {

	return &Worker{
		ID:          id,
		RedisClient: client,
	}
}

func (w *Worker) Start(wg *sync.WaitGroup) {

	defer wg.Done()

	fmt.Printf("Worker %d started...\n", w.ID)

	for {
		task, err := w.RedisClient.BLPop(context.Background(), 0, "task_queue").Result()
		if err != nil {
			log.Printf("Worker %d: Error fetching task: %v\n", w.ID, err)
			continue
		}

		taskData := task[1]
		fmt.Printf("Worker %d processing task: %s\n", w.ID, taskData)

		time.Sleep(time.Duration(rand.Intn(3)+1) * time.Second)

		w.RedisClient.Set(context.Background(), "task_result:"+taskData, fmt.Sprintf("Processed by worker %d", w.ID), 0)
		fmt.Printf("Worker %d completed task: %s\n", w.ID, taskData)
	}

}
