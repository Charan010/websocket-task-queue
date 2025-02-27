package workerpool

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

type WorkerPool struct {
	workers     int
	redisClient *redis.Client
}

type Task struct {
	TaskID  string `json:"task_id"`
	Action  string `json:"action"`
	Payload string `json:"payload"`
	Result  string `json:"result"`
	Status  string `json:"status"`
}

func NewWorkerPool(workers int, redisClient *redis.Client) *WorkerPool {
	return &WorkerPool{
		workers:     workers,
		redisClient: redisClient,
	}
}

// Worker Process the task and publishes the JSON result to the task_completed channel.
func (wp *WorkerPool) processTask(taskID string) {

	ctx := context.Background()

	taskJSON, err := wp.redisClient.Get(ctx, "task:"+taskID).Result()
	if err != nil {
		log.Println("Error fetching task:", err)
		return
	}

	var task Task
	err = json.Unmarshal([]byte(taskJSON), &task)
	if err != nil {
		log.Println("Error unmarshalling task:", err)
		return
	}

	task.Status = "Processing"

	// Simulating processing time as there is no real task.
	time.Sleep(2 * time.Second)

	task.Result = "Processed: " + task.Payload

	resultJSON, _ := json.Marshal(task)
	err = wp.redisClient.Set(ctx, "task:result:"+task.TaskID, resultJSON, 0).Err()
	if err != nil {
		log.Println("Error storing task result:", err)
		return
	}

	/* Publish the task into task_completed where notifyClients
	function is subscribed to this channel to send messages to the client.

	*/
	err = wp.redisClient.Publish(ctx, "task_completed", task.TaskID).Err()
	if err != nil {
		log.Println("Error publishing task completion:", err)
		return
	}

	log.Printf("Task %s processed\n", task.TaskID)
	task.Status = "Completed"
}

// Workers from workerpool start executing the tasks from the task_queue queue.
func (wp *WorkerPool) Start() {
	ctx := context.Background()

	for i := 0; i < wp.workers; i++ {
		go func(workerID int) {
			for {
				taskID, err := wp.redisClient.LPop(ctx, "task_queue").Result()
				if err == redis.Nil {
					time.Sleep(1 * time.Second)
					continue
				} else if err != nil {
					log.Println("Error popping task from queue:", err)
					continue
				}

				log.Printf("Worker %d processing task %s\n", workerID, taskID)
				wp.processTask(taskID)
			}
		}(i)
	}
}
