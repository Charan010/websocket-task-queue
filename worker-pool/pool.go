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

const MAX_RETRIES = 10

type Task struct {
	TaskID  string `json:"task_id"`
	Action  string `json:"action"`
	Payload string `json:"payload"`
	Result  string `json:"result"`
	Status  string `json:"status"`
}

// Creating a workerpool to work on the tasks in the queue.
func NewWorkerPool(workers int, redisClient *redis.Client) *WorkerPool {
	return &WorkerPool{
		workers:     workers,
		redisClient: redisClient,
	}
}

// Worker Process the task and publishes the JSON result to the task_completed channel.
func (wp *WorkerPool) processTask(taskID string, redisClient redis.Client) {

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

	Doable := true

	if !Doable {
		_, err := redisClient.XAdd(context.Background(), &redis.XAddArgs{
			Stream: "dlq_queue",
			Values: map[string]interface{}{
				"task_id":      task.TaskID,
				"task_action":  task.Action,
				"task_payload": task.Payload,
				"task_status":  "failed",
			},
		}).Result()

		if err != nil {
			log.Fatalf("[ERROR] Failed to push task %v to stream\n", task.TaskID)
			return
		}

		log.Fatalf("Pushed task %v into stream. Will be debugged later by the admin\n", task.TaskID)
	}

	task.Status = "Processing"

	// Simulating processing time as there is no real task.
	time.Sleep(time.Second * 3)

	task.Result = "Processed: " + task.Payload

	//Storing the processed result into redis.
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
				wp.processTask(taskID, *wp.redisClient)
			}
		}(i)
	}
}
