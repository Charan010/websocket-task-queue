package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"

	workerpool "github.com/Charan010/websocket-task-queue/worker-pool"
	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

var (
	redisClient = redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 0})
	ctx         = context.Background()

	// upgrader upgrades HTTP connections to WebSocket connections.
	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}

	//map of whether client already exists in the clients
	clients   = make(map[*websocket.Conn]bool)
	clientsMu sync.Mutex // Mutex for protecting the clients map and to avoid race conditions.
)

// Task struct represents the structure of data that client sends.
type Task struct {
	TaskID  string `json:"task_id"`
	Action  string `json:"action"`
	Payload string `json:"payload"`
	Result  string `json:"result,omitempty"`
}

// handleConnection handles each new WebSocket connection.
func handleConnection(w http.ResponseWriter, r *http.Request) {

	//upgrade the HTTP connection to a WebSocket connection.
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("[ERROR] WebSocket Upgrade Failed:", err)
		return
	}
	defer conn.Close()

	// Register client using proper mutex locking.
	clientsMu.Lock()
	clients[conn] = true
	clientsMu.Unlock()

	log.Println("[INFO] New WebSocket Client Connected")

	for {
		var task Task

		// Read JSON data from the client.
		if err := conn.ReadJSON(&task); err != nil {
			log.Println("[ERROR] Failed to Read JSON:", err)
			break
		}

		//To make sure that JSON data has valid taskID
		if task.TaskID == "" {
			log.Println("[ERROR] Invalid Task: Missing Task ID")
			continue
		}

		// Convert the task into JSON.
		taskJSON, err := json.Marshal(task)
		if err != nil {
			log.Println("[ERROR] Failed to Marshal Task JSON:", err)
			continue
		}

		// Define a Redis key using the TaskID.
		taskKey := "task:" + task.TaskID
		log.Printf("Storing data with key %v\n", taskKey)

		// Store the full task data in Redis with no expiration.
		if err := redisClient.SetNX(ctx, taskKey, taskJSON, 0).Err(); err != nil {
			log.Println("[ERROR] Failed to Store Task in Redis:", err)
			continue
		}

		// Push the TaskID to the Redis queue for worker consumption.
		if err := redisClient.RPush(ctx, "task_queue", task.TaskID).Err(); err != nil {
			log.Println("[ERROR] Failed to Push Task ID to Redis Queue:", err)
			continue
		}
		log.Printf("[INFO] Task ID %s Added to Redis Queue\n", task.TaskID)

		//Acknowledgement to the client that the task has been recieved and sent to processing.
		response := map[string]string{"status": "received", "task_id": task.TaskID}
		if err := conn.WriteJSON(response); err != nil {
			log.Printf("[ERROR] Failed to Send Acknowledgment for taskID %v: %v\n", task.TaskID, err)
		}
	}

	// Remove the client from the active clients map when the connection closes.
	clientsMu.Lock()
	delete(clients, conn)
	clientsMu.Unlock()

	log.Println("[INFO] WebSocket Client Disconnected")
}

// notifyClients sends the processed task result to all connected WebSocket clients.
func notifyClients(taskID string) {
	// Retrieve the processed task result from Redis.
	taskJSON, err := redisClient.Get(ctx, "task:result:"+taskID).Result()
	if err != nil {
		log.Println("Error fetching task result:", err)
		return
	}

	// Send the result to each connected client.
	clientsMu.Lock()
	for client := range clients {
		err := client.WriteMessage(websocket.TextMessage, []byte(taskJSON))
		if err != nil {
			client.Close()
			delete(clients, client)
		}
	}
	clientsMu.Unlock()
}

/* watchTaskResults subscribes to the "task_completed" Redis channel
and notifies clients when a task is done. */

func watchTaskResults() {
	pubsub := redisClient.Subscribe(ctx, "task_completed")
	defer pubsub.Close()

	for msg := range pubsub.Channel() {
		notifyClients(msg.Payload)
	}
}

func main() {

	_, err := redisClient.Ping(ctx).Result() //Checking whether go can connect to redis or not.
	if err != nil {
		log.Fatal("Cannot connect to Redis:", err)
	}

	fmt.Println("Connected to Redis successfully!")

	workerPool := workerpool.NewWorkerPool(10, redisClient)
	go workerPool.Start()

	go watchTaskResults()

	// Set up the WebSocket handler.
	http.HandleFunc("/ws", handleConnection)

	fmt.Println("WebSocket server started on ws://localhost:8080/ws")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
