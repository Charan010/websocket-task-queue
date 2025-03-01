package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"

	workerpool "github.com/Charan010/websocket-task-queue/worker-pool"
	"github.com/gorilla/mux"
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
		EnableCompression: true,
	}

	clients   = make(map[*websocket.Conn]bool)
	clientsMu sync.Mutex //to avoid race conditions.
)

// Task struct represents the structure of data that client sends.
type Task struct {
	TaskID  string `json:"task_id"`
	Action  string `json:"action"`
	Payload string `json:"payload"`
	Result  string `json:"result,omitempty"`
}

type TaskResult struct {
	TaskID string `json:"task_id"`
	Status string `json:"status"`
	Result string `json:"result,omitempty"`
}

func FetchTaskResultHandler(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()

	vars := mux.Vars(r)
	taskID := vars["task_id"]

	resultJSON, err := redisClient.Get(ctx, "task:result:"+taskID).Result()
	if err == redis.Nil {

		status, err := redisClient.Get(ctx, "task:status:"+taskID).Result()
		if err == redis.Nil {
			http.Error(w, `{"error": "Task not found"}`, http.StatusNotFound)
			return
		} else if err != nil {
			http.Error(w, `{"error": "Internal Server Error"}`, http.StatusInternalServerError)
			return
		}

		response := TaskResult{
			TaskID: taskID,
			Status: status,
		}

		json.NewEncoder(w).Encode(response)
		return

	} else if err != nil {
		http.Error(w, `{"error": "Internal Server Error"}`, http.StatusInternalServerError)
		return
	}

	var task Task
	err = json.Unmarshal([]byte(resultJSON), &task)
	if err != nil {
		http.Error(w, `{"error": "Error parsing task result"}`, http.StatusInternalServerError)
		return
	}

	response := TaskResult{
		TaskID: task.TaskID,
		Status: "completed",
		Result: task.Result,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)

}

// handles each websocket connection.
func handleConnection(w http.ResponseWriter, r *http.Request) {

	//Upgrading from http to websocket connection.
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

	ip, port, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		fmt.Println("Error parsing RemoteAddr:", err)
		return
	}

	log.Printf("[INFO] New WebSocket Client %v:%v Connected\n", ip, port)

	for {
		var task Task

		if err := conn.ReadJSON(&task); err != nil {
			log.Println("[ERROR] Failed to Read JSON:", err)
			break
		}

		//tasks should have valid taskID
		if task.TaskID == "" {
			log.Println("[ERROR] Invalid Task: Missing Task ID")
			continue
		}

		taskJSON, err := json.Marshal(task)
		if err != nil {
			log.Println("[ERROR] Failed to Marshal Task JSON:", err)
			continue
		}

		taskKey := "task:" + task.TaskID

		if err := redisClient.SetNX(ctx, taskKey, taskJSON, 0).Err(); err != nil {
			log.Println("[ERROR] Failed to Store Task in Redis:", err)
			continue
		}

		err = redisClient.RPush(ctx, "task_queue", task.TaskID).Err()

		if err != nil {
			log.Printf("[ERROR] Failed to Push Task ID %s to Redis Queue: %v \n", task.TaskID, err)
		} else {
			log.Printf("[INFO] Task ID %s Added to Redis Queue\n", task.TaskID)

		}

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

	//retrieves the task from redis queue.
	taskJSON, err := redisClient.Get(ctx, "task:result:"+taskID).Result()
	if err != nil {
		log.Println("Error fetching task result:", err)
		return
	}

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

	_, err := redisClient.Ping(ctx).Result() //Redis working or not.
	if err != nil {
		log.Fatal("Cannot connect to Redis:", err)
	}

	fmt.Println("Connected to Redis successfully!")

	workerPool := workerpool.NewWorkerPool(10, redisClient)
	go workerPool.Start()

	go watchTaskResults()

	r := mux.NewRouter()

	r.HandleFunc("/ws", handleConnection)
	r.HandleFunc("/task/results/{task_id}", FetchTaskResultHandler).Methods("GET")

	fmt.Println("WebSocket server started on ws://localhost:8080/ws")
	log.Fatal(http.ListenAndServe(":8080", r))
}
