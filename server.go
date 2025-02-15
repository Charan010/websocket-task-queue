package main

import (
	"context"
	"fmt"
	"log"
	"net/http"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

var redisClient *redis.Client
var ctx = context.Background()

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

type Task struct {
	TaskID  string `json:"task_id"`
	Action  string `json:"action"`
	Payload string `json:"payload"`
}

func handleConnection(w http.ResponseWriter, r *http.Request) {

	conn, err := upgrader.Upgrade(w, r, nil)

	if err != nil {
		log.Println("Error upgrading to websocket:", err)
		return
	}

	defer conn.Close()

	for {
		var task Task

		err := conn.ReadJSON(&task)

		if err != nil {
			log.Println("Error reading JSON:", err)
			break
		}

		err = redisClient.RPush(ctx, "task_queue", task.TaskID).Err()

		if err != nil {
			log.Println("Failed to push task to Redis:", err)
			break
		}

		log.Printf("Task %s received and added to queue\n", task.TaskID)

		response := map[string]string{"status": "received", "task_id": task.TaskID}
		conn.WriteJSON(response)

	}

}

func main() {

	redisClient = redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	http.HandleFunc("/ws", handleConnection)

	fmt.Println("Websocket server started on ws://localhost:8080/ws")

	log.Println(http.ListenAndServe(":8080", nil))

}
