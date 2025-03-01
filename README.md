# WebSocket-based Distributed Task queue


## Project Description:
- Clients submit tasks via Websockets.
- Task are stored in **Redis**.
- Acknowledgment message is sent whenever task is recived.
- processed message is sent to the WebSocket using pub/sub model. 

## Features:
- Distributes tasks to multiple workers to process faster.
- Uses redis for faster look up by caching.
- Used permessage-deflate extension to reduce bandwith by compressing WebSocket messages.
- Implemented a pub/sub model which notifies Clients when task is processed succesfully.
- Implemented a Dead Letter Queue(DLQ) where tasks go when it reaches max retries and can be debugged later.
- Added RESTful api to check tasks status.


## Installation & Setup:

1. Clone the repository:
   ```sh
   git clone https://github.com/yourusername/yourproject.git
   cd websocket-task-queue
   ```

2. Install required packages:
    ```sh
    go get "github.com/gorilla/mux"
	go get "github.com/gorilla/websocket"
	go get "github.com/redis/go-redis/v9"

    ```

3. Make sure redis is installed and running on your PC:
 
    ```sh
    docker ps
    ```
(If you are using docker to install redis).

4. Running the server:
    ```sh
    go run server.go
    ```
    The Server is running on port 8080 and accepts tasks in /ws route.


