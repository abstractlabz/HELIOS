package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"bufio"

	"github.com/0xPCDefenders/HELIOS/utils"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

//
// ─── TYPES & STRUCTS ────────────────────────────────────────────────────────────
//

type PromptPayload struct {
	Prompt string `json:"prompt"`
}

type QueryJob struct {
	ID      string
	Prompt  string
	Conn    *websocket.Conn
	Context map[string]interface{}
}

type QueryResult struct {
	JobID   string `json:"job_id"`
	Content string `json:"content"`
	Final   bool   `json:"final"`
	Error   string `json:"error,omitempty"`
}

// WorkerPool manages concurrent query processing
type WorkerPool struct {
	workers  int
	jobQueue chan *QueryJob
	timeout  time.Duration
	wg       sync.WaitGroup
}

//
// ─── INITIALIZATION ─────────────────────────────────────────────────────────────
//

func NewWorkerPool(numWorkers int) *WorkerPool {
	return &WorkerPool{
		workers: numWorkers,
		// Increased buffer so you can handle occasional bursts:
		jobQueue: make(chan *QueryJob, 200),
		// Match your HTTP client timeout:
		timeout: 90 * time.Second,
	}
}

func (wp *WorkerPool) Start() {
	for i := 0; i < wp.workers; i++ {
		wp.wg.Add(1)
		go wp.worker()
	}
}

//
// ─── WORKER LOOP ────────────────────────────────────────────────────────────────
//

func (wp *WorkerPool) worker() {
	defer wp.wg.Done()

	for job := range wp.jobQueue {
		// Create a context to bound the total work time
		ctx, cancel := context.WithTimeout(context.Background(), wp.timeout)

		// Run the entire query + streaming in this goroutine:
		response, err := processQueryWithStreaming(ctx, job)

		// Always write back a final JSON payload:
		final := &QueryResult{
			JobID: job.ID,
			Final: true,
		}
		if err != nil {
			final.Error = err.Error()
		} else {
			final.Content = response
		}
		if writeErr := job.Conn.WriteJSON(final); writeErr != nil {
			log.Printf("Error writing final JSON to websocket: %v", writeErr)
		}

		cancel()
	}
}

//
// ─── QUERY PROCESSING ────────────────────────────────────────────────────────────
//

func processQueryWithStreaming(ctx context.Context, job *QueryJob) (string, error) {
	// (All of your embedding + Pinecone search + prompt construction here,
	//  exactly as before—omitted for brevity.)
	searchInfo, err := utils.Search(job.Prompt)
	if err != nil {
		log.Printf("Error searching: %v", err)
		return "", err
	}

	prompt := fmt.Sprintf("Here is the search information: %s\n\n%s", searchInfo, job.Prompt)

	job.Prompt = prompt

	// At the end, call OpenAI with streaming, passing along the job.Conn
	return callOpenAIWithStreaming(ctx, job)
}

func callOpenAIWithStreaming(ctx context.Context, job *QueryJob) (string, error) {
	apiKey := os.Getenv("OPENAI_API_KEY")
	if apiKey == "" {
		return "", fmt.Errorf("OPENAI_API_KEY not set")
	}

	reqBody := map[string]interface{}{
		"model": "gpt-4o",
		"messages": []map[string]string{
			{"role": "system", "content": "You are a financial analyst. Answer the user's question as directly as possible and use as much of the search information provided as possible in your answer when relevant. Embed the search information in your answer in the format [title](url)."},
			{"role": "user", "content": job.Prompt},
		},
		"stream":      true,
		"max_tokens":  768,
		"temperature": 0.7,
	}
	jsonData, _ := json.Marshal(reqBody)

	req, _ := http.NewRequestWithContext(ctx, "POST", "https://api.openai.com/v1/chat/completions", bytes.NewBuffer(jsonData))
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 90 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("openai API error: %s", resp.Status)
	}

	var fullResponse strings.Builder
	scanner := bufio.NewScanner(resp.Body)

	for scanner.Scan() {
		line := strings.TrimPrefix(scanner.Text(), "data: ")
		if line == "[DONE]" {
			break
		}

		var chunk struct {
			Choices []struct {
				Delta struct{ Content string } `json:"delta"`
			} `json:"choices"`
		}
		if err := json.Unmarshal([]byte(line), &chunk); err != nil {
			continue
		}
		if len(chunk.Choices) > 0 {
			piece := chunk.Choices[0].Delta.Content
			// **ignore empty deltas** (e.g. role announcements)
			if piece == "" {
				continue
			}
			fullResponse.WriteString(piece)

			// stream partial back immediately:
			if err := job.Conn.WriteJSON(&QueryResult{
				JobID:   job.ID,
				Content: piece,
				Final:   false,
			}); err != nil {
				return "", err
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return "", err
	}

	return fullResponse.String(), nil
}

//
// ─── WEBSOCKET HANDLER ─────────────────────────────────────────────────────────
//

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		origin := r.Header.Get("Origin")
		return origin == "https://app.fineas.ai" ||
			origin == "https://test-fineas.netlify.app" ||
			origin == "http://localhost:3000"
	},
}

func handleWebSocketConnection(conn *websocket.Conn, wp *WorkerPool) {
	defer conn.Close()

	// read loop
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return
		}

		var job QueryJob
		if err := json.Unmarshal(msg, &job); err != nil {
			continue
		}
		job.Conn = conn

		// enqueue with 10 s backpressure timeout
		select {
		case wp.jobQueue <- &job:
		case <-time.After(10 * time.Second):
			conn.WriteJSON(map[string]string{
				"error": "Server busy, please try again",
			})
		}
	}
}

//
// ─── MAIN & ROUTES ─────────────────────────────────────────────────────────────
//

func main() {
	// load .env, init pinecone, etc. (omitted)

	wp := NewWorkerPool(60)
	wp.Start()

	router := gin.Default()
	router.GET("/chatbot/ws", func(c *gin.Context) {
		conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
		if err != nil {
			return
		}
		handleWebSocketConnection(conn, wp)
	})

	// 4) Create a TLS server on :6002
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	srv := &http.Server{
		Addr:      ":6002",
		Handler:   router,
		TLSConfig: tlsConfig,
		// tune timeouts as needed:
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  5 * time.Minute,
	}

	// graceful shutdown, TLS config, etc. (omitted)
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-quit
		log.Println("Shutting down server…")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			log.Fatalf("Server forced to shutdown: %v", err)
		}
	}()

	log.Println("Server listening on wss://llm.fineasapp.io:2087/chatbot/ws")
	if err := srv.ListenAndServeTLS("server.crt", "server.key"); err != nil && err != http.ErrServerClosed {
		log.Fatalf("ListenAndServeTLS: %v", err)
	}
}
