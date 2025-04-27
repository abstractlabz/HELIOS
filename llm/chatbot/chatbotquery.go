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

	"github.com/pinecone-io/go-pinecone/pinecone"

	"github.com/0xPCDefenders/HELIOS/utils"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"sync/atomic"

	"github.com/joho/godotenv"
)

type PromptPayload struct {
	Prompt string `json:"prompt"`
}

type CoursePromptPayload struct {
	Prompt     string `json:"prompt"`
	Idhash     string `json:"idhash"`
	Coursehash string `json:"coursehash"`
}

type UserParams struct {
	ExperienceLevel  string `json:"experiencelevel"`
	Age              string `json:"age"`
	QuestioningStyle string `json:"questioningstyle"`
	InteractionSpeed string `json:"interactionspeed"`
	FeedbackStyle    string `json:"feedbackstyle"`
	SocraticDepth    string `json:"socraticdepth"`
}

type CourseHost struct {
	PineconeHost string `json:"pineconehost"`
}

// WorkerPool manages concurrent query processing
type WorkerPool struct {
	workers  int
	jobQueue chan *QueryJob
	results  chan *QueryResult
	wg       sync.WaitGroup
}

type QueryJob struct {
	ID      string
	Prompt  string
	Conn    *websocket.Conn
	Context map[string]interface{}
}

type QueryResult struct {
	JobID   string
	Content string
	Final   bool
	Error   error
}

// ConnectionManager handles WebSocket connections
type ConnectionManager struct {
	connections sync.Map
	maxConns    int
	connCount   int32
	mu          sync.Mutex
	// Add monitoring fields
	lastCleanup time.Time
	stats       struct {
		totalConnections    int64
		activeConnections   int64
		rejectedConnections int64
		closedConnections   int64
	}
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		origin := r.Header.Get("Origin")
		log.Printf("WebSocket connection attempt from origin: %s", origin)
		// Allow connections from test-fineas.netlify.app
		// Allow connections from app.fineas.ai
		// Allow connections from localhost:3000
		return origin == "https://test-fineas.netlify.app" || origin == "https://app.fineas.ai" || origin == "http://localhost:3000"
	},
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

var (
	index      *pinecone.IndexConnection
	indexMutex sync.RWMutex
)

// Add these type definitions:
type DeepSeekRequest struct {
	Model     string        `json:"model"`
	Messages  []ChatMessage `json:"messages"`
	Stream    bool          `json:"stream"`
	MaxTokens int           `json:"max_tokens,omitempty"`
}

type ChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// Add this type to track connection activity
type trackedConnection struct {
	conn       *websocket.Conn
	lastActive time.Time
}

// Update the request type
type OpenAIRequest struct {
	Model       string        `json:"model"`
	Messages    []ChatMessage `json:"messages"`
	Stream      bool          `json:"stream"`
	MaxTokens   int           `json:"max_tokens,omitempty"`
	Temperature float32       `json:"temperature,omitempty"`
}

func initPineconeIndex(apiKey, host string) error {
	indexMutex.Lock()
	defer indexMutex.Unlock()

	// Initialize Pinecone client
	pc, err := pinecone.NewClient(pinecone.NewClientParams{
		ApiKey: apiKey,
		Host:   host,
	})
	if err != nil {
		return fmt.Errorf("failed to initialize Pinecone client: %v", err)
	}

	// Initialize Pinecone index
	idx, err := pc.Index(pinecone.NewIndexConnParams{
		Host: host,
	})
	if err != nil {
		return fmt.Errorf("failed to initialize Pinecone index2: %v", err)
	}

	index = idx
	return nil
}

func NewWorkerPool(numWorkers int) *WorkerPool {
	return &WorkerPool{
		workers:  numWorkers,
		jobQueue: make(chan *QueryJob, 100),
		results:  make(chan *QueryResult, 100),
	}
}

func (wp *WorkerPool) Start() {
	for i := 0; i < wp.workers; i++ {
		wp.wg.Add(1)
		go wp.worker()
	}
}

func (wp *WorkerPool) worker() {
	defer wp.wg.Done()

	for job := range wp.jobQueue {
		// Distinguish "course" from "regular" if needed
		if coursehashAny, ok := job.Context["coursehash"]; ok {
			// This is a course chat job
			result, err := processCourseQueryWithStreaming(job, coursehashAny.(string))
			if err != nil {
				wp.results <- &QueryResult{JobID: job.ID, Error: err, Final: true}
				continue
			}
			if err := job.Conn.WriteJSON(result); err != nil {
				log.Printf("Error writing final chunk to websocket: %v", err)
			}
		} else {
			// Normal chat
			result, err := processQueryWithStreaming(job)
			if err != nil {
				wp.results <- &QueryResult{JobID: job.ID, Error: err, Final: true}
				continue
			}
			if err := job.Conn.WriteJSON(result); err != nil {
				log.Printf("Error writing final chunk to websocket: %v", err)
			}
		}
	}
}

func processQueryWithStreaming(job *QueryJob) (*QueryResult, error) {
	log.Printf("Starting query processing for job ID: %s", job.ID)

	// Initialize ENVs
	pineApiKey := os.Getenv("PINECONE_API_KEY")
	if pineApiKey == "" {
		log.Printf("Error: PINECONE_API_KEY is empty")
		return nil, fmt.Errorf("PINECONE_API_KEY is empty")
	}

	queryVector, err := embedQuery(job.Prompt, pineApiKey)
	if err != nil {
		log.Printf("Failed to embed query: %v", err)
		return nil, err
	}

	ctx := context.Background()
	searchLimit := uint32(2)
	searchRes, err := index.QueryByVectorValues(ctx, &pinecone.QueryByVectorValuesRequest{
		Vector:          queryVector,
		TopK:            searchLimit,
		IncludeValues:   true,
		IncludeMetadata: true,
	})
	if err != nil {
		log.Printf("Failed to perform similarity search: %v", err)
		return nil, err
	}

	var contextData []interface{}
	if len(searchRes.Matches) > 0 {
		vectorIds := make([]string, len(searchRes.Matches))
		for i, match := range searchRes.Matches {
			vectorIds[i] = match.Vector.Id
		}
		fetchRes, err := index.FetchVectors(ctx, vectorIds)
		if err != nil {
			log.Printf("Failed to fetch vectors: %v", err)
			return nil, err
		}

		for _, vector := range fetchRes.Vectors {
			if vector.Metadata != nil {
				contextData = append(contextData, vector.Metadata)
			}
		}
	}

	searchInfo, err := utils.Search(job.Prompt)
	if err != nil {
		log.Printf("Search error: %v", err)
		return nil, err
	}

	combinedPrompt := fmt.Sprintf(`
		You are a financial analyst. Use the following external search information to answer user questions.
		You will also include only the most relevant links and descriptions from the search results. As much as possible
		if relevant, include the links and descriptions in your response in the format [Title](URL).
		%s

		The user's prompt is:
		%s
	`,
		prettifyStruct(searchInfo),
		job.Prompt,
	)

	job.Prompt = combinedPrompt

	response, err := callOpenAIWithStreaming(job)
	if err != nil {
		log.Printf("OpenAI streaming error: %v", err)
		return nil, err
	}

	log.Printf("Successfully completed query processing for job ID: %s", job.ID)
	return &QueryResult{
		JobID:   job.ID,
		Content: response,
		Final:   true,
	}, nil
}

func callOpenAIWithStreaming(job *QueryJob) (string, error) {
	apiKey := os.Getenv("OPENAI_API_KEY")
	if apiKey == "" {
		log.Printf("Error: OPENAI_API_KEY is empty")
		return "", fmt.Errorf("OPENAI_API_KEY not set")
	}

	url := "https://api.openai.com/v1/chat/completions"
	request := OpenAIRequest{
		Model: "gpt-4o",
		Messages: []ChatMessage{
			{
				Role:    "system",
				Content: "You are a financial analyst. Analyze the following financial data...",
			},
			{
				Role:    "user",
				Content: job.Prompt,
			},
		},
		Stream:      true,
		MaxTokens:   768,
		Temperature: 0.7,
	}

	jsonData, err := json.Marshal(request)
	if err != nil {
		log.Printf("Error marshaling request: %v", err)
		return "", fmt.Errorf("error marshaling request: %v", err)
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		log.Printf("Error creating request: %v", err)
		return "", fmt.Errorf("error creating request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)

	client := &http.Client{Timeout: 90 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Error making request: %v", err)
		return "", fmt.Errorf("error making request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("API request failed with status %d", resp.StatusCode)
		return "", fmt.Errorf("API request failed with status %d", resp.StatusCode)
	}

	var fullResponse strings.Builder
	scanner := bufio.NewScanner(resp.Body)

	for scanner.Scan() {
		rawLine := scanner.Text()
		if rawLine == "" {
			continue
		}
		rawLine = strings.TrimPrefix(rawLine, "data: ")

		if rawLine == "[DONE]" {
			break
		}

		var chunk struct {
			Choices []struct {
				Delta struct {
					Content string `json:"content"`
				} `json:"delta"`
			} `json:"choices"`
		}
		if err := json.Unmarshal([]byte(rawLine), &chunk); err != nil {
			log.Printf("Error parsing chunk: %v", err)
			continue
		}

		if len(chunk.Choices) > 0 && chunk.Choices[0].Delta.Content != "" {
			piece := chunk.Choices[0].Delta.Content
			fullResponse.WriteString(piece)

			partialResult := &QueryResult{
				JobID:   job.ID,
				Content: piece,
				Final:   false,
			}
			if err := job.Conn.WriteJSON(partialResult); err != nil {
				log.Printf("Error writing to websocket: %v", err)
				return "", err
			}
		}
	}

	if err := scanner.Err(); err != nil {
		log.Printf("Error reading stream: %v", err)
		return "", fmt.Errorf("error reading stream: %v", err)
	}

	return fullResponse.String(), nil
}

func processCourseQueryWithStreaming(job *QueryJob, coursehash string) (*QueryResult, error) {
	log.Println("processCourseQueryWithStreaming -> original prompt:", job.Prompt)

	// 1) Retrieve Pinecone host for this coursehash and re-initialize the index.
	hoststring := getPineconeHost(coursehash)

	// Initialize ENVs
	pineApiKey := os.Getenv("PINECONE_API_KEY")

	if err := initPineconeIndex(pineApiKey, hoststring); err != nil {
		log.Println("Failed to initialize Pinecone index:", err)
		return nil, err
	}

	// 2) Get user parameters from MongoDB based on idhash in the job context
	idhash, ok := job.Context["idhash"].(string)
	if !ok {
		log.Println("Error: No valid idhash found in job context")
		return nil, fmt.Errorf("missing or invalid idhash in job context")
	}

	userParams := getUserParams(idhash)
	log.Println("User parameters for idhash", idhash, ":", prettifyStruct(userParams))

	// 3) Embed the user's prompt

	queryVector, err := embedQuery(job.Prompt, pineApiKey)
	if err != nil {
		log.Println("Failed to embed query:", err)
		return nil, err
	}
	log.Println("Query vector:", queryVector)

	// 4) Perform a similarity search in Pinecone
	ctx := context.Background()
	searchLimit := uint32(2) // Number of similar documents to retrieve
	searchRes, err := index.QueryByVectorValues(ctx, &pinecone.QueryByVectorValuesRequest{
		Vector:          queryVector,
		TopK:            searchLimit,
		IncludeValues:   true,
		IncludeMetadata: true,
	})
	if err != nil {
		log.Println("Failed to perform similarity search:", err)
		return nil, err
	}
	log.Println("Search results:", prettifyStruct(searchRes))

	// 5) Fetch the matching vectors to gather context
	var contextData []interface{}
	if len(searchRes.Matches) > 0 {
		vectorIds := make([]string, len(searchRes.Matches))
		for i, match := range searchRes.Matches {
			vectorIds[i] = match.Vector.Id
		}
		fetchRes, err := index.FetchVectors(ctx, vectorIds)
		if err != nil {
			log.Println("Failed to fetch vectors:", err)
			return nil, err
		}
		log.Println("Fetched vectors:", prettifyStruct(fetchRes))

		for _, vector := range fetchRes.Vectors {
			if vector.Metadata != nil {
				log.Println("Vector metadata:", vector.Metadata)
				contextData = append(contextData, vector.Metadata)
			} else {
				log.Println("No metadata found for vector ID:", vector.Id)
			}
		}
	}

	contextString := prettifyStruct(contextData)
	log.Println("Context string:", contextString)

	// 6) As an example, also do a Google search for outside context
	searchInfo, err := utils.Search(job.Prompt)
	if err != nil {
		log.Printf("Search error: %v\n", err)
		return nil, err
	}

	// 7) Build a combined prompt that includes user parameters, fetched Pinecone context, and any external search info
	combinedPrompt := fmt.Sprintf(`
		You are a financial analyst. 
		
		Adapt your teaching style to these user parameters:
		- Experience Level: %s
		- Age: %s
		- Questioning Style: %s
		- Interaction Speed: %s
		- Feedback Style: %s
		- Socratic Depth: %s
		
		Always include as many relevant external search information as possible. Use the format [Title](URL) in your response:
		%s

		Relevant Pinecone context:
		%s

		External search information:
		%s

		The user's prompt is:
		%s
	`,
		userParams.ExperienceLevel,
		userParams.Age,
		userParams.QuestioningStyle,
		userParams.InteractionSpeed,
		userParams.FeedbackStyle,
		userParams.SocraticDepth,
		prettifyStruct(searchInfo),
		contextString,
		prettifyStruct(searchInfo), // Added search info here
		job.Prompt,
	)
	job.Prompt = combinedPrompt

	// 8) Stream final results from DeepSeek
	response, err := callOpenAIWithStreaming(job)
	if err != nil {
		return nil, err
	}

	// 9) Return the final QueryResult
	return &QueryResult{
		JobID:   job.ID,
		Content: response,
		Final:   true,
	}, nil
}

func ChatbotQuery() http.Handler {
	// Load environment variables from .env file
	err := godotenv.Load(".env")
	if err != nil {
		// Try a few other common locations
		alternativePaths := []string{"../.env", "../../.env", "../config/.env", "./config/.env"}
		loaded := false
		for _, path := range alternativePaths {
			if err := godotenv.Load(path); err == nil {
				log.Printf("Loaded environment from %s", path)
				loaded = true
				break
			}
		}
		if !loaded {
			log.Println("Warning: .env file not found. Proceeding with system environment variables.")
		}
	}

	router := gin.Default()
	router.Use(corsMiddleware())

	// Initialize ENVs
	pineApiKey := os.Getenv("PINECONE_API_KEY")
	pineHost := os.Getenv("PINECONE_HOST")

	// Log the key (first few characters only for security)
	if pineApiKey != "" {
		visiblePart := pineApiKey
		if len(pineApiKey) > 4 {
			visiblePart = pineApiKey[:4] + "..."
		}
		log.Printf("Pinecone API Key found: %s", visiblePart)
	} else {
		log.Println("Warning: PINECONE_API_KEY is empty")
	}

	// Initialize Pinecone index
	if err := initPineconeIndex(
		pineApiKey,
		pineHost,
	); err != nil {
		log.Fatalf("Failed to initialize Pinecone index: %v", err)
	}

	// Create connection manager with limits
	connManager := NewConnectionManager(1000) // Maximum concurrent connections

	wp := NewWorkerPool(5)
	wp.Start()

	// WebSocket endpoint for streaming results
	router.GET("/chatbot/ws", func(c *gin.Context) {
		log.Printf("Received WebSocket upgrade request from %s", c.Request.RemoteAddr)
		log.Printf("Request headers: %v", c.Request.Header)
		log.Printf("Request method: %s", c.Request.Method)
		log.Printf("Request URL: %s", c.Request.URL.String())

		// Set WebSocket headers
		c.Writer.Header().Set("Upgrade", "websocket")
		c.Writer.Header().Set("Connection", "Upgrade")

		conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
		if err != nil {
			log.Printf("Failed to upgrade connection: %v", err)
			return
		}
		defer conn.Close()

		log.Printf("WebSocket connection established with %s", c.Request.RemoteAddr)

		// Handle WebSocket connection
		handleWebSocketConnection(conn, wp, connManager)
	})

	// WebSocket route for "course chat"
	router.GET("/chatbot/coursews", func(c *gin.Context) {
		conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
		if err != nil {
			log.Printf("Failed to upgrade connection: %v", err)
			return
		}
		defer conn.Close()
		handleCourseWebSocketConnection(conn, wp, connManager)
	})

	return router
}

func prettifyStruct(obj interface{}) string {
	bytes, err := json.MarshalIndent(obj, "", "  ")
	if err != nil {
		log.Printf("Error marshaling struct: %v", err)
		return "{}"
	}
	return string(bytes)
}

func corsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "https://test-fineas.netlify.app")
		c.Writer.Header().Set("Access-Control-Allow-Origin", "http://localhost:3000")
		c.Writer.Header().Set("Access-Control-Allow-Origin", "https://app.fineas.ai")
		c.Writer.Header().Set("Access-Control-Allow-Credentials", "true")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, Upgrade, Connection, Sec-WebSocket-Key, Sec-WebSocket-Version, Sec-WebSocket-Extensions")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS, PUT, DELETE")
		c.Writer.Header().Set("Access-Control-Expose-Headers", "Upgrade, Connection")
		c.Writer.Header().Set("Access-Control-Max-Age", "86400")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}

		c.Next()
	}
}

func embedQuery(prompt, apiKey string) ([]float32, error) {
	ctx := context.Background()

	// Create a new Pinecone client
	pc, err := pinecone.NewClient(pinecone.NewClientParams{
		ApiKey: apiKey,
	})
	if err != nil {
		log.Printf("Failed to create Pinecone client: %v", err)
		return nil, fmt.Errorf("failed to create Pinecone client: %v", err)
	}

	embeddingModel := "multilingual-e5-large"
	queryParameters := pinecone.EmbedParameters{
		InputType: "query",
		Truncate:  "END",
	}

	// Embed the query using Pinecone's inference API
	queryEmbeddingsResponse, err := pc.Inference.Embed(ctx, &pinecone.EmbedRequest{
		Model:      embeddingModel,
		TextInputs: []string{prompt},
		Parameters: queryParameters,
	})
	if err != nil {
		log.Printf("Failed to embed query: %v", err)
		return nil, fmt.Errorf("failed to embed query: %v", err)
	}

	// Assuming the response contains embeddings in a similar structure
	if len(*queryEmbeddingsResponse.Data) == 0 {
		log.Println("No embedding data found in response")
		return nil, fmt.Errorf("no embedding data found")
	}

	// Log and return the embedding data
	//log.Println("Embedding data:", (*queryEmbeddingsResponse.Data)[0].Values)
	return *(*queryEmbeddingsResponse.Data)[0].Values, nil
}

func getUserParams(s string) UserParams {
	//connects to mongo db and gets the pinecone host based on the coursehash
	MONGO_DB_LOGGER_PASSWORD := os.Getenv("MONGO_DB_LOGGER_PASSWORD")
	serverAPI := options.ServerAPI(options.ServerAPIVersion1)
	opts := options.Client().ApplyURI("mongodb+srv://kobenaidun:" + MONGO_DB_LOGGER_PASSWORD + "@cluster0.z9znpv9.mongodb.net/?retryWrites=true&w=majority").SetServerAPIOptions(serverAPI)
	// Create a new client and connect to the server
	client, err := mongo.Connect(context.TODO(), opts)
	if err != nil {
		log.Println("Couldn't connect to database")
		panic(err)
	}

	defer client.Disconnect(context.TODO())

	collection := client.Database("Courses").Collection("UserParams")

	var userParams UserParams
	err = collection.FindOne(context.TODO(), bson.M{"id_hash": s}).Decode(&userParams)

	//based on the document, return a json object of the user params
	json.Marshal(userParams)

	return userParams
}

func getPineconeHost(coursehash string) string {
	//connects to mongo db and gets the pinecone host based on the coursehash
	MONGO_DB_LOGGER_PASSWORD := os.Getenv("MONGO_DB_LOGGER_PASSWORD")
	if MONGO_DB_LOGGER_PASSWORD == "" {
		log.Println("Warning: MONGO_DB_LOGGER_PASSWORD environment variable not set. Using default Pinecone host.")
		return "https://main-uajrq2f.svc.aped-4627-b74a.pinecone.io" // Default fallback host
	}

	serverAPI := options.ServerAPI(options.ServerAPIVersion1)
	opts := options.Client().ApplyURI("mongodb+srv://kobenaidun:" + MONGO_DB_LOGGER_PASSWORD + "@cluster0.z9znpv9.mongodb.net/?retryWrites=true&w=majority").SetServerAPIOptions(serverAPI)

	// Create a new client and connect to the server
	client, err := mongo.Connect(context.TODO(), opts)
	if err != nil {
		log.Println("Error connecting to MongoDB:", err)
		log.Println("Using default Pinecone host instead.")
		return "https://main-uajrq2f.svc.aped-4627-b74a.pinecone.io" // Default fallback host
	}

	defer client.Disconnect(context.TODO())

	db := client.Database("Courses")
	collectioncli := db.Collection("CoursePineconeHosts")

	//finds the pinecone host based on the coursehash
	var hostdoc bson.M
	err = collectioncli.FindOne(context.TODO(), bson.M{"CourseHash": coursehash}).Decode(&hostdoc)
	if err != nil {
		log.Println("Error finding Pinecone host for coursehash", coursehash, ":", err)
		log.Println("Using default Pinecone host instead.")
		return "https://main-uajrq2f.svc.aped-4627-b74a.pinecone.io" // Default fallback host
	}

	// Ensure type assertion is safe
	hoststring, ok := hostdoc["PineconeHost"].(string)
	if !ok {
		log.Println("Error: PineconeHost field is not a string. Using default host.")
		return "https://main-uajrq2f.svc.aped-4627-b74a.pinecone.io" // Default fallback host
	}

	return hoststring
}

func handleWebSocketConnection(conn *websocket.Conn, wp *WorkerPool, connManager *ConnectionManager) {
	// Check if we can accept more connections
	if !connManager.AddConnection(conn) {
		log.Printf("Connection rejected: maximum connections reached")
		conn.WriteMessage(websocket.CloseMessage, []byte("Server is at capacity"))
		conn.Close()
		return
	}
	defer connManager.RemoveConnection(conn)
	defer conn.Close() // Ensure connection is closed

	log.Printf("New WebSocket connection established from %s", conn.RemoteAddr().String())

	// Set read and write deadlines
	conn.SetReadDeadline(time.Now().Add(600 * time.Second))
	conn.SetWriteDeadline(time.Now().Add(600 * time.Second))

	// Set up ping/pong handler
	conn.SetPingHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return conn.WriteControl(websocket.PongMessage, []byte{}, time.Now().Add(10*time.Second))
	})

	// Set up pong handler
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	// Start ping ticker
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	// Start a goroutine to send periodic pings
	go func() {
		for range ticker.C {
			if err := conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(10*time.Second)); err != nil {
				log.Printf("Error sending ping: %v", err)
				return
			}
		}
	}()

	// Create a context for this connection
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Channel to signal when the connection should be closed
	done := make(chan struct{})

	// Start a goroutine to monitor the connection
	go func() {
		select {
		case <-ctx.Done():
			// Context was cancelled
		case <-done:
			// Normal closure
		}
		conn.Close()
	}()

	for {
		// Read message from WebSocket
		_, message, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("Error reading websocket message: %v", err)
			}
			break
		}

		log.Printf("Received WebSocket message: %s", string(message))

		// Parse the query
		var query QueryJob
		if err := json.Unmarshal(message, &query); err != nil {
			log.Printf("Error unmarshaling query: %v", err)
			continue
		}

		// Add connection to the job
		query.Conn = conn

		// Submit job to worker pool with timeout
		select {
		case wp.jobQueue <- &query:
			// Job submitted successfully
		case <-time.After(5 * time.Second):
			log.Printf("Timeout submitting job to worker pool")
			conn.WriteMessage(websocket.TextMessage, []byte(`{"error": "Server busy, please try again"}`))
		}
	}

	// Signal that we're done
	close(done)
}

func handleCourseWebSocketConnection(conn *websocket.Conn, wp *WorkerPool, connManager *ConnectionManager) {
	for {
		// Read next message from WebSocket
		_, message, err := conn.ReadMessage()
		if err != nil {
			log.Printf("Error reading websocket message: %v", err)
			break
		}

		// We expect JSON containing "prompt", "idhash", "coursehash", etc.
		var courseJob struct {
			ID         string `json:"id"`
			Prompt     string `json:"prompt"`
			Idhash     string `json:"idhash"`
			Coursehash string `json:"coursehash"`
		}
		if err := json.Unmarshal(message, &courseJob); err != nil {
			log.Printf("Error unmarshaling course job: %v", err)
			continue
		}

		// Check passkey just like in your POST /coursechat
		//passhash := conn.RemoteAddr().String() // or you can't get HTTP headers easily with gorilla/websocket
		// In practice, you might embed the hash in your JSON or define a subprotocol handshake.

		// For demonstration, let's skip the passkey check or do it from a field in courseJob
		// If you need passkey:
		//   courseJob.Passhash = ...
		//   compare with os.Getenv("PASS_KEY") hashed, etc.

		// For each incoming prompt, build a QueryJob to process streaming
		job := &QueryJob{
			ID:     courseJob.ID,
			Prompt: courseJob.Prompt,
			Conn:   conn,
			Context: map[string]interface{}{
				"idhash":     courseJob.Idhash,
				"coursehash": courseJob.Coursehash,
			},
		}

		// Post the job to the WorkerPool
		wp.jobQueue <- job
	}
}

func main() {
	// Load environment variables from .env file
	if err := godotenv.Load(); err != nil {
		// Try alternative locations
		locations := []string{".env", "../.env", "../../.env", "config/.env", "../config/.env"}
		loaded := false
		for _, loc := range locations {
			if err := godotenv.Load(loc); err == nil {
				log.Printf("Loaded environment variables from %s", loc)
				loaded = true
				break
			}
		}
		if !loaded {
			log.Println("Warning: No .env file found. Using environment variables from system.")
		}
	} else {
		log.Println("Loaded environment variables from .env in current directory")
	}

	// Initialize Gin router
	router := gin.Default()

	// Add CORS middleware
	router.Use(corsMiddleware())

	// Initialize ENVs
	pineApiKey := os.Getenv("PINECONE_API_KEY")
	pineHost := os.Getenv("PINECONE_HOST")

	// If environment variables are empty, set default values or warn
	if pineApiKey == "" {
		log.Println("ERROR: PINECONE_API_KEY environment variable is not set")
		// Optionally exit the program
		// os.Exit(1)
	}

	if pineHost == "" {
		log.Println("WARNING: PINECONE_HOST environment variable is not set, using default host")
		pineHost = "https://api.pinecone.io" // Default value, adjust if needed
	}

	fmt.Println("pineApiKey:", pineApiKey)
	fmt.Println("pineHost:", pineHost)

	// Create connection manager with limits
	// Adjust these values based on your server's resources
	maxConns := 1000 // Maximum concurrent connections
	connManager := NewConnectionManager(maxConns)

	// Instead of wrapping your entire sub-engine, register routes:
	RegisterChatbotRoutes(router, pineApiKey, pineHost, connManager)

	// Create TLS config with more robust settings
	tlsConfig := &tls.Config{
		MinVersion:               tls.VersionTLS12,
		PreferServerCipherSuites: true,
		CipherSuites: []uint16{
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
		},
	}

	// Create HTTPS server with connection management settings
	server := &http.Server{
		Addr:      ":6002",
		Handler:   router,
		TLSConfig: tlsConfig,
		// Add connection management settings
		ReadTimeout:    30 * time.Second,
		WriteTimeout:   30 * time.Second,
		IdleTimeout:    600 * time.Second,
		MaxHeaderBytes: 1 << 20, // 1MB
	}

	// Set up graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-quit
		log.Println("Shutting down server...")

		// Create shutdown context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := server.Shutdown(ctx); err != nil {
			log.Fatal("Server forced to shutdown:", err)
		}
	}()

	// Start server with TLS using local certificate files
	log.Printf("Server starting on port 6002 with TLS")
	if err := server.ListenAndServeTLS("server.crt", "server.key"); err != nil && err != http.ErrServerClosed {
		log.Fatal("Failed to start server:", err)
	}
}

func RegisterChatbotRoutes(r *gin.Engine, pineApiKey, pineHost string, connManager *ConnectionManager) {
	// 1) Use the same CORS middleware if needed
	r.Use(corsMiddleware())

	// 2) Initialize Pinecone index once
	if err := initPineconeIndex(
		pineApiKey,
		pineHost,
	); err != nil {
		log.Fatalf("Failed to initialize Pinecone index: %v", err)
	}

	// 3) Start your WorkerPool
	wp := NewWorkerPool(5)
	wp.Start()

	// 4) WebSocket endpoint at /chatbot/ws
	r.GET("/chatbot/ws", func(c *gin.Context) {
		conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
		if err != nil {
			log.Printf("Failed to upgrade connection: %v", err)
			return
		}
		defer conn.Close()

		handleWebSocketConnection(conn, wp, connManager)
	})

	// 5) GET /chatbot/coursews
	r.GET("/chatbot/coursews", func(c *gin.Context) {
		conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
		if err != nil {
			log.Printf("Failed to upgrade connection: %v", err)
			return
		}
		defer conn.Close()

		handleCourseWebSocketConnection(conn, wp, connManager)
	})
}

func NewConnectionManager(maxConns int) *ConnectionManager {
	cm := &ConnectionManager{
		maxConns:    maxConns,
		lastCleanup: time.Now(),
	}
	// Start cleanup goroutine
	go cm.cleanupRoutine()
	return cm
}

func (cm *ConnectionManager) cleanupRoutine() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		cm.cleanupStaleConnections()
	}
}

func (cm *ConnectionManager) cleanupStaleConnections() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	now := time.Now()
	cm.connections.Range(func(key, value interface{}) bool {
		tc := key.(trackedConnection)
		// Check if connection is stale (no activity for 2 minutes)
		if now.Sub(tc.lastActive) > 2*time.Minute {
			tc.conn.Close()
			cm.connections.Delete(tc)
			atomic.AddInt32(&cm.connCount, -1)
			atomic.AddInt64(&cm.stats.closedConnections, 1)
		}
		return true
	})
}

func (cm *ConnectionManager) AddConnection(conn *websocket.Conn) bool {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if int(atomic.LoadInt32(&cm.connCount)) >= cm.maxConns {
		atomic.AddInt64(&cm.stats.rejectedConnections, 1)
		return false
	}

	tc := trackedConnection{
		conn:       conn,
		lastActive: time.Now(),
	}
	cm.connections.Store(tc, true)
	atomic.AddInt32(&cm.connCount, 1)
	atomic.AddInt64(&cm.stats.totalConnections, 1)
	atomic.AddInt64(&cm.stats.activeConnections, 1)

	log.Printf("Connection added. Total connections: %d, Active: %d, Rejected: %d, Closed: %d",
		atomic.LoadInt32(&cm.connCount),
		atomic.LoadInt64(&cm.stats.activeConnections),
		atomic.LoadInt64(&cm.stats.rejectedConnections),
		atomic.LoadInt64(&cm.stats.closedConnections))
	return true
}

func (cm *ConnectionManager) RemoveConnection(conn *websocket.Conn) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if _, exists := cm.connections.LoadAndDelete(trackedConnection{conn: conn}); exists {
		atomic.AddInt32(&cm.connCount, -1)
		atomic.AddInt64(&cm.stats.activeConnections, -1)
		log.Printf("Connection removed. Total connections: %d, Active: %d",
			atomic.LoadInt32(&cm.connCount),
			atomic.LoadInt64(&cm.stats.activeConnections))
	}
}
