package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"bufio"

	"github.com/pinecone-io/go-pinecone/pinecone"

	"github.com/0xPCDefenders/HELIOS/utils"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

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
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // Adjust based on your security needs
	},
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
	log.Println("processQueryWithStreaming -> original prompt:", job.Prompt)

	//pinecone operations
	// Embedding the user's prompt

	// Initialize ENVs
	pineApiKey := os.Getenv("PINECONE_API_KEY")

	queryVector, err := embedQuery(job.Prompt, pineApiKey)
	if err != nil {
		log.Println("Failed to embed query:", err)
		return nil, err
	}

	log.Println("Query vector:", queryVector)

	ctx := context.Background()
	// Perform a similarity search in Pinecone
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

	// Once we have vector IDs, we can fetch their metadata to get the text
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

		// Log the fetched vectors to debug
		log.Println("Fetched vectors:", prettifyStruct(fetchRes))

		// Extract the metadata (which should contain your text)
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

	//google search operations

	searchInfo, err := utils.Search(job.Prompt)
	if err != nil {
		log.Printf("Search error: %v\n", err)
		return nil, err
	}

	// 2) Build a combined prompt that includes the search info
	combinedPrompt := fmt.Sprintf(`
		You are a financial analyst. Use the following external search information to answer user questions:
		%s

		The user's prompt is:
		%s
	`,
		prettifyStruct(searchInfo), // or however you want to embed
		job.Prompt,
	)

	// 3) Reassign job.Prompt to include the search info
	job.Prompt = combinedPrompt

	// 4) Pass this new job.Prompt to DeepSeek. Then stream partial results.
	response, err := callDeepSeekAPIWithStreaming(job)
	if err != nil {
		log.Printf("DeepSeek streaming error: %v\n", err)
		return nil, err
	}

	// 5) Return the final QueryResult
	return &QueryResult{
		JobID:   job.ID,
		Content: response,
		Final:   true,
	}, nil
}

func callDeepSeekAPIWithStreaming(job *QueryJob) (string, error) {
	apiKey := os.Getenv("DEEPSEEK_API_KEY")
	if apiKey == "" {
		return "", fmt.Errorf("DEEPSEEK_API_KEY not set")
	}

	// Prepare our request
	url := "https://api.deepseek.com/chat/completions"
	request := DeepSeekRequest{
		Model: "deepseek-chat",
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
		Stream:    true,
		MaxTokens: 512, // Set fixed limit of 768 tokens
	}

	jsonData, err := json.Marshal(request)
	if err != nil {
		return "", fmt.Errorf("error marshaling request: %v", err)
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return "", fmt.Errorf("error creating request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)

	client := &http.Client{Timeout: 90 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("error making request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		return "", fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(body))
	}

	// We'll accumulate the entire response for "final" display,
	// but also send partial data to the client as soon as each chunk arrives.
	var fullResponse strings.Builder
	scanner := bufio.NewScanner(resp.Body)

	// Reading the chunked stream line by line
	for scanner.Scan() {
		rawLine := scanner.Text()
		log.Println("Raw line from DeepSeek:", rawLine)
		if rawLine == "" {
			continue
		}
		// Remove "data: " prefix
		rawLine = strings.TrimPrefix(rawLine, "data: ")

		// "[DONE]" signals the end of streaming
		if rawLine == "[DONE]" {
			break
		}

		// Parse JSON chunk
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

		// If there's content, send it over WebSocket now
		if len(chunk.Choices) > 0 && chunk.Choices[0].Delta.Content != "" {
			piece := chunk.Choices[0].Delta.Content
			fullResponse.WriteString(piece) // accumulate

			// Send partial chunk to the WebSocket
			partialResult := &QueryResult{
				JobID:   job.ID,
				Content: piece,
				Final:   false, // not final
			}
			if err := job.Conn.WriteJSON(partialResult); err != nil {
				log.Printf("Error writing partial chunk to websocket: %v", err)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return "", fmt.Errorf("error reading stream: %v", err)
	}

	// Return the entire combined string after the stream is done
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
	response, err := callDeepSeekAPIWithStreaming(job)
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

	wp := NewWorkerPool(5)
	wp.Start()

	// WebSocket endpoint for streaming results
	router.GET("/chatbot/ws", func(c *gin.Context) {
		conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
		if err != nil {
			log.Printf("Failed to upgrade connection: %v", err)
			return
		}
		defer conn.Close()

		// Handle WebSocket connection
		handleWebSocketConnection(conn, wp)
	})

	// ADD THIS NEW WebSocket route for "course chat"
	router.GET("/chatbot/coursews", func(c *gin.Context) {
		conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
		if err != nil {
			log.Printf("Failed to upgrade connection: %v", err)
			return
		}
		defer conn.Close()
		handleCourseWebSocketConnection(conn, wp)
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
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Allow-Credentials", "true")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")

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
	log.Println("Embedding data:", (*queryEmbeddingsResponse.Data)[0].Values)
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

func handleWebSocketConnection(conn *websocket.Conn, wp *WorkerPool) {
	for {
		// Read message from WebSocket
		_, message, err := conn.ReadMessage()
		if err != nil {
			log.Printf("Error reading websocket message: %v", err)
			break
		}

		// Parse the query
		var query QueryJob
		if err := json.Unmarshal(message, &query); err != nil {
			log.Printf("Error unmarshaling query: %v", err)
			continue
		}

		// Add connection to the job
		query.Conn = conn

		// Submit job to worker pool
		wp.jobQueue <- &query
	}
}

func handleCourseWebSocketConnection(conn *websocket.Conn, wp *WorkerPool) {
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

	// Instead of wrapping your entire sub-engine, register routes:
	RegisterChatbotRoutes(router, pineApiKey, pineHost)

	// Start server
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	log.Printf("Server starting on port %s", port)
	if err := http.ListenAndServe(":"+port, router); err != nil {
		log.Fatal(err)
	}
}

func RegisterChatbotRoutes(r *gin.Engine, pineApiKey, pineHost string) {
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

		// Handle WebSocket connection
		handleWebSocketConnection(conn, wp)
	})

	// 5) GET /chatbot/coursews
	r.GET("/chatbot/coursews", func(c *gin.Context) {
		conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
		if err != nil {
			log.Printf("Failed to upgrade connection: %v", err)
			return
		}
		defer conn.Close()

		// handleCourseWebSocketConnection is your function to handle
		// the WebSocket workflow for course chat
		handleCourseWebSocketConnection(conn, wp)
	})
}
