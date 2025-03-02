package main

import (
	"log"

	"github.com/gorilla/websocket"
)

type ChatClient struct {
	conn *websocket.Conn
	send chan []byte
}

func NewChatClient(url string) (*ChatClient, error) {
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		return nil, err
	}

	return &ChatClient{
		conn: conn,
		send: make(chan []byte, 256),
	}, nil
}

func (c *ChatClient) SendQuery(query string) error {
	return c.conn.WriteJSON(map[string]interface{}{
		"prompt": query,
	})
}

func (c *ChatClient) ReceiveResults(handler func(result *QueryResult)) {
	for {
		var result QueryResult
		err := c.conn.ReadJSON(&result)
		if err != nil {
			log.Printf("Error reading from websocket: %v", err)
			return
		}

		handler(&result)

		if result.Final {
			return
		}
	}
}
