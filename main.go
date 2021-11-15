package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

func main() {
	consumer()
}

func producer() {
	topic := "go_topic"
	// make a writer that produces to topic-A, using the least-bytes distribution
	w := &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}

	data := map[string]string{"username": "username", "password": "password"}
	jsonValue, _ := json.Marshal(data)

	err := w.WriteMessages(context.Background(),

		kafka.Message{
			Key:   []byte("key"),
			Value: []byte(jsonValue),
		},
	)
	if err != nil {
		log.Fatal("failed to write messages:", err)
	}

	if err := w.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}
}

func consumer() {
	// make a new reader that consumes from topic-A, partition 0, at offset 42
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"localhost:9092"},
		Topic:     "go_topic",
		Partition: 0,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})
	// r.SetOffset(0)

	for {
		var message map[string]interface{}
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			break
		}
		json.Unmarshal(m.Value, &message)
		fmt.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), message["username"])
	}

	if err := r.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}

}
