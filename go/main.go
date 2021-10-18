package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	GO_FACTORIAL_TOPIC  = "GO_FACTORIAL_TOPIC"
	GO_FACTORIAL_UPDATE = "GO_FACTORIAL_UPDATE"
	NODE_PRIME_TOPIC    = "NODE_PRIME_TOPIC"
	NODE_PRIME_UPDATE   = "NODE_PRIME_UPDATE"
)

func newKafkaWriter(kafkaURL, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(kafkaURL),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: time.Millisecond * 500,
	}
}

func getKafkaReader(kafkaURL, topic, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaURL},
		GroupID:  groupID,
		Topic:    topic,
		MaxBytes: 10e6, // 10MB

	})
}

func main() {
	writer := newKafkaWriter("localhost:29092", NODE_PRIME_TOPIC)
	go consume()

	defer writer.Close()
	fmt.Println("start producing ... !!")
	for i := 0; ; i++ {
		key := fmt.Sprintf("Key-%d", i)
		msg := kafka.Message{
			Key:   []byte(key),
			Value: []byte(fmt.Sprintf("%d", i)),
		}
		err := writer.WriteMessages(context.Background(), msg)
		if err != nil {
			fmt.Println(err)
		} else {
			fmt.Println("produced", key)
		}
	}
}

func consume() {
	reader := getKafkaReader("localhost:29092", NODE_PRIME_UPDATE, "west-side")
	defer reader.Close()

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println(string(msg.Value), string(msg.Key))
	}
}

func factorial(num int64) int64 {
	answer := int64(1)
	if num == 0 || num == 1 {
		return num
	} else {
		for i := num; i >= 1; i-- {
			answer = answer * i
		}
		return answer
	}
}
