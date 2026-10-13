package main

import (
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	var periodValue int
	var topicName string
	var clientRole string
	var kafkaServer string
	var hostName string
	var err error

	hostName, err = os.Hostname()
	if err != nil {
		fmt.Println("hostname retrieval error")
		os.Exit(1)
	}

	periodValue, err = strconv.Atoi(os.Getenv("CLIENT_PERIOD"))
	if err != nil {
		fmt.Println("CLIENT_PERIOD missing")
		os.Exit(1)
	}

	topicName = os.Getenv("CLIENT_TOPIC_NAME")
	if len(topicName) == 0 {
		fmt.Println("CLIENT_TOPIC_NAME missing")
		os.Exit(1)
	}

	clientRole = os.Getenv("CLIENT_ROLE")
	if len(clientRole) == 0 {
		fmt.Println("CLIENT_ROLE missing")
		os.Exit(1)
	}

	kafkaServer = os.Getenv("CLIENT_CONNECT")
	if len(kafkaServer) == 0 {
		fmt.Println("CLIENT_CONNECT missing")
		os.Exit(1)
	}

	if clientRole == "producer" {
		p, err := kafka.NewProducer(&kafka.ConfigMap{
			"bootstrap.servers": kafkaServer,
			"client.id":         hostName,
			"acks":              "all"})

		if err != nil {
			fmt.Printf("Failed to create producer: %s\n", err)
			os.Exit(1)
		}

		for true {
			err = p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topicName, Partition: kafka.PartitionAny},
				Value:          []byte(strconv.Itoa(rand.IntN(100))),
				Key:            []byte(strconv.Itoa(rand.IntN(10)))}, nil)

			p.Flush(10000)

			time.Sleep(time.Duration(periodValue) * time.Second)
		}
	}
}
