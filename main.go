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

			time.Sleep(time.Duration(periodValue) * time.Millisecond)
		}
	} else if clientRole == "consumer" {
		consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers":  kafkaServer,
			"group.id":           topicName,
			"client.id":          hostName,
			"auto.offset.reset":  "earliest",
			"enable.auto.commit": "true"})

		if err != nil {
			fmt.Printf("Failed to create consumer: %s\n", err)
			os.Exit(1)
		}

		err = consumer.SubscribeTopics([]string{topicName}, nil)

		var run bool = true
		for run == true {
			ev := consumer.Poll(1000)
			switch e := ev.(type) {
			case *kafka.Message:
				fmt.Printf("%s consumer got %s from %s topic\n", hostName, string(e.Value), topicName)
				time.Sleep(time.Duration(periodValue) * time.Millisecond)
			case kafka.Error:
				fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
				run = false
			default:
				fmt.Printf("Ignored %v\n", e)
				time.Sleep(time.Duration(periodValue) * time.Millisecond)
			}
		}

	} else {
		fmt.Println("CLIENT_ROLE Role is not valid")
		os.Exit(1)
	}
}
