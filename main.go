package main

import (
	"fmt"
	"log"

	"github.com/Shopify/sarama"
	"github.com/yanzay/tbot"
)

var brokers = []string{"localhost:9092"}
var token string = "441748577:AAGb3HXzCSqw_jHUoar_CzBrKZzILTMb1ec"

func main() {
	bot, err := tbot.NewServer(token)
	if err != nil {
		log.Fatal(err)
	}

	bot.HandleFunc("{topic}", Handler)

	err = bot.ListenAndServe()
	if err != nil {
		log.Fatal(err)
	}
}

func Handler(m *tbot.Message) {
	var inChan = make(chan string)
	var outChan = make(chan string)
	topic := m.Vars["topic"]
	go kafkaRoutine(inChan, topic)
	go func() {
		for {
			msg := <-inChan
			outChan <- msg
		}
	}()

	for {
		message := <-outChan
		fmt.Println("received message from kafka:", message)
		m.Reply(message)
	}
}

func kafkaRoutine(inChan chan string, topic string) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		panic(err)
	}

	topics, _ := consumer.Topics()
	if !(containsTopic(topics, topic)) {
		inChan <- "There is no such a topic"
		fmt.Println("kafkaroutine exited")
		return
	}

	partitionList, err := consumer.Partitions(topic)
	for _, partition := range partitionList {
		pc, _ := consumer.ConsumePartition(topic, partition, sarama.OffsetOldest)
		go func(pc sarama.PartitionConsumer) {
			for {
				select {
				case msg := <-pc.Messages():
					inChan <- string(msg.Value)
				}
			}
		}(pc)
	}
	fmt.Println("kafkaRoutine exited")
}

func containsTopic(topics []string, topic string) bool {
	for _, v := range topics {
		if topic == v {
			return true
		}
	}
	return false
}
