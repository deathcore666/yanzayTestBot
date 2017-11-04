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

	bot.HandleFunc("/start", Start)
	bot.HandleFunc("/kafka {topic}", Kafka)

	err = bot.ListenAndServe()
	if err != nil {
		log.Fatal(err)
	}
}

func Kafka(m *tbot.Message) {
	var reqChan = make(chan string)
	var respChan = make(chan string, 1)
	topic := m.Vars["topic"]
	go kafkaRoutine(reqChan, topic)
	go func() {
		for {
			msg := <-reqChan
			respChan <- msg
		}
	}()

	for {
		message := <-respChan
		fmt.Println("received message from kafka:", message)
		m.Reply(message)
	}
}

func kafkaRoutine(reqChan chan string, topic string) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		panic(err)
	}
	partitionList, err := consumer.Partitions(topic)
	for _, partition := range partitionList {
		pc, _ := consumer.ConsumePartition(topic, partition, sarama.OffsetOldest)
		go func(pc sarama.PartitionConsumer) {
			for {
				select {
				case msg := <-pc.Messages():
					reqChan <- string(msg.Value)
				}
			}
		}(pc)
	}
	fmt.Println("kafkaRoutine exited")
}

//Start Hello
func Start(m *tbot.Message) {
	response := fmt.Sprint("This is the test bot")
	m.Reply(response, tbot.WithMarkdown)
}
