// Package kafka_project
// @author Daud Valentino
package main

import (
	"github.com/Shopify/sarama"
	"fmt"
	"os"
	"os/signal"
)

func main()  {


	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.ClientID = "test2"

	brokers := []string{"localhost:9092"}
	master, err := sarama.NewConsumer(brokers, config)

	if err != nil {
		fmt.Println(err)

		return
	}


	consumer, err := master.ConsumePartition("tester", 0 ,sarama.OffsetNewest)

	if err != nil {
		fmt.Println(err)

		return
	}



	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// Count how many message processed
	msgCount := 0

	// Get signnal for finish
	doneCh := make(chan struct{})
	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				fmt.Println(err)
			case msg := <-consumer.Messages():
				msgCount++


				fmt.Println("Received messages", string(msg.Key), string(msg.Value), msg.Offset)
			case <-signals:
				fmt.Println("Interrupt is detected")
				doneCh <- struct{}{}
			}
		}
	}()

	<-doneCh
	fmt.Println("Processed", msgCount, "messages")

}