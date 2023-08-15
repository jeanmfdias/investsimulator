package main

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/jeanmfdias/investsimulator/internal/infra/kafka"
	"github.com/jeanmfdias/investsimulator/internal/market/dto"
	"github.com/jeanmfdias/investsimulator/internal/market/entity"
	"github.com/jeanmfdias/investsimulator/internal/market/transformer"

	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	ordersIn := make(chan *entity.Order)
	ordersOut := make(chan *entity.Order)
	wg := &sync.WaitGroup{}
	defer wg.Wait()

	kafkaMsgChan := make(chan *ckafka.Message)
	configMap := &ckafka.ConfigMap{
		"bootstrap.servers": "host.docker.internal:9094",
		"group.id":          "myGroup",
		"auto.offset.reset": "latest",
	}

	producer := kafka.NewKafkaProducer(configMap)
	kafka := kafka.NewConsumer(configMap, []string{"input"})

	go kafka.Consume(kafkaMsgChan) // new thread

	book := entity.NewBook(ordersIn, ordersOut, wg)
	go book.Trade() // another thread

	go func() {
		for msg := range kafkaMsgChan {
			wg.Add(1)
			tradeInput := dto.TradeInput{}
			err := json.Unmarshal(msg.Value, &tradeInput)
			if err != nil {
				panic(err)
			}
			order := transformer.TransformInput(tradeInput)
			ordersIn <- order

			fmt.Println(string(msg.Value))
		}
	}()

	for res := range ordersOut {
		output := transformer.TransformOutput(res)
		outputJson, err := json.MarshalIndent(output, "", "   ")
		if err != nil {
			fmt.Println(err)
		}

		fmt.Println(string(outputJson))
		producer.Publish(outputJson, []byte("orders"), "output")
	}
}
