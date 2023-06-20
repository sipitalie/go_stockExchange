package main

import (
	"encoding/json"
	"fmt"
	"sync"

	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/sipitalie/go_stockExchange/internal/infra/kafka"
	"github.com/sipitalie/go_stockExchange/internal/market/dto"
	"github.com/sipitalie/go_stockExchange/internal/market/entity"
	"github.com/sipitalie/go_stockExchange/internal/market/transformer"
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
	go kafka.Consume(kafkaMsgChan) //tred2
	//recebe a msg do canal do kafka e passa para o input,processa e passa no output e depois publica novamente no kafka
	book := entity.NewBook(ordersIn, ordersOut, wg)
	go book.Trade() //tred3

	go func() {
		for msg := range kafkaMsgChan {
			wg.Add(1)
			fmt.Println(string(msg.Value))
			tradeInput := dto.TradeInput{}
			err := json.Unmarshal(msg.Value, &tradeInput)
			if err != nil {
				panic(err)

			}
			order := transformer.TransformerInput(tradeInput)
			ordersIn <- order
		}
	}()

	for res := range ordersOut {
		output := transformer.TranformOutput(res)
		outputJson, err := json.MarshalIndent(output, "", "   ")
		fmt.Println(string(outputJson))

		if err != nil {
			fmt.Println(err)
		}

		producer.Publish(outputJson, []byte("orders"), "output")

	}
}
