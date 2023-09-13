package main

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/marcostota/kafkarab/infra/database"
	"github.com/marcostota/kafkarab/pkg/rabbitmq"
	"github.com/marcostota/kafkarab/usecase"
	_ "github.com/mattn/go-sqlite3"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {

	db, err := sql.Open("sqlite3", "db.sqlite3")
	if err != nil {
		panic(err)
	}
	defer db.Close()
	orderRepository := database.NewOrderRepository(db)
	uc := usecase.NewCalculateFinalPrice(orderRepository)
	ch, err := rabbitmq.OpenChannel()
	if err != nil {
		panic(err)
	}
	defer ch.Close()
	msgRabbitmqChannel := make(chan amqp.Delivery)
	go rabbitmq.Consume(ch, msgRabbitmqChannel)
	rabbitmqWorker(msgRabbitmqChannel, uc)

	// input := usecase.OrderInput{
	// 	ID:    "14",
	// 	Price: 29.1,
	// 	Tax:   1.3,
	// }
	// output, err := uc.Execute(input)

	// if err != nil {
	// 	panic(err)
	// }
	// fmt.Println(output)
}

func rabbitmqWorker(msgChan chan amqp.Delivery, uc *usecase.CalculateFinalPrice) {
	fmt.Println("Starting rabbitmq")
	for msg := range msgChan {
		var input usecase.OrderInput
		err := json.Unmarshal(msg.Body, &input)
		if err != nil {
			panic(err)
		}
		output, err := uc.Execute(input)
		if err != nil {
			panic(err)
		}
		msg.Ack(true)
		fmt.Println("Mensagem processada e salva no banco de dados:", output)
	}
}
