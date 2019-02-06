package main

import (
	"fmt"
	"io/ioutil"
	"log"

	"github.com/streadway/amqp"
)

func main() {
	// connect to the server
	conn, err := amqp.Dial("amqp://guest:guest@localhost:32775")
	chkError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	// open a channel
	ch, err := conn.Channel()
	chkError(err, "Failed to open a channel")
	defer ch.Close()

	// choose a queue - will not be created if it exists already
	q, err := ch.QueueDeclare(
		"hello", // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	chkError(err, "Failed to declare a queue")

	// subscribe and stay alive
	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	chkError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			fileName := fmt.Sprintf("%s", d.Headers["fileName"])
			fileSize := fmt.Sprintf("%s", d.Headers["fileSize"])
			err := ioutil.WriteFile(fileName, d.Body, 0644)
			chkError(err, "Failed to write file")
			log.Printf("Consumed file: '%s' (%s b)", fileName, fileSize)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}

func chkError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
