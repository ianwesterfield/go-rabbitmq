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

	// get the test file to send as the body of the message
	fileName := "minishift"
	dat, err := ioutil.ReadFile(fileName)
	chkError(err, "Failed to open file")
	fileSize := fmt.Sprintf("%d", len(dat))

	// create the amqp table to hold header metadata
	m := make(amqp.Table)
	m["fileName"] = fileName
	m["fileSize"] = fileSize

	// publish a test message
	err = ch.Publish(
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			Headers: m,
			Body:    dat,
		})

	chkError(err, "Failed to publish a message")
	log.Printf("Published file: '%s' (%s b)", fileName, fileSize)
}

func chkError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
