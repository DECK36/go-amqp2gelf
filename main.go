/*
amqp2gelf

A simple tool to read from an AMQP queue and forwards as GELF/UPD packet.

For JSON messages it tries to not change any fields.
Other messages are put inside the GELF "message" field.


2014, DECK36 GmbH & Co. KG, <martin.schuette@deck36.de>
*/
package main

import (
	"flag"
	"fmt"
	"github.com/DECK36/go-gelf/gelf"
	"github.com/streadway/amqp"
	"log"
	"os"
	"os/signal"
	"syscall"
	"encoding/json"
	"strconv"
	"time"
)

const thisVersion = "0.3"
const thisProgram = "amqp2gelf"

// all command line options
type CommandLineOptions struct {
	uri         *string
	queueName   *string
	gelf_server *string
	gelf_port   *int
	verbose     *bool
}

var options CommandLineOptions

func init() {
	// this does not look right...
	// I am looking for a pattern how to group command line arguments in a struct
	options = CommandLineOptions{
		flag.String("uri", "amqp://user:password@broker.example.com:5672/vhost", "AMQP URI"),
		flag.String("queue", "logging_queue", "Durable AMQP queue name"),
		flag.String("server", "localhost", "Graylog2 server"),
		flag.Int("port", 12201, "Graylog2 GELF/UDP port"),
		flag.Bool("v", false, "Verbose output"),
	}
	flag.Parse()
}

type Consumer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	tag     string
	done    chan error
}

func amqpConsumer(amqpURI string, amqpQueue string, shutdown chan<- string) (c *Consumer, err error) {
	amqpConfig := amqp.Config{
		Properties: amqp.Table{
			"product": thisProgram,
			"version": thisVersion,
		},
	}
	c = &Consumer{
		conn:    nil,
		channel: nil,
		tag:     fmt.Sprintf("amqp2gelf-%d", os.Getpid()),
		done:    make(chan error),
	}

	// this is the important part:
	if *options.verbose {
		log.Println("connecting to ", amqpURI, "...")
	}
	c.conn, err = amqp.DialConfig(amqpURI, amqpConfig)
	if err != nil {
		return nil, fmt.Errorf("AMQP Dial: %s", err)
	}
	c.channel, err = c.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("AMQP Channel: %s", err)
	}

	// here we only ensure the AMQP queue exists
	q, err := c.channel.QueueDeclare(
		amqpQueue, // name
		true,      // durable
		false,     // auto-deleted
		false,     // exclusive
		false,     // noWait
		nil,       // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("Queue Declare: %v", err)
	}

	// init the consumer
	deliveries, err := c.channel.Consume(
		q.Name, // name
		c.tag,  // consumerTag,
		false,  // noAck
		false,  // exclusive
		false,  // noLocal
		false,  // noWait
		nil,    // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("Queue Consume: %s", err)
	}

	go writeLogsToGelf(deliveries, c.done)

	go func() {
		notification := c.channel.NotifyClose(make(chan *amqp.Error))
		n := <-notification
		c.done <- fmt.Errorf("AMQP server closed connection: %v", n)
	}()

	return
}

// convert JSON (possibly already GELF) input to GELF
func buildGelfMessage_json(message []byte) (gm gelf.Message, err error) {
	// list of "reserved" field names
	// cf. https://github.com/Graylog2/graylog2-server/blob/0.20/graylog2-plugin-interfaces/src/main/java/org/graylog2/plugin/Message.java#L61 and #L81
	// Go does not allow const maps :-/
	gelfReservedField := map[string]bool{
		"_id":     true,
		"_ttl":    true,
		"_source": true,
		"_all":    true,
		"_index":  true,
		"_type":   true,
		"_score":  true,
	}

	var emptyinterface interface{}
	err = json.Unmarshal(message, &emptyinterface)
	if err != nil {
		if *options.verbose {
			log.Printf("Cannot parse JSON, err: %v, msg: '%s'", err, message)
		}
		return
	}
	jm := emptyinterface.(map[string]interface{})

	// rename reserved field names (with and w/o '_')
	// note: we do not double check if 'renamed_xyz' is already present
	for k, v := range jm {
		if gelfReservedField[k] {
			jm["renamed"+k] = v
			delete(jm, k)
		} else if gelfReservedField["_"+k] {
			jm["renamed_"+k] = v
			delete(jm, k)
		}
	}

	// ensure some required fields are set, use defaults if missing
	var gelf_hostname string = "unknown_amqp"
	if _, ok := jm["host"]; ok {
		gelf_hostname = jm["host"].(string)
	}

	var gelf_shortmsg string = ""
	if _, ok := jm["short_message"]; ok {
		gelf_shortmsg = jm["short_message"].(string)
	}

	var gelf_timestamp float64 = 0.0
	if _, ok := jm["timestamp"]; ok {
		switch tsval := jm["timestamp"].(type) {
		case float64:
			gelf_timestamp = tsval
		case string:
			gelf_timestamp, _ = strconv.ParseFloat(tsval, 64)
		}
	}

	var gelf_level int32 = 6 // info
	if _, ok := jm["level"]; ok {
		gelf_level = jm["level"].(int32)
	}

	var gelf_version string = "1.1"
	if _, ok := jm["version"]; ok {
		gelf_version = jm["version"].(string)
	}

	gm = gelf.Message{
		Version:  gelf_version,
		Host:     gelf_hostname,
		Short:    gelf_shortmsg,
		TimeUnix: gelf_timestamp,
		Level:    gelf_level,
		Extra:    jm,
	}
	return gm, nil

}

// package text input in GELF
func buildGelfMessage_text(message []byte) (gm gelf.Message, err error) {
	gm = gelf.Message{
		Version:  "1.1",
		Host:     "unknown_amqp",
		Short:    string(message),
		TimeUnix: 0.0,
		Level:    6, // info
		Extra:    map[string]interface{}{},
	}
	return gm, nil
}

func buildGelfMessage(message []byte, ctype string) (gm gelf.Message, err error) {
	if (ctype == "application/json" || ctype == "text/json") &&
		message[0] == '{' && message[len(message)-1] == '}' {
		gm, err = buildGelfMessage_json(message)
	} else {
		gm, err = buildGelfMessage_text(message)
	}
	return
}

// handle AMQP delivery
func writeLogsToGelf(deliveries <-chan amqp.Delivery, done chan error) {
	graylogAddr := fmt.Sprintf("%s:%d", *options.gelf_server, *options.gelf_port)
	gelfWriter, err := gelf.NewWriter(graylogAddr)
	if err != nil {
		done <- fmt.Errorf("Cannot create gelf writer: %v", err)
		return
	}

	for d := range deliveries {
		gm, err := buildGelfMessage(d.Body, d.ContentType)
		if err != nil {
			d.Reject(false) // do not requeue
			continue
		}
		if *options.verbose {
			log.Printf("Gelf Msg Obj: %#v\n", gm)
		}
		err = gelfWriter.WriteMessage(&gm)
		if err != nil {
			done <- fmt.Errorf("Cannot send gelf msg: %v", err)
			d.Reject(false) // do not requeue
			continue
		}
		d.Ack(false) // don't ack multiple
	}
	done <- fmt.Errorf("done")
	return
}

// let the OS tell us to shutdown
func osSignalHandler(shutdown chan<- string) {
	var sigs = make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigs  // this is the blocking part

	go func(){
		time.Sleep(2*time.Second)
		log.Fatalf("shutdown was ignored, bailing out now.\n")
	}()

	shutdown <- fmt.Sprintf("received signal %v", sig)
}

func main() {
	if *options.verbose {
		log.Printf("Start %s %s", thisProgram, thisVersion)
	}
	// let goroutines tell us to shutdown (on error)
	var shutdown = make(chan string)

	// let the OS tell us to shutdown
	go osSignalHandler(shutdown)

	// start input
	c, err := amqpConsumer(*options.uri, *options.queueName, shutdown)
	if err != nil {
		// cannot use shutdown channel, no listener yet
		log.Fatalln("Fatal Error: ", err.Error())
	}
	go func() {
		err = <-c.done
		shutdown <- fmt.Sprintln(err)
	}()

	message := <-shutdown
	log.Println("The End.", message)
}
