go-amqp2gelf
============

A simple tool to read from an AMQP queue and forwards as GELF/UPD packet.

For incoming JSON messages it tries to not change any fields.
Other messages are put inside the GELF "message" field.

```
Usage of ./go-amqp2gelf:
  -port=12201: Graylog2 GELF/UDP port
  -queue="logging_queue": Durable AMQP queue name
  -server="localhost": Graylog2 server
  -uri="amqp://user:password@broker.example.com:5672/vhost": AMQP URI
  -v=false: Verbose output
```
