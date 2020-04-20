package rabbitc

import (
	"log"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

const (
	reconnectDelay = 5 * time.Second
)

type Client struct {
	logger               *log.Logger
	conn                 *amqp.Connection
	consumeCh            *amqp.Channel
	uri                  string
	isConnected          bool
	notifyConsumeChClose chan *amqp.Error
	notifyDestroy        chan bool
	producer             producer
	consumers            []Consumer
	consumerWaitGroup    sync.WaitGroup
}

func NewV1(amqpUri string, logger *log.Logger) *Client {
	client := &Client{
		logger:        logger,
		uri:           amqpUri,
		notifyDestroy: make(chan bool),
		producer:      producer{logger: logger},
	}
	go client.handleReconnect()
	return client
}

func (c *Client) handleReconnect() {
	for {
		c.isConnected = false
		c.logger.Printf("Attemp to conenct %s ...", c.uri)
		for !c.connect() {
			c.logger.Printf("Reconnect %s after %vs ...", c.uri, reconnectDelay)
			time.Sleep(reconnectDelay)
		}
		select {
		case <-c.notifyDestroy:
			c.producer.destroy()
			if c.conn != nil {
				_ = c.conn.Close()
				return
			}
		case <-c.producer.notifyClose:
		case <-c.notifyConsumeChClose:
		}
	}
}

func (c *Client) connect() bool {
	var conn *amqp.Connection
	if c.conn == nil || c.conn.IsClosed() {
		var err error
		conn, err = amqp.Dial(c.uri)
		if err != nil {
			c.logger.Printf("Fail to connect %s: %v", c.uri, err)
			return false
		}
	} else {
		conn = c.conn
	}
	consumeCh, err := conn.Channel()
	if err != nil {
		return false
	}
	produceCh, err := conn.Channel()
	if err != nil {
		return false
	}
	produceCh.Confirm(false)
	c.conn = conn
	c.isConnected = true
	c.consumeCh = consumeCh
	c.notifyConsumeChClose = make(chan *amqp.Error)
	consumeCh.NotifyClose(c.notifyConsumeChClose)
	//config producer
	c.producer.channel = produceCh
	c.producer.notifyClose = make(chan *amqp.Error)
	c.producer.notifyConfirm = make(chan amqp.Confirmation)
	produceCh.NotifyClose(c.producer.notifyClose)
	produceCh.NotifyPublish(c.producer.notifyConfirm)
	c.logger.Printf("Connected MQ: %s", c.uri)
	return true
}

func (c *Client) Producer() Producer {
	return c.producer
}
