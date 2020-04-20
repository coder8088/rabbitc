package rabbitc

import (
	"fmt"
	"sync"
	"time"

	"github.com/streadway/amqp"
	"github.com/tedux/log"
)

const (
	maxReconnectDelay = 30 * time.Second
)

type ClientV2 interface {
	Connection(timeout time.Duration) (*amqp.Connection, error)
	Close()
}

type client struct {
	rw          sync.RWMutex
	uri         string
	conn        *amqp.Connection
	isConnected bool
	quit        chan bool
	connClose   chan *amqp.Error
}

func New(amqpUri string) (ClientV2, error) {
	c := &client{uri: amqpUri, quit: make(chan bool), connClose: make(chan *amqp.Error)}
	err := c.connect()
	if err != nil {
		return nil, err
	}

	go c.reconnect()

	return c, err
}

func (c *client) reconnect() {
	log.Info("Reconnect goroutine is running ...")
	reconnectDelay := 100 * time.Millisecond
	for {
		select {
		case err := <-c.connClose:
			log.Debugf("MQ connection is closing ...")
			c.setIsConnected(false)
			if err != nil {
				log.Warnf("Connection close notification with error: %v", err)
			}
		case <-c.quit:
			log.Info("Client is quit")
			return
		}
		//reconnect
		if !c.conn.IsClosed() {
			if err := c.conn.Close(); err != nil {
				log.Errorf("Fail to close mq connection: %v", err)
			}
		}
		//important
		for err := range c.connClose {
			log.Warnf("%v", err)
		}

	reconnectLoop:
		for {
			select {
			case <-c.quit:
				log.Info("Client is quit")
				return
			default:
				log.Infof("Reconnect: %s ...", c.uri)
				if err := c.connect(); err != nil {
					if reconnectDelay > maxReconnectDelay {
						time.Sleep(maxReconnectDelay)
					} else {
						time.Sleep(reconnectDelay)
					}
					reconnectDelay *= 2
					continue
				}
				break reconnectLoop
			}
		}
	}
}

func (c *client) connect() (err error) {
	c.rw.Lock()
	defer c.rw.Unlock()

	c.isConnected = false
	c.conn, err = amqp.Dial(c.uri)
	if err != nil {
		log.Warnf("Fail to connect %s: %v", c.uri, err)
		return
	}
	c.connClose = make(chan *amqp.Error)
	c.conn.NotifyClose(c.connClose)
	c.isConnected = true
	log.Infof("MQ Connected: %s", c.uri)
	return
}

func (c *client) setIsConnected(b bool) {
	c.rw.Lock()
	defer c.rw.Unlock()

	c.isConnected = b
}

func (c *client) Connection(timeout time.Duration) (*amqp.Connection, error) {
	c.rw.RLock()
	defer c.rw.RUnlock()

	t := time.After(timeout)
	for !c.isConnected {
		select {
		case <-t:
			return nil, fmt.Errorf("get mq connection timeout [%v]", timeout)
		default:
			time.Sleep(200 * time.Millisecond)
		}
	}

	return c.conn, nil
}

func (c *client) Close() {
	if c.quit != nil {
		close(c.quit)
	}

	if c.conn != nil && !c.conn.IsClosed() {
		if err := c.conn.Close(); err != nil {
			log.Errorf("Fail to close mq connection: %v", err)
		}
	}
}
