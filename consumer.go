package rabbitc

import (
	"fmt"
	"time"
)

type Consumer interface {
	Name() string
	Queue() string
	Consume([]byte) bool
	OnError(error)
}

func (c Client) StartConsume() {
	for {
		c.runConsumers()

		time.Sleep(3 * time.Second)
	}
}

func (c Client) RegisterConsumer(consumer Consumer) {
	c.consumers = append(c.consumers, consumer)
}

func (c Client) runConsumers() {
	c.logger.Printf("Run consumers ...")

	for _, consumer := range c.consumers {
		c.consumerWaitGroup.Add(1)
		go c.listen(consumer)
	}

	c.consumerWaitGroup.Wait()
	c.logger.Print("All consumers down ...")
}

func (c Client) listen(consumer Consumer) {
	defer c.consumerWaitGroup.Done()

	name := consumer.Name()
	queue := consumer.Queue()

	_, err := c.consumeCh.QueueDeclare(queue, true, false, false, false, nil)
	if err != nil {
		consumer.OnError(fmt.Errorf("fail to declare queue [%s]: %v", queue, err))
	}

	c.consumeCh.Qos(1, 0, true)
	msgs, err := c.consumeCh.Consume(queue, name, false, false, false, false, nil)
	if err != nil {
		consumer.OnError(err)
	}

	for msg := range msgs {
		for !consumer.Consume(msg.Body) {
			c.logger.Printf("Fail to consume message, retry after 3s ...")
			time.Sleep(3 * time.Second)
		}
		msg.Ack(false)
	}
}
