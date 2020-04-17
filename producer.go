package rabbitc

import (
	"fmt"
	"log"
	"time"

	"github.com/streadway/amqp"
)

const (
	resendDelay = 3 * time.Second //重发间隔
	resendTimes = 5               //重发次数
)

type Producer interface {
	Push(exchange, routingKey string, data []byte) error
	ReliablePush(exchange, routingKey string, data []byte)
	UnreliablePush(exchange, routingKey string, data []byte) error
}

type producer struct {
	logger        *log.Logger
	channel       *amqp.Channel
	notifyClose   chan *amqp.Error
	notifyConfirm chan amqp.Confirmation
}

func (p producer) ReliablePush(exchange, routingKey string, data []byte) {
	err := p.Push(exchange, routingKey, data)
	for err != nil {
		err = p.Push(exchange, routingKey, data)
	}
}

func (p producer) Push(exchange, routingKey string, data []byte) error {
	if p.channel == nil {
		return fmt.Errorf("no connected channel")
	}
	delay := resendDelay
	currentTimes := 1
	for {
		err := p.UnreliablePush(exchange, routingKey, data)
		if err != nil {
			if currentTimes > resendTimes {
				return err
			}
			p.logger.Printf("Resend times %v, after %vs ...", currentTimes, delay)
		}
		ticker := time.NewTicker(delay)
		select {
		case confirm := <-p.notifyConfirm:
			if confirm.Ack {
				p.logger.Printf("Push confirmed.")
				return nil
			}
		case <-ticker.C:
		}
		currentTimes += 1
		delay = resendDelay * time.Duration(currentTimes)
	}
}

func (p producer) UnreliablePush(exchange, routingKey string, data []byte) error {
	if p.channel == nil {
		return fmt.Errorf("no connected channel")
	}
	return p.channel.Publish(
		exchange,
		routingKey,
		false,
		false,
		amqp.Publishing{
			Timestamp:    time.Now(),
			Body:         data,
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
		})
}

func (p producer) destroy() {
	close(p.notifyClose)
	close(p.notifyConfirm)
	if p.channel != nil {
		p.channel.Close()
	}
}
