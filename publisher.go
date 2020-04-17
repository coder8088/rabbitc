package rabbitc

import (
	"errors"
	"sync"
	"time"

	"github.com/streadway/amqp"
	"github.com/tedux/log"
)

type AmqpConnection struct {
	Lock sync.RWMutex
	Conn *amqp.Connection
	Uri  string
}

//One client hold one connection and some channels
type Publisher struct {
	Connection *AmqpConnection
	//channel cache
	ChannelContexts map[string]*ChannelContext
}

func NewPublisher() Publisher {
	return Publisher{ChannelContexts: make(map[string]*ChannelContext)}
}

func (p Publisher) refreshConnectionAndChannel(ctx *ChannelContext) (err error) {
	p.Connection.Lock.Lock()
	defer p.Connection.Lock.Lock()

	if p.Connection.Conn != nil {
		ctx.Channel, err = p.Connection.Conn.Channel()
	} else {
		err = errors.New("connection nil")
	}
	//reconnect
	if err != nil {
		for {
			p.Connection.Conn, err = amqp.Dial(p.Connection.Uri)
			if err != nil {
				log.Warnf("Fail to connect to [%s], retry...", p.Connection.Uri)
				time.Sleep(3 * time.Second)
			} else {
				ctx.Channel, _ = p.Connection.Conn.Channel()
				break
			}
		}
	}

	err = ctx.Channel.ExchangeDeclare(ctx.Exchange, ctx.ExchangeType, ctx.Durable,
		false, false, false, nil)
	if err != nil {
		log.Warnf("Fail to declare exchange [%s]: %v", ctx.Exchange, err)
		return
	}

	if ctx.Reliable {
		log.Info("Enable publishing confirms...")
		if err = ctx.Channel.Confirm(false); err != nil {
			log.Warnf("Channel could not be put into confirm mode: %v", err)
			return
		}
	}

	//Add channel to channel cache
	p.ChannelContexts[ctx.ChannelId] = ctx
	return
}
