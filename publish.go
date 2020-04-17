package rabbitc

import (
	"fmt"
	"time"

	"github.com/streadway/amqp"
	"github.com/tedux/log"
)

func (p Publisher) PublishWithUnlimitedRetry(ctx *ChannelContext, body string) {
	ctx.ChannelId = ctx.GenerateChannelId()
	if p.ChannelContexts[ctx.ChannelId] == nil {
		_ = p.refreshConnectionAndChannel(ctx)
	} else {
		ctx = p.ChannelContexts[ctx.ChannelId]
	}

	msg := amqp.Publishing{
		Headers:      amqp.Table{},
		ContentType:  "application/json",
		Body:         []byte(body),
		DeliveryMode: amqp.Transient,
	}

	if ctx.Reliable {
		msg.DeliveryMode = amqp.Persistent
		go p.publishWithConfirmAndUnlimitedRetry(ctx, &msg)
	} else {
		go p.publishWithUnlimitedRetry(ctx, &msg)
	}

	return
}

func (p Publisher) publishWithUnlimitedRetry(ctx *ChannelContext, msg *amqp.Publishing) {
	err := p.publish(ctx, msg)
	for err != nil {
		log.Info("Resend message after 3s ...")
		time.Sleep(3 * time.Second)
		err = p.publish(ctx, msg)
	}
}

func (p Publisher) publish(ctx *ChannelContext, msg *amqp.Publishing) error {
	if ctx == nil || ctx.Channel == nil {
		_ = p.refreshConnectionAndChannel(ctx)
		return fmt.Errorf("no usable channel")
	}
	return ctx.Channel.Publish(ctx.Exchange, ctx.RoutingKey, false, false, *msg)
}

func (p Publisher) publishWithConfirmAndUnlimitedRetry(ctx *ChannelContext, msg *amqp.Publishing) {
	err := p.publishWithConfirm(ctx, msg)
	for err != nil {
		log.Info("Resend message after 3s ...")
		time.Sleep(3 * time.Second)
		err = p.publishWithConfirm(ctx, msg)
	}
}

func (p Publisher) publishWithConfirm(ctx *ChannelContext, msg *amqp.Publishing) error {
	if ctx == nil || ctx.Channel == nil {
		_ = p.refreshConnectionAndChannel(ctx)
		return fmt.Errorf("no usable channel")
	}

	confirms := ctx.Channel.NotifyPublish(make(chan amqp.Confirmation, 1))
	defer close(confirms)

	err := ctx.Channel.Publish(ctx.Exchange, ctx.RoutingKey, false, false, *msg)
	if err != nil {
		return err
	} else if confirmed := <-confirms; confirmed.Ack {
		return nil
	} else {
		return fmt.Errorf("nack")
	}
}
