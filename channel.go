package rabbitc

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"

	"github.com/streadway/amqp"
)

type ChannelContext struct {
	Exchange     string
	ExchangeType string
	RoutingKey   string
	Reliable     bool
	Durable      bool
	ChannelId    string
	Channel      *amqp.Channel
}

func (c *ChannelContext) GenerateChannelId() string {
	stringTag := fmt.Sprintf(`%v:%v:%v:%v:%v`, c.Exchange, c.ExchangeType, c.RoutingKey, c.Durable, c.Reliable)
	hasher := md5.New()
	hasher.Write([]byte(stringTag))
	return hex.EncodeToString(hasher.Sum(nil))
}
