package rabbitc

type Receiver interface {
	Queue() string
	OnError(error)
	OnReceive([]byte) bool
}
