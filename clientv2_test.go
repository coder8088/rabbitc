package rabbitc

import (
	"os"
	"testing"
	"time"

	"github.com/tedux/log"
)

func TestClient(t *testing.T) {
	uri := os.Getenv("RABBITMQ_URI")
	client, err := New(uri)
	if err != nil {
		panic(err)
	}

	go func() {
		for {
			_, err = client.Connection(1 * time.Second)
			if err != nil {
				log.Error(err)
				break
			}
			log.Debugf("Got connection")
			time.Sleep(200 * time.Millisecond)
		}
	}()

	time.Sleep(2 * time.Second)
	client.Close()
}
