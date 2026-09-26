// Package conn opens a NATS connection the way module 4.2 recommends.
package conn

import (
	"log"
	"os"
	"time"

	"github.com/nats-io/nats.go"
)

// DefaultURLs points at the three-node cluster from module 2.5.
const DefaultURLs = "nats://localhost:4222,nats://localhost:4223,nats://localhost:4224"

// URLs returns $NATS_URL or the course cluster.
func URLs() string {
	if u := os.Getenv("NATS_URL"); u != "" {
		return u
	}
	return DefaultURLs
}

// Connect opens one connection per process with reconnect handling and logging.
func Connect(name string, extra ...nats.Option) (*nats.Conn, error) {
	opts := []nats.Option{
		nats.Name(name),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(500 * time.Millisecond),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			if err != nil {
				log.Printf("[%s] disconnected: %v", name, err)
			}
		}),
		nats.ReconnectHandler(func(c *nats.Conn) {
			log.Printf("[%s] reconnected to %s", name, c.ConnectedUrl())
		}),
		nats.ErrorHandler(func(_ *nats.Conn, sub *nats.Subscription, err error) {
			// slow consumer and permission errors show up here (module 4.2)
			if sub != nil {
				log.Printf("[%s] async error on %q: %v", name, sub.Subject, err)
				return
			}
			log.Printf("[%s] async error: %v", name, err)
		}),
	}
	return nats.Connect(URLs(), append(opts, extra...)...)
}
