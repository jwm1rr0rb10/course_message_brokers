// Package conn holds connection defaults for the course cluster (module 2.2).
package conn

import (
	"os"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
)

// DefaultBrokers are the EXTERNAL listeners of the three-node cluster.
const DefaultBrokers = "localhost:9092,localhost:9192,localhost:9292"

// Brokers returns $KAFKA_BROKERS or the course cluster.
func Brokers() []string {
	b := os.Getenv("KAFKA_BROKERS")
	if b == "" {
		b = DefaultBrokers
	}
	return strings.Split(b, ",")
}

// Opts returns the seed brokers plus a client id.
func Opts(clientID string, extra ...kgo.Opt) []kgo.Opt {
	return append([]kgo.Opt{kgo.SeedBrokers(Brokers()...), kgo.ClientID(clientID)}, extra...)
}
