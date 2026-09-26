// Package conn holds connection defaults for the course cluster (module 2.3).
package conn

import "os"

// DefaultURL points at rabbit-1 from the course docker-compose file.
const DefaultURL = "amqp://admin:admin@localhost:5672/"

// URL returns $RABBITMQ_URL or the course default.
func URL() string {
	if u := os.Getenv("RABBITMQ_URL"); u != "" {
		return u
	}
	return DefaultURL
}
