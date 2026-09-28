// Package emu holds the emulator endpoints from examples/emulators.
package emu

import (
	"net"
	"net/url"
	"os"
	"time"
)

// Env returns the environment variable or a default.
func Env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// AWSEndpoint is moto (or LocalStack). Empty AWS_ENDPOINT_URL is not allowed here on purpose:
// the examples never talk to real AWS unless you change the code.
func AWSEndpoint() string { return Env("AWS_ENDPOINT_URL", "http://localhost:4566") }

// ServiceBusConnectionString is the Service Bus emulator connection string.
func ServiceBusConnectionString() string {
	return Env("SERVICEBUS_CONNECTION_STRING",
		"Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;"+
			"SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;")
}

// PubSubHost is the Pub/Sub emulator; the Go client uses it when PUBSUB_EMULATOR_HOST is set.
func PubSubHost() string { return Env("PUBSUB_EMULATOR_HOST", "localhost:8085") }

// PubSubProject is the project used in the emulator.
func PubSubProject() string { return Env("PUBSUB_PROJECT_ID", "course-project") }

// Reachable reports whether host:port (or a URL) accepts TCP connections.
func Reachable(addr string) bool {
	if u, err := url.Parse(addr); err == nil && u.Host != "" {
		addr = u.Host
	}
	c, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return false
	}
	_ = c.Close()
	return true
}
