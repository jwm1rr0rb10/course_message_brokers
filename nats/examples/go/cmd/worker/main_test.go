package main

import (
	"testing"
	"time"
)

func TestRetryDelay(t *testing.T) {
	for n, want := range map[uint64]time.Duration{
		0: time.Second, 1: time.Second, 2: 5 * time.Second, 3: 30 * time.Second,
		4: time.Minute, 5: time.Minute, 100: time.Minute,
	} {
		if got := retryDelay(n); got != want {
			t.Errorf("retryDelay(%d) = %v, want %v", n, got, want)
		}
	}
}
