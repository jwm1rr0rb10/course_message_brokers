package main

import (
	"testing"
	"time"
)

func TestNextBackoffDoublesUpToMax(t *testing.T) {
	d := minBackoff
	var got []time.Duration
	for i := 0; i < 10; i++ {
		d = nextBackoff(d)
		got = append(got, d)
	}
	if got[0] != 2*minBackoff {
		t.Fatalf("first step %v, want %v", got[0], 2*minBackoff)
	}
	if last := got[len(got)-1]; last != maxBackoff {
		t.Fatalf("backoff grew to %v, want it capped at %v", last, maxBackoff)
	}
}
