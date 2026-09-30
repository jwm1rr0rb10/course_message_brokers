//go:build integration

// Package cloudtest checks the course's claims against the emulators from
// examples/emulators. Tests for a cloud skip themselves if its emulator is down
// (or fail, if COURSE_REQUIRE_BROKER=1 as in CI).
package cloudtest

import (
	"crypto/rand"
	"encoding/hex"
)

func unique(prefix string) string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return prefix + "-" + hex.EncodeToString(b)
}
