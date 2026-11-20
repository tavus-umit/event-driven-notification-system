package domain

import (
	"strings"
	"testing"
)

func TestValidation(t *testing.T) {
	for _, tc := range []struct {
		c    Channel
		r, b string
		ok   bool
	}{{SMS, "+123", strings.Repeat("界", 1600), true}, {SMS, "+123", strings.Repeat("界", 1601), false}, {SMS, "123", "ok", false}, {Email, "a@example.com", "body", true}, {Email, "a", "x", false}, {Push, strings.Repeat("A", 32), "hello", true}, {Push, "short", "a", false}} {
		n := Notification{Summary: Summary{Channel: tc.c, Recipient: tc.r, Priority: Normal}, Content: tc.b}
		if (n.Validate() == nil) != tc.ok {
			t.Errorf("%s %s: expected valid=%v", tc.c, tc.r, tc.ok)
		}
	}
}
