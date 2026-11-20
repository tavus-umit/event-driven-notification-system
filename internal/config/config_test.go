package config

import "testing"

func TestConfiguration(t *testing.T) {
	t.Setenv("RATE_LIMIT_SMS", "100")
	c, e := Load()
	if e != nil || c.MaxRetries != 3 {
		t.Fatal(c.MaxRetries, e)
	}
	t.Setenv("RATE_LIMIT_SMS", "101")
	if _, e = Load(); e == nil {
		t.Fatal("accepted rate above ceiling")
	}
}
