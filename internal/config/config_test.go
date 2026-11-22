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

func TestProviderURL(t *testing.T) {
	for _, endpoint := range []string{"", "https://provider.example/webhook"} {
		t.Setenv("PROVIDER_WEBHOOK_URL", endpoint)
		c, err := Load()
		if err != nil || c.ProviderURL != endpoint {
			t.Fatalf("expected provider URL %q, got %q (%v)", endpoint, c.ProviderURL, err)
		}
	}
	t.Setenv("PROVIDER_WEBHOOK_URL", "relative/path")
	if _, err := Load(); err == nil {
		t.Fatal("accepted a relative provider URL")
	}
}
