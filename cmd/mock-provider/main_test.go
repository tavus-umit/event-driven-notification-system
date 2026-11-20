package main

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMockFailure(t *testing.T) {
	t.Setenv("MOCK_FAILURE_STATUS", "503")
	t.Setenv("MOCK_FAILURE_RECIPIENT", "+123")
	for _, test := range []struct {
		to   string
		code int
	}{{"+123", 503}, {"+124", 202}} {
		w := httptest.NewRecorder()
		r := httptest.NewRequest("POST", "/webhook", strings.NewReader(`{"to":"`+test.to+`","channel":"sms","content":"message"}`))
		handler().ServeHTTP(w, r)
		if w.Code != test.code {
			t.Fatal(w.Code)
		}
	}
}
